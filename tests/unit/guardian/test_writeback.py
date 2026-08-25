from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from sub2api_mcp.guardian.contracts import (
    GuardianFieldName,
    GuardianFieldOwner,
    GuardianPolicy,
    GuardianWriteOutcome,
    GuardianWriteProposal,
)
from sub2api_mcp.guardian.repository import GuardianRepository
from sub2api_mcp.guardian.writeback import GuardianWritebackService

Scalar = int | float | bool | str


def _empty_calls() -> list[tuple[str, GuardianFieldName, object]]:
    return []


@dataclass
class FakeWriter:
    calls: list[tuple[str, GuardianFieldName, object]] = field(
        default_factory=_empty_calls
    )

    async def write_field(
        self,
        account_id: str,
        field_name: GuardianFieldName,
        value: object,
    ) -> int | bool:
        self.calls.append((account_id, field_name, value))
        if isinstance(value, (bool, int)):
            return value
        raise TypeError("unsupported fake write value")


class MismatchedWriter(FakeWriter):
    async def write_field(
        self,
        account_id: str,
        field_name: GuardianFieldName,
        value: object,
    ) -> int | bool:
        await super().write_field(account_id, field_name, value)
        return 79


async def _repository(tmp_path: Path) -> GuardianRepository:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()
    return repository


def _proposal(
    *,
    current: Scalar = 100,
    desired: Scalar = 80,
    key: str = "write-1",
    field_name: GuardianFieldName = GuardianFieldName.LOAD_FACTOR,
) -> GuardianWriteProposal:
    return GuardianWriteProposal(
        channel_id="channel-1",
        account_id="42",
        field_name=field_name,
        current_value=current,
        desired_value=desired,
        reason="health_weight",
        idempotency_key=key,
    )


@pytest.mark.asyncio
async def test_disabled_guardian_and_missing_adapter_never_write(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    writer = FakeWriter()
    service = GuardianWritebackService(repository, writer)

    disabled = await service.apply(
        _proposal(),
        policy=GuardianPolicy(),
    )
    missing_adapter = await GuardianWritebackService(repository, None).apply(
        _proposal(key="write-2"),
        policy=GuardianPolicy(enabled=True),
    )

    assert disabled.outcome is GuardianWriteOutcome.BLOCKED
    assert disabled.reason == "guardian_disabled"
    assert missing_adapter.outcome is GuardianWriteOutcome.BLOCKED
    assert missing_adapter.reason == "writeback_adapter_disabled"
    assert writer.calls == []


@pytest.mark.asyncio
async def test_human_change_claims_field_and_blocks_guardian(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    writer = FakeWriter()
    service = GuardianWritebackService(repository, writer)
    policy = GuardianPolicy(enabled=True)

    await service.apply(_proposal(current=100, desired=80), policy=GuardianPolicy())
    decision = await service.apply(
        _proposal(current=120, desired=80, key="write-human"),
        policy=policy,
    )
    ownership = await repository.get_field_ownership(
        "channel-1",
        GuardianFieldName.LOAD_FACTOR,
        account_id="42",
    )

    assert decision.outcome is GuardianWriteOutcome.BLOCKED
    assert decision.reason == "human_field_takeover"
    assert ownership is not None
    assert ownership.owner is GuardianFieldOwner.HUMAN
    assert writer.calls == []


@pytest.mark.asyncio
async def test_enabled_write_is_applied_once_and_is_idempotent(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    writer = FakeWriter()
    service = GuardianWritebackService(repository, writer)
    policy = GuardianPolicy(enabled=True)
    proposal = _proposal()

    first = await service.apply(proposal, policy=policy)
    repeated = await service.apply(proposal, policy=policy)
    ownership = await repository.get_field_ownership(
        "channel-1",
        GuardianFieldName.LOAD_FACTOR,
        account_id="42",
    )

    assert first.outcome is GuardianWriteOutcome.APPLIED
    assert repeated == first
    assert writer.calls == [("42", GuardianFieldName.LOAD_FACTOR, 80)]
    assert ownership is not None
    assert ownership.owner is GuardianFieldOwner.GUARDIAN
    assert ownership.last_guardian_value == 80


@pytest.mark.parametrize(
    ("field_name", "current", "desired"),
    [
        (GuardianFieldName.LOAD_FACTOR, 100, 80),
        (GuardianFieldName.PRIORITY, 50, 52),
        (GuardianFieldName.SCHEDULABLE, True, False),
    ],
)
@pytest.mark.asyncio
async def test_enabled_direct_mode_authorizes_every_verified_field(
    tmp_path: Path,
    field_name: GuardianFieldName,
    current: int | bool,
    desired: int | bool,
) -> None:
    repository = await _repository(tmp_path)
    writer = FakeWriter()
    service = GuardianWritebackService(repository, writer)

    decision = await service.apply(
        _proposal(
            current=current,
            desired=desired,
            field_name=field_name,
            key=f"write-{field_name.value}",
        ),
        policy=GuardianPolicy(enabled=True),
    )

    assert decision.outcome is GuardianWriteOutcome.APPLIED
    assert writer.calls == [("42", field_name, desired)]


@pytest.mark.asyncio
async def test_writer_verification_mismatch_fails_without_claiming_ownership(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    writer = MismatchedWriter()
    service = GuardianWritebackService(repository, writer)
    policy = GuardianPolicy(enabled=True)

    decision = await service.apply(_proposal(), policy=policy)
    ownership = await repository.get_field_ownership(
        "channel-1",
        GuardianFieldName.LOAD_FACTOR,
        account_id="42",
    )

    assert decision.outcome is GuardianWriteOutcome.FAILED
    assert decision.reason == "writeback_verification_mismatch"
    assert writer.calls == [("42", GuardianFieldName.LOAD_FACTOR, 80)]
    assert ownership is not None
    assert ownership.owner is GuardianFieldOwner.UPSTREAM
