from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sub2api_mcp.actor_bridge import (
    ActorAccount,
    ActorBridgeRequest,
    ActorCommand,
    ActorIdentity,
    ActorRequestVerifier,
    ActorService,
    actor_key,
    sign_actor_request,
)
from sub2api_mcp.errors import ServiceError
from sub2api_mcp.repository import SqliteRepository


class MutableClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


@dataclass
class FakeAccountProvider:
    account: ActorAccount | None

    async def find_active_account(self, email: str) -> ActorAccount | None:
        return self.account

    async def account_report(self, user_id: str) -> str:
        return f"account report for {user_id}"


def _identity(**overrides: str) -> ActorIdentity:
    values = {
        "workspace_uuid": "workspace-1",
        "bot_uuid": "bot-1",
        "adapter": "future-adapter",
        "launcher_type": "person",
        "launcher_id": "raw-platform-user-id",
    }
    values.update(overrides)
    return ActorIdentity.model_validate(values)


async def _repository(tmp_path: Path) -> SqliteRepository:
    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    return repository


def test_actor_key_is_platform_neutral_and_does_not_reveal_raw_id() -> None:
    first = actor_key(_identity(), "s" * 32)
    second = actor_key(_identity(bot_uuid="bot-2"), "s" * 32)

    assert first.startswith("v1:")
    assert "raw-platform-user-id" not in first
    assert first != second


@pytest.mark.asyncio
async def test_signed_request_is_accepted_once_and_replay_is_rejected(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    clock = MutableClock()
    verifier = ActorRequestVerifier(repository, "s" * 32, replay_window_seconds=300, clock=clock)
    body = json.dumps({"command": "ACCOUNT"}, separators=(",", ":")).encode()
    timestamp = str(int(clock.now.timestamp()))
    nonce = "nonce-1234567890"
    signature = sign_actor_request("s" * 32, timestamp, nonce, body)

    await verifier.verify(timestamp, nonce, body, signature)
    with pytest.raises(ServiceError) as replay:
        await verifier.verify(timestamp, nonce, body, signature)

    assert replay.value.code == "ACTOR_REPLAY"


@pytest.mark.asyncio
async def test_stale_actor_request_is_rejected(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    clock = MutableClock()
    verifier = ActorRequestVerifier(repository, "s" * 32, replay_window_seconds=60, clock=clock)
    body = b"{}"
    stale = str(int((clock.now - timedelta(seconds=61)).timestamp()))
    signature = sign_actor_request("s" * 32, stale, "nonce-1234567890", body)

    with pytest.raises(ServiceError) as captured:
        await verifier.verify(stale, "nonce-1234567890", body, signature)

    assert captured.value.code == "ACTOR_TIMESTAMP_INVALID"


@pytest.mark.asyncio
async def test_actor_can_bind_query_and_unbind_without_storing_raw_platform_id(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    provider = FakeAccountProvider(
        ActorAccount(user_id="user-1", email_masked="u***@example.com", status="active")
    )
    service = ActorService(repository, provider, actor_secret="s" * 32)
    identity = _identity()

    bound = await service.execute(
        ActorBridgeRequest(identity=identity, command=ActorCommand.BIND, email="user@example.com")
    )
    report = await service.execute(
        ActorBridgeRequest(identity=identity, command=ActorCommand.ACCOUNT)
    )
    unbound = await service.execute(
        ActorBridgeRequest(identity=identity, command=ActorCommand.UNBIND)
    )

    assert bound.text == "绑定成功：u***@example.com。"
    assert report.text == "account report for user-1"
    assert unbound.text == "绑定已解除。"
    database_bytes = (tmp_path / "state.db").read_bytes()
    assert b"raw-platform-user-id" not in database_bytes


@pytest.mark.asyncio
async def test_binding_requires_an_active_verified_account(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    provider = FakeAccountProvider(
        ActorAccount(user_id="user-1", email_masked="u***@example.com", status="disabled")
    )
    service = ActorService(repository, provider, actor_secret="s" * 32)

    with pytest.raises(ServiceError) as captured:
        await service.execute(
            ActorBridgeRequest(
                identity=_identity(),
                command=ActorCommand.BIND,
                email="user@example.com",
            )
        )

    assert captured.value.code == "ACCOUNT_NOT_BINDABLE"

