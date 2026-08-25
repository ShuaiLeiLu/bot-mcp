from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sub2api_mcp.contracts import AccountObservation, AccountObservationStatus, ProbeResult
from sub2api_mcp.guardian.contracts import (
    GuardianAccountSchedulingState,
    GuardianEvidenceBucket,
    GuardianFieldName,
    GuardianSampleSource,
)
from sub2api_mcp.guardian.engine import GuardianEngine
from sub2api_mcp.guardian.repository import GuardianRepository
from sub2api_mcp.repository import SqliteRepository


@dataclass
class FakeOperations:
    calls: int = 0

    async def probe(self) -> ProbeResult:
        self.calls += 1
        return ProbeResult(
            snapshot={
                "version": 1,
                "entries": [
                    {
                        "monitor_id": "11",
                        "name": "Claude",
                        "status": "operational",
                        "group_id": "3",
                        "available_count": 2,
                        "error_count": 0,
                        "temporary_unavailable_count": 0,
                        "closed_count": 1,
                    },
                    {
                        "monitor_id": "12",
                        "name": "Team",
                        "status": "failed",
                        "group_id": "4",
                        "available_count": 1,
                        "error_count": 1,
                        "temporary_unavailable_count": 0,
                        "closed_count": 0,
                    },
                ],
            },
            report="report",
        )


@dataclass
class PausedOperations(FakeOperations):
    async def probe(self) -> ProbeResult:
        result = await super().probe()
        result.snapshot["entries"][0]["upstream_schedulable"] = False
        return result


@dataclass
class BlockingOperations(FakeOperations):
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def probe(self) -> ProbeResult:
        self.started.set()
        await self.release.wait()
        return await super().probe()


@dataclass
class DirectWriteOperations:
    load_factor: int = 10
    account_count: int = 1
    mismatch: bool = False
    reads: list[str] = field(default_factory=lambda: list[str]())
    writes: list[tuple[str, GuardianFieldName, object]] = field(
        default_factory=lambda: list[tuple[str, GuardianFieldName, object]]()
    )

    async def probe(self) -> ProbeResult:
        return ProbeResult(
            snapshot={
                "version": 1,
                "entries": [
                    {
                        "monitor_id": "11",
                        "name": "Claude",
                        "status": "operational",
                        "group_id": "3",
                        "available_count": 1,
                        "error_count": 0,
                        "temporary_unavailable_count": 0,
                        "closed_count": 0,
                    }
                ],
                "accounts": [
                    AccountObservation(
                        account_id=str(41 + index),
                        group_ids=("3",),
                        status=AccountObservationStatus.ACTIVE,
                        schedulable=True,
                    ).model_dump(mode="json")
                    for index in range(1, self.account_count + 1)
                ],
            },
            report="report",
        )

    async def read_account_scheduling_state(
        self,
        account_id: str,
    ) -> GuardianAccountSchedulingState:
        self.reads.append(account_id)
        return GuardianAccountSchedulingState(
            account_id=account_id,
            success=True,
            status=AccountObservationStatus.ACTIVE,
            schedulable=True,
            priority=50,
            load_factor=self.load_factor,
            concurrency=4,
            effective_load_factor=self.load_factor,
        )

    async def write_field(
        self,
        account_id: str,
        field_name: GuardianFieldName,
        value: object,
    ) -> int | bool:
        self.writes.append((account_id, field_name, value))
        if field_name is GuardianFieldName.LOAD_FACTOR and isinstance(value, int):
            if self.mismatch:
                return value + 1
            self.load_factor = value
            return value
        if isinstance(value, (bool, int)):
            return value
        raise TypeError("unsupported direct write value")


@pytest.mark.asyncio
async def test_engine_runs_complete_direct_mode_cycle_without_implicit_writes(
    tmp_path: Path,
) -> None:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()
    operations = FakeOperations()
    engine = GuardianEngine(repository, operations)

    run = await engine.run_once(dry_run=False, idempotency_key="cycle-1")
    repeated = await engine.run_once(dry_run=False, idempotency_key="cycle-1")
    channels = await repository.list_channels(limit=20)

    assert run["status"] == "SUCCEEDED"
    assert run["result"]["channels_evaluated"] == 2
    assert run["result"]["writes_applied"] == 0
    assert run["result"]["scheduling_mode"] == "DIRECT"
    assert run["result"]["transitions"][0]["event_type"] == "PERFECT"
    assert repeated["run_id"] == run["run_id"]
    assert operations.calls == 1
    assert {item["channel_id"] for item in channels["items"]} == {"11", "12"}
    assert await repository.get_open_channel_error_episode("12") is None


@pytest.mark.asyncio
async def test_direct_engine_applies_bounded_account_write_then_honors_cooldown(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 25, 4, tzinfo=UTC)
    repository = GuardianRepository(tmp_path / "state.db", clock=lambda: now)
    await repository.initialize()
    current = await repository.get_policy()
    await repository.update_policy(
        current.model_copy(update={"enabled": True}),
        expected_revision=current.revision,
    )
    operations = DirectWriteOperations()
    engine = GuardianEngine(
        repository,
        operations,
        clock=lambda: now,
        scheduling_operations=operations,
    )

    first = await engine.run_once(dry_run=False, idempotency_key="direct-1")
    second = await engine.run_once(dry_run=False, idempotency_key="direct-2")

    assert first["result"]["writes_proposed"] == 1
    assert first["result"]["writes_applied"] == 1
    assert first["result"]["writes_failed"] == 0
    assert operations.writes == [("42", GuardianFieldName.LOAD_FACTOR, 12)]
    assert second["result"]["writes_proposed"] == 1
    assert second["result"]["writes_blocked"] == 1
    assert second["result"]["writeback_reasons"] == {"write_cooldown": 1}
    assert operations.writes == [("42", GuardianFieldName.LOAD_FACTOR, 12)]


@pytest.mark.asyncio
async def test_direct_engine_stops_all_remaining_accounts_after_verification_failure(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 25, 4, tzinfo=UTC)
    repository = GuardianRepository(tmp_path / "state.db", clock=lambda: now)
    await repository.initialize()
    current = await repository.get_policy()
    await repository.update_policy(
        current.model_copy(
            update={
                "enabled": True,
                "writes": current.writes.model_copy(
                    update={"max_channels_per_run": 2}
                ),
            }
        ),
        expected_revision=current.revision,
    )
    operations = DirectWriteOperations(account_count=2, mismatch=True)
    engine = GuardianEngine(
        repository,
        operations,
        clock=lambda: now,
        scheduling_operations=operations,
    )

    run = await engine.run_once(dry_run=False, idempotency_key="direct-failed")

    assert run["result"]["writes_failed"] == 1
    assert run["result"]["writeback_blocked_reason"] == "verification_failed"
    assert operations.reads == ["42"]
    assert operations.writes == [("42", GuardianFieldName.LOAD_FACTOR, 12)]


@pytest.mark.asyncio
async def test_shared_snapshot_is_consumed_once_without_guardian_upstream_calls(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    scheduler_repository = SqliteRepository(path)
    repository = GuardianRepository(path)
    await scheduler_repository.initialize()
    await repository.initialize()
    seed = await FakeOperations().probe()
    captured_at = datetime(2026, 8, 24, 2, 0, tzinfo=UTC)
    shared_payload = {
        **seed.snapshot,
        "accounts": [
            AccountObservation(
                account_id="997",
                group_ids=("3",),
                status=AccountObservationStatus.ERROR,
                schedulable=False,
            ).model_dump(mode="json")
        ],
    }
    await scheduler_repository.publish_guardian_snapshot(
        shared_payload,
        captured_at=captured_at,
    )
    operations = FakeOperations()
    engine = GuardianEngine(repository, operations, clock=lambda: captured_at)

    first = await engine.run_once(dry_run=True, idempotency_key="shared-1")
    channel = await repository.get_channel("11")
    second = await engine.run_once(dry_run=True, idempotency_key="shared-2")
    reopened = GuardianRepository(path)
    await reopened.initialize()
    third = await GuardianEngine(reopened, operations, clock=lambda: captured_at).run_once(
        dry_run=True,
        idempotency_key="shared-3",
    )

    assert first["result"]["channels_evaluated"] == 2
    assert first["result"]["snapshot_id"]
    assert first["result"]["account_observations_ingested"] == 1
    assert channel is not None
    assert channel["score"] == 100
    assert channel["confidence"] == pytest.approx(0.46, abs=1e-12)
    assert channel["freshness_state"] == "FRESH"
    assert channel["warmup_buckets"] == 1
    assert channel["details"]["evidence_sources"] == ["SHARED_MONITOR"]
    assert [
        item.account_id
        for item in await reopened.list_account_observations(
            first["result"]["snapshot_id"]
        )
    ] == ["997"]
    assert second["result"]["no_new_evidence"] is True
    assert third["result"]["no_new_evidence"] is True
    assert operations.calls == 0
    assert await reopened.pending_input_snapshot_count() == 0


@pytest.mark.asyncio
async def test_shared_monitor_and_traffic_evidence_are_fused_in_one_bucket(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    scheduler_repository = SqliteRepository(path)
    repository = GuardianRepository(path)
    await scheduler_repository.initialize()
    await repository.initialize()
    captured_at = datetime(2026, 8, 24, 2, 0, tzinfo=UTC)
    seed = await FakeOperations().probe()
    await scheduler_repository.publish_guardian_snapshot(
        seed.snapshot,
        captured_at=captured_at,
    )
    await repository.upsert_traffic_buckets(
        [
            GuardianEvidenceBucket(
                channel_id="11",
                bucket_at=captured_at,
                score=25,
                quality=1,
                sources=frozenset({GuardianSampleSource.TRAFFIC}),
                event_count=5,
                ttfb_p95_ms=4000,
            )
        ]
    )

    await GuardianEngine(
        repository,
        FakeOperations(),
        clock=lambda: captured_at,
    ).run_once(dry_run=True, idempotency_key="fused-evidence")
    channel = await repository.get_channel("11")

    assert channel is not None
    assert channel["score"] == pytest.approx((100 * 0.85 + 25) / 1.85, abs=1e-12)
    assert channel["confidence"] == pytest.approx(0.52, abs=1e-12)
    assert channel["details"]["evidence_sources"] == ["SHARED_MONITOR", "TRAFFIC"]


@pytest.mark.asyncio
async def test_engine_preserves_manual_pause_on_sync(tmp_path: Path) -> None:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()
    operations = FakeOperations()
    engine = GuardianEngine(repository, operations)
    await engine.run_once(dry_run=True)
    await repository.set_manual_control("11", "PAUSED")

    run = await engine.run_once(dry_run=True)
    channel = await repository.get_channel("11")

    assert run["status"] == "SUCCEEDED"
    assert channel is not None
    assert channel["health"] == "MANUALLY_PAUSED"
    assert channel["desired_schedulable"] is False


@pytest.mark.asyncio
async def test_engine_honors_upstream_pause_and_persists_weights(tmp_path: Path) -> None:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()
    engine = GuardianEngine(repository, PausedOperations())

    await engine.run_once(dry_run=True)
    disabled = await repository.get_channel("11")
    weighted = await repository.get_channel("12")

    assert disabled is not None
    assert disabled["health"] == "UPSTREAM_DISABLED"
    assert disabled["desired_schedulable"] is False
    assert weighted is not None
    assert weighted["details"]["candidate_weight"] is not None


@pytest.mark.asyncio
async def test_running_cycle_honors_cancellation_before_evaluation(tmp_path: Path) -> None:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()
    operations = BlockingOperations()
    engine = GuardianEngine(repository, operations)

    task = asyncio.create_task(engine.run_once(dry_run=True))
    await operations.started.wait()
    run = (await repository.list_runs(limit=1))[0]
    await repository.cancel_run(run["run_id"])
    operations.release.set()
    result = await task

    assert result["status"] == "CANCELLED"
    assert result["result"]["channels_evaluated"] == 0
