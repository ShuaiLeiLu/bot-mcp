from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from sub2api_mcp.contracts import ProbeResult
from sub2api_mcp.guardian.engine import GuardianEngine
from sub2api_mcp.guardian.repository import GuardianRepository


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


@pytest.mark.asyncio
async def test_engine_runs_complete_observe_only_cycle(tmp_path: Path) -> None:
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
    assert run["result"]["observe_only"] is True
    assert run["result"]["transitions"][0]["event_type"] == "PERFECT"
    assert repeated["run_id"] == run["run_id"]
    assert operations.calls == 1
    assert {item["channel_id"] for item in channels["items"]} == {"11", "12"}


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
