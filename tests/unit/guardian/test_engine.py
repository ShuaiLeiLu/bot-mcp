from __future__ import annotations

from dataclasses import dataclass
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
