from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sub2api_mcp.contracts import (
    DeliveryPurpose,
    DeliveryTargetCreate,
    JobType,
    MediaPolicy,
    ProbeResult,
    TargetType,
)
from sub2api_mcp.errors import ServiceError
from sub2api_mcp.jobs import JobManager
from sub2api_mcp.metrics import Metrics
from sub2api_mcp.repository import SqliteRepository
from sub2api_mcp.scheduler import SchedulerPolicy, SchedulerService


def _empty_results() -> list[dict[str, object]]:
    return []


@dataclass
class FakeSub2APIAdapter:
    results: list[ProbeResult]
    calls: int = 0
    recovery_results: list[dict[str, object]] = field(default_factory=_empty_results)
    maintenance_results: list[dict[str, object]] = field(default_factory=_empty_results)
    recovery_calls: int = 0
    maintenance_calls: int = 0

    async def probe(self) -> ProbeResult:
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return result

    async def recover(self) -> list[dict[str, object]]:
        self.recovery_calls += 1
        return self.recovery_results

    async def maintain(self, probe: ProbeResult) -> list[dict[str, object]]:
        self.maintenance_calls += 1
        return self.maintenance_results


async def _repository(tmp_path: Path) -> SqliteRepository:
    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    return repository


async def _status_target(repository: SqliteRepository) -> None:
    await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="status-everywhere",
            bot_uuid="any-adapter-bot",
            target_type=TargetType.GROUP,
            target_id="opaque-group-id",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.AUTO,
            required=True,
        )
    )


def _probe(latency: float, available: int = 2) -> ProbeResult:
    return ProbeResult(
        snapshot={
            "channels": [
                {
                    "name": "channel-a",
                    "status": "active",
                    "available": available,
                    "error": 0,
                    "temporary": 0,
                    "closed": 0,
                }
            ]
        },
        report=f"channel-a latency={latency}",
        image_base64=None,
    )


@pytest.mark.asyncio
async def test_scheduler_queues_only_one_probe_cycle_under_one_lease(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    adapter = FakeSub2APIAdapter([_probe(1.0)])
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, lease_seconds=60),
        owner_id="scheduler-a",
    )

    assert await service.queue_cycle() is True
    assert await service.queue_cycle() is False
    assert await repository.active_job_count(JobType.PROBE) == 1


@pytest.mark.asyncio
async def test_latency_only_changes_do_not_enqueue_duplicate_notifications(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    await _status_target(repository)
    adapter = FakeSub2APIAdapter([_probe(1.0), _probe(99.0)])
    metrics = Metrics.create()
    service = SchedulerService(
        repository,
        adapter,
        metrics,
        SchedulerPolicy(enabled=True, lease_seconds=60),
        owner_id="scheduler-a",
    )
    manager = JobManager(repository, metrics)
    manager.register(JobType.PROBE, service.handle_probe)

    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")
    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")

    assert adapter.calls == 2
    assert await repository.outbox_backlog() == 1


@pytest.mark.asyncio
async def test_account_count_change_enqueues_a_new_notification(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    await _status_target(repository)
    adapter = FakeSub2APIAdapter([_probe(1.0, 2), _probe(1.0, 1)])
    metrics = Metrics.create()
    service = SchedulerService(
        repository,
        adapter,
        metrics,
        SchedulerPolicy(enabled=True, lease_seconds=60),
        owner_id="scheduler-a",
    )
    manager = JobManager(repository, metrics)
    manager.register(JobType.PROBE, service.handle_probe)

    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")
    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")

    assert await repository.outbox_backlog() == 2


def test_admin_delivery_purposes_cannot_target_groups() -> None:
    with pytest.raises(ValueError, match="administrator delivery purposes require a person"):
        DeliveryTargetCreate(
            name="unsafe-admin-group",
            bot_uuid="bot",
            target_type=TargetType.GROUP,
            target_id="group",
            purposes=frozenset({DeliveryPurpose.RECOVERY_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )


@pytest.mark.asyncio
async def test_disabled_scheduler_does_not_queue_work(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    service = SchedulerService(
        repository,
        FakeSub2APIAdapter([_probe(1.0)]),
        Metrics.create(),
        SchedulerPolicy(enabled=False, lease_seconds=60),
        owner_id="scheduler-a",
    )

    assert await service.queue_cycle() is False


@pytest.mark.asyncio
async def test_quiet_hours_defer_notifications_but_keep_probe_work(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    await _status_target(repository)
    metrics = Metrics.create()
    service = SchedulerService(
        repository,
        FakeSub2APIAdapter([_probe(1.0)]),
        metrics,
        SchedulerPolicy(
            enabled=True,
            lease_seconds=60,
            quiet_hours_enabled=True,
            quiet_hours_start="23:00",
            quiet_hours_end="08:00",
        ),
        owner_id="scheduler-a",
        clock=lambda: datetime(2026, 8, 23, 23, 30, tzinfo=UTC),
    )
    manager = JobManager(repository, metrics)
    manager.register(JobType.PROBE, service.handle_probe)

    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")

    assert await repository.outbox_backlog() == 0
    assert await repository.get_snapshot("delivered") is None


@pytest.mark.asyncio
async def test_recovery_and_maintenance_fail_closed_without_admin_target(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    adapter = FakeSub2APIAdapter([_probe(1.0)])
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, recovery_enabled=True, maintenance_enabled=True),
    )
    recovery_job = await repository.create_job(JobType.RECOVERY, {})
    maintenance_job = await repository.create_job(JobType.MAINTENANCE, {})

    with pytest.raises(ServiceError) as recovery_error:
        await service.handle_recovery(recovery_job)
    with pytest.raises(ServiceError) as maintenance_error:
        await service.handle_maintenance(maintenance_job)

    assert recovery_error.value.code == "RECOVERY_ADMIN_TARGET_REQUIRED"
    assert maintenance_error.value.code == "MAINTENANCE_ADMIN_TARGET_REQUIRED"
    assert adapter.recovery_calls == 0
    assert adapter.maintenance_calls == 0
