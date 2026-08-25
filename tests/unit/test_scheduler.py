from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sub2api_mcp.contracts import (
    AccountQuarantineReason,
    DeliveryPurpose,
    DeliveryTargetCreate,
    JobType,
    MediaPolicy,
    OutboxEventType,
    ProbeResult,
    TargetType,
)
from sub2api_mcp.errors import ServiceError
from sub2api_mcp.guardian.repository import GuardianRepository
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
async def test_scheduler_publishes_one_canonical_guardian_snapshot_without_extra_probe(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    repository = SqliteRepository(path)
    guardian_repository = GuardianRepository(path)
    await repository.initialize()
    await guardian_repository.initialize()
    captured_at = datetime(2026, 8, 24, 2, 0, tzinfo=UTC)
    result = ProbeResult(
        snapshot={"channels": []},
        report="no channels",
        guardian_snapshot={
            "version": 1,
            "entries": [
                {
                    "monitor_id": "7",
                    "name": "Claude",
                    "status": "operational",
                    "group_id": "3",
                    "group_name": "Claude",
                    "available_count": 1,
                    "error_count": 0,
                    "temporary_unavailable_count": 0,
                    "closed_count": 0,
                    "latency_ms": 1200,
                    "upstream_schedulable": True,
                }
            ],
        },
        captured_at=captured_at,
    )
    adapter = FakeSub2APIAdapter([result, result])
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, lease_seconds=60),
        owner_id="scheduler-a",
    )
    manager = JobManager(repository, Metrics.create())
    manager.register(JobType.PROBE, service.handle_probe)

    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")
    await service.queue_cycle()
    await manager.run_once({JobType.PROBE}, "worker-1")

    assert adapter.calls == 2
    assert await guardian_repository.pending_input_snapshot_count() == 1


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

    assert await repository.outbox_backlog() == 1


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


@pytest.mark.asyncio
async def test_maintenance_notification_uses_readable_chinese_layout(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="maintenance-admin",
            bot_uuid="bot",
            target_type=TargetType.PERSON,
            target_id="admin",
            purposes=frozenset({DeliveryPurpose.MAINTENANCE_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        maintenance_results=[
            {
                "outcome": "QUARANTINED",
                "account_id": "1032",
                "account_name": "ai 特惠",
                "reason": "SLOW_FIRST_TOKEN",
                "group_ids": ["7"],
                "threshold_ms": 30_000,
                "observed_count": 3,
            }
        ],
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=True),
        clock=lambda: datetime(2026, 8, 24, 2, 15, 30, tzinfo=UTC),
    )

    job = await repository.create_job(JobType.MAINTENANCE, {})
    await service.handle_maintenance(job)
    marker = await repository.get_account_quarantine("1032")
    delivery = await repository.claim_next_delivery("worker", lease_seconds=30)

    assert marker is not None
    assert marker.reason is AccountQuarantineReason.SLOW_FIRST_TOKEN
    assert marker.group_ids == ("7",)
    assert marker.observed_count == 3
    assert delivery is not None
    assert delivery.event_type is OutboxEventType.MAINTENANCE_RESULT
    assert delivery.payload["notification"]["text"] == (
        "账号维护结果｜1 个账号\n"
        "触发时间：2026-08-24 10:15:30（北京时间）\n\n"
        "1. ai 特惠（账号 #1032）\n"
        "类型：系统延迟隔离\n"
        "原因：首字响应延迟连续超过 30 秒\n"
        "结果：已隔离并关闭账号调度"
    )
    assert target.delivery_target_id == delivery.target.delivery_target_id


@pytest.mark.asyncio
async def test_no_healthy_account_notice_never_creates_a_quarantine_marker(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="maintenance-admin",
            bot_uuid="bot",
            target_type=TargetType.PERSON,
            target_id="admin",
            purposes=frozenset({DeliveryPurpose.MAINTENANCE_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        maintenance_results=[
            {
                "outcome": "NO_HEALTHY_ACCOUNT",
                "group_id": "7",
                "group_name": "特惠渠道",
            }
        ],
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=True),
        clock=lambda: datetime(2026, 8, 24, 2, 15, 30, tzinfo=UTC),
    )

    job = await repository.create_job(JobType.MAINTENANCE, {})
    result = await service.handle_maintenance(job)
    delivery = await repository.claim_next_delivery("worker", lease_seconds=30)

    assert await repository.account_quarantine_count() == 0
    assert result["adjustments"] == []
    assert delivery is not None
    assert "渠道无健康账号" in delivery.payload["notification"]["text"]
    assert "未自动禁用任何账号" in delivery.payload["notification"]["text"]


@pytest.mark.asyncio
async def test_invalid_maintenance_result_fails_closed_before_persistence(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="maintenance-admin",
            bot_uuid="bot",
            target_type=TargetType.PERSON,
            target_id="admin",
            purposes=frozenset({DeliveryPurpose.MAINTENANCE_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        maintenance_results=[
            {
                "outcome": "QUARANTINED",
                "account_id": "1032",
                "account_name": "缺少分组",
                "reason": "SLOW_FIRST_TOKEN",
                "threshold_ms": 30_000,
                "observed_count": 3,
            }
        ],
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=True),
    )

    job = await repository.create_job(JobType.MAINTENANCE, {})
    with pytest.raises(ServiceError) as invalid_result:
        await service.handle_maintenance(job)

    assert invalid_result.value.code == "MAINTENANCE_RESULT_INVALID"
    assert await repository.account_quarantine_count() == 0
    assert await repository.outbox_backlog() == 0


@pytest.mark.asyncio
async def test_recovery_notification_uses_readable_chinese_layout(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="recovery-admin",
            bot_uuid="bot",
            target_type=TargetType.PERSON,
            target_id="admin",
            purposes=frozenset({DeliveryPurpose.RECOVERY_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        recovery_results=[
            {
                "account_id": "42",
                "name": "Claude 主账号",
                "bucket": "error",
                "result": "recovered",
            }
        ],
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, recovery_enabled=True),
        clock=lambda: datetime(2026, 8, 24, 2, 16, 0, tzinfo=UTC),
    )

    job = await repository.create_job(JobType.RECOVERY, {})
    await service.handle_recovery(job)
    delivery = await repository.claim_next_delivery("worker", lease_seconds=30)

    assert delivery is not None
    assert delivery.event_type is OutboxEventType.RECOVERY_RESULT
    assert delivery.payload["notification"]["text"] == (
        "账号恢复结果｜1 个账号\n"
        "触发时间：2026-08-24 10:16:00（北京时间）\n\n"
        "1. Claude 主账号（账号 #42）\n"
        "原状态：错误\n"
        "结果：已恢复正常"
    )
