from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sub2api_mcp.contracts import (
    AccountObservation,
    AccountObservationStatus,
    AccountQuarantineIntent,
    AccountQuarantineReason,
    AccountQuarantineRecord,
    DeliveryPurpose,
    DeliveryTargetCreate,
    JobType,
    MaintenanceOutcome,
    MaintenanceOutcomeCode,
    MediaPolicy,
    NotificationPayload,
    OutboxEventType,
    ProbeResult,
    QuarantineProbeAttempt,
    QuarantineProbeResult,
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


def _empty_quarantine_results() -> dict[str, QuarantineProbeAttempt]:
    return {}


def _empty_intent_actions() -> dict[str, str]:
    return {}


@dataclass
class FakeSub2APIAdapter:
    results: list[ProbeResult]
    calls: int = 0
    recovery_results: list[dict[str, object]] = field(default_factory=_empty_results)
    maintenance_results: list[dict[str, object]] = field(default_factory=_empty_results)
    quarantine_results: dict[str, QuarantineProbeAttempt] = field(
        default_factory=_empty_quarantine_results
    )
    intent_actions: dict[str, str] = field(default_factory=_empty_intent_actions)
    restore_actions: dict[str, str] = field(default_factory=_empty_intent_actions)
    recovery_calls: int = 0
    recovery_excluded_ids: frozenset[str] = frozenset()
    maintenance_calls: int = 0
    maintenance_excluded_ids: frozenset[str] = frozenset()
    quarantine_calls: list[str] = field(default_factory=lambda: list[str]())
    maintenance_started: asyncio.Event | None = None
    maintenance_release: asyncio.Event | None = None

    async def probe(self) -> ProbeResult:
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return result

    async def recover(
        self,
        *,
        excluded_account_ids: frozenset[str] = frozenset(),
    ) -> list[dict[str, object]]:
        self.recovery_calls += 1
        self.recovery_excluded_ids = excluded_account_ids
        return self.recovery_results

    async def maintain(
        self,
        probe: ProbeResult,
        *,
        excluded_account_ids: frozenset[str] = frozenset(),
        before_quarantine: Callable[[dict[str, object]], Awaitable[None]] | None = None,
        after_quarantine: Callable[[str, bool, bool], Awaitable[None]] | None = None,
    ) -> list[dict[str, object]]:
        self.maintenance_calls += 1
        self.maintenance_excluded_ids = excluded_account_ids
        if self.maintenance_started is not None:
            self.maintenance_started.set()
        if self.maintenance_release is not None:
            await self.maintenance_release.wait()
        if before_quarantine is not None and after_quarantine is not None:
            for result in self.maintenance_results:
                if result.get("outcome") != "QUARANTINED":
                    continue
                required = {
                    "account_id",
                    "reason",
                    "group_ids",
                    "threshold_ms",
                    "observed_count",
                }
                if not required <= result.keys():
                    continue
                account_id = str(result["account_id"])
                await before_quarantine(
                    {
                        "account_id": account_id,
                        "reason": result["reason"],
                        "group_ids": result["group_ids"],
                        "threshold_ms": result["threshold_ms"],
                        "observed_count": result["observed_count"],
                        "previous_status": "active",
                        "previous_schedulable": True,
                    }
                )
                await after_quarantine(account_id, True, False)
        return self.maintenance_results

    async def probe_quarantined(
        self,
        marker: AccountQuarantineRecord,
        *,
        before_restore: Callable[[str], Awaitable[None]] | None = None,
        after_restore: Callable[[str, bool, bool], Awaitable[None]] | None = None,
    ) -> QuarantineProbeAttempt:
        self.quarantine_calls.append(marker.account_id)
        result = self.quarantine_results[marker.account_id]
        if result.recovered and before_restore is not None and after_restore is not None:
            await before_restore(marker.account_id)
            await after_restore(marker.account_id, True, False)
        return result

    async def reconcile_quarantine_intent(
        self,
        intent: AccountQuarantineIntent,
    ) -> str:
        return self.intent_actions.get(intent.account_id, "KEEP")

    async def reconcile_quarantine_restore(
        self,
        marker: AccountQuarantineRecord,
    ) -> str:
        return self.restore_actions.get(marker.account_id, "KEEP")


async def _repository(tmp_path: Path) -> SqliteRepository:
    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    return repository


class LeaseLossRepository(SqliteRepository):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.control_acquire_calls = 0

    async def acquire_account_control_lease(
        self,
        owner: str,
        *,
        lease_seconds: int,
    ) -> bool:
        del owner, lease_seconds
        self.control_acquire_calls += 1
        return self.control_acquire_calls == 1


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
        account_observations=(
            AccountObservation(
                account_id="997",
                group_ids=("3",),
                status=AccountObservationStatus.ERROR,
                schedulable=False,
            ),
        ),
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
    claimed = await guardian_repository.claim_input_snapshot(
        "scheduler-test",
        lease_seconds=30,
    )
    assert claimed is not None
    assert claimed["payload"]["accounts"][0]["account_id"] == "997"


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
async def test_recovery_and_maintenance_control_paths_are_serialized(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    for name, purpose in (
        ("maintenance", DeliveryPurpose.MAINTENANCE_ADMIN),
        ("recovery", DeliveryPurpose.RECOVERY_ADMIN),
    ):
        await repository.upsert_delivery_target(
            DeliveryTargetCreate(
                name=name,
                bot_uuid="bot",
                target_type=TargetType.PERSON,
                target_id=name,
                purposes=frozenset({purpose}),
                media_policy=MediaPolicy.TEXT_ONLY,
                required=True,
            )
        )
    started = asyncio.Event()
    release = asyncio.Event()
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        maintenance_started=started,
        maintenance_release=release,
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, recovery_enabled=True, maintenance_enabled=True),
    )
    maintenance_job = await repository.create_job(JobType.MAINTENANCE, {})
    recovery_job = await repository.create_job(JobType.RECOVERY, {})

    maintenance_task = asyncio.create_task(service.handle_maintenance(maintenance_job))
    await started.wait()
    recovery_task = asyncio.create_task(service.handle_recovery(recovery_job))
    await asyncio.sleep(0)
    assert adapter.recovery_calls == 0
    release.set()
    await asyncio.gather(maintenance_task, recovery_task)

    assert adapter.recovery_calls == 1


@pytest.mark.asyncio
async def test_lost_account_control_lease_stops_the_running_operation(
    tmp_path: Path,
) -> None:
    repository = LeaseLossRepository(tmp_path / "state.db")
    await repository.initialize()
    await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="maintenance",
            bot_uuid="bot",
            target_type=TargetType.PERSON,
            target_id="maintenance",
            purposes=frozenset({DeliveryPurpose.MAINTENANCE_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    started = asyncio.Event()
    release = asyncio.Event()
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        maintenance_started=started,
        maintenance_release=release,
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(
            enabled=True,
            maintenance_enabled=True,
            lease_seconds=1,
        ),
    )
    job = await repository.create_job(JobType.MAINTENANCE, {})

    task = asyncio.create_task(service.handle_maintenance(job))
    await started.wait()
    with pytest.raises(ServiceError) as lost:
        await asyncio.wait_for(task, timeout=2)

    assert lost.value.code == "ACCOUNT_CONTROL_LEASE_LOST"
    assert repository.control_acquire_calls == 2
    assert not release.is_set()


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


def test_maintenance_notification_is_bounded_before_persistence() -> None:
    outcomes = [
        MaintenanceOutcome(
            outcome=MaintenanceOutcomeCode.NO_HEALTHY_ACCOUNT,
            group_id=str(index + 1),
            group_name="超长渠道" * 25,
        )
        for index in range(100)
    ]

    text = SchedulerService._format_maintenance_results(  # pyright: ignore[reportPrivateUsage]
        outcomes,
        triggered_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    payload = NotificationPayload(text=text)
    assert len(payload.text) <= 10_000
    assert "其余 75 项已省略" in payload.text


def test_minimum_pool_notification_bounds_large_group_membership() -> None:
    outcomes = [
        MaintenanceOutcome(
            outcome=MaintenanceOutcomeCode.MIN_POOL_PROTECTED,
            account_id=str(index + 1),
            account_name="账号",
            protected_group_ids=tuple(str(group_id) for group_id in range(1, 101)),
        )
        for index in range(25)
    ]

    text = SchedulerService._format_maintenance_results(  # pyright: ignore[reportPrivateUsage]
        outcomes,
        triggered_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert len(NotificationPayload(text=text).text) <= 10_000
    assert "等 100 个分组" in text


@pytest.mark.asyncio
async def test_omitted_maintenance_notices_remain_pending_for_next_delivery(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    service = SchedulerService(
        repository,
        FakeSub2APIAdapter([_probe(1.0)]),
        Metrics.create(),
        SchedulerPolicy(enabled=True),
    )
    outcomes = [
        MaintenanceOutcome(
            outcome=MaintenanceOutcomeCode.NO_HEALTHY_ACCOUNT,
            group_id=str(index + 1),
            group_name=f"渠道 {index + 1}",
        )
        for index in range(30)
    ]

    first, first_signatures = (
        await service._new_maintenance_notification_outcomes(  # pyright: ignore[reportPrivateUsage]
            outcomes
        )
    )
    await repository.set_scheduler_value(
        "maintenance_notice_signatures",
        first_signatures,
    )
    second, second_signatures = (
        await service._new_maintenance_notification_outcomes(  # pyright: ignore[reportPrivateUsage]
            outcomes
        )
    )

    assert len(first) == 25
    assert len(second) == 5
    assert len(second_signatures) == 30


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
async def test_quarantine_probes_are_bounded_and_recovered_markers_are_removed(
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
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    for account_id in ("101", "102", "103", "104", "105", "106"):
        await repository.upsert_account_quarantine(
            AccountQuarantineRecord(
                account_id=account_id,
                reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
                group_ids=("7",),
                threshold_ms=30_000,
                observed_count=3,
                quarantined_at=now,
            )
        )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        quarantine_results={
            "101": QuarantineProbeAttempt(
                account_id="101",
                result=QuarantineProbeResult.SLOW,
                latency_ms=45_000,
            ),
            "102": QuarantineProbeAttempt(
                account_id="102",
                result=QuarantineProbeResult.INVALID,
            ),
            "103": QuarantineProbeAttempt(
                account_id="103",
                result=QuarantineProbeResult.FAILED,
            ),
            "104": QuarantineProbeAttempt(
                account_id="104",
                result=QuarantineProbeResult.RECOVERED,
                latency_ms=2_000,
                recovered=True,
            ),
            "105": QuarantineProbeAttempt(
                account_id="105",
                result=QuarantineProbeResult.SLOW,
                latency_ms=50_000,
            ),
        },
    )
    metrics = Metrics.create()
    service = SchedulerService(
        repository,
        adapter,
        metrics,
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
        clock=lambda: now,
    )

    job = await repository.create_job(JobType.MAINTENANCE, {})
    result = await service.handle_maintenance(job)
    delivery = await repository.claim_next_delivery("worker", lease_seconds=30)

    assert adapter.quarantine_calls == ["101", "102", "103", "104", "105"]
    assert adapter.maintenance_excluded_ids == frozenset(
        {"101", "102", "103", "104", "105", "106"}
    )
    assert len(result["probes"]) == 5
    assert await repository.get_account_quarantine("104") is None
    assert await repository.get_account_quarantine("106") is not None
    assert await repository.account_quarantine_count() == 5
    assert delivery is not None
    assert "恢复回池" in delivery.payload["notification"]["text"]
    assert "继续隔离" in delivery.payload["notification"]["text"]
    rendered_metrics = metrics.render().decode()
    assert (
        'sub2api_account_quarantine_probes_total{reason="SLOW_FIRST_TOKEN",result="RECOVERED"} 1.0'
        in rendered_metrics
    )
    assert (
        "sub2api_account_quarantine_transitions_total"
        '{action="recovered",reason="SLOW_FIRST_TOKEN"} 1.0'
        in rendered_metrics
    )
    assert (
        'sub2api_account_quarantines{reason="SLOW_FIRST_TOKEN"} 5.0'
        in rendered_metrics
    )


@pytest.mark.asyncio
async def test_existing_quarantine_queues_probes_when_detection_guards_are_off(
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
    await repository.upsert_account_quarantine(
        AccountQuarantineRecord(
            account_id="101",
            reason=AccountQuarantineReason.CHANNEL_TEST_FAILED,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=1,
            quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    service = SchedulerService(
        repository,
        FakeSub2APIAdapter([_probe(1.0)]),
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
    )
    job = await repository.create_job(JobType.PROBE, {})

    await service.handle_probe(job)

    assert await repository.active_job_count(JobType.MAINTENANCE) == 1


@pytest.mark.asyncio
async def test_quarantine_recovery_continues_without_notification_target(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    marker = AccountQuarantineRecord(
        account_id="101",
        reason=AccountQuarantineReason.CHANNEL_TEST_FAILED,
        group_ids=("7",),
        threshold_ms=30_000,
        observed_count=1,
        quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )
    await repository.upsert_account_quarantine(marker)
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        quarantine_results={
            "101": QuarantineProbeAttempt(
                account_id="101",
                result=QuarantineProbeResult.RECOVERED,
                recovered=True,
            )
        },
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
    )

    probe_job = await repository.create_job(JobType.PROBE, {})
    await service.handle_probe(probe_job)
    maintenance_job = await repository.create_job(JobType.MAINTENANCE, {})
    await service.handle_maintenance(maintenance_job)

    assert adapter.maintenance_calls == 0
    assert adapter.quarantine_calls == ["101"]
    assert await repository.get_account_quarantine("101") is None
    assert await repository.outbox_backlog() == 0


@pytest.mark.asyncio
async def test_restart_reconciles_partial_disable_intent_without_target(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_account_quarantine_intent(
        AccountQuarantineIntent(
            account_id="101",
            reason=AccountQuarantineReason.CHANNEL_TEST_FAILED,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=1,
            previous_status="active",
            previous_schedulable=True,
            created_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        intent_actions={"101": "PROMOTE"},
        quarantine_results={
            "101": QuarantineProbeAttempt(
                account_id="101",
                result=QuarantineProbeResult.INVALID,
            )
        },
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
    )

    probe_job = await repository.create_job(JobType.PROBE, {})
    await service.handle_probe(probe_job)
    maintenance_job = await repository.create_job(JobType.MAINTENANCE, {})
    result = await service.handle_maintenance(maintenance_job)

    assert result["reconciled_intents"] == [
        {"account_id": "101", "action": "PROMOTE"}
    ]
    assert await repository.account_quarantine_intent_count() == 0
    assert await repository.get_account_quarantine("101") is not None


@pytest.mark.asyncio
async def test_restart_clears_unapplied_quarantine_intent(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_account_quarantine_intent(
        AccountQuarantineIntent(
            account_id="101",
            reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=3,
            previous_status="active",
            previous_schedulable=True,
            created_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        intent_actions={"101": "CLEAR"},
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
    )

    maintenance_job = await repository.create_job(JobType.MAINTENANCE, {})
    result = await service.handle_maintenance(maintenance_job)

    assert result["reconciled_intents"] == [
        {"account_id": "101", "action": "CLEAR"}
    ]
    assert await repository.account_quarantine_intent_count() == 0
    assert await repository.get_account_quarantine("101") is None


@pytest.mark.asyncio
async def test_restart_finishes_verified_restore_before_removing_marker(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_account_quarantine(
        AccountQuarantineRecord(
            account_id="101",
            reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=3,
            quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    await repository.begin_account_quarantine_restore("101")
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        restore_actions={"101": "RECOVERED"},
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
    )

    maintenance_job = await repository.create_job(JobType.MAINTENANCE, {})
    result = await service.handle_maintenance(maintenance_job)

    assert result["reconciled_restores"] == [
        {"account_id": "101", "action": "RECOVERED"}
    ]
    assert await repository.get_account_quarantine("101") is None
    assert await repository.account_quarantine_restore_intent_count() == 0


@pytest.mark.asyncio
async def test_uncertain_restore_keeps_intent_and_marker_for_reconciliation(
    tmp_path: Path,
) -> None:
    repository = await _repository(tmp_path)
    await repository.upsert_account_quarantine(
        AccountQuarantineRecord(
            account_id="101",
            reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=3,
            quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    await repository.begin_account_quarantine_restore("101")
    service = SchedulerService(
        repository,
        FakeSub2APIAdapter([_probe(1.0)]),
        Metrics.create(),
        SchedulerPolicy(enabled=True),
    )

    await service._finish_quarantine_restore(  # pyright: ignore[reportPrivateUsage]
        "101",
        False,
        True,
    )

    assert await repository.get_account_quarantine("101") is not None
    assert await repository.account_quarantine_restore_intent_count() == 1


@pytest.mark.asyncio
async def test_ordinary_recovery_excludes_durable_quarantine_markers(
    tmp_path: Path,
) -> None:
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
    await repository.upsert_account_quarantine(
        AccountQuarantineRecord(
            account_id="101",
            reason=AccountQuarantineReason.CHANNEL_TEST_FAILED,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=1,
            quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    adapter = FakeSub2APIAdapter([_probe(1.0)])
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, recovery_enabled=True),
    )

    job = await repository.create_job(JobType.RECOVERY, {})
    await service.handle_recovery(job)

    assert adapter.recovery_excluded_ids == frozenset({"101"})


@pytest.mark.asyncio
async def test_unchanged_quarantine_probe_does_not_flood_notifications(
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
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    await repository.upsert_account_quarantine(
        AccountQuarantineRecord(
            account_id="101",
            reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=3,
            quarantined_at=now,
        )
    )
    await repository.update_account_quarantine_probe(
        "101",
        probed_at=now,
        latency_ms=45_000,
        result=QuarantineProbeResult.SLOW,
    )
    adapter = FakeSub2APIAdapter(
        [_probe(1.0)],
        quarantine_results={
            "101": QuarantineProbeAttempt(
                account_id="101",
                result=QuarantineProbeResult.SLOW,
                latency_ms=44_000,
            )
        },
    )
    service = SchedulerService(
        repository,
        adapter,
        Metrics.create(),
        SchedulerPolicy(enabled=True, maintenance_enabled=False),
        clock=lambda: now,
    )

    job = await repository.create_job(JobType.MAINTENANCE, {})
    await service.handle_maintenance(job)

    assert await repository.outbox_backlog() == 0


@pytest.mark.asyncio
async def test_unchanged_maintenance_notice_is_durably_deduplicated(
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
    )

    first = await repository.create_job(JobType.MAINTENANCE, {})
    second = await repository.create_job(JobType.MAINTENANCE, {})
    await service.handle_maintenance(first)
    await service.handle_maintenance(second)

    assert await repository.outbox_backlog() == 1


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
