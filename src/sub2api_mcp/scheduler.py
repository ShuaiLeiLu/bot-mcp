"""Durable scheduler queueing and Sub2API operation handlers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from .contracts import (
    AccountQuarantineIntent,
    AccountQuarantineReason,
    AccountQuarantineRecord,
    DeliveryPurpose,
    JobRecord,
    JobType,
    MaintenanceOutcome,
    MaintenanceOutcomeBatch,
    MaintenanceOutcomeCode,
    NotificationPayload,
    OutboxEventType,
    ProbeResult,
    QuarantineProbeAttempt,
    QuarantineProbeResult,
)
from .errors import ServiceError
from .metrics import Metrics
from .repository import SqliteRepository

_MAINTENANCE_REASON_LABELS = {
    AccountQuarantineReason.CHANNEL_TEST_FAILED: "渠道异常且可用性测试失败",
    AccountQuarantineReason.SLOW_FIRST_TOKEN: "首字响应延迟连续超过 30 秒",
}
_LOGGER = logging.getLogger("sub2api_mcp.scheduler")


class Sub2APIOperations(Protocol):
    async def probe(self) -> ProbeResult: ...

    async def maintain(
        self,
        probe: ProbeResult,
        *,
        excluded_account_ids: frozenset[str] = frozenset(),
        before_quarantine: Callable[[dict[str, object]], Awaitable[None]] | None = None,
        after_quarantine: Callable[[str, bool, bool], Awaitable[None]] | None = None,
    ) -> list[dict[str, object]]: ...

    async def probe_quarantined(
        self,
        marker: AccountQuarantineRecord,
        *,
        before_restore: Callable[[str], Awaitable[None]] | None = None,
        after_restore: Callable[[str, bool, bool], Awaitable[None]] | None = None,
    ) -> QuarantineProbeAttempt: ...

    async def reconcile_quarantine_intent(
        self,
        intent: AccountQuarantineIntent,
    ) -> str: ...

    async def reconcile_quarantine_restore(
        self,
        marker: AccountQuarantineRecord,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class SchedulerPolicy:
    enabled: bool = False
    interval_seconds: int = 60
    lease_seconds: int = 120
    maintenance_enabled: bool = False
    quiet_hours_enabled: bool = False
    quiet_hours_start: str = "23:00"
    quiet_hours_end: str = "08:00"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _quiet_hours_active(now: datetime, policy: SchedulerPolicy) -> bool:
    if not policy.quiet_hours_enabled:
        return False
    local_time = now.astimezone(ZoneInfo("Asia/Shanghai")).time().replace(tzinfo=None)
    start = time.fromisoformat(policy.quiet_hours_start)
    end = time.fromisoformat(policy.quiet_hours_end)
    if start == end:
        return True
    if start < end:
        return start <= local_time < end
    return local_time >= start or local_time < end


class SchedulerService:
    def __init__(
        self,
        repository: SqliteRepository,
        adapter: Sub2APIOperations,
        metrics: Metrics,
        policy: SchedulerPolicy,
        *,
        owner_id: str | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._repository = repository
        self._adapter = adapter
        self._metrics = metrics
        self._policy = policy
        self._owner_id = owner_id or f"scheduler-{uuid.uuid4()}"
        self._clock = clock
        self._stop = asyncio.Event()
        self._control_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._latest_probe: ProbeResult | None = None

    async def queue_cycle(self) -> bool:
        stored_enabled = await self._repository.get_scheduler_value("enabled")
        enabled = self._policy.enabled if stored_enabled is None else bool(stored_enabled)
        if not enabled:
            return False
        has_lease = await self._repository.acquire_scheduler_lease(
            self._owner_id, lease_seconds=self._policy.lease_seconds
        )
        if not has_lease:
            return False
        created = await self._repository.create_job_with_capacity(
            JobType.PROBE,
            {},
            max_active=1,
        )
        return created is not None

    async def set_enabled(self, enabled: bool) -> None:
        await self._repository.set_scheduler_value("enabled", bool(enabled))

    async def is_enabled(self) -> bool:
        stored = await self._repository.get_scheduler_value("enabled")
        return self._policy.enabled if stored is None else bool(stored)

    async def handle_probe(self, job: JobRecord) -> dict[str, Any]:
        del job
        result = await self._adapter.probe()
        self._latest_probe = result
        if result.guardian_snapshot is not None and result.captured_at is not None:
            try:
                guardian_payload = {
                    **result.guardian_snapshot,
                    "accounts": [
                        item.model_dump(mode="json")
                        for item in result.account_observations
                    ],
                }
                await self._repository.publish_guardian_snapshot(
                    guardian_payload,
                    captured_at=result.captured_at,
                )
            except Exception:
                _LOGGER.exception("guardian_snapshot_publish_failed")
        previous = await self._repository.get_snapshot("pending")
        if previous is None:
            previous = await self._repository.get_snapshot("delivered")
        changed = previous != result.snapshot
        if changed and not _quiet_hours_active(self._clock(), self._policy):
            targets = await self._targets_for(DeliveryPurpose.STATUS)
            if targets:
                notification = NotificationPayload(
                    text=result.report,
                    image_base64=result.image_base64,
                )
                await self._repository.enqueue_outbox(
                    OutboxEventType.STATUS_CHANGED,
                    {
                        "notification": notification.model_dump(mode="json", exclude_none=True),
                        "deliveredSnapshot": result.snapshot,
                    },
                    targets,
                )
                await self._repository.set_snapshot("pending", result.snapshot)

        quarantine_count = await self._repository.account_quarantine_count()
        quarantine_intent_count = (
            await self._repository.account_quarantine_intent_count()
        )
        maintenance_targets = await self._targets_for(DeliveryPurpose.MAINTENANCE_ADMIN)
        maintenance_required = quarantine_count > 0 or quarantine_intent_count > 0 or (
            self._policy.maintenance_enabled and bool(maintenance_targets)
        )
        if maintenance_required:
            await self._repository.create_job_with_capacity(JobType.MAINTENANCE, {}, max_active=1)
        return {"changed": changed, "snapshot": result.snapshot}

    async def handle_maintenance(self, job: JobRecord) -> dict[str, Any]:
        async with self._control_lock:
            return await self._run_with_control_lease(
                job,
                lambda: self._handle_maintenance_locked(job),
            )

    async def _run_with_control_lease(
        self,
        job: JobRecord,
        action: Callable[[], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        owner = f"{self._owner_id}:{job.job_id}"
        acquired = await self._repository.acquire_account_control_lease(
            owner,
            lease_seconds=self._policy.lease_seconds,
        )
        if not acquired:
            raise ServiceError(
                "ACCOUNT_CONTROL_BUSY",
                "Another account control operation is already running",
                retryable=True,
            )
        stop_heartbeat = asyncio.Event()
        async def run_operation() -> dict[str, Any]:
            return await action()

        operation: asyncio.Task[dict[str, Any]] = asyncio.create_task(
            run_operation(),
            name=f"account-control-operation-{job.job_id}",
        )
        heartbeat = asyncio.create_task(
            self._control_lease_heartbeat(owner, stop_heartbeat),
            name=f"account-control-lease-{job.job_id}",
        )
        try:
            completed, _ = await asyncio.wait(
                (operation, heartbeat),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in completed:
                if not operation.done():
                    operation.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await operation
                await heartbeat
                raise ServiceError(
                    "ACCOUNT_CONTROL_LEASE_LOST",
                    "The account control lease ended unexpectedly",
                    retryable=True,
                )
            return await operation
        finally:
            stop_heartbeat.set()
            if not heartbeat.done():
                await heartbeat
            await self._repository.release_account_control_lease(owner)

    async def _control_lease_heartbeat(
        self,
        owner: str,
        stop: asyncio.Event,
    ) -> None:
        interval = max(1.0, self._policy.lease_seconds / 3)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                renewed = await self._repository.acquire_account_control_lease(
                    owner,
                    lease_seconds=self._policy.lease_seconds,
                )
                if not renewed:
                    _LOGGER.error("account_control_lease_lost")
                    raise ServiceError(
                        "ACCOUNT_CONTROL_LEASE_LOST",
                        "The account control lease could not be renewed",
                        retryable=True,
                    ) from None

    async def _handle_maintenance_locked(self, job: JobRecord) -> dict[str, Any]:
        del job
        intents = await self._repository.list_account_quarantine_intents(limit=5)
        reconciled_intents: list[dict[str, str]] = []
        for intent in intents:
            action = await self._adapter.reconcile_quarantine_intent(intent)
            if action == "PROMOTE":
                marker = await self._repository.promote_account_quarantine_intent(
                    intent.account_id
                )
                if marker is not None:
                    self._metrics.account_quarantine_transitions.labels(
                        reason=marker.reason.value,
                        action="reconciled",
                    ).inc()
            elif action == "CLEAR":
                await self._repository.remove_account_quarantine_intent(
                    intent.account_id
                )
            elif action != "KEEP":
                raise ServiceError(
                    "QUARANTINE_RECONCILIATION_INVALID",
                    "The quarantine reconciliation result is invalid",
                )
            reconciled_intents.append(
                {"account_id": intent.account_id, "action": action}
            )
        restore_intents = (
            await self._repository.list_account_quarantine_restore_intents(
                limit=5
            )
        )
        reconciled_restores: list[dict[str, str]] = []
        for restore_intent in restore_intents:
            restore_marker = await self._repository.get_account_quarantine(
                restore_intent.account_id
            )
            if restore_marker is None:
                await self._repository.cancel_account_quarantine_restore(
                    restore_intent.account_id
                )
                continue
            action = await self._adapter.reconcile_quarantine_restore(restore_marker)
            if action == "RECOVERED":
                marker = await self._repository.complete_account_quarantine_restore(
                    restore_intent.account_id
                )
                if marker is not None:
                    self._metrics.account_quarantine_transitions.labels(
                        reason=marker.reason.value,
                        action="recovered",
                    ).inc()
            elif action == "CANCEL":
                await self._repository.cancel_account_quarantine_restore(
                    restore_intent.account_id
                )
            elif action != "KEEP":
                raise ServiceError(
                    "QUARANTINE_RECONCILIATION_INVALID",
                    "The quarantine restore reconciliation result is invalid",
                )
            reconciled_restores.append(
                {"account_id": restore_intent.account_id, "action": action}
            )
        selected_markers = await self._repository.list_account_quarantines_for_probe(
            limit=5
        )
        targets = await self._targets_for(DeliveryPurpose.MAINTENANCE_ADMIN)
        if not targets and not selected_markers and not intents and not restore_intents:
            raise ServiceError(
                "MAINTENANCE_ADMIN_TARGET_REQUIRED",
                "A personal administrator delivery target is required",
            )
        excluded_account_ids = await self._quarantined_account_ids()
        if targets:
            probe = self._latest_probe or await self._adapter.probe()
            raw_outcomes = await self._adapter.maintain(
                probe,
                excluded_account_ids=excluded_account_ids,
                before_quarantine=self._record_quarantine_intent,
                after_quarantine=self._record_quarantine_disable_result,
            )
        else:
            raw_outcomes = []
        try:
            outcomes = MaintenanceOutcomeBatch.model_validate(
                {"items": raw_outcomes}
            ).items
        except ValidationError as exc:
            raise ServiceError(
                "MAINTENANCE_RESULT_INVALID",
                "The account maintenance result is invalid",
            ) from exc
        notification_outcomes, maintenance_notice_signatures = (
            await self._new_maintenance_notification_outcomes(outcomes)
            if targets
            else ([], [])
        )
        adjustments: list[dict[str, Any]] = []
        for outcome in outcomes:
            if outcome.outcome is not MaintenanceOutcomeCode.QUARANTINED:
                continue
            assert (
                outcome.account_id is not None
                and outcome.account_name is not None
                and outcome.reason is not None
                and outcome.threshold_ms is not None
                and outcome.observed_count is not None
            )
            persisted = await self._repository.get_account_quarantine(
                outcome.account_id
            )
            if persisted is None:
                raise ServiceError(
                    "QUARANTINE_MARKER_MISSING",
                    "Verified account quarantine marker is missing",
                )
            adjustments.append(outcome.model_dump(mode="json", exclude_none=True))
        quarantine_probes: list[dict[str, Any]] = []
        quarantine_notifications: list[dict[str, Any]] = []
        for marker in selected_markers:
            try:
                attempt = await self._adapter.probe_quarantined(
                    marker,
                    before_restore=self._begin_quarantine_restore,
                    after_restore=self._finish_quarantine_restore,
                )
            except Exception:
                _LOGGER.exception("account_quarantine_probe_failed")
                attempt = QuarantineProbeAttempt(
                    account_id=marker.account_id,
                    result=QuarantineProbeResult.INVALID,
                )
            if attempt.account_id != marker.account_id:
                _LOGGER.warning("account_quarantine_probe_identity_mismatch")
                attempt = QuarantineProbeAttempt(
                    account_id=marker.account_id,
                    result=QuarantineProbeResult.INVALID,
                )
            probed_at = self._clock()
            if not attempt.recovered:
                await self._repository.update_account_quarantine_probe(
                    marker.account_id,
                    probed_at=probed_at,
                    latency_ms=attempt.latency_ms,
                    result=attempt.result,
                )
            self._metrics.account_quarantine_probes.labels(
                reason=marker.reason.value,
                result=attempt.result.value,
            ).inc()
            probe_result = {
                **attempt.model_dump(mode="json", exclude_none=True),
                "reason": marker.reason.value,
            }
            quarantine_probes.append(probe_result)
            if (
                attempt.recovered
                or marker.last_probe_at is None
                or marker.last_probe_result is not attempt.result
            ):
                quarantine_notifications.append(probe_result)
        if targets and (notification_outcomes or quarantine_notifications):
            notification_sections: list[str] = []
            if notification_outcomes:
                notification_sections.append(
                    self._format_maintenance_results(
                        notification_outcomes,
                        triggered_at=self._clock(),
                    )
                )
            if quarantine_notifications:
                notification_sections.append(
                    self._format_quarantine_probe_results(
                        quarantine_notifications,
                        triggered_at=self._clock(),
                    )
                )
            await self._repository.enqueue_outbox(
                OutboxEventType.MAINTENANCE_RESULT,
                {
                    "notification": NotificationPayload(
                        text="\n\n".join(notification_sections)
                    ).model_dump(mode="json", exclude_none=True),
                },
                targets,
            )
        if targets:
            await self._repository.set_scheduler_value(
                "maintenance_notice_signatures",
                maintenance_notice_signatures,
            )
        await self._refresh_quarantine_metrics()
        return {
            "adjustments": adjustments,
            "outcomes": [
                outcome.model_dump(mode="json", exclude_none=True)
                for outcome in outcomes
            ],
            "probes": quarantine_probes,
            "reconciled_intents": reconciled_intents,
            "reconciled_restores": reconciled_restores,
        }

    async def _begin_quarantine_restore(self, account_id: str) -> None:
        await self._repository.begin_account_quarantine_restore(account_id)

    async def _finish_quarantine_restore(
        self,
        account_id: str,
        success: bool,
        state_uncertain: bool,
    ) -> None:
        if success:
            marker = await self._repository.complete_account_quarantine_restore(
                account_id
            )
            if marker is None:
                raise ServiceError(
                    "QUARANTINE_RESTORE_INTENT_MISSING",
                    "The account quarantine restore intent is missing",
                )
            self._metrics.account_quarantine_transitions.labels(
                reason=marker.reason.value,
                action="recovered",
            ).inc()
        elif not state_uncertain:
            await self._repository.cancel_account_quarantine_restore(account_id)

    async def _record_quarantine_intent(
        self,
        payload: dict[str, object],
    ) -> None:
        intent = AccountQuarantineIntent.model_validate(
            {**payload, "created_at": self._clock()}
        )
        await self._repository.upsert_account_quarantine_intent(intent)

    async def _record_quarantine_disable_result(
        self,
        account_id: str,
        success: bool,
        state_uncertain: bool,
    ) -> None:
        if success:
            marker = await self._repository.promote_account_quarantine_intent(account_id)
            if marker is None:
                raise ServiceError(
                    "QUARANTINE_INTENT_MISSING",
                    "The account quarantine intent is missing",
                )
            self._metrics.account_quarantine_transitions.labels(
                reason=marker.reason.value,
                action="quarantined",
            ).inc()
        elif not state_uncertain:
            await self._repository.remove_account_quarantine_intent(account_id)

    async def _refresh_quarantine_metrics(self) -> None:
        for reason in AccountQuarantineReason:
            count = await self._repository.account_quarantine_count(reason)
            self._metrics.account_quarantines.labels(reason=reason.value).set(count)

    async def _new_maintenance_notification_outcomes(
        self,
        outcomes: list[MaintenanceOutcome],
    ) -> tuple[list[MaintenanceOutcome], list[str]]:
        previous_raw = await self._repository.get_scheduler_value(
            "maintenance_notice_signatures"
        )
        previous: set[str] = set()
        if isinstance(previous_raw, list):
            previous.update(
                item
                for item in cast(list[object], previous_raw)
                if isinstance(item, str)
            )
        notices = [
            outcome
            for outcome in outcomes
            if outcome.outcome is not MaintenanceOutcomeCode.QUARANTINED
        ]
        current = {
            outcome.model_dump_json(exclude_none=True, exclude_defaults=True)
            for outcome in notices
        }
        quarantined = [
            outcome
            for outcome in outcomes
            if outcome.outcome is MaintenanceOutcomeCode.QUARANTINED
        ]
        unseen_notices = [
            outcome
            for outcome in notices
            if outcome.model_dump_json(exclude_none=True, exclude_defaults=True)
            not in previous
        ]
        selected = [*quarantined, *unseen_notices][:25]
        selected_notice_signatures = {
            outcome.model_dump_json(exclude_none=True, exclude_defaults=True)
            for outcome in selected
            if outcome.outcome is not MaintenanceOutcomeCode.QUARANTINED
        }
        next_signatures = (previous & current) | selected_notice_signatures
        return (
            selected,
            sorted(next_signatures),
        )

    async def _quarantined_account_ids(self) -> frozenset[str]:
        account_ids: set[str] = set()
        cursor: str | None = None
        for _ in range(100):
            page = await self._repository.list_account_quarantines(
                limit=100,
                cursor=cursor,
            )
            account_ids.update(item.account_id for item in page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        else:
            raise ServiceError(
                "QUARANTINE_SCAN_LIMIT_REACHED",
                "The quarantine registry exceeds the safe scan limit",
            )
        intent_ids = {
            intent.account_id
            for intent in await self._repository.list_account_quarantine_intents(
                limit=10000
            )
        }
        account_ids.update(intent_ids)
        return frozenset(account_ids)

    async def require_control_target(self, job_type: JobType) -> list[str]:
        if job_type is not JobType.MAINTENANCE:
            raise ValueError("unsupported control job type")
        targets = await self._targets_for(DeliveryPurpose.MAINTENANCE_ADMIN)
        if not targets:
            raise ServiceError(
                "MAINTENANCE_ADMIN_TARGET_REQUIRED",
                "A personal administrator delivery target is required",
            )
        return targets

    async def _targets_for(self, purpose: DeliveryPurpose) -> list[str]:
        targets = await self._repository.list_delivery_targets()
        return [
            target.delivery_target_id
            for target in targets
            if target.enabled and purpose in target.purposes
        ]

    @classmethod
    def _format_maintenance_results(
        cls,
        values: list[MaintenanceOutcome],
        *,
        triggered_at: datetime,
    ) -> str:
        blocks = [
            f"账号维护结果｜{len(values)} 个账号\n"
            f"{cls._format_trigger_time(triggered_at)}"
        ]
        displayed = values[:25]
        for index, value in enumerate(displayed, start=1):
            account_name = cls._display_text(value.account_name, "未知账号")
            account_id = cls._display_text(value.account_id, "--")
            group_name = cls._display_text(value.group_name, "未知渠道")
            if value.outcome is MaintenanceOutcomeCode.QUARANTINED:
                reason_code = value.reason
                reason = (
                    _MAINTENANCE_REASON_LABELS.get(
                        reason_code,
                        "触发账号健康规则",
                    )
                    if reason_code is not None
                    else "触发账号健康规则"
                )
                quarantine_type = (
                    "系统延迟隔离"
                    if reason_code is AccountQuarantineReason.SLOW_FIRST_TOKEN
                    else "渠道失败隔离"
                )
                blocks.append(
                    f"{index}. {account_name}（账号 #{account_id}）\n"
                    f"类型：{quarantine_type}\n"
                    f"原因：{reason}\n"
                    "结果：已隔离并关闭账号调度"
                )
            elif value.outcome is MaintenanceOutcomeCode.NO_HEALTHY_ACCOUNT:
                blocks.append(
                    f"{index}. {group_name}\n"
                    "类型：渠道无健康账号\n"
                    "结果：未自动禁用任何账号，请人工补充或检查可用账号"
                )
            elif value.outcome is MaintenanceOutcomeCode.MIN_POOL_PROTECTED:
                visible_groups = value.protected_group_ids[:5]
                protected_groups = "、".join(visible_groups) or "未解析"
                if len(value.protected_group_ids) > len(visible_groups):
                    protected_groups += (
                        f" 等 {len(value.protected_group_ids)} 个分组"
                    )
                blocks.append(
                    f"{index}. {account_name}（账号 #{account_id}）\n"
                    "类型：最小池保护\n"
                    f"保护分组：{protected_groups}\n"
                    "结果：为保证渠道至少保留 1 个可用账号，本次未禁用"
                )
            elif value.outcome is MaintenanceOutcomeCode.AMBIGUOUS_GROUP_MAPPING:
                blocks.append(
                    f"{index}. {group_name}\n"
                    "类型：渠道分组映射不明确\n"
                    "结果：安全停止，未测试或禁用账号"
                )
            elif value.outcome is MaintenanceOutcomeCode.SWEEP_LIMIT_REACHED:
                blocks.append(
                    f"{index}. {group_name}\n"
                    "类型：探测数量超过安全上限\n"
                    "结果：安全停止，未禁用账号"
                )
            else:
                blocks.append(
                    f"{index}. {account_name}（账号 #{account_id}）\n"
                    "类型：账号状态无法确认\n"
                    "结果：已停止本轮后续禁用，请人工检查"
                )
        if len(values) > len(displayed):
            blocks.append(
                f"其余 {len(values) - len(displayed)} 项已省略，请使用 MCP 隔离列表查询"
            )
        return "\n\n".join(blocks)

    @classmethod
    def _format_quarantine_probe_results(
        cls,
        values: list[dict[str, Any]],
        *,
        triggered_at: datetime,
    ) -> str:
        blocks = [
            f"隔离复测结果｜{len(values)} 个账号\n"
            f"{cls._format_trigger_time(triggered_at)}"
        ]
        for index, value in enumerate(values, start=1):
            account_id = cls._display_text(value.get("account_id"), "--")
            result = str(value.get("result") or "")
            reason = str(value.get("reason") or "")
            reason_label = (
                "首字延迟隔离"
                if reason == AccountQuarantineReason.SLOW_FIRST_TOKEN.value
                else "渠道失败隔离"
            )
            latency_value = value.get("latency_ms")
            latency = (
                f"{int(latency_value)} ms"
                if isinstance(latency_value, int) and not isinstance(latency_value, bool)
                else "未取得有效延迟"
            )
            if result == QuarantineProbeResult.RECOVERED.value:
                result_label = "恢复回池：测试及账号状态读回均成功"
            elif result == QuarantineProbeResult.SLOW.value:
                result_label = "继续隔离：首字延迟仍高于阈值"
            elif result == QuarantineProbeResult.FAILED.value:
                result_label = "继续隔离：测试或恢复验证失败"
            else:
                result_label = "继续隔离：探测结果无效，已安全停止"
            blocks.append(
                f"{index}. 账号 #{account_id}\n"
                f"类型：{reason_label}\n"
                f"首事件延迟：{latency}\n"
                f"结果：{result_label}"
            )
        return "\n\n".join(blocks)

    @staticmethod
    def _format_trigger_time(triggered_at: datetime) -> str:
        if triggered_at.tzinfo is None:
            raise ValueError("triggered_at must be timezone-aware")
        local = triggered_at.astimezone(ZoneInfo("Asia/Shanghai"))
        return f"触发时间：{local:%Y-%m-%d %H:%M:%S}（北京时间）"

    @staticmethod
    def _display_text(value: object, fallback: str) -> str:
        normalized = " ".join(str(value or "").split()).strip()
        return normalized[:200] or fallback

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._refresh_quarantine_metrics()
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(), name="sub2api-scheduler")

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                queued = await self.queue_cycle()
                self._metrics.scheduler_runs.labels(
                    status="queued" if queued else "skipped"
                ).inc()
            except Exception:
                self._metrics.scheduler_runs.labels(status="error").inc()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._policy.interval_seconds
                )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
