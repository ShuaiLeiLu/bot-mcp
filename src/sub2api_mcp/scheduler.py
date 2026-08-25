"""Durable scheduler queueing and Sub2API operation handlers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from pydantic import TypeAdapter, ValidationError

from .contracts import (
    AccountQuarantineReason,
    AccountQuarantineRecord,
    DeliveryPurpose,
    JobRecord,
    JobType,
    MaintenanceOutcome,
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
_MAINTENANCE_OUTCOME_ADAPTER = TypeAdapter(list[MaintenanceOutcome])
_RECOVERY_BUCKET_LABELS = {
    "error": "错误",
    "temporary": "临时不可调度",
    "closed": "关闭",
}
_RECOVERY_RESULT_LABELS = {
    "recovered": "已恢复正常",
    "test_failed": "测试失败，未调整",
    "recovery_failed": "测试成功，但恢复失败",
}
_LOGGER = logging.getLogger("sub2api_mcp.scheduler")


class Sub2APIOperations(Protocol):
    async def probe(self) -> ProbeResult: ...

    async def recover(self) -> list[dict[str, object]]: ...

    async def maintain(self, probe: ProbeResult) -> list[dict[str, object]]: ...

    async def probe_quarantined(
        self,
        marker: AccountQuarantineRecord,
    ) -> QuarantineProbeAttempt: ...


@dataclass(frozen=True, slots=True)
class SchedulerPolicy:
    enabled: bool = False
    interval_seconds: int = 60
    lease_seconds: int = 120
    recovery_enabled: bool = False
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
                await self._repository.publish_guardian_snapshot(
                    result.guardian_snapshot,
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

        if self._policy.recovery_enabled and await self._targets_for(
            DeliveryPurpose.RECOVERY_ADMIN
        ):
            await self._repository.create_job_with_capacity(JobType.RECOVERY, {}, max_active=1)
        maintenance_required = (
            self._policy.maintenance_enabled
            or await self._repository.account_quarantine_count() > 0
        )
        if maintenance_required and await self._targets_for(DeliveryPurpose.MAINTENANCE_ADMIN):
            await self._repository.create_job_with_capacity(JobType.MAINTENANCE, {}, max_active=1)
        return {"changed": changed, "snapshot": result.snapshot}

    async def handle_recovery(self, job: JobRecord) -> dict[str, Any]:
        del job
        targets = await self.require_control_target(JobType.RECOVERY)
        outcomes = await self._adapter.recover()
        if outcomes:
            await self._repository.enqueue_outbox(
                OutboxEventType.RECOVERY_RESULT,
                {
                    "notification": {
                        "text": self._format_recovery_results(
                            outcomes,
                            triggered_at=self._clock(),
                        )
                    }
                },
                targets,
            )
        return {"outcomes": outcomes}

    async def handle_maintenance(self, job: JobRecord) -> dict[str, Any]:
        del job
        targets = await self.require_control_target(JobType.MAINTENANCE)
        selected_markers = await self._repository.list_account_quarantines_for_probe(
            limit=5
        )
        probe = self._latest_probe or await self._adapter.probe()
        raw_outcomes = await self._adapter.maintain(probe)
        try:
            outcomes = _MAINTENANCE_OUTCOME_ADAPTER.validate_python(raw_outcomes)
        except ValidationError as exc:
            raise ServiceError(
                "MAINTENANCE_RESULT_INVALID",
                "The account maintenance result is invalid",
            ) from exc
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
            marker = AccountQuarantineRecord(
                account_id=outcome.account_id,
                reason=outcome.reason,
                group_ids=outcome.group_ids,
                threshold_ms=outcome.threshold_ms,
                observed_count=outcome.observed_count,
                quarantined_at=self._clock(),
            )
            await self._repository.upsert_account_quarantine(marker)
            self._metrics.account_quarantine_transitions.labels(
                reason=outcome.reason.value,
                action="quarantined",
            ).inc()
            adjustments.append(outcome.model_dump(mode="json", exclude_none=True))
        quarantine_probes: list[dict[str, Any]] = []
        quarantine_notifications: list[dict[str, Any]] = []
        for marker in selected_markers:
            try:
                attempt = await self._adapter.probe_quarantined(marker)
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
            if attempt.recovered:
                await self._repository.remove_verified_account_quarantine(
                    marker.account_id
                )
                self._metrics.account_quarantine_transitions.labels(
                    reason=marker.reason.value,
                    action="recovered",
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
        if outcomes or quarantine_notifications:
            notification_sections: list[str] = []
            if outcomes:
                notification_sections.append(
                    self._format_maintenance_results(
                        outcomes,
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
                    "notification": {
                        "text": "\n\n".join(notification_sections)
                    },
                },
                targets,
            )
        await self._refresh_quarantine_metrics()
        return {
            "adjustments": adjustments,
            "outcomes": [
                outcome.model_dump(mode="json", exclude_none=True)
                for outcome in outcomes
            ],
            "probes": quarantine_probes,
        }

    async def _refresh_quarantine_metrics(self) -> None:
        for reason in AccountQuarantineReason:
            count = await self._repository.account_quarantine_count(reason)
            self._metrics.account_quarantines.labels(reason=reason.value).set(count)

    async def require_control_target(self, job_type: JobType) -> list[str]:
        purpose_by_type = {
            JobType.RECOVERY: DeliveryPurpose.RECOVERY_ADMIN,
            JobType.MAINTENANCE: DeliveryPurpose.MAINTENANCE_ADMIN,
        }
        purpose = purpose_by_type.get(job_type)
        if purpose is None:
            raise ValueError("unsupported control job type")
        targets = await self._targets_for(purpose)
        if not targets:
            code = (
                "RECOVERY_ADMIN_TARGET_REQUIRED"
                if job_type is JobType.RECOVERY
                else "MAINTENANCE_ADMIN_TARGET_REQUIRED"
            )
            raise ServiceError(
                code,
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
        for index, value in enumerate(values, start=1):
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
            elif value.outcome is MaintenanceOutcomeCode.MINIMUM_POOL_PROTECTED:
                blocks.append(
                    f"{index}. {account_name}（账号 #{account_id}）\n"
                    "类型：最小池保护\n"
                    "结果：为保证渠道至少保留 1 个可用账号，本次未禁用"
                )
            elif value.outcome is MaintenanceOutcomeCode.AMBIGUOUS_GROUP_MAPPING:
                blocks.append(
                    f"{index}. {group_name}\n"
                    "类型：渠道分组映射不明确\n"
                    "结果：安全停止，未测试或禁用账号"
                )
            else:
                blocks.append(
                    f"{index}. {group_name}\n"
                    "类型：探测数量超过安全上限\n"
                    "结果：安全停止，未禁用账号"
                )
        return "\n\n".join(blocks)

    @classmethod
    def _format_recovery_results(
        cls,
        values: list[dict[str, object]],
        *,
        triggered_at: datetime,
    ) -> str:
        blocks = [
            f"账号恢复结果｜{len(values)} 个账号\n"
            f"{cls._format_trigger_time(triggered_at)}"
        ]
        for index, value in enumerate(values, start=1):
            account_name = cls._display_text(value.get("name"), "未知账号")
            account_id = cls._display_text(value.get("account_id"), "--")
            bucket = _RECOVERY_BUCKET_LABELS.get(
                str(value.get("bucket") or ""),
                "未知",
            )
            result = _RECOVERY_RESULT_LABELS.get(
                str(value.get("result") or ""),
                "处理完成",
            )
            blocks.append(
                f"{index}. {account_name}（账号 #{account_id}）\n"
                f"原状态：{bucket}\n"
                f"结果：{result}"
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
            if result == QuarantineProbeResult.SUCCESS.value:
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
