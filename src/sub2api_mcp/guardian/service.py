"""Guardian application service and safe background direct scheduler."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from ..contracts import DeliveryPurpose, JobRecord, JobType, NotificationPayload, OutboxEventType
from ..errors import ServiceError
from ..logging import log_event
from ..metrics import Metrics
from ..repository import SqliteRepository
from .account_recovery import AccountRecoveryExecutor, AccountRecoveryOperations
from .contracts import (
    AccountRecoveryOwner,
    AccountRecoveryResult,
    AccountRecoveryRunStatus,
    AccountRecoveryRunTrigger,
    ChannelPolicyOverride,
    GroupPolicyOverride,
    GuardianAccountRecoveryRecord,
    GuardianAccountRecoveryRun,
    GuardianFieldName,
    GuardianFieldOwner,
    GuardianFieldOwnership,
    GuardianPolicy,
    GuardianWriteOutcome,
    ManualControl,
)
from .engine import GuardianEngine
from .repository import GuardianRepository

_RETENTION_INTERVAL = timedelta(minutes=10)
_RETENTION_BATCH_SIZE = 20_000
_RETENTION_TOTAL_KEYS = frozenset({"processed_total", "deleted_total"})


def _sqlite_database_bytes(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm"))
        if candidate.exists()
    )


def _merge_dict(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dict(
                cast(dict[str, Any], merged[key]),
                cast(dict[str, Any], value),
            )
        else:
            merged[key] = value
    return merged


def _format_trigger_time(value: object) -> str:
    triggered_at: datetime
    if isinstance(value, datetime):
        triggered_at = value
    elif isinstance(value, str):
        try:
            triggered_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            triggered_at = datetime.now(UTC)
    else:
        triggered_at = datetime.now(UTC)
    if triggered_at.tzinfo is None:
        triggered_at = triggered_at.replace(tzinfo=UTC)
    local = triggered_at.astimezone(ZoneInfo("Asia/Shanghai"))
    return f"触发时间：{local:%Y-%m-%d %H:%M:%S}（北京时间）"


_RECOVERY_REASON_LABELS = {
    "verified": "已完成精确回读",
    "verified_enabled": "已启用并完成精确回读",
    "verified_disabled": "已禁用并完成精确回读",
    "already_enabled": "原状态已启用",
    "already_disabled": "原状态已禁用",
    "healthy_no_change": "测活通过，账号状态保持不变",
    "manual_pause": "人工暂停，保持不变",
    "expired": "账号已过期，保持不变",
    "temporary_unavailable": "临时不可调度，保持不变",
    "account_state_unavailable": "账号状态无法确认",
    "account_test_failed": "账号测试结果不确定",
    "run_stopped_after_unverified_mutation": "前项回读失败，本轮已安全停止",
}


class GuardianService:
    def __init__(
        self,
        repository: GuardianRepository,
        engine: GuardianEngine,
        metrics: Metrics | None = None,
        notification_repository: SqliteRepository | None = None,
        account_operations: AccountRecoveryOperations | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.repository = repository
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._logger = logging.getLogger("sub2api_mcp.guardian")
        self._metrics = metrics
        self._notification_repository = notification_repository
        self._clock = clock or (lambda: datetime.now(UTC))
        self._account_recovery = (
            AccountRecoveryExecutor(repository, account_operations, clock=self._clock)
            if account_operations is not None
            else None
        )
        self._notified_account_recovery_run_ids: set[str] = set()
        self._metered_account_recovery_run_ids: set[str] = set()
        self._last_retention_at: datetime | None = None
        self._recovery_metric_requests = 0
        self._recovery_metric_tokens = 0
        self._recovery_metric_blocked = 0

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run_loop(), name="guardian-scheduler")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        await self._task
        self._task = None

    async def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                policy = await self.repository.get_policy()
                await asyncio.wait_for(self._stop.wait(), timeout=policy.scan_interval_seconds)
                continue
            except TimeoutError:
                pass
            if self._stop.is_set():
                break
            await self._run_retention_if_due(now=datetime.now(UTC))
            try:
                policy = await self.repository.get_policy()
                if policy.enabled:
                    slot = int(datetime.now(UTC).timestamp()) // policy.scan_interval_seconds
                    await self.run_once(dry_run=False, idempotency_key=f"scheduled:{slot}")
            except Exception:
                self._logger.exception("guardian_scheduled_cycle_failed")

    async def get_policy(self) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        return {
            "policy": policy.model_dump(mode="json"),
            "defaults": GuardianPolicy().model_dump(mode="json"),
            "scheduling_enabled": policy.enabled,
        }

    async def update_policy(
        self, patch: dict[str, Any], *, expected_revision: int
    ) -> dict[str, Any]:
        deprecated = {"observe_only", "auto_apply", "rollout"}.intersection(patch)
        if deprecated:
            raise ServiceError(
                "DEPRECATED_GUARDIAN_CONTROL",
                "Observe mode and rollout controls were removed; update enabled instead",
            )
        current = await self.repository.get_policy()
        merged = _merge_dict(current.model_dump(mode="json"), patch)
        merged["revision"] = current.revision
        try:
            candidate = GuardianPolicy.model_validate(merged)
        except ValidationError:
            raise
        saved = await self.repository.update_policy(candidate, expected_revision=expected_revision)
        await self.repository.add_event(
            event_type="POLICY_UPDATED",
            severity="INFO",
            message=f"Guardian policy revision {saved.revision} saved",
            details={"revision": saved.revision},
        )
        return {
            "policy": saved.model_dump(mode="json"),
            "scheduling_enabled": saved.enabled,
        }

    async def overview(self) -> dict[str, Any]:
        return await self.repository.overview()

    async def status(self) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        runs = await self.repository.list_runs(limit=1)
        return {
            "enabled": policy.enabled,
            "scheduling_mode": policy.scheduling_mode.value,
            "background_task_running": self._task is not None and not self._task.done(),
            "scan_interval_seconds": policy.scan_interval_seconds,
            "last_run": runs[0] if runs else None,
            "writeback_adapter": "verified_account_fields",
        }

    async def set_scheduling_enabled(
        self,
        *,
        enabled: bool,
        confirm: bool,
        expected_revision: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not confirm:
            raise ServiceError(
                "CONFIRMATION_REQUIRED",
                "Scheduling start or stop requires confirm=true",
            )
        if not 1 <= len(idempotency_key) <= 128:
            raise ServiceError(
                "IDEMPOTENCY_KEY_REQUIRED",
                "A bounded idempotency key is required",
            )
        subject = "enabled" if enabled else "disabled"
        saved = await self.repository.get_idempotent_result(
            idempotency_key,
            "guardian_set_scheduling",
            subject,
        )
        if saved is not None:
            return saved
        current = await self.repository.get_policy()
        updated = current.model_copy(update={"enabled": enabled})
        policy = await self.repository.update_policy(
            updated,
            expected_revision=expected_revision,
        )
        result = {
            "enabled": policy.enabled,
            "scheduling_mode": policy.scheduling_mode.value,
            "policy_revision": policy.revision,
        }
        await self.repository.add_event(
            event_type="SCHEDULING_STARTED" if enabled else "SCHEDULING_STOPPED",
            severity="WARNING",
            message=(
                "Guardian direct scheduling started"
                if enabled
                else "Guardian direct scheduling stopped"
            ),
            details={"enabled": enabled, "revision": policy.revision},
        )
        await self.repository.save_idempotent_result(
            idempotency_key,
            "guardian_set_scheduling",
            subject,
            result,
        )
        await self._notify_control_event(
            "Guardian 直接调度已启动" if enabled else "Guardian 直接调度已停止",
            [
                f"状态：{'启用' if enabled else '停止'}",
                f"策略版本：{policy.revision}",
                "写入模式：逐账号单字段写入并精确回读",
                "原因：管理员显式确认",
            ],
            coalesce_key="guardian:scheduling",
        )
        return result

    async def recovery_status(self, *, limit: int = 20) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ServiceError("VALIDATION_ERROR", "limit must be between 1 and 100")
        policy = await self.repository.get_policy()
        episodes = await self.repository.list_open_channel_error_episodes(limit=limit)
        runs = await self.repository.list_account_recovery_runs(limit=limit)
        latest_active_check = await self.repository.latest_account_recovery_run(
            AccountRecoveryRunTrigger.HOURLY_ACTIVE_CHECK
        )
        return {
            "enabled": policy.enabled,
            "owner": policy.account_recovery.owner.value,
            "trigger": policy.account_recovery.trigger.value,
            "retry_cooldown_seconds": policy.account_recovery.retry_cooldown_seconds,
            "active_check": {
                "enabled": policy.probe.enabled,
                "interval_seconds": policy.probe.interval_seconds,
                "template": "default_model_hi",
                "last_run_at": (
                    latest_active_check.started_at.isoformat()
                    if latest_active_check is not None
                    else None
                ),
            },
            "latest_abnormal_snapshot": (
                await self.repository.latest_abnormal_account_snapshot()
            ),
            "open_episodes": [
                {
                    "episode_id": item.episode_id,
                    "channel_id": item.channel_id,
                    "group_id": item.group_id,
                    "opened_snapshot_id": item.opened_snapshot_id,
                    "opened_at": item.opened_at.isoformat(),
                }
                for item in episodes
            ],
            "recent_runs": [item.model_dump(mode="json") for item in runs],
        }

    async def submit_pending_recovery(
        self,
        *,
        confirm: bool,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not confirm:
            raise ServiceError(
                "CONFIRMATION_REQUIRED",
                "Manual account recovery requires confirm=true",
            )
        if not 1 <= len(idempotency_key) <= 128:
            raise ServiceError(
                "IDEMPOTENCY_KEY_REQUIRED",
                "A bounded idempotency key is required",
            )
        saved = await self.repository.get_idempotent_result(
            idempotency_key,
            "guardian_submit_recovery",
            "pending",
        )
        if saved is not None:
            return saved
        if self._notification_repository is None:
            raise ServiceError(
                "ACCOUNT_RECOVERY_ADAPTER_UNAVAILABLE",
                "Durable recovery jobs are unavailable",
            )
        payload = await self.prepare_recovery_job()
        created = await self._notification_repository.create_job_with_capacity(
            JobType.RECOVERY,
            payload,
            max_active=1,
        )
        if created is None:
            raise ServiceError(
                "JOB_ALREADY_ACTIVE",
                "A recovery job is already active",
            )
        job, queue_count = created
        result = {
            "job": job.model_dump(mode="json"),
            "queue_count": queue_count,
        }
        await self.repository.save_idempotent_result(
            idempotency_key,
            "guardian_submit_recovery",
            "pending",
            result,
        )
        return result

    async def run_once(
        self, *, dry_run: bool, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        started = time.monotonic()
        mode = "dry_run" if dry_run else "requested_apply"
        status = "failed"
        try:
            result = await self.engine.run_once(dry_run=dry_run, idempotency_key=idempotency_key)
            status = str(result.get("status", "unknown")).casefold()
            run_result = cast(dict[str, Any], result.get("result") or {})
            if (
                status == "succeeded"
                and not result.get("idempotent_replay")
                and int(run_result.get("state_transitions", 0)) > 0
            ):
                await self._notify_run(result)
            await self._run_conditional_account_recovery(result)
            await self._after_run_operations(result)
            return result
        finally:
            if self._metrics is not None:
                self._metrics.guardian_runs.labels(status=status, mode=mode).inc()
                self._metrics.guardian_duration.labels(mode=mode).observe(
                    time.monotonic() - started
                )

    async def execute_account_recovery(
        self,
        *,
        snapshot_id: str,
        trigger: AccountRecoveryRunTrigger,
        episode_id: str | None = None,
        channel_id: str | None = None,
        group_id: str | None = None,
        already_processed_account_ids: frozenset[str] = frozenset(),
    ) -> GuardianAccountRecoveryRun:
        if self._account_recovery is None:
            raise ServiceError(
                "ACCOUNT_RECOVERY_ADAPTER_UNAVAILABLE",
                "Guardian account recovery operations are unavailable",
            )
        policy = await self.repository.get_policy()
        if not policy.enabled:
            raise ServiceError(
                "GUARDIAN_DISABLED",
                "Guardian scheduling is disabled",
            )
        monitored_group_ids = await self.repository.monitored_group_ids_for_snapshot(
            snapshot_id
        )
        if monitored_group_ids is None:
            raise ServiceError(
                "GUARDIAN_MONITORED_SCOPE_UNAVAILABLE",
                "The monitored channel scope is unavailable for this account snapshot",
            )
        run = await self._account_recovery.execute(
            snapshot_id=snapshot_id,
            trigger=trigger,
            policy=policy.account_recovery,
            policy_revision=policy.revision,
            monitored_group_ids=monitored_group_ids,
            episode_id=episode_id,
            channel_id=channel_id,
            group_id=group_id,
            quarantined_account_ids=await self._quarantined_account_ids(),
            already_processed_account_ids=already_processed_account_ids,
            probe_interval_seconds=policy.probe.interval_seconds,
        )
        records = await self.repository.list_account_recovery_results(run.run_id)
        if run.status is not AccountRecoveryRunStatus.RUNNING:
            if (
                self._metrics is not None
                and run.run_id not in self._metered_account_recovery_run_ids
            ):
                allowed_results = {item.value for item in AccountRecoveryResult}
                for record in records:
                    if (
                        run.trigger is AccountRecoveryRunTrigger.HOURLY_ACTIVE_CHECK
                        and record.reason == "healthy_no_change"
                    ):
                        continue
                    if record.result.value in allowed_results:
                        self._metrics.guardian_account_recovery_results.labels(
                            result=record.result.value
                        ).inc()
                self._metered_account_recovery_run_ids.add(run.run_id)
            await self._notify_account_recovery(run, records)
        return run

    async def prepare_recovery_job(self) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        if (
            not policy.enabled
            or not policy.account_recovery.enabled
            or policy.account_recovery.owner is not AccountRecoveryOwner.GUARDIAN
        ):
            raise ServiceError(
                "ACCOUNT_RECOVERY_NOT_GUARDIAN_OWNED",
                "Guardian account recovery is not enabled and owned by Guardian",
            )
        await self._require_recovery_admin_target()
        snapshot_id = await self.repository.latest_abnormal_account_snapshot()
        if snapshot_id is not None:
            return {
                "snapshot_id": snapshot_id,
                "trigger": AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE.value,
            }
        episode = await self.repository.latest_open_channel_error_episode()
        if episode is not None and episode.group_id is not None:
            return {
                "snapshot_id": episode.opened_snapshot_id,
                "trigger": AccountRecoveryRunTrigger.CHANNEL_ERROR.value,
                "episode_id": episode.episode_id,
                "channel_id": episode.channel_id,
                "group_id": episode.group_id,
            }
        raise ServiceError(
            "NO_ABNORMAL_ACCOUNT_SNAPSHOT",
            "The latest account snapshot has no recoverable abnormal accounts",
        )

    async def handle_recovery(self, job: JobRecord) -> dict[str, Any]:
        if job.job_type is not JobType.RECOVERY:
            raise ValueError("Guardian can only handle recovery jobs")
        await self._require_recovery_admin_target()
        payload = job.payload
        raw_snapshot_id = payload.get("snapshot_id")
        raw_trigger = payload.get("trigger")
        if (
            not isinstance(raw_snapshot_id, str)
            or len(raw_snapshot_id) != 64
            or any(character not in "0123456789abcdef" for character in raw_snapshot_id)
            or not isinstance(raw_trigger, str)
        ):
            raise ServiceError("INVALID_RECOVERY_JOB", "The recovery job payload is invalid")
        try:
            trigger = AccountRecoveryRunTrigger(raw_trigger)
        except ValueError as exc:
            raise ServiceError(
                "INVALID_RECOVERY_JOB",
                "The recovery job trigger is invalid",
            ) from exc
        if trigger is AccountRecoveryRunTrigger.MANUAL:
            raise ServiceError(
                "INVALID_RECOVERY_JOB",
                "Manual recovery jobs must resolve to conditional evidence first",
            )
        episode_id = payload.get("episode_id")
        channel_id = payload.get("channel_id")
        group_id = payload.get("group_id")
        if trigger is AccountRecoveryRunTrigger.CHANNEL_ERROR and not all(
            isinstance(value, str) and value
            for value in (episode_id, channel_id, group_id)
        ):
            raise ServiceError(
                "INVALID_RECOVERY_JOB",
                "The channel-error recovery job is incomplete",
            )
        run = await self.execute_account_recovery(
            snapshot_id=raw_snapshot_id,
            trigger=trigger,
            episode_id=cast(str | None, episode_id),
            channel_id=cast(str | None, channel_id),
            group_id=cast(str | None, group_id),
        )
        return {"recovery_run": run.model_dump(mode="json")}

    async def _require_recovery_admin_target(self) -> list[str]:
        if self._notification_repository is None:
            raise ServiceError(
                "RECOVERY_ADMIN_TARGET_REQUIRED",
                "A personal administrator delivery target is required",
            )
        targets = [
            target.delivery_target_id
            for target in await self._notification_repository.list_delivery_targets()
            if target.enabled and DeliveryPurpose.RECOVERY_ADMIN in target.purposes
        ]
        if not targets:
            raise ServiceError(
                "RECOVERY_ADMIN_TARGET_REQUIRED",
                "A personal administrator delivery target is required",
            )
        return targets

    async def _run_conditional_account_recovery(
        self,
        guardian_run: dict[str, Any],
    ) -> None:
        if self._account_recovery is None or guardian_run.get("status") != "SUCCEEDED":
            return
        result = cast(dict[str, Any], guardian_run.get("result") or {})
        snapshot_id = result.get("snapshot_id")
        if not isinstance(snapshot_id, str):
            return
        policy = await self.repository.get_policy()
        if (
            not policy.enabled
            or not policy.account_recovery.enabled
            or policy.account_recovery.owner is not AccountRecoveryOwner.GUARDIAN
        ):
            return
        processed: set[str] = set()
        completed_runs: list[GuardianAccountRecoveryRun] = []
        raw_value = result.get("account_recovery_triggers")
        raw_triggers = cast(list[object], raw_value) if isinstance(raw_value, list) else []
        for raw_value in raw_triggers:
            if not isinstance(raw_value, dict):
                continue
            raw = cast(dict[str, object], raw_value)
            episode_id = raw.get("episode_id")
            channel_id = raw.get("channel_id")
            group_id = raw.get("group_id")
            if not all(isinstance(value, str) for value in (episode_id, channel_id, group_id)):
                continue
            run = await self.execute_account_recovery(
                snapshot_id=snapshot_id,
                trigger=AccountRecoveryRunTrigger.CHANNEL_ERROR,
                episode_id=cast(str, episode_id),
                channel_id=cast(str, channel_id),
                group_id=cast(str, group_id),
                already_processed_account_ids=frozenset(processed),
            )
            completed_runs.append(run)
            records = await self.repository.list_account_recovery_results(run.run_id)
            processed.update(item.account_id for item in records if item.tested)
        bad_state_run = await self.execute_account_recovery(
            snapshot_id=snapshot_id,
            trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
            already_processed_account_ids=frozenset(processed),
        )
        completed_runs.append(bad_state_run)
        bad_state_records = await self.repository.list_account_recovery_results(
            bad_state_run.run_id
        )
        processed.update(item.account_id for item in bad_state_records if item.tested)
        if policy.probe.enabled and await self._hourly_active_check_due(
            interval_seconds=policy.probe.interval_seconds
        ):
            completed_runs.append(
                await self.execute_account_recovery(
                    snapshot_id=snapshot_id,
                    trigger=AccountRecoveryRunTrigger.HOURLY_ACTIVE_CHECK,
                    already_processed_account_ids=frozenset(processed),
                )
            )
        result["account_recovery_runs"] = [
            item.model_dump(mode="json") for item in completed_runs
        ]

    async def _hourly_active_check_due(self, *, interval_seconds: int) -> bool:
        latest = await self.repository.latest_account_recovery_run(
            AccountRecoveryRunTrigger.HOURLY_ACTIVE_CHECK
        )
        if latest is None:
            return True
        if latest.status in {
            AccountRecoveryRunStatus.FAILED,
            AccountRecoveryRunStatus.INTERRUPTED,
        }:
            return True
        if latest.status is AccountRecoveryRunStatus.RUNNING:
            return False
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Guardian clock must be timezone-aware")
        return (now.astimezone(UTC) - latest.started_at).total_seconds() >= interval_seconds

    async def _quarantined_account_ids(self) -> frozenset[str]:
        if self._notification_repository is None:
            return frozenset()
        account_ids: set[str] = set()
        cursor: str | None = None
        for _ in range(100):
            page = await self._notification_repository.list_account_quarantines(
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
        intents = await self._notification_repository.list_account_quarantine_intents(
            limit=10_000
        )
        account_ids.update(item.account_id for item in intents)
        return frozenset(account_ids)

    async def _notify_account_recovery(
        self,
        run: GuardianAccountRecoveryRun,
        records: list[GuardianAccountRecoveryRecord],
    ) -> bool:
        if run.trigger is AccountRecoveryRunTrigger.HOURLY_ACTIVE_CHECK:
            records = [
                item
                for item in records
                if not (
                    item.result is AccountRecoveryResult.ENABLED
                    and item.reason in {"already_enabled", "healthy_no_change"}
                )
            ]
        if (
            self._notification_repository is None
            or run.run_id in self._notified_account_recovery_run_ids
            or (run.status is AccountRecoveryRunStatus.SUCCEEDED and not records)
        ):
            return False
        try:
            targets = [
                target.delivery_target_id
                for target in await self._notification_repository.list_delivery_targets()
                if target.enabled and DeliveryPurpose.RECOVERY_ADMIN in target.purposes
            ]
            if not targets:
                return False
            result_labels = {
                "ENABLED": "已启用（已回读确认）",
                "DISABLED": "已禁用（已回读确认）",
                "INDETERMINATE": "状态不确定，未改动",
                "SKIPPED": "已跳过",
            }
            details = [
                (
                    f"{index}. 账号 #{item.account_id}｜"
                    f"渠道 {item.channel_id or '快照'}｜分组 {item.group_id or '未分组'}｜"
                    f"{result_labels[item.result.value]}｜"
                    f"{_RECOVERY_REASON_LABELS.get(item.reason, '已记录安全结果')}"
                )
                for index, item in enumerate(records[:30], start=1)
            ]
            if len(records) > len(details):
                details.append(f"其余 {len(records) - len(details)} 项已写入 Guardian 账本")
            summary = run.result or {}
            notification = NotificationPayload(
                text="\n".join(
                    [
                        "Guardian 账号恢复结果",
                        _format_trigger_time(run.finished_at or run.started_at),
                        f"触发：{run.trigger.value}",
                        f"已测试：{summary.get('tested', 0)}｜启用：{summary.get('enabled', 0)}｜"
                        f"禁用：{summary.get('disabled', 0)}｜不确定："
                        f"{summary.get('indeterminate', 0)}｜跳过：{summary.get('skipped', 0)}",
                        *details,
                    ]
                )
            )
            await self._notification_repository.enqueue_outbox(
                OutboxEventType.RECOVERY_RESULT,
                {
                    "dedupKey": f"guardian:account-recovery:{run.run_id}",
                    "coalesceKey": "guardian:account-recovery",
                    "notification": notification.model_dump(mode="json", exclude_none=True),
                },
                targets,
            )
            self._notified_account_recovery_run_ids.add(run.run_id)
            return True
        except Exception:
            self._logger.exception(
                "guardian_account_recovery_notification_enqueue_failed",
                extra={"recoveryRunId": run.run_id},
            )
            return False

    async def _after_run_operations(self, run: dict[str, Any]) -> None:
        try:
            await self._refresh_v2_metrics(run)
            await self._check_recovery_budget_alert()
            await self._run_retention_if_due(now=datetime.now(UTC))
        except Exception:
            self._logger.exception(
                "guardian_post_run_operations_failed",
                extra={"runId": run.get("run_id")},
            )

    async def _run_retention_if_due(self, *, now: datetime) -> None:
        if now.tzinfo is None:
            raise ValueError("retention schedule time must be timezone-aware")
        if (
            self._last_retention_at is not None
            and now - self._last_retention_at < _RETENTION_INTERVAL
        ):
            return
        self._last_retention_at = now
        started = time.monotonic()
        stores: tuple[tuple[str, GuardianRepository | SqliteRepository], ...] = (
            (("guardian", self.repository),)
            if self._notification_repository is None
            else (
                ("guardian", self.repository),
                ("primary", self._notification_repository),
            )
        )
        processed_total = 0
        deleted_total = 0
        try:
            for store, repository in stores:
                result = await repository.cleanup_retention(
                    now=now,
                    batch_size=_RETENTION_BATCH_SIZE,
                )
                processed_total += int(result.get("processed_total", 0))
                deleted_total += int(result.get("deleted_total", 0))
                if self._metrics is not None:
                    for operation, raw_count in result.items():
                        if operation in _RETENTION_TOTAL_KEYS:
                            continue
                        count = int(raw_count)
                        if count > 0:
                            self._metrics.retention_rows.labels(
                                store=store,
                                operation=operation,
                            ).inc(count)
            database_bytes = _sqlite_database_bytes(self.repository.path)
            if self._metrics is not None:
                self._metrics.retention_runs.labels(status="success").inc()
                self._metrics.database_size_bytes.set(database_bytes)
            log_event(
                self._logger,
                logging.INFO,
                "guardian_retention_completed",
                "scheduled retention completed",
                processedRows=processed_total,
                deletedRows=deleted_total,
                databaseBytes=database_bytes,
                durationMs=round((time.monotonic() - started) * 1000),
            )
        except Exception:
            if self._metrics is not None:
                self._metrics.retention_runs.labels(status="failed").inc()
            self._logger.exception(
                "scheduled retention failed",
                extra={
                    "event": "guardian_retention_failed",
                    "durationMs": round((time.monotonic() - started) * 1000),
                },
            )

    async def _refresh_v2_metrics(self, run: dict[str, Any]) -> None:
        if self._metrics is None:
            return
        result = cast(dict[str, Any], run.get("result") or {})
        replayed = bool(run.get("idempotent_replay"))
        if result.get("snapshot_id") and not replayed:
            self._metrics.guardian_shared_snapshots.labels(status="consumed").inc()
        elif result.get("no_new_evidence") and not replayed:
            self._metrics.guardian_shared_snapshots.labels(status="empty").inc()
        reason = str(result.get("writeback_blocked_reason") or "")
        if reason and not replayed:
            self._metrics.guardian_write_frozen.labels(reason=reason).inc()
        raw_field_outcomes = result.get("writeback_field_outcomes")
        field_outcomes = (
            cast(dict[str, object], raw_field_outcomes)
            if isinstance(raw_field_outcomes, dict)
            else {}
        )
        for field_name in GuardianFieldName:
            raw_outcomes = field_outcomes.get(field_name.value)
            outcomes = (
                cast(dict[str, object], raw_outcomes)
                if isinstance(raw_outcomes, dict)
                else {}
            )
            for outcome in (
                GuardianWriteOutcome.APPLIED,
                GuardianWriteOutcome.BLOCKED,
                GuardianWriteOutcome.FAILED,
                GuardianWriteOutcome.NO_CHANGE,
            ):
                count = outcomes.get(outcome.value)
                if (
                    not replayed
                    and isinstance(count, int)
                    and not isinstance(count, bool)
                    and count > 0
                ):
                    self._metrics.guardian_scheduling_writes.labels(
                        field=field_name.value,
                        outcome=outcome.value,
                    ).inc(count)
        duplicates = int(result.get("duplicate_observations") or 0)
        if duplicates and not replayed:
            self._metrics.guardian_duplicate_observations.labels(
                source="SHARED_MONITOR"
            ).inc(duplicates)
        traffic_processed = int(result.get("traffic_buckets_processed") or 0)
        if traffic_processed and not replayed:
            self._metrics.guardian_traffic_buckets.labels(status="fused").inc(
                traffic_processed
            )
        sampling = await self.repository.sampling_status()
        latest = sampling.get("latest_snapshot_at")
        if latest:
            captured = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
            if captured.tzinfo is None:
                captured = captured.replace(tzinfo=UTC)
            self._metrics.guardian_snapshot_age_seconds.set(
                max(0, (datetime.now(UTC) - captured).total_seconds())
            )
        for state, count in cast(
            dict[str, int], sampling.get("channels_by_freshness") or {}
        ).items():
            self._metrics.guardian_channels_by_freshness.labels(state=state).set(count)
        channels = await self.repository.list_channels(limit=200)
        confidence_values = [
            float(channel.get("confidence") or 0)
            for channel in cast(list[dict[str, Any]], channels.get("items") or [])
        ]
        self._metrics.guardian_channel_confidence_min.set(
            min(confidence_values, default=0)
        )
        self._metrics.guardian_channel_confidence_average.set(
            sum(confidence_values) / len(confidence_values)
            if confidence_values
            else 0
        )
        budget = await self.probe_budget()
        requests = int(budget["request_count"])
        tokens = int(budget["total_tokens"])
        blocked = int(budget["blocked_count"])
        if requests > self._recovery_metric_requests:
            self._metrics.guardian_recovery_probe_requests.labels(
                result="completed"
            ).inc(requests - self._recovery_metric_requests)
        if blocked > self._recovery_metric_blocked:
            self._metrics.guardian_recovery_probe_requests.labels(result="blocked").inc(
                blocked - self._recovery_metric_blocked
            )
        if tokens > self._recovery_metric_tokens:
            self._metrics.guardian_recovery_probe_tokens.labels(priced="unknown").inc(
                tokens - self._recovery_metric_tokens
            )
        self._recovery_metric_requests = requests
        self._recovery_metric_tokens = tokens
        self._recovery_metric_blocked = blocked

    async def _check_recovery_budget_alert(self) -> None:
        policy = await self.repository.get_policy()
        if not policy.recovery_budget.enabled:
            return
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        usage = await self.repository.recovery_probe_budget_summary(now.date())
        request_ratio = float(usage["request_count"]) / policy.recovery_budget.daily_requests
        token_ratio = float(usage["total_tokens"]) / policy.recovery_budget.daily_tokens
        ratio = max(request_ratio, token_ratio)
        if ratio < 0.8:
            return
        exhausted = ratio >= 1
        event_type = (
            "RECOVERY_BUDGET_EXHAUSTED" if exhausted else "RECOVERY_BUDGET_WARNING"
        )
        existing = await self.repository.list_events(limit=20, event_type=event_type)
        for event in cast(list[dict[str, Any]], existing.get("items") or []):
            created = datetime.fromisoformat(str(event["created_at"]).replace("Z", "+00:00"))
            if created.astimezone(ZoneInfo("Asia/Shanghai")).date() == now.date():
                return
        await self.repository.add_event(
            event_type=event_type,
            severity="ERROR" if exhausted else "WARNING",
            message=(
                "Guardian recovery probe budget exhausted"
                if exhausted
                else "Guardian recovery probe budget reached 80 percent"
            ),
            details={
                "request_count": usage["request_count"],
                "daily_requests": policy.recovery_budget.daily_requests,
                "total_tokens": usage["total_tokens"],
                "daily_tokens": policy.recovery_budget.daily_tokens,
            },
        )
        await self._notify_control_event(
            "Guardian 恢复探测预算告警",
            [
                f"状态：{'已耗尽' if exhausted else '已达到 80%'}",
                f"请求：{usage['request_count']} / {policy.recovery_budget.daily_requests}",
                f"Token：{usage['total_tokens']} / {policy.recovery_budget.daily_tokens}",
                "目标动作：停止新增恢复探测" if exhausted else "目标动作：继续监控预算",
                "实际写回：否",
                "原因：恢复探测硬预算保护",
            ],
            coalesce_key=f"guardian:budget:{now.date().isoformat()}",
        )

    async def _notify_run(self, run: dict[str, Any]) -> None:
        if self._notification_repository is None:
            return
        try:
            targets = [
                target.delivery_target_id
                for target in await self._notification_repository.list_delivery_targets()
                if target.enabled and DeliveryPurpose.STATUS in target.purposes
            ]
            if not targets:
                return
            result = cast(dict[str, Any], run.get("result") or {})
            transition_items = cast(
                list[dict[str, Any]], result.get("transitions") or []
            )
            transition_lines: list[str] = []
            for index, item in enumerate(transition_items[:10], start=1):
                sources = cast(list[object], item.get("evidence_sources") or [])
                source_text = "、".join(str(value) for value in sources) or "暂无"
                age = int(item.get("evidence_age_seconds") or 0)
                transition_lines.extend(
                    [
                        "",
                        f"{index}. {item.get('name') or item.get('channel_id') or '未知渠道'}"
                        f"（分组 {item.get('group_id') or '未分组'}）",
                        f"   状态：{item.get('from', '—')} → {item.get('to', '—')}",
                        f"   健康分：{float(item.get('score', 0)):.1f}｜"
                        f"置信度：{float(item.get('confidence', 0)) * 100:.0f}%",
                        f"   证据来源：{source_text}｜证据年龄：{age} 秒",
                        f"   目标动作：{item.get('action', 'NO_CHANGE')}｜"
                        f"实际写回：{'是' if item.get('writes_applied') else '否'}",
                        f"   探测：{item.get('event_type', '—')}｜"
                        f"原因：{item.get('reason', '—')}",
                    ]
                )
            detail_lines = (
                ["", "渠道变化：", *transition_lines] if transition_lines else []
            )
            notification = NotificationPayload(
                text="\n".join(
                    [
                        "Guardian 调度状态更新",
                        _format_trigger_time(run.get("started_at")),
                        f"评估渠道：{result.get('channels_evaluated', 0)}",
                        f"状态变化：{result.get('state_transitions', 0)}",
                        f"预期差异：{result.get('expected_changes', 0)}",
                        f"实际写入：{result.get('writes_applied', 0)}",
                        *detail_lines,
                    ]
                )
            )
            await self._notification_repository.enqueue_outbox(
                OutboxEventType.STATUS_CHANGED,
                {
                    "coalesceKey": "guardian:run",
                    "notification": notification.model_dump(mode="json", exclude_none=True),
                },
                targets,
            )
        except Exception:
            self._logger.exception(
                "guardian_notification_enqueue_failed",
                extra={"runId": run.get("run_id")},
            )

    async def _notify_control_event(
        self,
        title: str,
        lines: list[str],
        *,
        coalesce_key: str | None = None,
    ) -> None:
        if self._notification_repository is None:
            return
        try:
            targets = [
                target.delivery_target_id
                for target in await self._notification_repository.list_delivery_targets()
                if target.enabled and DeliveryPurpose.STATUS in target.purposes
            ]
            if not targets:
                return
            notification = NotificationPayload(
                text="\n".join([title, _format_trigger_time(datetime.now(UTC)), *lines])
            )
            await self._notification_repository.enqueue_outbox(
                OutboxEventType.STATUS_CHANGED,
                {
                    "coalesceKey": coalesce_key or f"guardian:control:{title}",
                    "notification": notification.model_dump(mode="json", exclude_none=True),
                },
                targets,
            )
        except Exception:
            self._logger.exception("guardian_control_notification_enqueue_failed")

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        return await self.repository.cancel_run(run_id)

    async def list_groups(self) -> dict[str, Any]:
        return {"items": await self.repository.list_groups()}

    async def update_group_policy(self, group_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        validated = GroupPolicyOverride.model_validate(patch)
        data = validated.model_dump(mode="json", exclude_none=True)
        saved = await self.repository.upsert_group_override(group_id, data)
        await self.repository.add_event(
            event_type="GROUP_POLICY_UPDATED",
            severity="INFO",
            group_id=group_id,
            message=f"Group {group_id} override updated",
            details=data,
        )
        return saved

    async def delete_group_policy(self, group_id: str) -> dict[str, bool]:
        await self.repository.delete_group_override(group_id)
        await self.repository.add_event(
            event_type="GROUP_POLICY_CLEARED",
            severity="INFO",
            group_id=group_id,
            message=f"Group {group_id} now inherits the global policy",
        )
        return {"deleted": True}

    async def list_channels(
        self,
        *,
        limit: int,
        cursor: str | None,
        group_id: str | None,
        health: str | None,
        query: str | None,
    ) -> dict[str, Any]:
        return await self.repository.list_channels(
            limit=limit,
            cursor=cursor,
            group_id=group_id,
            health=health,
            query=query,
        )

    async def get_channel(self, channel_id: str) -> dict[str, Any]:
        channel = await self.repository.get_channel(channel_id)
        if channel is None:
            raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
        channel["samples"] = [
            sample.model_dump(mode="json")
            for sample in await self.repository.list_samples(channel_id, limit=60)
        ]
        return channel

    async def update_channel(self, channel_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        channel = await self.repository.get_channel(channel_id)
        if channel is None:
            raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
        current = await self.repository.get_channel_override(channel_id)
        base = (
            current.model_dump(mode="json")
            if current is not None
            else ChannelPolicyOverride().model_dump(mode="json")
        )
        candidate = ChannelPolicyOverride.model_validate({**base, **patch})
        await self.repository.upsert_channel_override(channel_id, candidate)
        await self.repository.add_event(
            event_type="CHANNEL_OVERRIDE_UPDATED",
            severity="INFO",
            channel_id=channel_id,
            group_id=cast(str | None, channel["group_id"]),
            message="Channel scheduling override updated",
            details={"fields": sorted(patch)},
        )
        saved = await self.repository.get_channel(channel_id)
        assert saved is not None
        return saved

    async def channel_action(
        self,
        channel_id: str,
        action: str,
        *,
        idempotency_key: str | None = None,
        minutes: int | None = None,
    ) -> dict[str, Any]:
        normalized = action.strip().casefold()
        controls = {
            "pause": ManualControl.PAUSED,
            "resume": ManualControl.NONE,
            "exclude": ManualControl.EXCLUDED,
            "include": ManualControl.NONE,
            "fuse": ManualControl.FUSED,
            "recover": ManualControl.NONE,
        }
        if normalized == "probe":
            return await self.run_once(
                dry_run=True,
                idempotency_key=idempotency_key or f"probe:{channel_id}",
            )
        if normalized in {"boost", "unboost"}:
            channel = await self.repository.get_channel(channel_id)
            if channel is None:
                raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
            current = await self.repository.get_channel_override(channel_id)
            current = current or ChannelPolicyOverride()
            if normalized == "boost":
                duration = minutes if minutes is not None else 30
                if not 1 <= duration <= 10_080:
                    raise ServiceError(
                        "VALIDATION_ERROR", "Boost duration must be 1 to 10080 minutes"
                    )
                updated = current.model_copy(
                    update={
                        "boost_until": datetime.now(UTC) + timedelta(minutes=duration),
                        "boost_load_delta": 1000,
                    }
                )
            else:
                updated = current.model_copy(update={"boost_until": None, "boost_load_delta": None})
            await self.repository.upsert_channel_override(channel_id, updated)
            await self.repository.add_event(
                event_type=f"CHANNEL_{normalized.upper()}",
                severity="INFO",
                channel_id=channel_id,
                group_id=cast(str | None, channel["group_id"]),
                message=f"Temporary channel boost action: {normalized}",
                details={"minutes": minutes if normalized == "boost" else None},
            )
            saved = await self.repository.get_channel(channel_id)
            assert saved is not None
            return saved
        if normalized not in controls:
            raise ServiceError("INVALID_CHANNEL_ACTION", "The channel action is invalid")
        channel = await self.repository.set_manual_control(channel_id, controls[normalized])
        await self.repository.add_event(
            event_type=f"CHANNEL_{normalized.upper()}",
            severity="WARNING" if normalized in {"pause", "exclude", "fuse"} else "INFO",
            channel_id=channel_id,
            group_id=cast(str | None, channel["group_id"]),
            message=f"Manual channel action: {normalized}",
            details={"idempotency_key_present": bool(idempotency_key)},
        )
        return channel

    async def live_routing(self) -> dict[str, Any]:
        page = await self.repository.list_channels(limit=200)
        items = cast(list[dict[str, Any]], page["items"])
        return {
            "items": [
                {
                    "channel_id": item["channel_id"],
                    "name": item["name"],
                    "group_id": item["group_id"],
                    "health": item["health"],
                    "score": item["score"],
                    "upstream_schedulable": item["upstream_schedulable"],
                    "desired_schedulable": item["desired_schedulable"],
                    "expected_action": item["details"].get("expected_action"),
                    "candidate_weight": item["details"].get("candidate_weight"),
                }
                for item in items
            ]
        }

    async def list_events(
        self,
        *,
        limit: int,
        cursor: str | None,
        event_type: str | None,
        severity: str | None,
    ) -> dict[str, Any]:
        return await self.repository.list_events(
            limit=limit,
            cursor=cursor,
            event_type=event_type,
            severity=severity,
        )

    async def probe_spend(self) -> dict[str, Any]:
        return await self.repository.probe_spend()

    async def sampling_status(self) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        status = await self.repository.sampling_status()
        return {
            **status,
            "mode": policy.sampling.mode.value,
            "fresh_seconds": policy.sampling.fresh_seconds,
            "expire_seconds": policy.sampling.expire_seconds,
        }

    async def channel_explanation(self, channel_id: str) -> dict[str, Any]:
        channel = await self.repository.get_channel(channel_id)
        if channel is None:
            raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
        return {
            "channel_id": channel_id,
            "health": channel["health"],
            "score": channel["score"],
            "confidence": channel["confidence"],
            "freshness_state": channel["freshness_state"],
            "last_evidence_at": channel["last_evidence_at"],
            "warmup_buckets": channel["warmup_buckets"],
            "reason": channel["details"].get("reason"),
            "evidence_sources": channel["details"].get("evidence_sources", []),
            "short_score": channel["details"].get("short_score"),
            "long_score": channel["details"].get("long_score"),
            "desired_priority": channel["details"].get("desired_priority"),
            "desired_load_factor": channel["details"].get("desired_load_factor"),
            "expected_action": channel["details"].get("expected_action"),
        }

    async def write_ownership(self) -> dict[str, Any]:
        return {"items": await self.repository.list_field_ownership()}

    async def set_field_ownership(
        self,
        *,
        channel_id: str,
        field_name: str,
        owner: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        if policy.revision != expected_revision:
            raise ServiceError(
                "POLICY_REVISION_CONFLICT",
                "The Guardian policy was modified by another request",
            )
        try:
            parsed_field = GuardianFieldName(field_name)
            parsed_owner = GuardianFieldOwner(owner)
        except ValueError as exc:
            raise ServiceError(
                "VALIDATION_ERROR",
                "field_name or owner is invalid",
            ) from exc
        if parsed_owner not in {GuardianFieldOwner.HUMAN, GuardianFieldOwner.GUARDIAN}:
            raise ServiceError(
                "VALIDATION_ERROR",
                "owner must be HUMAN or GUARDIAN",
            )
        channel = await self.repository.get_channel(channel_id)
        if channel is None:
            raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
        previous = await self.repository.get_field_ownership(channel_id, parsed_field)
        details = cast(dict[str, Any], channel.get("details") or {})
        current_values: dict[GuardianFieldName, object] = {
            GuardianFieldName.SCHEDULABLE: channel["upstream_schedulable"],
            GuardianFieldName.PRIORITY: details.get("baseline_priority"),
            GuardianFieldName.LOAD_FACTOR: details.get("desired_load_factor"),
        }
        value = GuardianFieldOwnership(
            channel_id=channel_id,
            field_name=parsed_field,
            owner=parsed_owner,
            baseline_value=(
                previous.baseline_value
                if previous is not None
                else cast(int | float | bool | str | None, current_values[parsed_field])
            ),
            last_guardian_value=(
                previous.last_guardian_value if previous is not None else None
            ),
            last_write_at=previous.last_write_at if previous is not None else None,
        )
        await self.repository.save_field_ownership(value)
        previous_owner = (
            previous.owner if previous is not None else GuardianFieldOwner.UPSTREAM
        )
        if self._metrics is not None and previous_owner is not parsed_owner:
            self._metrics.guardian_field_ownership_changes.labels(
                from_owner=previous_owner.value,
                to_owner=parsed_owner.value,
            ).inc()
        await self.repository.add_event(
            event_type="FIELD_OWNERSHIP_CHANGED",
            severity="WARNING",
            channel_id=channel_id,
            group_id=cast(str | None, channel.get("group_id")),
            message=(
                f"{channel.get('name') or channel_id}: {parsed_field.value} ownership "
                f"changed to {parsed_owner.value}"
            ),
            details={
                "field_name": parsed_field.value,
                "from_owner": previous_owner.value,
                "to_owner": parsed_owner.value,
            },
        )
        await self._notify_control_event(
            "Guardian 字段归属已变更",
            [
                f"渠道：{channel.get('name') or channel_id}"
                f"（分组 {channel.get('group_id') or '未分组'}）",
                f"字段：{parsed_field.value}",
                f"归属：{previous_owner.value} → {parsed_owner.value}",
                "实际写回：否",
                "原因：管理员显式调整字段控制权",
            ],
            coalesce_key=(
                f"guardian:ownership:{channel_id}:{parsed_field.value}"
            ),
        )
        return value.model_dump(mode="json")

    async def probe_budget(self) -> dict[str, Any]:
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        policy = await self.repository.get_policy()
        usage = await self.repository.recovery_probe_budget_summary(now.date())
        return {
            **usage,
            "daily_request_limit": policy.recovery_budget.daily_requests,
            "daily_token_limit": policy.recovery_budget.daily_tokens,
            "enabled": policy.recovery_budget.enabled,
        }

    async def advance_rollout(
        self,
        *,
        confirm: bool,
        expected_revision: int,
    ) -> dict[str, Any]:
        del confirm, expected_revision
        raise ServiceError(
            "DEPRECATED_GUARDIAN_CONTROL",
            "Rollout controls were removed; update policy.enabled instead",
        )

    async def stop_writeback(self, *, expected_revision: int) -> dict[str, Any]:
        del expected_revision
        raise ServiceError(
            "DEPRECATED_GUARDIAN_CONTROL",
            "Observe-mode stop was removed; update policy.enabled to false instead",
        )

    async def restore_preview(self) -> dict[str, Any]:
        return await self.repository.restore_preview()

    async def execute_restore(self, *, confirm: bool) -> dict[str, Any]:
        if not confirm:
            raise ServiceError("CONFIRMATION_REQUIRED", "Restore requires confirm=true")
        raise ServiceError(
            "WRITEBACK_NOT_APPROVED",
            "Production writeback has not been explicitly approved",
        )
