"""Guardian application service and safe background observe-only scheduler."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from ..contracts import DeliveryPurpose, NotificationPayload, OutboxEventType
from ..errors import ServiceError
from ..metrics import Metrics
from ..repository import SqliteRepository
from .contracts import (
    AutoApplyPolicy,
    ChannelPolicyOverride,
    GroupPolicyOverride,
    GuardianFieldName,
    GuardianFieldOwner,
    GuardianFieldOwnership,
    GuardianPolicy,
    GuardianRolloutStage,
    ManualControl,
)
from .engine import GuardianEngine
from .repository import GuardianRepository


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


class GuardianService:
    def __init__(
        self,
        repository: GuardianRepository,
        engine: GuardianEngine,
        metrics: Metrics | None = None,
        notification_repository: SqliteRepository | None = None,
    ) -> None:
        self.repository = repository
        self.engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._logger = logging.getLogger("sub2api_mcp.guardian")
        self._metrics = metrics
        self._notification_repository = notification_repository
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
            try:
                policy = await self.repository.get_policy()
                if policy.enabled:
                    slot = int(datetime.now(UTC).timestamp()) // policy.scan_interval_seconds
                    await self.run_once(dry_run=True, idempotency_key=f"scheduled:{slot}")
            except Exception:
                self._logger.exception("guardian_scheduled_cycle_failed")

    async def get_policy(self) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        return {
            "policy": policy.model_dump(mode="json"),
            "defaults": GuardianPolicy().model_dump(mode="json"),
            "writeback_approved": False,
        }

    async def update_policy(
        self, patch: dict[str, Any], *, expected_revision: int
    ) -> dict[str, Any]:
        current = await self.repository.get_policy()
        merged = _merge_dict(current.model_dump(mode="json"), patch)
        merged["revision"] = current.revision
        try:
            candidate = GuardianPolicy.model_validate(merged)
        except ValidationError:
            raise
        if not candidate.observe_only or any(
            (
                candidate.auto_apply.schedulable,
                candidate.auto_apply.priority,
                candidate.auto_apply.load_factor,
            )
        ):
            raise ServiceError(
                "WRITEBACK_NOT_APPROVED",
                "Production writeback has not been explicitly approved",
            )
        saved = await self.repository.update_policy(candidate, expected_revision=expected_revision)
        await self.repository.add_event(
            event_type="POLICY_UPDATED",
            severity="INFO",
            message=f"Guardian policy revision {saved.revision} saved",
            details={"revision": saved.revision},
        )
        return {
            "policy": saved.model_dump(mode="json"),
            "writeback_approved": False,
        }

    async def overview(self) -> dict[str, Any]:
        return await self.repository.overview()

    async def status(self) -> dict[str, Any]:
        policy = await self.repository.get_policy()
        runs = await self.repository.list_runs(limit=1)
        return {
            "enabled": policy.enabled,
            "observe_only": policy.observe_only,
            "background_task_running": self._task is not None and not self._task.done(),
            "scan_interval_seconds": policy.scan_interval_seconds,
            "last_run": runs[0] if runs else None,
            "writeback_adapter": "disabled",
        }

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
            await self._after_run_operations(result)
            return result
        finally:
            if self._metrics is not None:
                self._metrics.guardian_runs.labels(status=status, mode=mode).inc()
                self._metrics.guardian_duration.labels(mode=mode).observe(
                    time.monotonic() - started
                )

    async def _after_run_operations(self, run: dict[str, Any]) -> None:
        try:
            await self._refresh_v2_metrics(run)
            await self._check_recovery_budget_alert()
            now = datetime.now(UTC)
            if (
                self._last_retention_at is None
                or now - self._last_retention_at >= timedelta(hours=1)
            ):
                await self.repository.cleanup_retention(now=now, batch_size=500)
                self._last_retention_at = now
        except Exception:
            self._logger.exception(
                "guardian_post_run_operations_failed",
                extra={"runId": run.get("run_id")},
            )

    async def _refresh_v2_metrics(self, run: dict[str, Any]) -> None:
        if self._metrics is None:
            return
        result = cast(dict[str, Any], run.get("result") or {})
        if result.get("snapshot_id"):
            self._metrics.guardian_shared_snapshots.labels(status="consumed").inc()
        elif result.get("no_new_evidence"):
            self._metrics.guardian_shared_snapshots.labels(status="empty").inc()
        reason = str(result.get("writeback_blocked_reason") or "")
        if reason:
            self._metrics.guardian_write_frozen.labels(reason=reason).inc()
        duplicates = int(result.get("duplicate_observations") or 0)
        if duplicates:
            self._metrics.guardian_duplicate_observations.labels(
                source="SHARED_MONITOR"
            ).inc(duplicates)
        traffic_processed = int(result.get("traffic_buckets_processed") or 0)
        if traffic_processed:
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
        for channel in cast(list[dict[str, Any]], channels.get("items") or []):
            self._metrics.guardian_channel_confidence.labels(
                channel=str(channel["channel_id"])
            ).set(float(channel.get("confidence") or 0))
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
                        f"实际写入：{result.get('writes_applied', 0)}（观察模式）",
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
        if not confirm:
            raise ServiceError("CONFIRMATION_REQUIRED", "Rollout advance requires confirm=true")
        current = await self.repository.get_policy()
        stages = list(GuardianRolloutStage)
        index = stages.index(current.rollout.stage)
        if index >= len(stages) - 1:
            raise ServiceError("ROLLOUT_COMPLETE", "Guardian rollout is already at the final stage")
        updated = current.model_copy(
            update={
                "rollout": current.rollout.model_copy(update={"stage": stages[index + 1]})
            }
        )
        saved = await self.repository.update_policy(
            updated,
            expected_revision=expected_revision,
        )
        await self.repository.add_event(
            event_type="ROLLOUT_ADVANCED",
            severity="WARNING",
            message=f"Guardian rollout advanced to {saved.rollout.stage.value}",
            details={"stage": saved.rollout.stage.value},
        )
        await self._notify_control_event(
            "Guardian 灰度阶段已变更",
            [
                f"阶段：{current.rollout.stage.value} → {saved.rollout.stage.value}",
                "实际写回：否（生产写回适配器仍未批准）",
                "原因：管理员确认推进灰度阶段",
            ],
            coalesce_key="guardian:rollout",
        )
        return {"policy": saved.model_dump(mode="json")}

    async def stop_writeback(self, *, expected_revision: int) -> dict[str, Any]:
        current = await self.repository.get_policy()
        updated = current.model_copy(
            update={
                "observe_only": True,
                "auto_apply": AutoApplyPolicy(),
                "rollout": current.rollout.model_copy(
                    update={"stage": GuardianRolloutStage.OBSERVE}
                ),
            }
        )
        saved = await self.repository.update_policy(
            updated,
            expected_revision=expected_revision,
        )
        await self.repository.add_event(
            event_type="ROLLOUT_STOPPED",
            severity="WARNING",
            message="Guardian writeback stopped and returned to observe mode",
            details={"stage": GuardianRolloutStage.OBSERVE.value},
        )
        await self._notify_control_event(
            "Guardian 写回已停止",
            [
                f"阶段：{current.rollout.stage.value} → {GuardianRolloutStage.OBSERVE.value}",
                "实际写回：否",
                "原因：管理员触发紧急停止",
            ],
            coalesce_key="guardian:rollout",
        )
        return {"policy": saved.model_dump(mode="json")}

    async def restore_preview(self) -> dict[str, Any]:
        return await self.repository.restore_preview()

    async def execute_restore(self, *, confirm: bool) -> dict[str, Any]:
        if not confirm:
            raise ServiceError("CONFIRMATION_REQUIRED", "Restore requires confirm=true")
        raise ServiceError(
            "WRITEBACK_NOT_APPROVED",
            "Production writeback has not been explicitly approved",
        )
