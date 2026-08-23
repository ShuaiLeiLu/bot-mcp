"""Guardian application service and safe background observe-only scheduler."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from pydantic import ValidationError

from ..contracts import DeliveryPurpose, NotificationPayload, OutboxEventType
from ..errors import ServiceError
from ..metrics import Metrics
from ..repository import SqliteRepository
from .contracts import (
    ChannelPolicyOverride,
    GroupPolicyOverride,
    GuardianPolicy,
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
                    await self.engine.run_once(dry_run=True, idempotency_key=f"scheduled:{slot}")
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
            return result
        finally:
            if self._metrics is not None:
                self._metrics.guardian_runs.labels(status=status, mode=mode).inc()
                self._metrics.guardian_duration.labels(mode=mode).observe(
                    time.monotonic() - started
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
            transition_lines = [
                "｜".join(
                    [
                        str(item.get("name") or item.get("channel_id") or "未知渠道"),
                        f"{item.get('from', '—')}→{item.get('to', '—')}",
                        f"健康分 {float(item.get('score', 0)):.1f}",
                        (
                            f"延迟 {item['latency_ms']}ms"
                            if item.get("latency_ms") is not None
                            else "延迟 --"
                        ),
                        f"探测 {item.get('event_type', '—')}",
                        f"原因 {item.get('reason', '—')}",
                    ]
                )
                for item in transition_items[:10]
            ]
            detail_lines = (
                ["", "渠道变化：", *transition_lines] if transition_lines else []
            )
            notification = NotificationPayload(
                text="\n".join(
                    [
                        "Guardian 调度状态更新",
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
                {"notification": notification.model_dump(mode="json", exclude_none=True)},
                targets,
            )
        except Exception:
            self._logger.exception(
                "guardian_notification_enqueue_failed",
                extra={"runId": run.get("run_id")},
            )

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

    async def restore_preview(self) -> dict[str, Any]:
        return await self.repository.restore_preview()

    async def execute_restore(self, *, confirm: bool) -> dict[str, Any]:
        if not confirm:
            raise ServiceError("CONFIRMATION_REQUIRED", "Restore requires confirm=true")
        raise ServiceError(
            "WRITEBACK_NOT_APPROVED",
            "Production writeback has not been explicitly approved",
        )
