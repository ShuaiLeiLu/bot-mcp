"""Durable scheduler queueing and Sub2API operation handlers."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, time
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .contracts import (
    DeliveryPurpose,
    JobRecord,
    JobType,
    NotificationPayload,
    OutboxEventType,
    ProbeResult,
)
from .errors import ServiceError
from .metrics import Metrics
from .repository import SqliteRepository


class Sub2APIOperations(Protocol):
    async def probe(self) -> ProbeResult: ...

    async def recover(self) -> list[dict[str, object]]: ...

    async def maintain(self, probe: ProbeResult) -> list[dict[str, object]]: ...


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
        if self._policy.maintenance_enabled and await self._targets_for(
            DeliveryPurpose.MAINTENANCE_ADMIN
        ):
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
                        "text": self._format_adjustments("Account recovery", outcomes)
                    }
                },
                targets,
            )
        return {"outcomes": outcomes}

    async def handle_maintenance(self, job: JobRecord) -> dict[str, Any]:
        del job
        targets = await self.require_control_target(JobType.MAINTENANCE)
        probe = self._latest_probe or await self._adapter.probe()
        adjustments = await self._adapter.maintain(probe)
        if adjustments:
            await self._repository.enqueue_outbox(
                OutboxEventType.MAINTENANCE_RESULT,
                {
                    "notification": {
                        "text": self._format_adjustments(
                            "Account maintenance", adjustments
                        )
                    },
                },
                targets,
            )
        return {"adjustments": adjustments}

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

    @staticmethod
    def _format_adjustments(title: str, values: list[dict[str, object]]) -> str:
        return "\n".join([title, *(str(item) for item in values)])

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
