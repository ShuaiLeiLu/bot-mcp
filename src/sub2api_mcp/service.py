"""Application service layer used by MCP tools and the actor bridge."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Protocol

from . import __version__
from .actor_bridge import ActorAccount
from .adapters.langbot import LangBotClient
from .contracts import (
    AccountQuarantineReason,
    DeliveryTargetCreate,
    DeliveryTargetRecord,
    JobStatus,
    JobType,
    NotificationPayload,
    ProbeResult,
    SubmitVideoInput,
)
from .delivery import DeliveryService
from .errors import ServiceError
from .jobs import VideoJobService
from .repository import SqliteRepository
from .scheduler import SchedulerService


class ServiceOperations(Protocol):
    async def probe(self) -> ProbeResult: ...

    async def find_active_account(self, email: str) -> ActorAccount | None: ...

    async def account_report(self, user_id: str) -> str: ...


class Sub2APIService:
    def __init__(
        self,
        *,
        repository: SqliteRepository,
        operations: ServiceOperations,
        scheduler: SchedulerService,
        video: VideoJobService,
        langbot: LangBotClient | None,
        delivery: DeliveryService | None,
        video_enabled: bool = True,
    ) -> None:
        self.repository = repository
        self._operations = operations
        self._scheduler = scheduler
        self._video = video
        self._langbot = langbot
        self._delivery = delivery
        self._video_enabled = video_enabled

    async def get_status(self) -> dict[str, Any]:
        job_counts = {
            job_type.value: await self.repository.active_job_count(job_type)
            for job_type in JobType
        }
        return {
            "version": __version__,
            "scheduler_enabled": await self._scheduler.is_enabled(),
            "active_jobs": job_counts,
            "outbox_backlog": await self.repository.outbox_backlog(),
            "delivery_targets": len(await self.repository.list_delivery_targets()),
            "account_quarantine_count": await self.repository.account_quarantine_count(),
            "account_quarantine_counts": {
                reason.value: await self.repository.account_quarantine_count(reason)
                for reason in AccountQuarantineReason
            },
            "langbot_configured": self._langbot is not None,
        }

    async def probe_channels(self) -> dict[str, Any]:
        result = await self._operations.probe()
        return result.model_dump(mode="json", exclude_none=True)

    async def get_job(self, job_id: str) -> dict[str, Any]:
        job = await self.repository.get_job(job_id)
        if job is None:
            raise ServiceError("JOB_NOT_FOUND", "The job does not exist")
        return job.model_dump(mode="json")

    async def list_jobs(
        self,
        limit: int,
        cursor: str | None,
        job_type: JobType | None = None,
        status: JobStatus | None = None,
    ) -> dict[str, Any]:
        page = await self.repository.list_jobs(
            limit=limit,
            cursor=cursor,
            job_type=job_type,
            status=status,
        )
        return page.model_dump(mode="json")

    async def get_bound_account(self, actor_key: str) -> dict[str, Any]:
        binding = await self.repository.get_binding(self._validate_actor_key(actor_key))
        if binding is None:
            raise ServiceError("ACCOUNT_NOT_BOUND", "No account is bound to this actor key")
        return {
            "masked_email": binding.masked_email,
            "bound_at": binding.bound_at.isoformat(),
        }

    async def list_delivery_bots(self) -> list[dict[str, Any]]:
        if self._langbot is None:
            raise ServiceError("LANGBOT_NOT_CONFIGURED", "LangBot delivery is not configured")
        return [item.model_dump(mode="json") for item in await self._langbot.list_bots()]

    async def list_delivery_targets(
        self, limit: int = 20, cursor: str | None = None
    ) -> dict[str, Any]:
        page = await self.repository.list_delivery_targets_page(
            limit=limit, cursor=cursor
        )
        targets: list[dict[str, Any]] = []
        for item in page.items:
            targets.append(self._redact_delivery_target(item))
        return {"items": targets, "next_cursor": page.next_cursor}

    async def list_account_quarantines(
        self,
        limit: int = 20,
        cursor: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        try:
            parsed_reason = (
                AccountQuarantineReason(reason) if reason is not None else None
            )
        except ValueError as exc:
            raise ServiceError(
                "VALIDATION_ERROR",
                "The account quarantine reason is invalid",
            ) from exc
        page = await self.repository.list_account_quarantines(
            limit=limit,
            cursor=cursor,
            reason=parsed_reason,
        )
        return page.model_dump(mode="json")

    async def set_scheduler_enabled(self, enabled: bool) -> dict[str, bool]:
        await self._scheduler.set_enabled(enabled)
        return {"enabled": enabled}

    async def submit_control_job(self, job_type: JobType) -> dict[str, Any]:
        if job_type not in {JobType.RECOVERY, JobType.MAINTENANCE}:
            raise ValueError("unsupported control job type")
        await self._scheduler.require_control_target(job_type)
        created = await self.repository.create_job_with_capacity(
            job_type, {}, max_active=1
        )
        if created is None:
            raise ServiceError("JOB_ALREADY_ACTIVE", "A job of this type is already active")
        job, queue_count = created
        return {"job": job.model_dump(mode="json"), "queue_count": queue_count}

    async def bind_account(self, actor_key: str, email: str) -> dict[str, Any]:
        key = self._validate_actor_key(actor_key)
        account = await self._operations.find_active_account(email)
        if account is None or account.status != "active":
            raise ServiceError(
                "ACCOUNT_NOT_BINDABLE", "The account does not exist or is not active"
            )
        binding = await self.repository.bind_actor(
            key, account.user_id, account.email_masked
        )
        return {
            "masked_email": binding.masked_email,
            "bound_at": binding.bound_at.isoformat(),
        }

    async def unbind_account(self, actor_key: str) -> dict[str, bool]:
        await self.repository.unbind_actor(self._validate_actor_key(actor_key))
        return {"unbound": True}

    async def submit_video(self, request: SubmitVideoInput) -> dict[str, Any]:
        if not self._video_enabled:
            raise ServiceError("VIDEO_DISABLED", "Video generation is disabled")
        submission = await self._video.submit(request)
        return submission.model_dump(mode="json")

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        return (await self.repository.cancel_job(job_id)).model_dump(mode="json")

    async def upsert_delivery_target(
        self, target: DeliveryTargetCreate
    ) -> dict[str, Any]:
        saved = await self.repository.upsert_delivery_target(target)
        return self._redact_delivery_target(saved)

    async def delete_delivery_target(self, delivery_target_id: str) -> dict[str, bool]:
        await self.repository.delete_delivery_target(delivery_target_id)
        return {"deleted": True}

    async def test_delivery_target(self, delivery_target_id: str) -> dict[str, bool]:
        if self._delivery is None:
            raise ServiceError("LANGBOT_NOT_CONFIGURED", "LangBot delivery is not configured")
        target = await self.repository.get_delivery_target(delivery_target_id)
        if target is None or not target.enabled:
            raise ServiceError("DELIVERY_TARGET_NOT_FOUND", "The delivery target does not exist")
        await self._delivery.deliver(
            target,
            NotificationPayload(text="Sub2API MCP delivery test"),
        )
        return {"sent": True}

    async def audit(
        self, principal: str, action: str, subject: str | None, outcome: str
    ) -> None:
        await self.repository.audit(principal, action, subject, outcome)

    @staticmethod
    def _validate_actor_key(value: str) -> str:
        normalized = value.strip()
        if not re.fullmatch(r"v1:[0-9a-f]{64}", normalized):
            raise ServiceError("ACTOR_KEY_INVALID", "The actor key is invalid")
        return normalized

    @staticmethod
    def _redact_delivery_target(target: DeliveryTargetRecord) -> dict[str, Any]:
        data = target.model_dump(mode="json", exclude={"target_id"})
        data["target_ref"] = hashlib.sha256(target.target_id.encode()).hexdigest()[:16]
        return data
