"""Curated FastMCP tool surface for the Sub2API scheduling service."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import ValidationError

from .auth import current_request_id, require_scope
from .config import DEFAULT_ALLOWED_HOSTS, Scope
from .contracts import DeliveryTargetCreate, JobStatus, JobType, SubmitVideoInput
from .errors import ServiceError
from .logging import log_event
from .metrics import Metrics
from .service import Sub2APIService

INSTRUCTIONS = """\
This server manages the complete Sub2API scheduler: channel probes, account
recovery and maintenance, durable video jobs, account bindings, and delivery
through every bot adapter registered in LangBot. Prefer read tools before
mutations. All identifiers are opaque. Never infer or invent platform user IDs.
"""


class Sub2APIMCPServer:
    def __init__(
        self,
        service: Sub2APIService,
        metrics: Metrics,
        *,
        allowed_hosts: list[str] | None = None,
    ) -> None:
        self.service = service
        self.metrics = metrics
        self._logger = logging.getLogger("sub2api_mcp")
        self.mcp = FastMCP(
            name="Sub2API Scheduler",
            instructions=INSTRUCTIONS,
            stateless_http=True,
            json_response=True,
            transport_security=TransportSecuritySettings(
                enable_dns_rebinding_protection=True,
                allowed_hosts=allowed_hosts or list(DEFAULT_ALLOWED_HOSTS),
                allowed_origins=[],
            ),
        )
        self._register_tools()

    async def _execute(
        self,
        tool: str,
        scope: Scope,
        action: Callable[[], Awaitable[Any]],
        *,
        mutation: bool = False,
        subject: str | None = None,
    ) -> str:
        request_id = current_request_id() or str(uuid.uuid4())
        started = time.monotonic()
        principal_name = "unknown"
        try:
            principal = require_scope(scope)
            principal_name = principal.name
            data = await action()
            envelope = {"ok": True, "requestId": request_id, "data": data}
            status = "ok"
            if mutation:
                await self.service.audit(principal.name, tool, subject, "success")
        except (ServiceError, ValidationError) as exc:
            if isinstance(exc, ServiceError):
                error_code = exc.code
                safe_message = exc.safe_message
                retryable = exc.retryable
            else:
                error_code = "VALIDATION_ERROR"
                safe_message = "The tool arguments are invalid"
                retryable = False
            envelope = {
                "ok": False,
                "requestId": request_id,
                "error": {
                    "code": error_code,
                    "message": safe_message,
                    "retryable": retryable,
                },
            }
            status = "error"
            if mutation and principal_name != "unknown":
                await self.service.audit(principal_name, tool, subject, error_code)
        except Exception:
            envelope = {
                "ok": False,
                "requestId": request_id,
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "The service failed unexpectedly",
                    "retryable": False,
                },
            }
            status = "error"
        self.metrics.mcp_calls.labels(tool=tool, status=status).inc()
        duration_seconds = time.monotonic() - started
        self.metrics.mcp_duration.labels(tool=tool).observe(duration_seconds)
        log_event(
            self._logger,
            logging.INFO if status == "ok" else logging.WARNING,
            "mcp_call_finished",
            requestId=request_id,
            tool=tool,
            status=status,
            durationMs=round(duration_seconds * 1000, 3),
            principal=principal_name,
        )
        return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), default=str)

    def _register_tools(self) -> None:
        mcp = self.mcp

        @mcp.tool(description="Get scheduler, job, outbox, delivery, and version status.")
        async def sub2api_get_status() -> str:
            return await self._execute(
                "sub2api_get_status", "sub2api:read", self.service.get_status
            )

        @mcp.tool(description="Run a read-only probe across all Sub2API channel types.")
        async def sub2api_probe_channels() -> str:
            return await self._execute(
                "sub2api_probe_channels", "sub2api:read", self.service.probe_channels
            )

        @mcp.tool(description="Get a durable job by opaque job ID.")
        async def sub2api_get_job(job_id: str) -> str:
            return await self._execute(
                "sub2api_get_job",
                "sub2api:read",
                lambda: self.service.get_job(job_id),
                subject=job_id,
            )

        @mcp.tool(description="List durable jobs with an opaque pagination cursor.")
        async def sub2api_list_jobs(
            limit: int = 20,
            cursor: str | None = None,
            job_type: str | None = None,
            status: str | None = None,
        ) -> str:
            return await self._execute(
                "sub2api_list_jobs",
                "sub2api:read",
                lambda: self._list_jobs(limit, cursor, job_type, status),
            )

        @mcp.tool(description="Get a masked binding by its HMAC actor key.")
        async def sub2api_get_bound_account(actor_key: str) -> str:
            return await self._execute(
                "sub2api_get_bound_account",
                "sub2api:read",
                lambda: self.service.get_bound_account(actor_key),
            )

        @mcp.tool(description="Discover every bot adapter currently configured in LangBot.")
        async def sub2api_list_delivery_bots() -> str:
            return await self._execute(
                "sub2api_list_delivery_bots",
                "sub2api:read",
                self.service.list_delivery_bots,
            )

        @mcp.tool(description="List platform-neutral LangBot delivery targets.")
        async def sub2api_list_delivery_targets(
            limit: int = 20, cursor: str | None = None
        ) -> str:
            return await self._execute(
                "sub2api_list_delivery_targets",
                "sub2api:read",
                lambda: self.service.list_delivery_targets(limit, cursor),
            )

        @mcp.tool(description="Persistently enable or disable periodic scheduling.")
        async def sub2api_set_scheduler_enabled(enabled: bool) -> str:
            return await self._execute(
                "sub2api_set_scheduler_enabled",
                "sub2api:admin",
                lambda: self.service.set_scheduler_enabled(enabled),
                mutation=True,
            )

        @mcp.tool(description="Submit one bounded account recovery job.")
        async def sub2api_submit_recovery() -> str:
            return await self._execute(
                "sub2api_submit_recovery",
                "sub2api:admin",
                lambda: self.service.submit_control_job(JobType.RECOVERY),
                mutation=True,
            )

        @mcp.tool(description="Submit one bounded account maintenance job.")
        async def sub2api_submit_maintenance() -> str:
            return await self._execute(
                "sub2api_submit_maintenance",
                "sub2api:admin",
                lambda: self.service.submit_control_job(JobType.MAINTENANCE),
                mutation=True,
            )

        @mcp.tool(description="Bind a verified active account to an opaque actor key.")
        async def sub2api_bind_account(actor_key: str, email: str) -> str:
            return await self._execute(
                "sub2api_bind_account",
                "sub2api:admin",
                lambda: self.service.bind_account(actor_key, email),
                mutation=True,
                subject=actor_key,
            )

        @mcp.tool(description="Idempotently remove an opaque actor-key binding.")
        async def sub2api_unbind_account(actor_key: str) -> str:
            return await self._execute(
                "sub2api_unbind_account",
                "sub2api:admin",
                lambda: self.service.unbind_account(actor_key),
                mutation=True,
                subject=actor_key,
            )

        @mcp.tool(description="Submit a durable video generation job and return queue count.")
        async def sub2api_submit_video(
            prompt: str,
            length: int = 22,
            steps: int = 20,
            width: int = 768,
            height: int = 448,
        ) -> str:
            return await self._execute(
                "sub2api_submit_video",
                "sub2api:write",
                lambda: self._submit_video(prompt, length, steps, width, height),
                mutation=True,
            )

        @mcp.tool(description="Cancel a queued job or request cancellation of a running job.")
        async def sub2api_cancel_job(job_id: str) -> str:
            return await self._execute(
                "sub2api_cancel_job",
                "sub2api:write",
                lambda: self.service.cancel_job(job_id),
                mutation=True,
                subject=job_id,
            )

        @mcp.tool(description="Create or update a delivery target for any LangBot adapter.")
        async def sub2api_upsert_delivery_target(
            name: str,
            bot_uuid: str,
            target_type: str,
            target_id: str,
            purposes: list[str],
            media_policy: str = "AUTO",
            required: bool = True,
            enabled: bool = True,
        ) -> str:
            return await self._execute(
                "sub2api_upsert_delivery_target",
                "sub2api:admin",
                lambda: self._upsert_delivery_target(
                    name,
                    bot_uuid,
                    target_type,
                    target_id,
                    purposes,
                    media_policy,
                    required,
                    enabled,
                ),
                mutation=True,
                subject=name,
            )

        @mcp.tool(description="Disable a delivery target idempotently.")
        async def sub2api_delete_delivery_target(delivery_target_id: str) -> str:
            return await self._execute(
                "sub2api_delete_delivery_target",
                "sub2api:admin",
                lambda: self.service.delete_delivery_target(delivery_target_id),
                mutation=True,
                subject=delivery_target_id,
            )

        @mcp.tool(description="Send a test message through the selected LangBot adapter.")
        async def sub2api_test_delivery_target(delivery_target_id: str) -> str:
            return await self._execute(
                "sub2api_test_delivery_target",
                "sub2api:admin",
                lambda: self.service.test_delivery_target(delivery_target_id),
                mutation=True,
                subject=delivery_target_id,
            )

    async def _submit_video(
        self,
        prompt: str,
        length: int,
        steps: int,
        width: int,
        height: int,
    ) -> dict[str, Any]:
        request = SubmitVideoInput(
            prompt=prompt,
            length=length,
            steps=steps,
            width=width,
            height=height,
        )
        return await self.service.submit_video(request)

    async def _list_jobs(
        self,
        limit: int,
        cursor: str | None,
        job_type: str | None,
        status: str | None,
    ) -> dict[str, Any]:
        try:
            parsed_type = JobType(job_type) if job_type is not None else None
            parsed_status = JobStatus(status) if status is not None else None
        except ValueError as exc:
            raise ServiceError(
                "VALIDATION_ERROR", "The job type or status filter is invalid"
            ) from exc
        return await self.service.list_jobs(limit, cursor, parsed_type, parsed_status)

    async def _upsert_delivery_target(
        self,
        name: str,
        bot_uuid: str,
        target_type: str,
        target_id: str,
        purposes: list[str],
        media_policy: str,
        required: bool,
        enabled: bool,
    ) -> dict[str, Any]:
        target = DeliveryTargetCreate.model_validate(
            {
                "name": name,
                "bot_uuid": bot_uuid,
                "target_type": target_type,
                "target_id": target_id,
                "purposes": purposes,
                "media_policy": media_policy,
                "required": required,
                "enabled": enabled,
            }
        )
        return await self.service.upsert_delivery_target(target)

    def streamable_http_app(self):  # type: ignore[no-untyped-def]
        return self.mcp.streamable_http_app()

    @property
    def session_manager(self):  # type: ignore[no-untyped-def]
        return self.mcp.session_manager
