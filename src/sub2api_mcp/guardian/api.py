"""Authenticated Guardian REST API v1 and static management UI routes."""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from ..auth import ApiKeyAuthenticator, Principal, bind_principal
from ..config import Scope
from ..errors import ServiceError
from .service import GuardianService

_STATIC_ROOT = Path(__file__).with_name("static")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class GuardianAPI:
    def __init__(
        self,
        service: GuardianService,
        authenticator: ApiKeyAuthenticator,
        audit: Callable[[str, str, str | None, str], Awaitable[None]],
    ) -> None:
        self.service = service
        self._authenticator = authenticator
        self._audit = audit
        self._logger = logging.getLogger("sub2api_mcp.guardian.api")

    def routes(self) -> list[Route]:
        return [
            Route("/guardian", self.redirect_ui, methods=["GET"]),
            Route("/guardian/", self.ui, methods=["GET"]),
            Route("/guardian/assets/{name:str}", self.asset, methods=["GET"]),
            Route("/api/guardian/v1/overview", self.overview, methods=["GET"]),
            Route("/api/guardian/v1/status", self.status, methods=["GET"]),
            Route("/api/guardian/v1/policy", self.policy, methods=["GET", "PATCH"]),
            Route("/api/guardian/v1/runs", self.runs, methods=["POST"]),
            Route(
                "/api/guardian/v1/runs/{run_id:str}/cancel",
                self.cancel_run,
                methods=["POST"],
            ),
            Route("/api/guardian/v1/syncs", self.sync, methods=["POST"]),
            Route("/api/guardian/v1/groups", self.groups, methods=["GET"]),
            Route(
                "/api/guardian/v1/groups/{group_id:str}/policy",
                self.group_policy,
                methods=["PATCH", "DELETE"],
            ),
            Route("/api/guardian/v1/channels", self.channels, methods=["GET"]),
            Route(
                "/api/guardian/v1/channels/{channel_id:str}",
                self.channel,
                methods=["GET", "PATCH"],
            ),
            Route(
                "/api/guardian/v1/channels/{channel_id:str}/actions",
                self.channel_action,
                methods=["POST"],
            ),
            Route("/api/guardian/v1/live-routing", self.live_routing, methods=["GET"]),
            Route("/api/guardian/v1/probe-spend", self.probe_spend, methods=["GET"]),
            Route("/api/guardian/v1/probe-budget", self.probe_budget, methods=["GET"]),
            Route("/api/guardian/v1/sampling/status", self.sampling_status, methods=["GET"]),
            Route("/api/guardian/v1/write-ownership", self.write_ownership, methods=["GET"]),
            Route(
                "/api/guardian/v1/channels/{channel_id:str}/explanation",
                self.channel_explanation,
                methods=["GET"],
            ),
            Route(
                "/api/guardian/v1/channels/{channel_id:str}/ownership",
                self.channel_ownership,
                methods=["POST"],
            ),
            Route("/api/guardian/v1/rollout/advance", self.advance_rollout, methods=["POST"]),
            Route("/api/guardian/v1/rollout/stop", self.stop_writeback, methods=["POST"]),
            Route("/api/guardian/v1/events", self.events, methods=["GET"]),
            Route(
                "/api/guardian/v1/restores/preview",
                self.restore_preview,
                methods=["POST"],
            ),
            Route("/api/guardian/v1/restores", self.restore, methods=["POST"]),
        ]

    async def redirect_ui(self, _: Request) -> Response:
        return RedirectResponse("/guardian/", status_code=308)

    async def ui(self, _: Request) -> Response:
        return FileResponse(_STATIC_ROOT / "index.html", media_type="text/html")

    async def asset(self, request: Request) -> Response:
        name = request.path_params["name"]
        media_types = {
            "app.css": "text/css",
            "app.js": "text/javascript",
        }
        if name not in media_types:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return FileResponse(_STATIC_ROOT / name, media_type=media_types[name])

    async def overview(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.overview)

    async def status(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.status)

    async def policy(self, request: Request) -> Response:
        if request.method == "GET":
            return await self._execute(request, "sub2api:read", self.service.get_policy)

        async def update() -> dict[str, Any]:
            body = await self._body(request)
            revision = self._revision(request)
            return await self.service.update_policy(body, expected_revision=revision)

        return await self._execute(
            request, "sub2api:admin", update, mutation="guardian_update_policy"
        )

    async def runs(self, request: Request) -> Response:
        async def run() -> dict[str, Any]:
            body = await self._body(request)
            dry_run = body.get("dry_run", True)
            if not isinstance(dry_run, bool):
                raise ServiceError("VALIDATION_ERROR", "dry_run must be a boolean")
            return await self.service.run_once(
                dry_run=dry_run,
                idempotency_key=self._idempotency_key(request, required=False),
            )

        return await self._execute(request, "sub2api:admin", run, mutation="guardian_run_once")

    async def cancel_run(self, request: Request) -> Response:
        run_id = request.path_params["run_id"]
        return await self._execute(
            request,
            "sub2api:admin",
            lambda: self.service.cancel_run(run_id),
            mutation="guardian_cancel_run",
            subject=run_id,
        )

    async def sync(self, request: Request) -> Response:
        async def synchronize() -> dict[str, Any]:
            await self._body(request)
            return await self.service.run_once(
                dry_run=True,
                idempotency_key=self._idempotency_key(request, required=False),
            )

        return await self._execute(request, "sub2api:admin", synchronize, mutation="guardian_sync")

    async def groups(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.list_groups)

    async def group_policy(self, request: Request) -> Response:
        group_id = request.path_params["group_id"][:128]

        async def update() -> dict[str, Any]:
            body = await self._body(request)
            return await self.service.update_group_policy(group_id, body)

        action: Callable[[], Awaitable[dict[str, Any] | dict[str, bool]]]
        action = (
            update
            if request.method == "PATCH"
            else lambda: self.service.delete_group_policy(group_id)
        )
        return await self._execute(
            request,
            "sub2api:admin",
            action,
            mutation="guardian_group_policy",
            subject=group_id,
        )

    async def channels(self, request: Request) -> Response:
        async def listing() -> dict[str, Any]:
            return await self.service.list_channels(
                limit=self._limit(request, default=100),
                cursor=request.query_params.get("cursor"),
                group_id=request.query_params.get("group_id"),
                health=request.query_params.get("health"),
                query=request.query_params.get("query"),
            )

        return await self._execute(request, "sub2api:read", listing)

    async def channel(self, request: Request) -> Response:
        channel_id = request.path_params["channel_id"][:128]
        if request.method == "GET":
            return await self._execute(
                request,
                "sub2api:read",
                lambda: self.service.get_channel(channel_id),
            )

        async def update() -> dict[str, Any]:
            body = await self._body(request)
            control = body.pop("manual_control", None)
            actions = {
                "NONE": "resume",
                "PAUSED": "pause",
                "EXCLUDED": "exclude",
                "FUSED": "fuse",
            }
            result: dict[str, Any] | None = None
            if control is not None:
                if not isinstance(control, str) or control not in actions:
                    raise ServiceError("VALIDATION_ERROR", "manual_control is invalid")
                result = await self.service.channel_action(channel_id, actions[control])
            if body:
                result = await self.service.update_channel(channel_id, body)
            if result is None:
                raise ServiceError("VALIDATION_ERROR", "No channel fields were supplied")
            return result

        return await self._execute(
            request,
            "sub2api:admin",
            update,
            mutation="guardian_update_channel",
            subject=channel_id,
        )

    async def channel_action(self, request: Request) -> Response:
        channel_id = request.path_params["channel_id"][:128]

        async def action() -> dict[str, Any]:
            body = await self._body(request)
            name = body.get("action")
            if not isinstance(name, str):
                raise ServiceError("VALIDATION_ERROR", "action must be a string")
            minutes = body.get("minutes")
            if minutes is not None and (isinstance(minutes, bool) or not isinstance(minutes, int)):
                raise ServiceError("VALIDATION_ERROR", "minutes must be an integer")
            return await self.service.channel_action(
                channel_id,
                name,
                idempotency_key=self._idempotency_key(request, required=False),
                minutes=minutes,
            )

        return await self._execute(
            request,
            "sub2api:admin",
            action,
            mutation="guardian_channel_action",
            subject=channel_id,
        )

    async def live_routing(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.live_routing)

    async def probe_spend(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.probe_spend)

    async def probe_budget(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.probe_budget)

    async def sampling_status(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.sampling_status)

    async def write_ownership(self, request: Request) -> Response:
        return await self._execute(request, "sub2api:read", self.service.write_ownership)

    async def channel_explanation(self, request: Request) -> Response:
        channel_id = request.path_params["channel_id"][:128]
        return await self._execute(
            request,
            "sub2api:read",
            lambda: self.service.channel_explanation(channel_id),
        )

    async def channel_ownership(self, request: Request) -> Response:
        channel_id = request.path_params["channel_id"][:128]

        async def change() -> dict[str, Any]:
            body = await self._body(request)
            field_name = body.get("field_name")
            owner = body.get("owner")
            if not isinstance(field_name, str) or not isinstance(owner, str):
                raise ServiceError(
                    "VALIDATION_ERROR",
                    "field_name and owner must be strings",
                )
            return await self.service.set_field_ownership(
                channel_id=channel_id,
                field_name=field_name,
                owner=owner,
                expected_revision=self._revision(request),
            )

        return await self._execute(
            request,
            "sub2api:admin",
            change,
            mutation="guardian_set_field_ownership",
            subject=channel_id,
            require_idempotency=True,
        )

    async def advance_rollout(self, request: Request) -> Response:
        async def advance() -> dict[str, Any]:
            body = await self._body(request)
            return await self.service.advance_rollout(
                confirm=body.get("confirm") is True,
                expected_revision=self._revision(request),
            )

        return await self._execute(
            request,
            "sub2api:admin",
            advance,
            mutation="guardian_advance_rollout",
            require_idempotency=True,
        )

    async def stop_writeback(self, request: Request) -> Response:
        async def stop() -> dict[str, Any]:
            await self._body(request)
            return await self.service.stop_writeback(
                expected_revision=self._revision(request)
            )

        return await self._execute(
            request,
            "sub2api:admin",
            stop,
            mutation="guardian_stop_writeback",
            require_idempotency=True,
        )

    async def events(self, request: Request) -> Response:
        async def listing() -> dict[str, Any]:
            return await self.service.list_events(
                limit=self._limit(request, default=50),
                cursor=request.query_params.get("cursor"),
                event_type=request.query_params.get("event_type"),
                severity=request.query_params.get("severity"),
            )

        return await self._execute(request, "sub2api:read", listing)

    async def restore_preview(self, request: Request) -> Response:
        async def preview() -> dict[str, Any]:
            await self._body(request)
            return await self.service.restore_preview()

        return await self._execute(
            request,
            "sub2api:admin",
            preview,
            mutation="guardian_preview_restore",
        )

    async def restore(self, request: Request) -> Response:
        async def execute() -> dict[str, Any]:
            body = await self._body(request)
            confirm = body.get("confirm")
            if not isinstance(confirm, bool):
                raise ServiceError("VALIDATION_ERROR", "confirm must be a boolean")
            return await self.service.execute_restore(confirm=confirm)

        return await self._execute(
            request,
            "sub2api:admin",
            execute,
            mutation="guardian_execute_restore",
        )

    async def _execute(
        self,
        request: Request,
        scope: Scope,
        action: Callable[[], Awaitable[Any]],
        *,
        mutation: str | None = None,
        subject: str | None = None,
        require_idempotency: bool = False,
    ) -> Response:
        request_id = self._request_id(request)
        principal = self._authenticator.authenticate(request.scope.get("headers", []))
        if principal is None:
            return self._error(
                request_id,
                ServiceError("UNAUTHENTICATED", "A valid API key is required"),
                401,
            )
        if not self._authorized(principal, scope):
            return self._error(
                request_id,
                ServiceError("FORBIDDEN", "The API key lacks the required scope"),
                403,
            )
        try:
            idempotency_key = (
                self._idempotency_key(request, required=require_idempotency)
                if mutation
                else None
            )
            with bind_principal(principal, request_id):
                if mutation and idempotency_key:
                    cached = await self.service.repository.get_idempotent_result(
                        idempotency_key, mutation, subject
                    )
                    if cached is not None:
                        return JSONResponse(
                            {"ok": True, "requestId": request_id, "data": cached},
                            headers={
                                "X-Request-ID": request_id,
                                "X-Idempotent-Replay": "true",
                            },
                        )
                data = await action()
                if mutation and idempotency_key and isinstance(data, dict):
                    await self.service.repository.save_idempotent_result(
                        idempotency_key,
                        mutation,
                        subject,
                        cast(dict[str, Any], data),
                    )
                if mutation:
                    await self._audit(principal.name, mutation, subject, "success")
            return JSONResponse(
                {"ok": True, "requestId": request_id, "data": data},
                headers={"X-Request-ID": request_id},
            )
        except ValidationError:
            error = ServiceError("VALIDATION_ERROR", "The request body is invalid")
            status_code = 422
        except ServiceError as exc:
            error = exc
            status_code = self._status_for(exc.code)
        except (json.JSONDecodeError, UnicodeError):
            error = ServiceError("VALIDATION_ERROR", "The request body is invalid")
            status_code = 422
        except Exception:
            self._logger.exception(
                "guardian_api_request_failed",
                extra={"requestId": request_id, "action": mutation or "read"},
            )
            error = ServiceError("INTERNAL_ERROR", "The Guardian service failed unexpectedly")
            status_code = 500
        if mutation:
            try:
                await self._audit(principal.name, mutation, subject, error.code)
            except Exception:
                self._logger.exception(
                    "guardian_api_audit_failed",
                    extra={"requestId": request_id, "action": mutation},
                )
        return self._error(request_id, error, status_code)

    @staticmethod
    def _authorized(principal: Principal, scope: Scope) -> bool:
        return "sub2api:admin" in principal.scopes or scope in principal.scopes

    @staticmethod
    def _status_for(code: str) -> int:
        if code in {"POLICY_REVISION_CONFLICT", "WRITEBACK_NOT_APPROVED"}:
            return 409
        if code.endswith("_NOT_FOUND"):
            return 404
        if code in {"POLICY_REVISION_REQUIRED"}:
            return 428
        if code == "REQUEST_TOO_LARGE":
            return 413
        if code in {"VALIDATION_ERROR", "INVALID_PAGE_SIZE", "INVALID_CURSOR"}:
            return 422
        return 409

    @staticmethod
    async def _body(request: Request) -> dict[str, Any]:
        raw = await request.body()
        if len(raw) > 64 * 1024:
            raise ServiceError("REQUEST_TOO_LARGE", "The request body is too large")
        if not raw:
            return {}
        value: object = json.loads(raw)
        if not isinstance(value, dict):
            raise ServiceError("VALIDATION_ERROR", "The request body must be an object")
        return dict(cast(dict[str, Any], value))

    @staticmethod
    def _revision(request: Request) -> int:
        supplied = request.headers.get("if-match", "").strip().strip('"')
        if not supplied:
            raise ServiceError("POLICY_REVISION_REQUIRED", "If-Match policy revision is required")
        try:
            revision = int(supplied)
        except ValueError as exc:
            raise ServiceError("VALIDATION_ERROR", "If-Match policy revision is invalid") from exc
        if revision < 1:
            raise ServiceError("VALIDATION_ERROR", "If-Match revision is invalid")
        return revision

    @staticmethod
    def _idempotency_key(request: Request, *, required: bool) -> str | None:
        supplied = request.headers.get("idempotency-key", "").strip()
        if not supplied and not required:
            return None
        if not _IDEMPOTENCY_KEY.fullmatch(supplied):
            raise ServiceError("VALIDATION_ERROR", "Idempotency-Key is missing or invalid")
        return supplied

    @staticmethod
    def _limit(request: Request, *, default: int) -> int:
        supplied = request.query_params.get("limit")
        if supplied is None:
            return default
        try:
            return int(supplied)
        except ValueError as exc:
            raise ServiceError("VALIDATION_ERROR", "limit must be an integer") from exc

    @staticmethod
    def _request_id(request: Request) -> str:
        supplied = request.headers.get("x-request-id", "").strip()
        if re.fullmatch(r"[A-Za-z0-9._-]{8,128}", supplied):
            return supplied
        return str(uuid.uuid4())

    @staticmethod
    def _error(request_id: str, error: ServiceError, status_code: int) -> JSONResponse:
        return JSONResponse(
            {
                "ok": False,
                "requestId": request_id,
                "error": {
                    "code": error.code,
                    "message": error.safe_message,
                    "retryable": error.retryable,
                },
            },
            status_code=status_code,
            headers={"X-Request-ID": request_id, "Cache-Control": "no-store"},
        )
