"""ASGI assembly and lifecycle for the deployment-neutral MCP service."""

from __future__ import annotations

import json
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Mount, Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .actor_bridge import ActorBridgeRequest, ActorRequestVerifier, ActorService
from .adapters.langbot import LangBotClient
from .adapters.video import LegacyVideoGenerator, VideoGenerator
from .auth import ApiKeyAuthenticator, Principal, bind_principal
from .bootstrap import bootstrap_legacy_core
from .config import Settings
from .contracts import JobType, ProbeResult
from .delivery import DeliveryService, OutboxWorker
from .errors import ServiceError
from .guardian.api import GuardianAPI
from .guardian.engine import GuardianEngine
from .guardian.repository import GuardianRepository
from .guardian.service import GuardianService
from .jobs import JobManager, VideoJobService
from .logging import configure_logging
from .metrics import Metrics
from .repository import SqliteRepository
from .scheduler import SchedulerPolicy, SchedulerService
from .service import ServiceOperations, Sub2APIService
from .tools import Sub2APIMCPServer


class RuntimeOperations(ServiceOperations, Protocol):
    async def recover(self) -> list[dict[str, object]]: ...

    async def maintain(self, probe: ProbeResult) -> list[dict[str, object]]: ...


@dataclass(slots=True)
class Runtime:
    settings: Settings
    repository: SqliteRepository
    metrics: Metrics
    authenticator: ApiKeyAuthenticator
    operations: RuntimeOperations
    scheduler: SchedulerService
    video: VideoJobService
    jobs: JobManager
    service: Sub2APIService
    mcp: Sub2APIMCPServer
    guardian_repository: GuardianRepository
    guardian: GuardianService
    langbot: LangBotClient | None
    outbox: OutboxWorker | None
    actor_verifier: ActorRequestVerifier | None
    actor_service: ActorService | None
    started: bool = False


def build_runtime(
    settings: Settings,
    *,
    operations: RuntimeOperations | None = None,
    video_generator: VideoGenerator | None = None,
    langbot_client: LangBotClient | None = None,
) -> Runtime:
    if operations is None or video_generator is None:
        bootstrap_legacy_core(settings.legacy_core_root)
    if operations is None:
        from .adapters.sub2api import build_sub2api_adapter

        operations = build_sub2api_adapter(settings)
    if video_generator is None:
        video_generator = LegacyVideoGenerator(settings.video_api_url)

    repository = SqliteRepository(settings.database_path)
    guardian_repository = GuardianRepository(settings.database_path)
    metrics = Metrics.create()
    authenticator = ApiKeyAuthenticator(settings.access_tokens)
    scheduler_policy = SchedulerPolicy(
        enabled=settings.scheduler_enabled,
        interval_seconds=settings.probe_interval_seconds,
        lease_seconds=settings.scheduler_lease_seconds,
        recovery_enabled=settings.recovery_enabled,
        maintenance_enabled=(
            settings.channel_account_sweep_enabled or settings.log_account_guard_enabled
        ),
        quiet_hours_enabled=settings.quiet_hours_enabled,
        quiet_hours_start=settings.quiet_hours_start,
        quiet_hours_end=settings.quiet_hours_end,
    )
    scheduler = SchedulerService(repository, operations, metrics, scheduler_policy)
    video = VideoJobService(
        repository,
        video_generator,
        max_pending=settings.video_max_pending,
    )

    langbot = langbot_client
    if langbot is None and settings.langbot_base_url and settings.langbot_api_key:
        langbot = LangBotClient(
            settings.langbot_base_url,
            settings.langbot_api_key.get_secret_value(),
            timeout_seconds=settings.langbot_timeout_seconds,
        )
    delivery = DeliveryService(langbot) if langbot is not None else None
    outbox = OutboxWorker(repository, delivery, metrics) if delivery is not None else None
    service = Sub2APIService(
        repository=repository,
        operations=operations,
        scheduler=scheduler,
        video=video,
        langbot=langbot,
        delivery=delivery,
        video_enabled=settings.video_enabled,
    )
    jobs = JobManager(repository, metrics)
    jobs.register(JobType.VIDEO, video.handle)
    jobs.register(JobType.PROBE, scheduler.handle_probe)
    jobs.register(JobType.RECOVERY, scheduler.handle_recovery)
    jobs.register(JobType.MAINTENANCE, scheduler.handle_maintenance)
    guardian = GuardianService(
        guardian_repository,
        GuardianEngine(guardian_repository, operations),
        metrics,
        repository,
    )
    mcp = Sub2APIMCPServer(
        service,
        metrics,
        guardian=guardian,
        allowed_hosts=settings.allowed_hosts,
    )

    actor_verifier = None
    actor_service = None
    if settings.actor_bridge_enabled and settings.actor_bridge_secret is not None:
        actor_secret = settings.actor_bridge_secret.get_secret_value()
        actor_verifier = ActorRequestVerifier(
            repository,
            actor_secret,
            replay_window_seconds=settings.actor_replay_window_seconds,
        )
        actor_service = ActorService(
            repository,
            operations,
            actor_secret=actor_secret,
        )

    return Runtime(
        settings=settings,
        repository=repository,
        metrics=metrics,
        authenticator=authenticator,
        operations=operations,
        scheduler=scheduler,
        video=video,
        jobs=jobs,
        service=service,
        mcp=mcp,
        guardian_repository=guardian_repository,
        guardian=guardian,
        langbot=langbot,
        outbox=outbox,
        actor_verifier=actor_verifier,
        actor_service=actor_service,
    )


class AuthenticatedASGI:
    def __init__(self, app: ASGIApp, authenticator: ApiKeyAuthenticator) -> None:
        self._app = app
        self._authenticator = authenticator

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        principal = self._authenticator.authenticate(scope.get("headers", []))
        request_id = _request_id(scope.get("headers", []))
        if principal is None:
            await _unauthorized(request_id)(scope, receive, send)
            return

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode()))
                message["headers"] = headers
            await send(message)

        with bind_principal(principal, request_id):
            await self._app(scope, receive, send_with_request_id)


class SecurityHeadersASGI:
    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def send_with_security_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                existing = {key.lower() for key, _ in headers}
                additions = (
                    (b"x-content-type-options", b"nosniff"),
                    (b"x-frame-options", b"DENY"),
                    (b"referrer-policy", b"no-referrer"),
                    (b"cache-control", b"no-store"),
                    (
                        b"content-security-policy",
                        b"default-src 'self'; script-src 'self'; style-src 'self'; "
                        b"img-src 'self' data:; connect-src 'self'; object-src 'none'; "
                        b"base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
                    ),
                )
                headers.extend(item for item in additions if item[0] not in existing)
                message["headers"] = headers
            await send(message)

        await self._app(scope, receive, send_with_security_headers)


def _request_id(headers: list[tuple[bytes, bytes]]) -> str:
    header_map = {key.lower(): value for key, value in headers}
    supplied = header_map.get(b"x-request-id", b"").decode("latin-1").strip()
    if re.fullmatch(r"[A-Za-z0-9._-]{8,128}", supplied):
        return supplied
    return str(uuid.uuid4())


def _unauthorized(request_id: str) -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized", "message": "A valid API key is required"},
        status_code=401,
        headers={"X-Request-ID": request_id, "Cache-Control": "no-store"},
    )


def _authenticate_request(request: Request, runtime: Runtime) -> Principal | None:
    return runtime.authenticator.authenticate(request.scope.get("headers", []))


def create_app(runtime: Runtime) -> ASGIApp:
    configure_logging(runtime.settings.log_level)

    async def health(_: Request) -> Response:
        return JSONResponse(
            {"status": "ok" if runtime.started else "starting", "version": "0.1.0"},
            status_code=200 if runtime.started else 503,
        )

    async def metrics(request: Request) -> Response:
        principal = _authenticate_request(request, runtime)
        request_id = _request_id(request.scope.get("headers", []))
        if principal is None:
            return _unauthorized(request_id)
        if not ("sub2api:admin" in principal.scopes or "sub2api:read" in principal.scopes):
            return JSONResponse(
                {"error": "forbidden", "message": "The API key lacks the required scope"},
                status_code=403,
                headers={"X-Request-ID": request_id},
            )
        return PlainTextResponse(
            runtime.metrics.render().decode(),
            media_type="text/plain; version=0.0.4",
        )

    async def actor(request: Request) -> Response:
        if runtime.actor_verifier is None or runtime.actor_service is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        body = await request.body()
        if len(body) > 64 * 1024:
            return JSONResponse({"error": "request_too_large"}, status_code=413)
        try:
            await runtime.actor_verifier.verify(
                request.headers.get("x-sub2api-timestamp", ""),
                request.headers.get("x-sub2api-nonce", ""),
                body,
                request.headers.get("x-sub2api-signature", ""),
            )
            raw: object = json.loads(body)
            command = ActorBridgeRequest.model_validate(raw)
            result = await runtime.actor_service.execute(command)
        except (json.JSONDecodeError, UnicodeError, ValidationError):
            return JSONResponse({"error": "invalid_request"}, status_code=422)
        except ServiceError as exc:
            status_code = 401 if exc.code.startswith("ACTOR_") else 409
            return JSONResponse(
                {"error": exc.code, "message": exc.safe_message},
                status_code=status_code,
            )
        return JSONResponse({"ok": True, "data": result.model_dump(mode="json")})

    @asynccontextmanager
    async def lifespan(_: Starlette):
        await runtime.repository.initialize()
        await runtime.guardian_repository.initialize()
        session_manager = runtime.mcp.session_manager.run()
        await session_manager.__aenter__()
        await runtime.jobs.start(video_workers=runtime.settings.video_concurrency)
        await runtime.scheduler.start()
        await runtime.guardian.start()
        if runtime.outbox is not None:
            await runtime.outbox.start()
        runtime.started = True
        try:
            yield
        finally:
            runtime.started = False
            await runtime.guardian.stop()
            await runtime.scheduler.stop()
            await runtime.jobs.stop()
            if runtime.outbox is not None:
                await runtime.outbox.stop()
            if runtime.langbot is not None:
                await runtime.langbot.close()
            await session_manager.__aexit__(None, None, None)

    mcp_app = AuthenticatedASGI(runtime.mcp.streamable_http_app(), runtime.authenticator)
    guardian_api = GuardianAPI(
        runtime.guardian,
        runtime.authenticator,
        runtime.repository.audit,
    )
    application = Starlette(
        routes=[
            Route("/healthz", health, methods=["GET"]),
            Route("/metrics", metrics, methods=["GET"]),
            Route("/bridge/v1/actor", actor, methods=["POST"]),
            *guardian_api.routes(),
            Mount("/", app=mcp_app),
        ],
        lifespan=lifespan,
    )
    return SecurityHeadersASGI(application)
