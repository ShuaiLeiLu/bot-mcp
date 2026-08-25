from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import SecretStr
from starlette.testclient import TestClient

from sub2api_mcp.app import build_runtime, create_app
from sub2api_mcp.config import AccessTokenConfig, Scope, Settings
from sub2api_mcp.contracts import (
    AccountQuarantineIntent,
    AccountQuarantineRecord,
    ProbeResult,
    QuarantineProbeAttempt,
    QuarantineProbeResult,
    SubmitVideoInput,
    VideoOutput,
)


@dataclass
class FakeOperations:
    async def probe(self) -> ProbeResult:
        return ProbeResult(snapshot={"entries": []}, report="no channels")

    async def recover(
        self,
        *,
        excluded_account_ids: frozenset[str] = frozenset(),
    ) -> list[dict[str, object]]:
        return []

    async def maintain(
        self,
        probe: ProbeResult,
        *,
        excluded_account_ids: frozenset[str] = frozenset(),
        before_quarantine: Callable[[dict[str, object]], Awaitable[None]] | None = None,
        after_quarantine: Callable[[str, bool, bool], Awaitable[None]] | None = None,
    ) -> list[dict[str, object]]:
        return []

    async def probe_quarantined(
        self,
        marker: AccountQuarantineRecord,
        *,
        before_restore: Callable[[str], Awaitable[None]] | None = None,
        after_restore: Callable[[str, bool, bool], Awaitable[None]] | None = None,
    ) -> QuarantineProbeAttempt:
        return QuarantineProbeAttempt(
            account_id=marker.account_id,
            result=QuarantineProbeResult.INVALID,
        )

    async def reconcile_quarantine_intent(
        self,
        intent: AccountQuarantineIntent,
    ) -> str:
        return "KEEP"

    async def reconcile_quarantine_restore(
        self,
        marker: AccountQuarantineRecord,
    ) -> str:
        return "KEEP"

    async def find_active_account(self, email: str):  # type: ignore[no-untyped-def]
        return None

    async def account_report(self, user_id: str) -> str:
        return f"report {user_id}"


@dataclass
class FakeVideoGenerator:
    async def generate(self, request: SubmitVideoInput) -> VideoOutput:
        return VideoOutput(url="https://video.example/outputs/a.mp4", filename="a.mp4")


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "access_tokens": [
            AccessTokenConfig(
                name="test",
                token=SecretStr("t" * 32),
                scopes=frozenset[Scope](
                    {"sub2api:read", "sub2api:write", "sub2api:admin"}
                ),
            ),
            AccessTokenConfig(
                name="actor",
                token=SecretStr("a" * 32),
                scopes=frozenset[Scope]({"sub2api:actor"}),
            ),
        ],
        "sub2api_admin_key": "k" * 32,
        "database_path": tmp_path / "state.db",
        "scheduler_enabled": False,
        "video_enabled": True,
    }
    values.update(overrides)
    return Settings.model_validate(values)


def test_health_auth_and_metrics_routes(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    app = create_app(runtime)

    with TestClient(app, base_url="http://127.0.0.1") as client:
        health = client.get("/healthz")
        unauthenticated_mcp = client.post("/mcp", json={})
        authenticated_mcp = client.post(
            "/mcp",
            headers={"X-API-Key": "t" * 32},
            json={},
        )
        unauthenticated_metrics = client.get("/metrics")
        actor_metrics = client.get("/metrics", headers={"X-API-Key": "a" * 32})
        metrics = client.get("/metrics", headers={"Authorization": "Bearer " + "t" * 32})

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.headers["x-content-type-options"] == "nosniff"
    assert health.headers["x-frame-options"] == "DENY"
    assert unauthenticated_mcp.status_code == 401
    assert authenticated_mcp.status_code != 401
    assert authenticated_mcp.headers["x-request-id"]
    assert unauthenticated_metrics.status_code == 401
    assert actor_metrics.status_code == 403
    assert metrics.status_code == 200
    assert "sub2api_mcp_calls_total" in metrics.text


def test_actor_route_is_not_exposed_when_disabled(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        response = client.post("/bridge/v1/actor", json={})

    assert response.status_code == 404


def test_mcp_accepts_a_configured_container_host(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        allowed_hosts=[
            "127.0.0.1:*",
            "localhost:*",
            "[::1]:*",
            "sub2api-scheduler-mcp:*",
        ],
    )
    runtime = build_runtime(
        settings,
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )

    with TestClient(
        create_app(runtime),
        base_url="http://sub2api-scheduler-mcp:5310",
    ) as client:
        response = client.post(
            "/mcp",
            headers={"X-API-Key": "t" * 32},
            json={},
        )

    assert response.status_code != 421
    assert response.status_code != 401
