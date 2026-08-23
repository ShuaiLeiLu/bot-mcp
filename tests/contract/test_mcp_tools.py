from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from mcp.types import TextContent

from sub2api_mcp.auth import Principal, bind_principal
from sub2api_mcp.contracts import (
    DeliveryPurpose,
    DeliveryTargetCreate,
    MediaPolicy,
    ProbeResult,
    SubmitVideoInput,
    TargetType,
    VideoOutput,
)
from sub2api_mcp.jobs import VideoJobService
from sub2api_mcp.metrics import Metrics
from sub2api_mcp.repository import SqliteRepository
from sub2api_mcp.scheduler import SchedulerPolicy, SchedulerService
from sub2api_mcp.service import Sub2APIService
from sub2api_mcp.tools import Sub2APIMCPServer


@dataclass
class FakeOperations:
    async def probe(self) -> ProbeResult:
        return ProbeResult(snapshot={"entries": []}, report="no channels")

    async def recover(self) -> list[dict[str, object]]:
        return []

    async def maintain(self, probe: ProbeResult) -> list[dict[str, object]]:
        return []

    async def find_active_account(self, email: str):  # type: ignore[no-untyped-def]
        return None

    async def account_report(self, user_id: str) -> str:
        return f"report {user_id}"


@dataclass
class FakeVideoGenerator:
    async def generate(self, request: SubmitVideoInput) -> VideoOutput:
        return VideoOutput(url="https://video.example/outputs/a.mp4", filename="a.mp4")


async def _server(tmp_path: Path) -> Sub2APIMCPServer:
    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    metrics = Metrics.create()
    operations = FakeOperations()
    scheduler = SchedulerService(
        repository,
        operations,
        metrics,
        SchedulerPolicy(enabled=False),
    )
    video = VideoJobService(repository, FakeVideoGenerator(), max_pending=20)
    service = Sub2APIService(
        repository=repository,
        operations=operations,
        scheduler=scheduler,
        video=video,
        langbot=None,
        delivery=None,
    )
    return Sub2APIMCPServer(service, metrics)


async def _call_json(
    server: Sub2APIMCPServer,
    name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    content = await server.mcp.call_tool(name, arguments)
    if isinstance(content, tuple):
        content = content[0]
    assert isinstance(content, list)
    assert isinstance(content[0], TextContent)
    return json.loads(content[0].text)


@pytest.mark.asyncio
async def test_tool_inventory_is_curated_and_deterministic(tmp_path: Path) -> None:
    server = await _server(tmp_path)

    names = [tool.name for tool in await server.mcp.list_tools()]

    assert names == [
        "sub2api_get_status",
        "sub2api_probe_channels",
        "sub2api_get_job",
        "sub2api_list_jobs",
        "sub2api_get_bound_account",
        "sub2api_list_delivery_bots",
        "sub2api_list_delivery_targets",
        "sub2api_set_scheduler_enabled",
        "sub2api_submit_recovery",
        "sub2api_submit_maintenance",
        "sub2api_bind_account",
        "sub2api_unbind_account",
        "sub2api_submit_video",
        "sub2api_cancel_job",
        "sub2api_upsert_delivery_target",
        "sub2api_delete_delivery_target",
        "sub2api_test_delivery_target",
    ]


@pytest.mark.asyncio
async def test_read_tool_returns_stable_json_envelope(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-1"):
        result = await _call_json(server, "sub2api_get_status", {})

    assert result["ok"] is True
    assert result["requestId"] == "request-1"
    assert result["data"]["version"] == "0.1.0"


@pytest.mark.asyncio
async def test_admin_tool_is_denied_to_read_only_principal(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-2"):
        result = await _call_json(
            server,
            "sub2api_set_scheduler_enabled",
            {"enabled": True},
        )

    assert result == {
        "ok": False,
        "requestId": "request-2",
        "error": {
            "code": "FORBIDDEN",
            "message": "The API key lacks the required scope",
            "retryable": False,
        },
    }


@pytest.mark.asyncio
async def test_video_tool_returns_job_id_and_queue_count(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    principal = Principal("writer", frozenset({"sub2api:write"}))

    with bind_principal(principal, "request-3"):
        result = await _call_json(
            server,
            "sub2api_submit_video",
            {
                "prompt": "cat",
                "length": 22,
                "steps": 20,
                "width": 768,
                "height": 448,
            },
        )

    assert result["ok"] is True
    assert result["data"]["queue_count"] == 1
    assert result["data"]["job"]["job_id"]


@pytest.mark.asyncio
async def test_authorization_happens_before_business_parameter_validation(
    tmp_path: Path,
) -> None:
    server = await _server(tmp_path)
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-4"):
        result = await _call_json(
            server,
            "sub2api_submit_video",
            {"prompt": "", "length": -1},
        )

    assert result["error"]["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_read_tools_redact_internal_account_and_platform_ids(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    actor_key = "v1:" + "a" * 64
    await server.service.repository.bind_actor(
        actor_key,
        "internal-user-id",
        "u***@example.com",
    )
    await server.service.repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="target",
            bot_uuid="bot-1",
            target_type=TargetType.PERSON,
            target_id="raw-platform-target-id",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.TEXT_ONLY,
        )
    )
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-5"):
        binding = await _call_json(
            server, "sub2api_get_bound_account", {"actor_key": actor_key}
        )
        targets = await _call_json(server, "sub2api_list_delivery_targets", {})

    serialized = json.dumps([binding, targets])
    assert "internal-user-id" not in serialized
    assert "raw-platform-target-id" not in serialized
    assert binding["data"]["masked_email"] == "u***@example.com"
    assert targets["data"]["items"][0]["target_ref"]
