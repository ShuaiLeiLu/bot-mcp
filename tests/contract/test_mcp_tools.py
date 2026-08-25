from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from mcp.types import TextContent

from sub2api_mcp.auth import Principal, bind_principal
from sub2api_mcp.contracts import (
    AccountQuarantineReason,
    AccountQuarantineRecord,
    DeliveryPurpose,
    DeliveryTargetCreate,
    MediaPolicy,
    ProbeResult,
    QuarantineProbeAttempt,
    QuarantineProbeResult,
    SubmitVideoInput,
    TargetType,
    VideoOutput,
)
from sub2api_mcp.guardian.engine import GuardianEngine
from sub2api_mcp.guardian.repository import GuardianRepository
from sub2api_mcp.guardian.service import GuardianService
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

    async def probe_quarantined(
        self,
        marker: AccountQuarantineRecord,
    ) -> QuarantineProbeAttempt:
        return QuarantineProbeAttempt(
            account_id=marker.account_id,
            result=QuarantineProbeResult.INVALID,
        )

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
    guardian_repository = GuardianRepository(tmp_path / "state.db")
    await guardian_repository.initialize()
    guardian = GuardianService(
        guardian_repository,
        GuardianEngine(guardian_repository, operations),
    )
    return Sub2APIMCPServer(service, metrics, guardian=guardian)


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
        "sub2api_list_account_quarantines",
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
        "guardian_get_policy",
        "guardian_update_policy",
        "guardian_get_overview",
        "guardian_list_groups",
        "guardian_list_channels",
        "guardian_get_channel",
        "guardian_run_once",
        "guardian_cancel_run",
        "guardian_channel_action",
        "guardian_list_events",
        "guardian_get_probe_spend",
        "guardian_get_sampling_status",
        "guardian_explain_channel_score",
        "guardian_get_write_ownership",
        "guardian_get_probe_budget",
        "guardian_advance_rollout",
        "guardian_stop_writeback",
        "guardian_preview_restore",
        "guardian_execute_restore",
    ]


@pytest.mark.asyncio
async def test_probe_tool_declares_stable_wechat_command_aliases(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    tools = {tool.name: tool for tool in await server.mcp.list_tools()}

    description = tools["sub2api_probe_channels"].description or ""

    assert "`/zs`" in description
    assert "`/zs status`" in description
    assert "`/zs 状态`" in description


@pytest.mark.asyncio
async def test_read_tool_returns_stable_json_envelope(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-1"):
        result = await _call_json(server, "sub2api_get_status", {})

    assert result["ok"] is True
    assert result["requestId"] == "request-1"
    assert result["data"]["version"] == "0.1.0"
    assert result["data"]["account_quarantine_count"] == 0


@pytest.mark.asyncio
async def test_quarantine_listing_is_bounded_typed_and_secret_free(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    await server.service.repository.upsert_account_quarantine(
        AccountQuarantineRecord(
            account_id="997",
            reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
            group_ids=("7",),
            threshold_ms=30_000,
            observed_count=3,
            quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
        )
    )
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-quarantine"):
        result = await _call_json(
            server,
            "sub2api_list_account_quarantines",
            {"limit": 20, "reason": "SLOW_FIRST_TOKEN"},
        )

    assert result["ok"] is True
    assert result["data"]["items"] == [
        {
            "account_id": "997",
            "reason": "SLOW_FIRST_TOKEN",
            "group_ids": ["7"],
            "threshold_ms": 30_000,
            "observed_count": 3,
            "quarantined_at": "2026-08-25T02:00:00Z",
            "last_probe_at": None,
            "last_probe_latency_ms": None,
            "last_probe_result": "NEVER",
        }
    ]
    assert "key" not in json.dumps(result, ensure_ascii=False).casefold()


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
        binding = await _call_json(server, "sub2api_get_bound_account", {"actor_key": actor_key})
        targets = await _call_json(server, "sub2api_list_delivery_targets", {})

    serialized = json.dumps([binding, targets])
    assert "internal-user-id" not in serialized
    assert "raw-platform-target-id" not in serialized
    assert binding["data"]["masked_email"] == "u***@example.com"
    assert targets["data"]["items"][0]["target_ref"]


@pytest.mark.asyncio
async def test_guardian_tools_keep_writeback_disabled(tmp_path: Path) -> None:
    server = await _server(tmp_path)
    principal = Principal("admin", frozenset({"sub2api:admin"}))

    with bind_principal(principal, "request-guardian"):
        run = await _call_json(
            server,
            "guardian_run_once",
            {"dry_run": False, "idempotency_key": "mcp-cycle"},
        )

    assert run["ok"] is True
    assert run["data"]["result"]["writes_applied"] == 0
    assert run["data"]["result"]["observe_only"] is True
