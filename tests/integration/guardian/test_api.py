from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import SecretStr
from starlette.testclient import TestClient

from sub2api_mcp.app import build_runtime, create_app
from sub2api_mcp.config import AccessTokenConfig, Scope, Settings
from sub2api_mcp.contracts import (
    AccountQuarantineIntent,
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


@dataclass
class FakeOperations:
    async def probe(self) -> ProbeResult:
        return ProbeResult(
            snapshot={
                "version": 1,
                "entries": [
                    {
                        "monitor_id": "11",
                        "name": "Claude",
                        "status": "operational",
                        "group_id": "3",
                        "available_count": 2,
                        "error_count": 0,
                        "temporary_unavailable_count": 0,
                        "closed_count": 1,
                    }
                ],
            },
            report="ok",
        )

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
        del probe
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
        del email
        return None

    async def account_report(self, user_id: str) -> str:
        return user_id


@dataclass
class FakeVideoGenerator:
    async def generate(self, request: SubmitVideoInput) -> VideoOutput:
        del request
        return VideoOutput(url="https://video.example/a.mp4", filename="a.mp4")


def _settings(tmp_path: Path) -> Settings:
    return Settings.model_validate(
        {
            "access_tokens": [
                AccessTokenConfig(
                    name="reader",
                    token=SecretStr("r" * 32),
                    scopes=frozenset[Scope]({"sub2api:read"}),
                ),
                AccessTokenConfig(
                    name="admin",
                    token=SecretStr("a" * 32),
                    scopes=frozenset[Scope]({"sub2api:admin"}),
                ),
            ],
            "sub2api_admin_key": "k" * 32,
            "database_path": tmp_path / "state.db",
            "scheduler_enabled": False,
        }
    )


def test_guardian_ui_and_read_api_authentication(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        page = client.get("/guardian/")
        unauthorized = client.get("/api/guardian/v1/overview")
        overview = client.get("/api/guardian/v1/overview", headers={"X-API-Key": "r" * 32})

    assert page.status_code == 200
    assert "Sub2API Guardian" in page.text
    assert "content-security-policy" in page.headers
    assert "localStorage" not in page.text
    assert unauthorized.status_code == 401
    assert unauthorized.json()["ok"] is False
    assert overview.status_code == 200
    assert overview.json()["data"]["observe_only"] is True


def test_guardian_ui_uses_the_persisted_light_design_system(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        page = client.get("/guardian/")
        styles = client.get("/guardian/assets/app.css")
        script = client.get("/guardian/assets/app.js")

    normalized_css = styles.text.casefold()
    assert page.text.count('class="nav-icon"') == 10
    assert "color-scheme: light" in normalized_css
    assert "--color-background: #f8fafc" in normalized_css
    assert "#07101c" not in normalized_css
    assert 'id="sampling-mode"' in page.text
    assert 'id="sampling-snapshot-age"' in page.text
    assert 'id="probe-budget-requests"' in page.text
    assert 'id="ownership-table"' in page.text
    assert 'id="rollout-stage"' in page.text
    assert 'id="rollout-advance"' in page.text
    assert 'id="rollout-stop"' in page.text
    assert "/sampling/status" in script.text
    assert "/probe-budget" in script.text
    assert "/write-ownership" in script.text
    assert "/explanation" in script.text
    assert "/rollout/advance" in script.text
    assert "/rollout/stop" in script.text


def test_policy_revision_and_writeback_safety_gate(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    admin = {"X-API-Key": "a" * 32}

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        denied = client.patch(
            "/api/guardian/v1/policy",
            headers={"X-API-Key": "r" * 32, "If-Match": "1"},
            json={"scan_interval_seconds": 30},
        )
        updated = client.patch(
            "/api/guardian/v1/policy",
            headers={**admin, "If-Match": "1"},
            json={"enabled": True, "scan_interval_seconds": 30},
        )
        conflict = client.patch(
            "/api/guardian/v1/policy",
            headers={**admin, "If-Match": "1"},
            json={"scan_interval_seconds": 45},
        )
        unsafe = client.patch(
            "/api/guardian/v1/policy",
            headers={**admin, "If-Match": "2"},
            json={"observe_only": False},
        )

    assert denied.status_code == 403
    assert updated.status_code == 200
    assert updated.json()["data"]["policy"]["revision"] == 2
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "POLICY_REVISION_CONFLICT"
    assert unsafe.status_code == 409
    assert unsafe.json()["error"]["code"] == "WRITEBACK_NOT_APPROVED"


def test_dry_run_populates_groups_channels_events_and_manual_pause(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    admin = {"X-API-Key": "a" * 32, "Idempotency-Key": "test-cycle"}

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        run = client.post("/api/guardian/v1/runs", headers=admin, json={"dry_run": False})
        groups = client.get("/api/guardian/v1/groups", headers={"X-API-Key": "r" * 32})
        channels = client.get("/api/guardian/v1/channels", headers={"X-API-Key": "r" * 32})
        paused = client.post(
            "/api/guardian/v1/channels/11/actions",
            headers={"X-API-Key": "a" * 32, "Idempotency-Key": "pause-11"},
            json={"action": "pause"},
        )
        events = client.get("/api/guardian/v1/events", headers={"X-API-Key": "r" * 32})

    assert run.status_code == 200
    assert run.json()["data"]["result"]["writes_applied"] == 0
    assert groups.json()["data"]["items"][0]["group_id"] == "3"
    assert channels.json()["data"]["items"][0]["channel_id"] == "11"
    assert paused.json()["data"]["manual_control"] == "PAUSED"
    assert events.json()["data"]["items"]


def test_restore_execution_is_unavailable_without_approved_writer(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        preview = client.post(
            "/api/guardian/v1/restores/preview",
            headers={"X-API-Key": "a" * 32},
            json={},
        )
        execute = client.post(
            "/api/guardian/v1/restores",
            headers={"X-API-Key": "a" * 32},
            json={"confirm": True},
        )

    assert preview.status_code == 200
    assert preview.json()["data"]["executable"] is False
    assert execute.status_code == 409
    assert execute.json()["error"]["code"] == "WRITEBACK_NOT_APPROVED"


def test_channel_override_and_temporary_boost_are_persisted(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    admin = {"X-API-Key": "a" * 32}

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        client.post(
            "/api/guardian/v1/runs",
            headers={**admin, "Idempotency-Key": "seed-channel"},
            json={"dry_run": True},
        )
        override = client.patch(
            "/api/guardian/v1/channels/11",
            headers=admin,
            json={
                "priority": 2,
                "load_factor": 80,
                "concurrency": 4,
                "schedule_multiplier": 1.25,
                "probe_model": "claude-test",
            },
        )
        boosted = client.post(
            "/api/guardian/v1/channels/11/actions",
            headers={**admin, "Idempotency-Key": "boost-channel"},
            json={"action": "boost", "minutes": 30},
        )
        replayed = client.post(
            "/api/guardian/v1/channels/11/actions",
            headers={**admin, "Idempotency-Key": "boost-channel"},
            json={"action": "boost", "minutes": 60},
        )

    assert override.status_code == 200
    assert override.json()["data"]["override"]["priority"] == 2
    assert boosted.status_code == 200
    assert boosted.json()["data"]["override"]["boost_until"]
    assert boosted.json()["data"]["override"]["boost_load_delta"] == 1000
    assert replayed.headers["x-idempotent-replay"] == "true"
    assert (
        replayed.json()["data"]["override"]["boost_until"]
        == boosted.json()["data"]["override"]["boost_until"]
    )


@pytest.mark.asyncio
async def test_state_transition_enqueues_existing_all_channel_notification(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    await runtime.repository.initialize()
    await runtime.guardian_repository.initialize()
    await runtime.repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="guardian-status",
            bot_uuid="bot-1",
            target_type=TargetType.GROUP,
            target_id="group-1",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=False,
        )
    )

    result = await runtime.guardian.run_once(dry_run=True, idempotency_key="notification-cycle")
    delivery = await runtime.repository.claim_next_delivery(
        "guardian-test", lease_seconds=30
    )

    assert result["status"] == "SUCCEEDED"
    assert delivery is not None
    text = delivery.payload["notification"]["text"]
    assert "触发时间：" in text
    assert "（北京时间）" in text
    assert "健康分" in text
    assert "置信度" in text
    assert "证据来源" in text
    assert "证据年龄" in text
    assert "目标动作" in text
    assert "探测：PERFECT" in text


@pytest.mark.asyncio
async def test_recovery_budget_threshold_enqueues_one_readable_alert(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    await runtime.repository.initialize()
    await runtime.guardian_repository.initialize()
    await runtime.repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="guardian-budget",
            bot_uuid="bot-1",
            target_type=TargetType.GROUP,
            target_id="group-1",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=False,
        )
    )
    policy = await runtime.guardian_repository.get_policy()
    await runtime.guardian_repository.update_policy(
        policy.model_copy(
            update={
                "recovery_budget": policy.recovery_budget.model_copy(
                    update={"enabled": True, "daily_requests": 5}
                )
            }
        ),
        expected_revision=1,
    )
    for index in range(4):
        await runtime.guardian_repository.record_recovery_probe(
            channel_id=f"channel-{index}",
            model="test-model",
            input_tokens=1,
            output_tokens=1,
            estimated_cost=None,
            priced=False,
            occurred_at=datetime.now(UTC),
        )

    await runtime.guardian.run_once(dry_run=True, idempotency_key="budget-alert")
    first_delivery = await runtime.repository.claim_next_delivery(
        "guardian-budget-test", lease_seconds=30
    )
    assert first_delivery is not None
    await runtime.repository.mark_delivery_succeeded(first_delivery.delivery_id)
    delivery = await runtime.repository.claim_next_delivery(
        "guardian-budget-test", lease_seconds=30
    )
    events = await runtime.guardian_repository.list_events(
        limit=20,
        event_type="RECOVERY_BUDGET_WARNING",
    )

    assert delivery is not None
    text = delivery.payload["notification"]["text"]
    assert "恢复探测预算告警" in text
    assert "请求：4 / 5" in text
    assert "触发时间：" in text
    assert len(events["items"]) == 1


def test_v2_sampling_reads_and_rollout_controls_are_guarded(tmp_path: Path) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    reader = {"X-API-Key": "r" * 32}
    admin = {"X-API-Key": "a" * 32}

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        sampling = client.get("/api/guardian/v1/sampling/status", headers=reader)
        budget = client.get("/api/guardian/v1/probe-budget", headers=reader)
        ownership = client.get("/api/guardian/v1/write-ownership", headers=reader)
        missing_key = client.post(
            "/api/guardian/v1/rollout/advance",
            headers={**admin, "If-Match": "1"},
            json={"confirm": True},
        )
        denied = client.post(
            "/api/guardian/v1/rollout/advance",
            headers={**admin, "If-Match": "1", "Idempotency-Key": "rollout-denied"},
            json={"confirm": False},
        )
        advanced = client.post(
            "/api/guardian/v1/rollout/advance",
            headers={**admin, "If-Match": "1", "Idempotency-Key": "rollout-advance"},
            json={"confirm": True},
        )
        stopped = client.post(
            "/api/guardian/v1/rollout/stop",
            headers={**admin, "If-Match": "2", "Idempotency-Key": "rollout-stop"},
            json={},
        )

    assert sampling.status_code == 200
    assert sampling.json()["data"]["mode"] == "SHARED"
    assert budget.status_code == 200
    assert budget.json()["data"]["enabled"] is False
    assert ownership.status_code == 200
    assert ownership.json()["data"]["items"] == []
    assert missing_key.status_code == 422
    assert missing_key.json()["error"]["code"] == "VALIDATION_ERROR"
    assert denied.status_code == 409
    assert denied.json()["error"]["code"] == "CONFIRMATION_REQUIRED"
    assert advanced.status_code == 200
    assert advanced.json()["data"]["policy"]["rollout"]["stage"] == "LOAD_FACTOR"
    assert stopped.status_code == 200
    assert stopped.json()["data"]["policy"]["rollout"]["stage"] == "OBSERVE"
    assert stopped.json()["data"]["policy"]["observe_only"] is True


def test_field_ownership_can_be_explicitly_transferred_with_audit_guards(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(
        _settings(tmp_path),
        operations=FakeOperations(),
        video_generator=FakeVideoGenerator(),
    )
    admin = {"X-API-Key": "a" * 32}
    mutation_headers = {
        **admin,
        "If-Match": "1",
        "Idempotency-Key": "ownership-11-schedulable-human",
    }

    with TestClient(create_app(runtime), base_url="http://127.0.0.1") as client:
        client.post(
            "/api/guardian/v1/runs",
            headers={**admin, "Idempotency-Key": "ownership-seed"},
            json={"dry_run": True},
        )
        changed = client.post(
            "/api/guardian/v1/channels/11/ownership",
            headers=mutation_headers,
            json={"field_name": "SCHEDULABLE", "owner": "HUMAN"},
        )
        replayed = client.post(
            "/api/guardian/v1/channels/11/ownership",
            headers=mutation_headers,
            json={"field_name": "SCHEDULABLE", "owner": "HUMAN"},
        )
        ownership = client.get(
            "/api/guardian/v1/write-ownership",
            headers={"X-API-Key": "r" * 32},
        )

    assert changed.status_code == 200
    assert changed.json()["data"]["owner"] == "HUMAN"
    assert replayed.headers["x-idempotent-replay"] == "true"
    assert ownership.json()["data"]["items"][0]["owner"] == "HUMAN"
