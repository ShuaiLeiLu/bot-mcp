from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sub2api_mcp.guardian.contracts import (
    GuardianFieldOwner,
    GuardianHealth,
    ManualControl,
    RecoveryProbeBudgetPolicy,
    RecoveryProbeCandidate,
)
from sub2api_mcp.guardian.recovery import select_recovery_probe_candidates
from sub2api_mcp.guardian.repository import GuardianRepository

NOW = datetime(2026, 8, 24, 2, 0, tzinfo=UTC)


def _candidate(channel_id: str, **updates: object) -> RecoveryProbeCandidate:
    values: dict[str, object] = {
        "channel_id": channel_id,
        "health": GuardianHealth.FUSED,
        "manual_control": ManualControl.NONE,
        "fuse_owner": GuardianFieldOwner.GUARDIAN,
        "uniquely_mapped": True,
        "last_probe_at": NOW - timedelta(seconds=301),
    }
    values.update(updates)
    return RecoveryProbeCandidate.model_validate(values)


def test_recovery_selection_excludes_human_ambiguous_and_healthy_channels() -> None:
    result = select_recovery_probe_candidates(
        [
            _candidate("eligible"),
            _candidate("healthy", health=GuardianHealth.HEALTHY),
            _candidate("paused", manual_control=ManualControl.PAUSED),
            _candidate("human", fuse_owner=GuardianFieldOwner.HUMAN),
            _candidate("ambiguous", uniquely_mapped=False),
        ],
        now=NOW,
        policy=RecoveryProbeBudgetPolicy(enabled=True),
        daily_requests_used=0,
        daily_tokens_used=0,
        hourly_requests_by_channel={},
    )

    assert result.selected_channel_ids == ("eligible",)
    assert result.blocked_counts == {
        "ambiguous_mapping": 1,
        "human_control": 2,
        "not_fused": 1,
    }


def test_recovery_selection_stops_at_global_and_channel_budgets() -> None:
    policy = RecoveryProbeBudgetPolicy(enabled=True, concurrency=2)
    exhausted = select_recovery_probe_candidates(
        [_candidate("one")],
        now=NOW,
        policy=policy,
        daily_requests_used=policy.daily_requests,
        daily_tokens_used=0,
        hourly_requests_by_channel={},
    )
    partial = select_recovery_probe_candidates(
        [_candidate("one"), _candidate("two"), _candidate("three")],
        now=NOW,
        policy=policy,
        daily_requests_used=0,
        daily_tokens_used=0,
        hourly_requests_by_channel={"one": policy.per_channel_hourly_requests},
    )

    assert exhausted.selected_channel_ids == ()
    assert exhausted.global_block_reason == "daily_request_budget_exhausted"
    assert partial.selected_channel_ids == ("three", "two")
    assert partial.blocked_counts["channel_hourly_budget_exhausted"] == 1


@pytest.mark.asyncio
async def test_recovery_probe_ledger_reports_requests_tokens_and_blocks(
    tmp_path: Path,
) -> None:
    repository = GuardianRepository(tmp_path / "state.db", clock=lambda: NOW)
    await repository.initialize()

    await repository.record_recovery_probe(
        channel_id="one",
        model="model",
        input_tokens=10,
        output_tokens=2,
        estimated_cost=0.001,
        priced=True,
        occurred_at=NOW,
    )
    await repository.record_recovery_probe_blocked(
        channel_id="two",
        reason="daily_token_budget_exhausted",
        occurred_at=NOW,
    )
    summary = await repository.recovery_probe_budget_summary(NOW.date())

    assert summary == {
        "request_count": 1,
        "total_tokens": 12,
        "estimated_cost": pytest.approx(0.001),
        "blocked_count": 1,
    }
