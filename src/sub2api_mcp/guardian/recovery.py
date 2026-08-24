"""Budgeted selection for Guardian-owned fused-channel recovery probes."""

from __future__ import annotations

from collections import Counter
from datetime import datetime

from .contracts import (
    GuardianFieldOwner,
    GuardianHealth,
    ManualControl,
    RecoveryProbeBudgetPolicy,
    RecoveryProbeCandidate,
    RecoveryProbeSelection,
)


def select_recovery_probe_candidates(
    candidates: list[RecoveryProbeCandidate],
    *,
    now: datetime,
    policy: RecoveryProbeBudgetPolicy,
    daily_requests_used: int,
    daily_tokens_used: int,
    hourly_requests_by_channel: dict[str, int],
) -> RecoveryProbeSelection:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if not policy.enabled:
        return RecoveryProbeSelection(
            selected_channel_ids=(),
            blocked_counts={},
            global_block_reason="recovery_probe_disabled",
        )
    if daily_requests_used >= policy.daily_requests:
        return RecoveryProbeSelection(
            selected_channel_ids=(),
            blocked_counts={},
            global_block_reason="daily_request_budget_exhausted",
        )
    if daily_tokens_used >= policy.daily_tokens:
        return RecoveryProbeSelection(
            selected_channel_ids=(),
            blocked_counts={},
            global_block_reason="daily_token_budget_exhausted",
        )

    blocked: Counter[str] = Counter()
    eligible: list[str] = []
    for candidate in sorted(candidates, key=lambda item: item.channel_id):
        if candidate.health is not GuardianHealth.FUSED:
            blocked["not_fused"] += 1
            continue
        if (
            candidate.manual_control is not ManualControl.NONE
            or candidate.fuse_owner is not GuardianFieldOwner.GUARDIAN
        ):
            blocked["human_control"] += 1
            continue
        if not candidate.uniquely_mapped:
            blocked["ambiguous_mapping"] += 1
            continue
        if (
            candidate.last_probe_at is not None
            and (now - candidate.last_probe_at).total_seconds() < policy.interval_seconds
        ):
            blocked["probe_interval"] += 1
            continue
        if (
            hourly_requests_by_channel.get(candidate.channel_id, 0)
            >= policy.per_channel_hourly_requests
        ):
            blocked["channel_hourly_budget_exhausted"] += 1
            continue
        eligible.append(candidate.channel_id)

    remaining_daily = max(0, policy.daily_requests - daily_requests_used)
    limit = min(policy.concurrency, remaining_daily)
    return RecoveryProbeSelection(
        selected_channel_ids=tuple(eligible[:limit]),
        blocked_counts=dict(sorted(blocked.items())),
    )
