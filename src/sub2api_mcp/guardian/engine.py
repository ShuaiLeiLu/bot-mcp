"""One bounded Guardian synchronization and dry-run evaluation cycle."""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast, runtime_checkable

from ..contracts import ProbeResult
from .contracts import (
    BreakerPolicy,
    ChannelDecision,
    ChannelDecisionInput,
    ChannelPolicyOverride,
    GroupPolicyOverride,
    GuardianEventType,
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    GuardianSampleSource,
    GuardianStrategy,
    ManualControl,
    RecoveryPolicy,
    UpstreamProbeEntry,
    UpstreamProbeSnapshot,
    WeightCandidate,
    WeightsPolicy,
)
from .repository import GuardianRepository
from .scoring import calculate_health_score
from .state_machine import decide_channel_state
from .weights import allocate_weights


class GuardianOperations(Protocol):
    async def probe(self) -> ProbeResult: ...


@runtime_checkable
class GuardianSnapshotOperations(Protocol):
    async def guardian_snapshot(self) -> dict[str, Any]: ...


def _event_for(entry: UpstreamProbeEntry, policy: GuardianPolicy) -> GuardianEventType:
    if entry.status == "operational":
        if entry.latency_ms is not None and entry.latency_ms > policy.scoring.slow_ttfb_ms:
            return GuardianEventType.SLOW_TTFB
        return GuardianEventType.PERFECT
    if entry.status == "degraded":
        return GuardianEventType.SLOW_TTFB
    if entry.status in {"failed", "error"}:
        return GuardianEventType.PROBE_FAIL
    return GuardianEventType.UPSTREAM_UNKNOWN


def _stored_datetime(details: dict[str, Any], key: str) -> datetime | None:
    value = details.get(key)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _scope_decision(
    entry: UpstreamProbeEntry,
    policy: GuardianPolicy,
    group_override: GroupPolicyOverride | None,
) -> ChannelDecision | None:
    group_id = entry.group_id or "ungrouped"
    scope = policy.scope
    if group_id in scope.excluded_group_ids:
        reason = "excluded_group"
    elif entry.monitor_id in scope.excluded_channel_ids:
        reason = "excluded_channel"
    elif scope.managed_group_mode == "selected" and group_id not in scope.managed_group_ids:
        reason = "outside_managed_groups"
    elif group_override is not None and group_override.enabled is False:
        reason = "group_guard_disabled"
    else:
        reason = ""
    if reason:
        return ChannelDecision(
            health=GuardianHealth.EXCLUDED,
            should_schedule=entry.upstream_schedulable,
            should_probe=False,
            can_auto_recover=False,
            reason=reason,
        )
    if entry.monitor_id in scope.paused_channel_ids:
        return ChannelDecision(
            health=GuardianHealth.MANUALLY_PAUSED,
            should_schedule=False,
            should_probe=True,
            can_auto_recover=False,
            reason="scope_pause",
        )
    return None


def _effective_group_policy(
    policy: GuardianPolicy,
    override: GroupPolicyOverride | None,
) -> tuple[BreakerPolicy, RecoveryPolicy, WeightsPolicy, GuardianStrategy]:
    if override is None:
        return policy.breaker, policy.recovery, policy.weights, policy.strategy
    breaker_updates: dict[str, Any] = {}
    recovery_updates: dict[str, Any] = {}
    weight_updates: dict[str, Any] = {}
    if override.breaker_enabled is not None:
        breaker_updates["enabled"] = override.breaker_enabled
    if override.min_pool_size is not None:
        breaker_updates["min_pool_size"] = override.min_pool_size
    if override.recovery_enabled is not None:
        recovery_updates["enabled"] = override.recovery_enabled
    if override.weights_enabled is not None:
        weight_updates["enabled"] = override.weights_enabled
    if override.weight_budget is not None:
        weight_updates["budget"] = override.weight_budget
    if override.balanced_price_ratio is not None:
        weight_updates["balanced_price_ratio"] = override.balanced_price_ratio
    return (
        policy.breaker.model_copy(update=breaker_updates),
        policy.recovery.model_copy(update=recovery_updates),
        policy.weights.model_copy(update=weight_updates),
        override.strategy or policy.strategy,
    )


class GuardianEngine:
    def __init__(self, repository: GuardianRepository, operations: GuardianOperations) -> None:
        self.repository = repository
        self._operations = operations

    async def run_once(
        self,
        *,
        dry_run: bool = True,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        run = await self.repository.create_run(dry_run=dry_run, idempotency_key=idempotency_key)
        if not cast(bool, run.pop("created")):
            run["idempotent_replay"] = True
            return run
        run_id = cast(str, run["run_id"])
        owner = f"run:{run_id}:{uuid.uuid4()}"
        leased = False
        try:
            policy = await self.repository.get_policy()
            leased = await self.repository.acquire_lease(
                "engine", owner, seconds=max(policy.scan_interval_seconds * 2, 30)
            )
            if not leased:
                return await self.repository.finish_run(
                    run_id,
                    status="FAILED",
                    error_code="GUARDIAN_BUSY",
                    error_message="Another Guardian cycle currently owns the lease",
                )
            if isinstance(self._operations, GuardianSnapshotOperations):
                raw_snapshot = await self._operations.guardian_snapshot()
            else:
                probe = await self._operations.probe()
                raw_snapshot = probe.snapshot
            snapshot = UpstreamProbeSnapshot.model_validate(raw_snapshot)
            result = await self._evaluate(
                snapshot,
                policy,
                requested_dry_run=dry_run,
                run_id=run_id,
            )
            cancelled = bool(result.pop("_cancelled"))
            finished = await self.repository.finish_run(
                run_id,
                status="CANCELLED" if cancelled else "SUCCEEDED",
                result=result,
            )
            finished["idempotent_replay"] = False
            return finished
        except Exception:
            await self.repository.finish_run(
                run_id,
                status="FAILED",
                error_code="GUARDIAN_RUN_FAILED",
                error_message="Guardian could not complete this cycle",
            )
            raise
        finally:
            if leased:
                await self.repository.release_lease("engine", owner)

    async def _evaluate(
        self,
        snapshot: UpstreamProbeSnapshot,
        policy: GuardianPolicy,
        *,
        requested_dry_run: bool,
        run_id: str,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        evaluated: list[
            tuple[
                UpstreamProbeEntry,
                float,
                GuardianHealth,
                ChannelPolicyOverride | None,
            ]
        ] = []
        group_overrides = await self.repository.list_group_overrides()
        group_weight_settings: dict[str, tuple[GuardianStrategy, WeightsPolicy]] = {}
        fuse_count = 0
        transitions = 0
        transition_summaries: list[dict[str, Any]] = []
        expected_changes = 0

        for entry in snapshot.entries:
            active_run = await self.repository.get_run(run_id)
            if active_run is not None and active_run["cancel_requested"]:
                return {
                    "_cancelled": True,
                    "observe_only": policy.observe_only,
                    "requested_dry_run": requested_dry_run,
                    "channels_evaluated": len(evaluated),
                    "state_transitions": transitions,
                    "transitions": transition_summaries,
                    "expected_changes": expected_changes,
                    "writes_applied": 0,
                    "strategy": policy.strategy.value,
                    "weight_candidates": {},
                    "writeback_blocked_reason": "cancelled",
                }
            existing = await self.repository.get_channel(entry.monitor_id)
            existing_details = cast(dict[str, Any], existing["details"] if existing else {})
            manual_control = ManualControl(
                existing["manual_control"] if existing else ManualControl.NONE.value
            )
            channel_override = (
                ChannelPolicyOverride.model_validate(existing["override"])
                if existing is not None and existing.get("override") is not None
                else None
            )
            raw_group_override = group_overrides.get(entry.group_id or "ungrouped")
            group_override = (
                GroupPolicyOverride.model_validate(raw_group_override["policy"])
                if raw_group_override is not None
                else None
            )
            breaker, recovery, weights_policy, strategy = _effective_group_policy(
                policy, group_override
            )
            group_weight_settings[entry.group_id or "ungrouped"] = (
                strategy,
                weights_policy,
            )
            scope_decision = _scope_decision(entry, policy, group_override)
            should_monitor = manual_control is not ManualControl.EXCLUDED and not (
                scope_decision is not None and scope_decision.health is GuardianHealth.EXCLUDED
            )
            event_type = _event_for(entry, policy)
            if should_monitor:
                await self.repository.append_sample(
                    GuardianSample(
                        channel_id=entry.monitor_id,
                        event_type=event_type,
                        score=policy.scoring.event_scores[event_type],
                        occurred_at=now,
                        source=GuardianSampleSource.PROBE,
                        ttfb_ms=entry.latency_ms,
                        message=entry.status,
                    )
                )
            samples = await self.repository.list_samples(
                entry.monitor_id, limit=policy.scoring.long_window
            )
            score = calculate_health_score(
                samples,
                short_window=policy.scoring.short_window,
                long_window=policy.scoring.long_window,
                latest_weight=policy.scoring.latest_weight,
                short_ratio=policy.scoring.short_ratio,
                decay=policy.scoring.decay,
            )
            score_value = (
                score.final_score
                if samples
                else float(existing["score"] if existing is not None else 0)
            )
            recent_events = tuple(item.event_type for item in samples)
            recent_ttfb = tuple(item.ttfb_ms for item in samples if item.ttfb_ms is not None)
            current_health = GuardianHealth(
                existing["health"] if existing else GuardianHealth.PENDING.value
            )
            previous_streak = int(existing_details.get("success_streak") or 0)
            healthy_since = _stored_datetime(existing_details, "healthy_since")
            fused_until = _stored_datetime(existing_details, "fused_until")
            if current_health is GuardianHealth.FUSED:
                if event_type is GuardianEventType.PERFECT and should_monitor:
                    success_streak = previous_streak + 1
                    healthy_since = healthy_since or now
                else:
                    success_streak = 0
                    healthy_since = None
            else:
                success_streak = 0
                for recent in recent_events:
                    if recent is not GuardianEventType.PERFECT:
                        break
                    success_streak += 1
                healthy_since = None
                fused_until = None

            decision = scope_decision
            if decision is None:
                decision = decide_channel_state(
                    ChannelDecisionInput(
                        channel_id=entry.monitor_id,
                        score=score_value,
                        recent_events=recent_events,
                        recent_ttfb_ms=recent_ttfb,
                        current_health=current_health,
                        manual_control=manual_control,
                        schedulable=entry.upstream_schedulable,
                        group_available_count=(
                            entry.available_count if entry.available_count is not None else 1
                        ),
                        success_streak=success_streak,
                        healthy_since=healthy_since,
                        fused_until=fused_until,
                        now=now,
                        breaker=breaker,
                        degrade=policy.degrade,
                        recovery=recovery,
                    )
                )
            if decision.health is GuardianHealth.FUSED:
                automatic_new_fuse = (
                    current_health is not GuardianHealth.FUSED and decision.reason != "manual_fuse"
                )
                if automatic_new_fuse and fuse_count >= breaker.max_switch_per_round:
                    decision = decision.model_copy(
                        update={
                            "health": GuardianHealth.DEGRADED,
                            "should_schedule": True,
                            "reason": "round_fuse_limit",
                        }
                    )
                elif automatic_new_fuse:
                    fuse_count += 1
                    fused_until = now + timedelta(seconds=breaker.fused_cooldown_seconds)
            elif current_health is GuardianHealth.FUSED:
                fused_until = None
                healthy_since = None

            if current_health is not decision.health:
                transitions += 1
                if len(transition_summaries) < 20:
                    transition_summaries.append(
                        {
                            "channel_id": entry.monitor_id,
                            "name": entry.name,
                            "group_id": entry.group_id,
                            "from": current_health.value,
                            "to": decision.health.value,
                            "score": score_value,
                            "latency_ms": entry.latency_ms,
                            "event_type": event_type.value,
                            "reason": decision.reason,
                        }
                    )
                await self.repository.add_event(
                    event_type=f"CHANNEL_{decision.health.value}",
                    severity=(
                        "WARNING"
                        if decision.health
                        in {
                            GuardianHealth.FUSED,
                            GuardianHealth.FORCED_KEEP,
                            GuardianHealth.DEGRADED,
                            GuardianHealth.UPSTREAM_DISABLED,
                        }
                        else "INFO"
                    ),
                    channel_id=entry.monitor_id,
                    group_id=entry.group_id,
                    message=f"{entry.name}: {decision.reason}",
                    details={
                        "from": current_health.value,
                        "to": decision.health.value,
                        "score": score_value,
                        "reason": decision.reason,
                    },
                )
            expected_action = (
                "NO_CHANGE"
                if entry.upstream_schedulable == decision.should_schedule
                else ("ENABLE" if decision.should_schedule else "DISABLE")
            )
            if expected_action != "NO_CHANGE":
                expected_changes += 1
            boost_active = bool(
                channel_override is not None
                and channel_override.boost_until is not None
                and channel_override.boost_until > now
            )
            await self.repository.upsert_channel(
                channel_id=entry.monitor_id,
                name=entry.name,
                group_id=entry.group_id,
                upstream_status=entry.status,
                upstream_schedulable=entry.upstream_schedulable,
                health=decision.health,
                score=score_value,
                latency_ms=entry.latency_ms,
                desired_schedulable=decision.should_schedule,
                manual_control=manual_control,
                details={
                    "reason": decision.reason,
                    "short_score": score.short_score,
                    "long_score": score.long_score,
                    "sample_count": score.sample_count,
                    "event_type": event_type.value,
                    "group_name": entry.group_name,
                    "available_count": entry.available_count,
                    "error_count": entry.error_count,
                    "temporary_unavailable_count": entry.temporary_unavailable_count,
                    "closed_count": entry.closed_count,
                    "expected_action": expected_action,
                    "candidate_weight": None,
                    "success_streak": success_streak,
                    "healthy_since": (
                        healthy_since.isoformat() if healthy_since is not None else None
                    ),
                    "fused_until": (fused_until.isoformat() if fused_until is not None else None),
                    "desired_priority": (
                        1
                        if boost_active
                        else (channel_override.priority if channel_override is not None else None)
                    ),
                    "desired_load_factor": self._desired_load_factor(
                        channel_override, boost_active
                    ),
                    "desired_concurrency": (
                        channel_override.concurrency if channel_override is not None else None
                    ),
                    "probe_model_override": (
                        channel_override.probe_model if channel_override is not None else None
                    ),
                    "boost_active": boost_active,
                },
                seen_at=now,
            )
            evaluated.append((entry, score_value, decision.health, channel_override))

        group_candidates: dict[str, list[WeightCandidate]] = defaultdict(list)
        for entry, score_value, health, channel_override in evaluated:
            if health not in {
                GuardianHealth.FUSED,
                GuardianHealth.EXCLUDED,
                GuardianHealth.MANUALLY_PAUSED,
                GuardianHealth.UPSTREAM_DISABLED,
            }:
                group_candidates[entry.group_id or "ungrouped"].append(
                    WeightCandidate(
                        channel_id=entry.monitor_id,
                        score=score_value,
                        effective_rate=entry.effective_rate,
                        ttfb_p95_ms=entry.latency_ms,
                        schedule_multiplier=(
                            channel_override.schedule_multiplier
                            if channel_override is not None
                            and channel_override.schedule_multiplier is not None
                            else 1
                        ),
                    )
                )
        weights: dict[str, dict[str, float]] = {}
        for group_id, candidates in group_candidates.items():
            strategy, weights_policy = group_weight_settings[group_id]
            weights[group_id] = (
                allocate_weights(
                    candidates,
                    strategy=strategy,
                    policy=weights_policy,
                )
                if weights_policy.enabled
                else {candidate.channel_id: 0.0 for candidate in candidates}
            )
            for channel_id, candidate_weight in weights[group_id].items():
                await self.repository.merge_channel_details(
                    channel_id, {"candidate_weight": candidate_weight}
                )
        return {
            "_cancelled": False,
            "observe_only": policy.observe_only,
            "requested_dry_run": requested_dry_run,
            "channels_evaluated": len(evaluated),
            "state_transitions": transitions,
            "transitions": transition_summaries,
            "expected_changes": expected_changes,
            "writes_applied": 0,
            "strategy": policy.strategy.value,
            "weight_candidates": weights,
            "writeback_blocked_reason": (
                "observe_only" if policy.observe_only else "writeback_adapter_not_enabled"
            ),
        }

    @staticmethod
    def _desired_load_factor(
        override: ChannelPolicyOverride | None, boost_active: bool
    ) -> int | None:
        if override is None:
            return None
        if not boost_active:
            return override.load_factor
        return (override.load_factor or 0) + (override.boost_load_delta or 0)
