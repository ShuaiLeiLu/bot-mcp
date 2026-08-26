"""One bounded Guardian synchronization and dry-run evaluation cycle."""

from __future__ import annotations

import hashlib
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast, runtime_checkable

from ..contracts import ProbeResult
from .classifier import classify_monitor_status
from .contracts import (
    AccountRecoveryOwner,
    BreakerPolicy,
    ChannelDecision,
    ChannelDecisionInput,
    ChannelPolicyOverride,
    GroupPolicyOverride,
    GuardianAccountSchedulingState,
    GuardianEventType,
    GuardianEvidence,
    GuardianFieldName,
    GuardianFreshness,
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    GuardianSampleSource,
    GuardianStrategy,
    GuardianWriteOutcome,
    GuardianWriteProposal,
    ManualControl,
    RecoveryPolicy,
    SamplingMode,
    UpstreamProbeEntry,
    UpstreamProbeSnapshot,
    WeightCandidate,
    WeightsPolicy,
)
from .repository import GuardianRepository
from .sampling import fuse_evidence_buckets
from .scoring import calculate_health_score, calculate_health_score_v2
from .state_machine import decide_channel_state
from .weights import allocate_weights_v2, bounded_load_factor, recommend_priority
from .writeback import GuardianFieldWriter, GuardianWritebackService


class GuardianOperations(Protocol):
    async def probe(self) -> ProbeResult: ...


class GuardianSchedulingOperations(GuardianFieldWriter, Protocol):
    async def read_account_scheduling_state(
        self,
        account_id: str,
    ) -> GuardianAccountSchedulingState: ...


@runtime_checkable
class GuardianSnapshotOperations(Protocol):
    async def guardian_snapshot(self) -> dict[str, Any]: ...


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
    def __init__(
        self,
        repository: GuardianRepository,
        operations: GuardianOperations,
        *,
        clock: Callable[[], datetime] | None = None,
        scheduling_operations: GuardianSchedulingOperations | None = None,
    ) -> None:
        self.repository = repository
        self._operations = operations
        self._clock = clock or (lambda: datetime.now(UTC))
        self._scheduling_operations = scheduling_operations
        self._writeback = GuardianWritebackService(
            repository,
            scheduling_operations,
            clock=self._clock,
        )

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
        claimed_snapshot: dict[str, Any] | None = None
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
            if (
                policy.sampling.mode is SamplingMode.SHARED
                and await self.repository.shared_sampling_started()
            ):
                claimed_snapshot = await self.repository.claim_input_snapshot(
                    owner,
                    lease_seconds=max(policy.scan_interval_seconds * 2, 30),
                )
                if claimed_snapshot is None:
                    return await self.repository.finish_run(
                        run_id,
                        status="SUCCEEDED",
                        result={
                            "scheduling_mode": policy.scheduling_mode.value,
                            "requested_dry_run": dry_run,
                            "channels_evaluated": 0,
                            "state_transitions": 0,
                            "transitions": [],
                            "expected_changes": 0,
                            "writes_applied": 0,
                            "strategy": policy.strategy.value,
                            "weight_candidates": {},
                            "writeback_blocked_reason": "no_new_evidence",
                            "no_new_evidence": True,
                            "snapshot_id": None,
                            "duplicate_observations": 0,
                            "traffic_buckets_processed": 0,
                        },
                    )
                raw_snapshot = cast(dict[str, Any], claimed_snapshot["payload"])
            elif isinstance(self._operations, GuardianSnapshotOperations):
                raw_snapshot = await self._operations.guardian_snapshot()
            else:
                probe = await self._operations.probe()
                raw_snapshot = probe.snapshot
            snapshot = UpstreamProbeSnapshot.model_validate(raw_snapshot)
            snapshot_id = None
            captured_at = None
            account_observations_ingested = 0
            if claimed_snapshot is not None:
                snapshot_id = cast(str, claimed_snapshot["snapshot_id"])
                captured_at = datetime.fromisoformat(
                    cast(str, claimed_snapshot["captured_at"]).replace("Z", "+00:00")
                )
                account_observations_ingested = (
                    await self.repository.upsert_account_observations(
                        snapshot_id=snapshot_id,
                        observed_at=captured_at,
                        observations=list(snapshot.accounts),
                    )
                )
            result = await self._evaluate(
                snapshot,
                policy,
                requested_dry_run=dry_run,
                run_id=run_id,
                snapshot_id=snapshot_id,
                captured_at=captured_at,
            )
            cancelled = bool(result.pop("_cancelled"))
            result["no_new_evidence"] = False
            result["snapshot_id"] = (
                claimed_snapshot["snapshot_id"] if claimed_snapshot is not None else None
            )
            result["account_observations_ingested"] = account_observations_ingested
            if claimed_snapshot is not None:
                snapshot_id = cast(str, claimed_snapshot["snapshot_id"])
                if cancelled:
                    await self.repository.release_input_snapshot(snapshot_id, owner)
                    claimed_snapshot = None
                elif not await self.repository.consume_input_snapshot(snapshot_id, owner):
                    raise RuntimeError("Guardian input snapshot claim was lost")
            finished = await self.repository.finish_run(
                run_id,
                status="CANCELLED" if cancelled else "SUCCEEDED",
                result=result,
            )
            finished["idempotent_replay"] = False
            return finished
        except Exception:
            if claimed_snapshot is not None:
                await self.repository.release_input_snapshot(
                    cast(str, claimed_snapshot["snapshot_id"]),
                    owner,
                )
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
        snapshot_id: str | None = None,
        captured_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Guardian engine clock must be timezone-aware")
        evaluated: list[
            tuple[
                UpstreamProbeEntry,
                float,
                GuardianHealth,
                ChannelPolicyOverride | None,
                float,
                GuardianFreshness,
                bool,
            ]
        ] = []
        group_overrides = await self.repository.list_group_overrides()
        group_weight_settings: dict[str, tuple[GuardianStrategy, WeightsPolicy]] = {}
        fuse_count = 0
        transitions = 0
        transition_summaries: list[dict[str, Any]] = []
        expected_changes = 0
        duplicate_observations = 0
        traffic_buckets_processed = 0
        account_recovery_triggers: list[dict[str, str]] = []

        for entry in snapshot.entries:
            active_run = await self.repository.get_run(run_id)
            if active_run is not None and active_run["cancel_requested"]:
                return {
                    "_cancelled": True,
                    "scheduling_mode": policy.scheduling_mode.value,
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
            if snapshot_id is not None and captured_at is not None:
                account_recovery_active = (
                    policy.enabled
                    and policy.account_recovery.enabled
                    and policy.account_recovery.owner is AccountRecoveryOwner.GUARDIAN
                )
                if (
                    account_recovery_active
                    and entry.status in {"failed", "error"}
                    and entry.group_id is not None
                ):
                    episode = await self.repository.open_channel_error_episode(
                        channel_id=entry.monitor_id,
                        group_id=entry.group_id,
                        snapshot_id=snapshot_id,
                        opened_at=captured_at,
                    )
                    if episode.opened_snapshot_id == snapshot_id:
                        account_recovery_triggers.append(
                            {
                                "episode_id": episode.episode_id,
                                "channel_id": episode.channel_id,
                                "group_id": entry.group_id,
                            }
                        )
                else:
                    await self.repository.close_channel_error_episode(
                        entry.monitor_id,
                        closed_at=captured_at,
                    )
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
            event_type = classify_monitor_status(
                status=entry.status,
                latency_ms=entry.latency_ms,
                available_count=entry.available_count,
                policy=policy.scoring,
            )
            if should_monitor:
                if snapshot_id is not None and captured_at is not None:
                    bucket_timestamp = int(captured_at.timestamp())
                    bucket_at = datetime.fromtimestamp(
                        bucket_timestamp
                        - bucket_timestamp % policy.sampling.bucket_seconds,
                        tz=UTC,
                    )
                    inserted = await self.repository.append_evidence(
                        GuardianEvidence(
                            source_event_id=f"{snapshot_id}:{entry.monitor_id}",
                            channel_id=entry.monitor_id,
                            source=GuardianSampleSource.SHARED_MONITOR,
                            event_type=event_type,
                            score=policy.scoring.event_scores[event_type],
                            occurred_at=captured_at,
                            reliability=0.85,
                            ttfb_ms=entry.latency_ms,
                            message=entry.status,
                        ),
                        bucket_at=bucket_at,
                    )
                    if not inserted:
                        duplicate_observations += 1
                else:
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
            assessment = None
            evidence: list[GuardianEvidence] = []
            if snapshot_id is not None:
                since = now - timedelta(minutes=policy.scoring.long_window_minutes)
                evidence = await self.repository.list_evidence(
                    entry.monitor_id,
                    since=since,
                )
                traffic_buckets = await self.repository.list_traffic_buckets(
                    entry.monitor_id,
                    since=since,
                )
                traffic_buckets_processed += len(traffic_buckets)
                fused_buckets = fuse_evidence_buckets(
                    evidence,
                    traffic_buckets,
                    bucket_seconds=policy.sampling.bucket_seconds,
                )
                assessment = calculate_health_score_v2(
                    fused_buckets,
                    now=now,
                    previous_score=float(existing["score"] if existing is not None else 0),
                    scoring=policy.scoring,
                    sampling=policy.sampling,
                )
                score_value = assessment.health_score
                short_score_value = assessment.short_score
                long_score_value = assessment.long_score
                sample_count_value = assessment.evidence_bucket_count
                evidence_sources = sorted(
                    {
                        source.value
                        for bucket in fused_buckets
                        for source in bucket.sources
                    }
                )
                recent_events = tuple(item.event_type for item in evidence)
                recent_ttfb = tuple(
                    item.ttfb_ms for item in evidence if item.ttfb_ms is not None
                )
                score = None
            else:
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
                short_score_value = score.short_score
                long_score_value = score.long_score
                sample_count_value = score.sample_count
                evidence_sources = ["PROBE"]
                recent_events = tuple(item.event_type for item in samples)
                recent_ttfb = tuple(
                    item.ttfb_ms for item in samples if item.ttfb_ms is not None
                )
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
            if decision is None and assessment is not None and assessment.warming_up:
                decision = ChannelDecision(
                    health=GuardianHealth.WARMING_UP,
                    should_schedule=entry.upstream_schedulable,
                    should_probe=False,
                    can_auto_recover=False,
                    reason="warming_up",
                )
            if (
                decision is None
                and assessment is not None
                and assessment.freshness is not GuardianFreshness.FRESH
            ):
                decision = ChannelDecision(
                    health=GuardianHealth.STALE,
                    should_schedule=entry.upstream_schedulable,
                    should_probe=False,
                    can_auto_recover=False,
                    reason=f"evidence_{assessment.freshness.value.casefold()}",
                )
            if decision is None:
                fatal_confirmed = any(
                    item.event_type is GuardianEventType.FATAL
                    and item.source
                    in {
                        GuardianSampleSource.RECOVERY_PROBE,
                        GuardianSampleSource.MANUAL_PROBE,
                    }
                    for item in evidence[:1]
                ) or (
                    sum(
                        item.event_type is GuardianEventType.FATAL
                        and item.source is GuardianSampleSource.TRAFFIC
                        and (now - item.occurred_at).total_seconds() <= 300
                        for item in evidence
                    )
                    >= 2
                )
                decision = decide_channel_state(
                    ChannelDecisionInput(
                        channel_id=entry.monitor_id,
                        score=score_value,
                        recent_events=recent_events,
                        recent_ttfb_ms=recent_ttfb,
                        current_health=current_health,
                        confidence=(
                            assessment.confidence if assessment is not None else 1.0
                        ),
                        freshness=(
                            assessment.freshness
                            if assessment is not None
                            else GuardianFreshness.FRESH
                        ),
                        warming_up=(
                            assessment.warming_up if assessment is not None else False
                        ),
                        fatal_confirmed=fatal_confirmed,
                        guardian_owned_fuse=bool(
                            existing_details.get("fuse_owner", "GUARDIAN") == "GUARDIAN"
                        ),
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
                        confidence_policy=policy.confidence,
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
                            "confidence": (
                                assessment.confidence if assessment is not None else 1.0
                            ),
                            "freshness": (
                                assessment.freshness.value
                                if assessment is not None
                                else GuardianFreshness.FRESH.value
                            ),
                            "evidence_sources": evidence_sources,
                            "evidence_age_seconds": (
                                max(
                                    0,
                                    int(
                                        (now - assessment.last_evidence_at).total_seconds()
                                    ),
                                )
                                if assessment is not None
                                and assessment.last_evidence_at is not None
                                else 0
                            ),
                            "latency_ms": entry.latency_ms,
                            "event_type": event_type.value,
                            "reason": decision.reason,
                            "action": (
                                "NO_CHANGE"
                                if entry.upstream_schedulable == decision.should_schedule
                                else ("ENABLE" if decision.should_schedule else "DISABLE")
                            ),
                            "writes_applied": 0,
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
            confidence_value = assessment.confidence if assessment is not None else 1.0
            freshness_value = (
                assessment.freshness if assessment is not None else GuardianFreshness.FRESH
            )
            baseline_priority = (
                channel_override.priority
                if channel_override is not None and channel_override.priority is not None
                else int(existing_details.get("baseline_priority") or 50)
            )
            desired_priority = (
                1
                if boost_active
                else recommend_priority(
                    baseline=baseline_priority,
                    score=score_value,
                    confidence=confidence_value,
                    freshness=freshness_value,
                    forced_keep=decision.health is GuardianHealth.FORCED_KEEP,
                )
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
                    "short_score": short_score_value,
                    "long_score": long_score_value,
                    "sample_count": sample_count_value,
                    "confidence": assessment.confidence if assessment is not None else 1.0,
                    "freshness_state": (
                        assessment.freshness.value if assessment is not None else "FRESH"
                    ),
                    "evidence_sources": evidence_sources,
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
                    "baseline_priority": baseline_priority,
                    "desired_priority": desired_priority,
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
                confidence=confidence_value,
                freshness_state=freshness_value,
                last_evidence_at=(
                    assessment.last_evidence_at if assessment is not None else now
                ),
                warmup_buckets=(
                    assessment.evidence_bucket_count if assessment is not None else 0
                ),
            )
            evaluated.append(
                (
                    entry,
                    score_value,
                    decision.health,
                    channel_override,
                    confidence_value,
                    freshness_value,
                    decision.should_schedule,
                )
            )

        group_candidates: dict[str, list[WeightCandidate]] = defaultdict(list)
        for (
            entry,
            score_value,
            health,
            channel_override,
            confidence_value,
            _freshness_value,
            _should_schedule,
        ) in evaluated:
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
                        confidence=confidence_value,
                        current_load_factor=(
                            channel_override.load_factor
                            if channel_override is not None
                            and channel_override.load_factor is not None
                            else 1
                        ),
                    )
                )
        weights: dict[str, dict[str, float]] = {}
        for group_id, candidates in group_candidates.items():
            strategy, weights_policy = group_weight_settings[group_id]
            allocation = (
                allocate_weights_v2(
                    candidates,
                    strategy=strategy,
                    policy=weights_policy,
                    confidence_floor=policy.confidence.weight_min,
                )
                if weights_policy.enabled
                else None
            )
            weights[group_id] = (
                {
                    channel_id: float(value)
                    for channel_id, value in allocation.target_load_factors.items()
                }
                if allocation is not None
                else {candidate.channel_id: 0.0 for candidate in candidates}
            )
            for channel_id, candidate_weight in weights[group_id].items():
                await self.repository.merge_channel_details(
                    channel_id,
                    {
                        "candidate_weight": candidate_weight,
                        "desired_load_factor": int(candidate_weight),
                        "unallocated_group_budget": (
                            allocation.unallocated_budget if allocation is not None else 0
                        ),
                    },
                )
        writeback = await self._apply_direct_writes(
            snapshot,
            policy,
            run_id=run_id,
            requested_dry_run=requested_dry_run,
            evaluated=evaluated,
            weights=weights,
        )
        for transition in transition_summaries:
            transition["writes_applied"] = writeback["applied_by_channel"].get(
                str(transition["channel_id"]),
                0,
            )
        return {
            "_cancelled": False,
            "scheduling_mode": policy.scheduling_mode.value,
            "requested_dry_run": requested_dry_run,
            "channels_evaluated": len(evaluated),
            "state_transitions": transitions,
            "transitions": transition_summaries,
            "expected_changes": expected_changes,
            "writes_proposed": writeback["proposed"],
            "writes_applied": writeback["applied"],
            "writes_blocked": writeback["blocked"],
            "writes_failed": writeback["failed"],
            "writeback_reasons": writeback["reasons"],
            "writeback_field_outcomes": writeback["field_outcomes"],
            "strategy": policy.strategy.value,
            "weight_candidates": weights,
            "duplicate_observations": duplicate_observations,
            "traffic_buckets_processed": traffic_buckets_processed,
            "account_recovery_triggers": account_recovery_triggers,
            "writeback_blocked_reason": writeback["global_reason"],
        }

    async def _apply_direct_writes(
        self,
        snapshot: UpstreamProbeSnapshot,
        policy: GuardianPolicy,
        *,
        run_id: str,
        requested_dry_run: bool,
        evaluated: list[
            tuple[
                UpstreamProbeEntry,
                float,
                GuardianHealth,
                ChannelPolicyOverride | None,
                float,
                GuardianFreshness,
                bool,
            ]
        ],
        weights: dict[str, dict[str, float]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "proposed": 0,
            "applied": 0,
            "blocked": 0,
            "failed": 0,
            "no_change": 0,
            "reasons": {},
            "field_outcomes": {},
            "applied_by_channel": {},
            "global_reason": "",
        }

        def count(reason: str, bucket: str = "blocked") -> None:
            result[bucket] = int(result[bucket]) + 1
            reasons = cast(dict[str, int], result["reasons"])
            reasons[reason] = reasons.get(reason, 0) + 1

        if not policy.enabled:
            result["global_reason"] = "guardian_disabled"
            return result
        if requested_dry_run:
            result["global_reason"] = "requested_dry_run"
            return result
        if self._scheduling_operations is None:
            result["global_reason"] = "writeback_adapter_unavailable"
            return result

        by_group: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
        for item in evaluated:
            group_id = item[0].group_id
            if group_id is not None:
                by_group[group_id].append(item)
        account_budget = policy.writes.max_channels_per_run
        accounts_applied = 0
        stop = False
        budget_exhausted = False
        for group_id in sorted(by_group, key=int):
            group_entries = by_group[group_id]
            if len(group_entries) != 1:
                count("ambiguous_monitor_group")
                continue
            (
                entry,
                score,
                health,
                _override,
                confidence,
                freshness,
                should_schedule,
            ) = group_entries[0]
            if health in {
                GuardianHealth.EXCLUDED,
                GuardianHealth.MANUALLY_PAUSED,
                GuardianHealth.STALE,
                GuardianHealth.WARMING_UP,
            }:
                count("channel_not_write_eligible")
                continue
            if confidence < policy.confidence.weight_min:
                count("low_confidence")
                continue
            if freshness is not GuardianFreshness.FRESH:
                count("stale_evidence")
                continue
            desired_weight = int(
                weights.get(group_id, {}).get(entry.monitor_id, 0)
            )
            if desired_weight <= 0:
                count("no_positive_weight")
                continue
            accounts = sorted(
                (
                    account
                    for account in snapshot.accounts
                    if account.group_ids == (group_id,)
                    and not account.expired
                    and not account.temporary_unavailable
                    and account.status.value == "active"
                    and account.schedulable
                ),
                key=lambda account: int(account.account_id),
            )
            for account in accounts:
                if accounts_applied >= account_budget:
                    count("per_run_account_cap")
                    budget_exhausted = True
                    break
                state = await self._scheduling_operations.read_account_scheduling_state(
                    account.account_id
                )
                if not state.success:
                    count("account_state_unavailable")
                    continue
                if (
                    state.status is None
                    or state.status.value != "active"
                    or state.schedulable is not True
                    or state.expired
                    or state.temporary_unavailable
                    or state.priority is None
                    or state.effective_load_factor is None
                ):
                    count("account_not_write_eligible")
                    continue
                targets = (
                    (
                        GuardianFieldName.LOAD_FACTOR,
                        state.load_factor
                        if state.load_factor is not None
                        else state.effective_load_factor,
                        bounded_load_factor(
                            state.effective_load_factor,
                            desired_weight,
                            policy=policy.writes,
                        ),
                        "bounded_group_weight",
                    ),
                    (
                        GuardianFieldName.PRIORITY,
                        state.priority,
                        recommend_priority(
                            baseline=state.priority,
                            score=score,
                            confidence=confidence,
                            freshness=freshness,
                            forced_keep=health is GuardianHealth.FORCED_KEEP,
                        ),
                        "baseline_relative_health",
                    ),
                )
                account_applied = False
                for field_name, current, desired, reason in targets:
                    if current == desired:
                        continue
                    digest = hashlib.sha256(
                        f"{run_id}:{entry.monitor_id}:{account.account_id}:"
                        f"{field_name.value}".encode()
                    ).hexdigest()[:32]
                    proposal = GuardianWriteProposal(
                        channel_id=entry.monitor_id,
                        account_id=account.account_id,
                        field_name=field_name,
                        current_value=current,
                        desired_value=desired,
                        reason=reason,
                        idempotency_key=f"guardian:{digest}",
                    )
                    result["proposed"] = int(result["proposed"]) + 1
                    decision = await self._writeback.apply(proposal, policy=policy)
                    field_outcomes = cast(
                        dict[str, dict[str, int]],
                        result["field_outcomes"],
                    )
                    outcomes = field_outcomes.setdefault(field_name.value, {})
                    outcomes[decision.outcome.value] = (
                        outcomes.get(decision.outcome.value, 0) + 1
                    )
                    if decision.outcome is GuardianWriteOutcome.APPLIED:
                        account_applied = True
                        result["applied"] = int(result["applied"]) + 1
                        applied = cast(dict[str, int], result["applied_by_channel"])
                        applied[entry.monitor_id] = applied.get(entry.monitor_id, 0) + 1
                    elif decision.outcome is GuardianWriteOutcome.NO_CHANGE:
                        result["no_change"] = int(result["no_change"]) + 1
                    elif decision.outcome is GuardianWriteOutcome.FAILED:
                        count(decision.reason, "failed")
                        stop = True
                        break
                    else:
                        count(decision.reason)
                if account_applied:
                    accounts_applied += 1
                if state.schedulable != should_schedule:
                    count("schedulable_owned_by_verified_recovery")
                if stop:
                    break
            if stop or budget_exhausted:
                break
        if stop:
            result["global_reason"] = "verification_failed"
        elif int(result["applied"]) == 0 and int(result["blocked"]) > 0:
            result["global_reason"] = "all_writes_blocked"
        return result

    @staticmethod
    def _desired_load_factor(
        override: ChannelPolicyOverride | None, boost_active: bool
    ) -> int | None:
        if override is None:
            return None
        if not boost_active:
            return override.load_factor
        return (override.load_factor or 0) + (override.boost_load_delta or 0)
