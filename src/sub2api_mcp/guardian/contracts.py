"""Strict contracts for Guardian scoring, policy, and state transitions."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class GuardianEventType(StrEnum):
    PERFECT = "PERFECT"
    SLOW_TTFB = "SLOW_TTFB"
    UPSTREAM_UNKNOWN = "UPSTREAM_UNKNOWN"
    GATEWAY_ERROR = "GATEWAY_ERROR"
    QUOTA_EXHAUSTED = "QUOTA_EXHAUSTED"
    PROBE_FAIL = "PROBE_FAIL"
    FATAL = "FATAL"


class GuardianSampleSource(StrEnum):
    # PROBE is retained for persisted V1 samples and is not a V2 writeback source.
    PROBE = "PROBE"
    TRAFFIC = "TRAFFIC"
    SHARED_MONITOR = "SHARED_MONITOR"
    RECOVERY_PROBE = "RECOVERY_PROBE"
    MANUAL_PROBE = "MANUAL_PROBE"


class SamplingMode(StrEnum):
    SHARED = "SHARED"
    ACTIVE = "ACTIVE"


class GuardianFreshness(StrEnum):
    FRESH = "FRESH"
    STALE = "STALE"
    EXPIRED = "EXPIRED"


class GuardianRolloutStage(StrEnum):
    OBSERVE = "OBSERVE"
    LOAD_FACTOR = "LOAD_FACTOR"
    PRIORITY = "PRIORITY"
    SCHEDULABLE = "SCHEDULABLE"


class GuardianFieldName(StrEnum):
    LOAD_FACTOR = "LOAD_FACTOR"
    PRIORITY = "PRIORITY"
    SCHEDULABLE = "SCHEDULABLE"


class GuardianFieldOwner(StrEnum):
    UPSTREAM = "UPSTREAM"
    HUMAN = "HUMAN"
    GUARDIAN = "GUARDIAN"


class GuardianHealth(StrEnum):
    PENDING = "PENDING"
    WARMING_UP = "WARMING_UP"
    STALE = "STALE"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    RATE_LIMITED = "RATE_LIMITED"
    FUSED = "FUSED"
    FORCED_KEEP = "FORCED_KEEP"
    MANUALLY_PAUSED = "MANUALLY_PAUSED"
    EXCLUDED = "EXCLUDED"
    UPSTREAM_DISABLED = "UPSTREAM_DISABLED"


class ManualControl(StrEnum):
    NONE = "NONE"
    PAUSED = "PAUSED"
    EXCLUDED = "EXCLUDED"
    FUSED = "FUSED"


class GuardianStrategy(StrEnum):
    PRICE = "PRICE"
    SPEED = "SPEED"
    BALANCED = "BALANCED"


class AutoApplyPolicy(StrictModel):
    schedulable: bool = False
    priority: bool = False
    load_factor: bool = False


class ScoringPolicy(StrictModel):
    short_window: int = Field(default=10, ge=1, le=1000)
    long_window: int = Field(default=60, ge=1, le=10000)
    latest_weight: float = Field(default=0.5, ge=0.05, le=1)
    short_ratio: float = Field(default=0.7, ge=0.05, le=1)
    decay: float = Field(default=0.5, gt=0, le=1)
    short_window_minutes: int = Field(default=10, ge=1, le=1440)
    long_window_minutes: int = Field(default=120, ge=1, le=10080)
    short_half_life_minutes: float = Field(default=3, gt=0, le=1440)
    long_half_life_minutes: float = Field(default=30, gt=0, le=10080)
    slow_ttfb_ms: int = Field(default=5000, ge=100, le=600000)
    event_scores: dict[GuardianEventType, int] = Field(
        default_factory=lambda: {
            GuardianEventType.PERFECT: 100,
            GuardianEventType.SLOW_TTFB: 65,
            GuardianEventType.UPSTREAM_UNKNOWN: 40,
            GuardianEventType.GATEWAY_ERROR: 25,
            GuardianEventType.QUOTA_EXHAUSTED: 15,
            GuardianEventType.PROBE_FAIL: 10,
            GuardianEventType.FATAL: 0,
        }
    )
    fatal_patterns: tuple[str, ...] = (
        "invalid api key",
        "unauthorized",
        "forbidden",
        "authentication",
        "account not found",
        "no api key",
        "no access token",
        "insufficient",
        "balance",
        "quota exceeded",
        "usage limit",
        "credit",
        "expired",
    )
    gateway_status_codes: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    @model_validator(mode="after")
    def validate_windows(self) -> ScoringPolicy:
        if self.short_window > self.long_window:
            raise ValueError("short_window cannot exceed long_window")
        if self.short_window_minutes > self.long_window_minutes:
            raise ValueError("short_window_minutes cannot exceed long_window_minutes")
        if any(not 0 <= score <= 100 for score in self.event_scores.values()):
            raise ValueError("event scores must be between 0 and 100")
        return self


class BreakerPolicy(StrictModel):
    enabled: bool = True
    http_window: int = Field(default=5, ge=1, le=1000)
    http_failures: int = Field(default=3, ge=1, le=1000)
    http_score_below: float = Field(default=60, ge=0, le=100)
    latency_window: int = Field(default=10, ge=1, le=1000)
    latency_occurrences: int = Field(default=5, ge=1, le=1000)
    latency_ttfb_ms: int = Field(default=15000, ge=100, le=600000)
    max_switch_per_round: int = Field(default=1, ge=1, le=1000)
    fused_cooldown_seconds: int = Field(default=180, ge=0, le=86400)
    min_pool_size: int = Field(default=1, ge=0, le=10000)
    min_pool_score: float = Field(default=3, ge=0, le=100)
    hard_fatal: bool = True
    http_degrade_only: bool = True
    latency_degrade_only: bool = True
    instant_status_codes: frozenset[int] = frozenset()

    @model_validator(mode="after")
    def validate_occurrences(self) -> BreakerPolicy:
        if self.http_failures > self.http_window:
            raise ValueError("http_failures cannot exceed http_window")
        if self.latency_occurrences > self.latency_window:
            raise ValueError("latency_occurrences cannot exceed latency_window")
        return self


class DegradePolicy(StrictModel):
    enabled: bool = True
    score_threshold: float = Field(default=75, ge=0, le=100)
    priority_step: int = Field(default=1, ge=1, le=4)
    load_factor_ratio: float = Field(default=0.5, ge=0.05, le=1)
    min_load_factor: int = Field(default=1, ge=1, le=100000)


class RecoveryPolicy(StrictModel):
    enabled: bool = True
    probe_interval_seconds: int = Field(default=180, ge=30, le=86400)
    target_score: float = Field(default=75, ge=0, le=100)
    success_count: int = Field(default=3, ge=1, le=1000)
    hold_seconds: int = Field(default=60, ge=0, le=86400)


class WeightsPolicy(StrictModel):
    enabled: bool = True
    budget: float = Field(default=400, gt=0, le=1_000_000)
    gate_floor: float = Field(default=40, ge=0, le=100)
    balanced_price_ratio: float = Field(default=0.5, ge=0, le=1)
    change_threshold: float = Field(default=0.1, ge=0.01, le=1)
    cooldown_seconds: int = Field(default=60, ge=0, le=86400)
    min_load_factor: int = Field(default=1, ge=1, le=100000)
    max_load_factor: int = Field(default=100, ge=1, le=100000)
    price_exp: float = Field(default=1, ge=0.1, le=10)
    speed_exp: float = Field(default=1, ge=0.1, le=10)

    @model_validator(mode="after")
    def validate_load_bounds(self) -> WeightsPolicy:
        if self.min_load_factor > self.max_load_factor:
            raise ValueError("min_load_factor cannot exceed max_load_factor")
        return self


class ProbePolicy(StrictModel):
    enabled: bool = False
    interval_seconds: int = Field(default=60, ge=30, le=86400)
    timeout_seconds: int = Field(default=60, ge=5, le=600)
    concurrency: int = Field(default=4, ge=1, le=32)
    traffic_fresh_seconds: int = Field(default=180, ge=10, le=86400)
    model: str = Field(default="", max_length=200)
    prompt: str = Field(default="hi", min_length=1, max_length=2000)
    skip_when_traffic_fresh: bool = False


class TrafficPolicy(StrictModel):
    enabled: bool = True
    refresh_seconds: int = Field(default=60, ge=10, le=86400)
    lookback_minutes: int = Field(default=120, ge=5, le=10080)
    max_samples_per_channel: int = Field(default=60, ge=5, le=200)


class SamplingPolicy(StrictModel):
    mode: SamplingMode = SamplingMode.SHARED
    shared_snapshot_interval_seconds: int = Field(default=60, ge=30, le=3600)
    bucket_seconds: int = Field(default=60, ge=30, le=300)
    fresh_seconds: int = Field(default=180, ge=30, le=3600)
    expire_seconds: int = Field(default=600, ge=60, le=86400)
    min_warmup_buckets: int = Field(default=5, ge=1, le=60)

    @model_validator(mode="after")
    def validate_freshness_bounds(self) -> SamplingPolicy:
        if self.fresh_seconds >= self.expire_seconds:
            raise ValueError("fresh_seconds must be lower than expire_seconds")
        if self.bucket_seconds > self.fresh_seconds:
            raise ValueError("bucket_seconds cannot exceed fresh_seconds")
        return self


class ConfidencePolicy(StrictModel):
    degrade_min: float = Field(default=0.60, ge=0, le=1)
    weight_min: float = Field(default=0.75, ge=0, le=1)
    fuse_min: float = Field(default=0.85, ge=0, le=1)
    recover_min: float = Field(default=0.85, ge=0, le=1)

    @model_validator(mode="after")
    def validate_threshold_order(self) -> ConfidencePolicy:
        if self.degrade_min > self.weight_min:
            raise ValueError("degrade_min cannot exceed weight_min")
        if self.weight_min > self.fuse_min:
            raise ValueError("weight_min cannot exceed fuse_min")
        if self.weight_min > self.recover_min:
            raise ValueError("weight_min cannot exceed recover_min")
        return self


class WritePolicy(StrictModel):
    max_channels_per_run: int = Field(default=1, ge=1, le=1000)
    load_cooldown_seconds: int = Field(default=600, ge=0, le=86400)
    priority_cooldown_seconds: int = Field(default=900, ge=0, le=86400)
    max_relative_step: float = Field(default=0.20, gt=0, le=1)
    min_relative_change: float = Field(default=0.15, ge=0, le=1)
    min_absolute_change: int = Field(default=2, ge=0, le=1_000_000)


class RecoveryProbeBudgetPolicy(StrictModel):
    enabled: bool = False
    interval_seconds: int = Field(default=300, ge=30, le=86400)
    concurrency: int = Field(default=1, ge=1, le=32)
    per_channel_hourly_requests: int = Field(default=12, ge=1, le=1000)
    daily_requests: int = Field(default=50, ge=1, le=100_000)
    daily_tokens: int = Field(default=10_000, ge=1, le=1_000_000_000)


class RolloutPolicy(StrictModel):
    stage: GuardianRolloutStage = GuardianRolloutStage.OBSERVE


class ScopePolicy(StrictModel):
    managed_group_mode: str = Field(default="all", pattern=r"^(all|selected)$")
    managed_group_ids: frozenset[str] = frozenset()
    excluded_group_ids: frozenset[str] = frozenset()
    managed_account_types: frozenset[str] = frozenset()
    managed_platforms: frozenset[str] = frozenset()
    paused_channel_ids: frozenset[str] = frozenset()
    excluded_channel_ids: frozenset[str] = frozenset()


class GuardianPolicy(StrictModel):
    revision: int = Field(default=1, ge=1)
    enabled: bool = False
    observe_only: bool = True
    scan_interval_seconds: int = Field(default=15, ge=5, le=3600)
    strategy: GuardianStrategy = GuardianStrategy.PRICE
    auto_apply: AutoApplyPolicy = Field(default_factory=AutoApplyPolicy)
    scoring: ScoringPolicy = Field(default_factory=ScoringPolicy)
    breaker: BreakerPolicy = Field(default_factory=BreakerPolicy)
    degrade: DegradePolicy = Field(default_factory=DegradePolicy)
    recovery: RecoveryPolicy = Field(default_factory=RecoveryPolicy)
    weights: WeightsPolicy = Field(default_factory=WeightsPolicy)
    probe: ProbePolicy = Field(default_factory=ProbePolicy)
    traffic: TrafficPolicy = Field(default_factory=TrafficPolicy)
    sampling: SamplingPolicy = Field(default_factory=SamplingPolicy)
    confidence: ConfidencePolicy = Field(default_factory=ConfidencePolicy)
    writes: WritePolicy = Field(default_factory=WritePolicy)
    recovery_budget: RecoveryProbeBudgetPolicy = Field(
        default_factory=RecoveryProbeBudgetPolicy
    )
    rollout: RolloutPolicy = Field(default_factory=RolloutPolicy)
    scope: ScopePolicy = Field(default_factory=ScopePolicy)


class GuardianSample(StrictModel):
    channel_id: str = Field(min_length=1, max_length=128)
    event_type: GuardianEventType
    score: int = Field(ge=0, le=100)
    occurred_at: datetime
    source: GuardianSampleSource
    ttfb_ms: int | None = Field(default=None, ge=0, le=3_600_000)
    status_code: int | None = Field(default=None, ge=0, le=999)
    message: str = Field(default="", max_length=1000)


class GuardianScore(StrictModel):
    short_score: float = Field(ge=0, le=100)
    long_score: float = Field(ge=0, le=100)
    final_score: float = Field(ge=0, le=100)
    sample_count: int = Field(ge=0)


class GuardianEvidence(StrictModel):
    source_event_id: str = Field(min_length=1, max_length=256)
    channel_id: str = Field(min_length=1, max_length=128)
    source: GuardianSampleSource
    event_type: GuardianEventType
    score: int = Field(ge=0, le=100)
    occurred_at: datetime
    reliability: float = Field(ge=0, le=1)
    event_count: int = Field(default=1, ge=1, le=1_000_000_000)
    ttfb_ms: int | None = Field(default=None, ge=0, le=3_600_000)
    status_code: int | None = Field(default=None, ge=0, le=999)
    message: str = Field(default="", max_length=1000)

    @model_validator(mode="after")
    def validate_occurred_at(self) -> GuardianEvidence:
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return self


class GuardianEvidenceBucket(StrictModel):
    channel_id: str = Field(min_length=1, max_length=128)
    bucket_at: datetime
    score: float = Field(ge=0, le=100)
    quality: float = Field(ge=0, le=1)
    sources: frozenset[GuardianSampleSource] = Field(min_length=1)
    event_count: int = Field(ge=1, le=1_000_000_000)
    ttfb_p95_ms: int | None = Field(default=None, ge=0, le=3_600_000)

    @model_validator(mode="after")
    def validate_bucket_at(self) -> GuardianEvidenceBucket:
        if self.bucket_at.tzinfo is None:
            raise ValueError("bucket_at must be timezone-aware")
        return self


class GuardianTrafficObservation(StrictModel):
    request_id_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    channel_id: str | None = Field(default=None, min_length=1, max_length=128)
    occurred_at: datetime
    event_type: GuardianEventType
    score: int = Field(ge=0, le=100)
    ttfb_ms: int | None = Field(default=None, ge=0, le=3_600_000)
    status_code: int | None = Field(default=None, ge=0, le=999)
    is_monitor_request: bool = False

    @model_validator(mode="after")
    def validate_observed_at(self) -> GuardianTrafficObservation:
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return self


class TrafficBucketBuildResult(StrictModel):
    buckets: tuple[GuardianEvidenceBucket, ...]
    duplicate_count: int = Field(ge=0)
    excluded_monitor_count: int = Field(ge=0)
    unattributed_count: int = Field(ge=0)


class GuardianScoreV2(StrictModel):
    short_score: float = Field(ge=0, le=100)
    long_score: float = Field(ge=0, le=100)
    health_score: float = Field(ge=0, le=100)
    confidence: float = Field(ge=0, le=1)
    freshness: GuardianFreshness
    evidence_bucket_count: int = Field(ge=0)
    last_evidence_at: datetime | None = None
    warming_up: bool = False

    @model_validator(mode="after")
    def validate_evidence_time(self) -> GuardianScoreV2:
        if self.evidence_bucket_count > 0 and self.last_evidence_at is None:
            raise ValueError("last_evidence_at is required when evidence exists")
        if self.last_evidence_at is not None and self.last_evidence_at.tzinfo is None:
            raise ValueError("last_evidence_at must be timezone-aware")
        return self


class GuardianFieldOwnership(StrictModel):
    channel_id: str = Field(min_length=1, max_length=128)
    field_name: GuardianFieldName
    owner: GuardianFieldOwner
    baseline_value: int | float | bool | str | None = None
    last_guardian_value: int | float | bool | str | None = None
    last_write_at: datetime | None = None

    @model_validator(mode="after")
    def validate_last_write_at(self) -> GuardianFieldOwnership:
        if self.last_write_at is not None and self.last_write_at.tzinfo is None:
            raise ValueError("last_write_at must be timezone-aware")
        return self


class ClassifiedSample(StrictModel):
    event_type: GuardianEventType
    score: int = Field(ge=0, le=100)
    safe_message: str = Field(default="", max_length=500)


class ChannelDecisionInput(StrictModel):
    channel_id: str
    score: float = Field(ge=0, le=100)
    recent_events: tuple[GuardianEventType, ...] = ()
    recent_ttfb_ms: tuple[int, ...] = ()
    current_health: GuardianHealth = GuardianHealth.PENDING
    confidence: float = Field(default=1, ge=0, le=1)
    freshness: GuardianFreshness = GuardianFreshness.FRESH
    warming_up: bool = False
    fatal_confirmed: bool = False
    guardian_owned_fuse: bool = True
    manual_control: ManualControl = ManualControl.NONE
    schedulable: bool = True
    group_available_count: int = Field(default=1, ge=0)
    success_streak: int = Field(default=0, ge=0)
    healthy_since: datetime | None = None
    fused_until: datetime | None = None
    now: datetime
    breaker: BreakerPolicy = Field(default_factory=BreakerPolicy)
    degrade: DegradePolicy = Field(default_factory=DegradePolicy)
    recovery: RecoveryPolicy = Field(default_factory=RecoveryPolicy)
    confidence_policy: ConfidencePolicy = Field(default_factory=ConfidencePolicy)


class ChannelDecision(StrictModel):
    health: GuardianHealth
    should_schedule: bool
    should_probe: bool
    can_auto_recover: bool
    reason: str


class WeightCandidate(StrictModel):
    channel_id: str
    score: float = Field(ge=0, le=100)
    effective_rate: float | None = Field(default=None, ge=0)
    ttfb_p95_ms: int | None = Field(default=None, ge=0)
    schedule_multiplier: float = Field(default=1, ge=0, le=10_000)


class UpstreamProbeEntry(StrictModel):
    monitor_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=200)
    status: str = Field(pattern=r"^(operational|degraded|failed|error|unknown)$")
    group_id: str | None = Field(default=None, max_length=128)
    group_name: str | None = Field(default=None, max_length=200)
    available_count: int | None = Field(default=None, ge=0)
    error_count: int | None = Field(default=None, ge=0)
    temporary_unavailable_count: int | None = Field(default=None, ge=0)
    closed_count: int | None = Field(default=None, ge=0)
    latency_ms: int | None = Field(default=None, ge=0, le=3_600_000)
    effective_rate: float | None = Field(default=None, ge=0)
    upstream_schedulable: bool = True

    @model_validator(mode="after")
    def validate_group_counts(self) -> UpstreamProbeEntry:
        counts = (
            self.available_count,
            self.error_count,
            self.temporary_unavailable_count,
            self.closed_count,
        )
        if any(value is None for value in counts) != all(value is None for value in counts):
            raise ValueError("upstream group counts must be complete")
        if (self.group_id is None) != all(value is None for value in counts):
            raise ValueError("upstream group and counts must be supplied together")
        return self


class UpstreamProbeSnapshot(StrictModel):
    version: int = Field(default=1, ge=1, le=1)
    entries: tuple[UpstreamProbeEntry, ...] = Field(max_length=10_000)


class GroupPolicyOverride(StrictModel):
    enabled: bool | None = None
    strategy: GuardianStrategy | None = None
    min_pool_size: int | None = Field(default=None, ge=0, le=10_000)
    weight_budget: float | None = Field(default=None, gt=0, le=1_000_000)
    balanced_price_ratio: float | None = Field(default=None, ge=0, le=1)
    breaker_enabled: bool | None = None
    recovery_enabled: bool | None = None
    weights_enabled: bool | None = None
    probe_enabled: bool | None = None
    probe_interval_seconds: int | None = Field(default=None, ge=30, le=86_400)
    probe_model: str | None = Field(default=None, max_length=200)


class ChannelPolicyOverride(StrictModel):
    priority: int | None = Field(default=None, ge=1, le=5)
    load_factor: int | None = Field(default=None, ge=1, le=1_000_000)
    concurrency: int | None = Field(default=None, ge=1, le=10_000)
    schedule_multiplier: float | None = Field(default=None, ge=0, le=10_000)
    probe_model: str | None = Field(default=None, max_length=200)
    boost_until: datetime | None = None
    boost_load_delta: int | None = Field(default=None, ge=1, le=100_000)
