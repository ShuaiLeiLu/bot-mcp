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
    PROBE = "PROBE"
    TRAFFIC = "TRAFFIC"


class GuardianHealth(StrEnum):
    PENDING = "PENDING"
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
    enabled: bool = True
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


class UpstreamProbeEntry(StrictModel):
    monitor_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=200)
    status: str = Field(pattern=r"^(operational|degraded|failed|error|unknown)$")
    group_id: str | None = Field(default=None, max_length=128)
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
