"""Stable public and persistence contracts for the scheduler service."""

from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class JobType(StrEnum):
    PROBE = "PROBE"
    RECOVERY = "RECOVERY"
    MAINTENANCE = "MAINTENANCE"
    VIDEO = "VIDEO"


class JobStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"


TERMINAL_JOB_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.INTERRUPTED}
)


class JobRecord(StrictModel):
    job_id: str
    job_type: JobType
    status: JobStatus
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    cancel_requested: bool = False
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class JobPage(StrictModel):
    items: list[JobRecord]
    next_cursor: str | None = None


class TargetType(StrEnum):
    PERSON = "person"
    GROUP = "group"


class DeliveryPurpose(StrEnum):
    STATUS = "STATUS"
    RECOVERY_ADMIN = "RECOVERY_ADMIN"
    MAINTENANCE_ADMIN = "MAINTENANCE_ADMIN"
    VIDEO_RESULT = "VIDEO_RESULT"


class MediaPolicy(StrEnum):
    AUTO = "AUTO"
    TEXT_ONLY = "TEXT_ONLY"
    IMAGE = "IMAGE"
    FILE = "FILE"
    LINK = "LINK"


class DeliveryTargetCreate(StrictModel):
    name: str = Field(min_length=1, max_length=100)
    bot_uuid: str = Field(min_length=1, max_length=128)
    target_type: TargetType
    target_id: str = Field(min_length=1, max_length=512)
    purposes: frozenset[DeliveryPurpose] = Field(min_length=1)
    media_policy: MediaPolicy = MediaPolicy.AUTO
    required: bool = True
    enabled: bool = True

    @model_validator(mode="after")
    def protect_administrator_details(self) -> DeliveryTargetCreate:
        administrator_purposes = {
            DeliveryPurpose.RECOVERY_ADMIN,
            DeliveryPurpose.MAINTENANCE_ADMIN,
        }
        if self.target_type is TargetType.GROUP and self.purposes & administrator_purposes:
            raise ValueError("administrator delivery purposes require a person target")
        return self


class DeliveryTargetRecord(DeliveryTargetCreate):
    delivery_target_id: str
    created_at: datetime
    updated_at: datetime


class DeliveryTargetPage(StrictModel):
    items: list[DeliveryTargetRecord]
    next_cursor: str | None = None


class OutboxEventType(StrEnum):
    STATUS_CHANGED = "STATUS_CHANGED"
    RECOVERY_RESULT = "RECOVERY_RESULT"
    MAINTENANCE_RESULT = "MAINTENANCE_RESULT"
    VIDEO_READY = "VIDEO_READY"
    VIDEO_FAILED = "VIDEO_FAILED"


class OutboxEventRecord(StrictModel):
    event_id: str
    event_type: OutboxEventType
    payload: dict[str, Any]
    created_at: datetime


class DeliveryStatus(StrEnum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class ClaimedDelivery(StrictModel):
    delivery_id: str
    event_id: str
    event_type: OutboxEventType
    payload: dict[str, Any]
    target: DeliveryTargetRecord
    attempt: int


class AccountBinding(StrictModel):
    actor_key: str
    user_id: str
    masked_email: str
    bound_at: datetime


class AccountQuarantineReason(StrEnum):
    SLOW_FIRST_TOKEN = "SLOW_FIRST_TOKEN"
    CHANNEL_TEST_FAILED = "CHANNEL_TEST_FAILED"


class QuarantineProbeResult(StrEnum):
    NEVER = "NEVER"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SLOW = "SLOW"
    INVALID = "INVALID"


class QuarantineProbeAttempt(StrictModel):
    account_id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    result: QuarantineProbeResult
    latency_ms: int | None = Field(default=None, ge=0, le=3_600_000)
    recovered: bool = False

    @model_validator(mode="after")
    def validate_probe_attempt(self) -> QuarantineProbeAttempt:
        if self.result is QuarantineProbeResult.NEVER:
            raise ValueError("a probe attempt cannot use NEVER")
        if self.result is QuarantineProbeResult.SLOW and self.latency_ms is None:
            raise ValueError("slow probe attempts require measured latency")
        if self.recovered != (self.result is QuarantineProbeResult.SUCCESS):
            raise ValueError("successful probe attempts require verified recovery")
        return self


class AccountQuarantineRecord(StrictModel):
    account_id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    reason: AccountQuarantineReason
    group_ids: tuple[str, ...] = Field(min_length=1, max_length=100)
    threshold_ms: int = Field(ge=1, le=3_600_000)
    observed_count: int = Field(ge=1, le=1_000_000)
    quarantined_at: datetime
    last_probe_at: datetime | None = None
    last_probe_latency_ms: int | None = Field(default=None, ge=0, le=3_600_000)
    last_probe_result: QuarantineProbeResult = QuarantineProbeResult.NEVER

    @field_validator("group_ids")
    @classmethod
    def validate_group_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not re.fullmatch(r"[1-9][0-9]{0,19}", item) for item in value):
            raise ValueError("group IDs must be positive decimal identifiers")
        normalized = tuple(sorted(set(value), key=int))
        if normalized != value:
            raise ValueError("group IDs must be unique and numerically sorted")
        return value

    @field_validator("quarantined_at", "last_probe_at")
    @classmethod
    def validate_quarantine_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("quarantine timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_probe_fields(self) -> AccountQuarantineRecord:
        if self.last_probe_result is QuarantineProbeResult.NEVER:
            if self.last_probe_at is not None or self.last_probe_latency_ms is not None:
                raise ValueError("an unprobed quarantine cannot have probe observations")
        elif self.last_probe_at is None:
            raise ValueError("last_probe_at is required after a quarantine probe")
        if (
            self.last_probe_result is QuarantineProbeResult.SLOW
            and self.last_probe_latency_ms is None
        ):
            raise ValueError("slow quarantine probes require measured latency")
        return self


class AccountQuarantinePage(StrictModel):
    items: list[AccountQuarantineRecord]
    next_cursor: str | None = None


class MaintenanceOutcomeCode(StrEnum):
    QUARANTINED = "QUARANTINED"
    MINIMUM_POOL_PROTECTED = "MINIMUM_POOL_PROTECTED"
    NO_HEALTHY_ACCOUNT = "NO_HEALTHY_ACCOUNT"
    AMBIGUOUS_GROUP_MAPPING = "AMBIGUOUS_GROUP_MAPPING"
    SWEEP_LIMIT_REACHED = "SWEEP_LIMIT_REACHED"


class MaintenanceOutcome(StrictModel):
    outcome: MaintenanceOutcomeCode
    account_id: str | None = Field(default=None, pattern=r"^[1-9][0-9]{0,19}$")
    account_name: str | None = Field(default=None, min_length=1, max_length=200)
    reason: AccountQuarantineReason | None = None
    group_ids: tuple[str, ...] = Field(default=(), max_length=100)
    group_id: str | None = Field(default=None, pattern=r"^[1-9][0-9]{0,19}$")
    group_name: str | None = Field(default=None, min_length=1, max_length=200)
    threshold_ms: int | None = Field(default=None, ge=1, le=3_600_000)
    observed_count: int | None = Field(default=None, ge=1, le=1_000_000)

    @field_validator("group_ids")
    @classmethod
    def validate_outcome_group_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not re.fullmatch(r"[1-9][0-9]{0,19}", item) for item in value):
            raise ValueError("group IDs must be positive decimal identifiers")
        if tuple(sorted(set(value), key=int)) != value:
            raise ValueError("group IDs must be unique and numerically sorted")
        return value

    @model_validator(mode="after")
    def validate_outcome_fields(self) -> MaintenanceOutcome:
        if self.outcome is MaintenanceOutcomeCode.QUARANTINED and (
            self.account_id is None
            or self.account_name is None
            or self.reason is None
            or not self.group_ids
            or self.threshold_ms is None
            or self.observed_count is None
        ):
            raise ValueError("quarantined outcomes require complete marker fields")
        if self.outcome is not MaintenanceOutcomeCode.QUARANTINED and self.group_ids:
            raise ValueError("non-quarantine outcomes cannot create marker groups")
        return self


class NotificationPayload(StrictModel):
    text: str = Field(min_length=1, max_length=10000)
    image_url: str | None = Field(default=None, max_length=2048)
    image_base64: str | None = Field(default=None, max_length=16 * 1024 * 1024)
    file_url: str | None = Field(default=None, max_length=2048)
    file_name: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("image_url", "file_url")
    @classmethod
    def validate_external_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("notification URLs must be absolute HTTPS URLs")
        return value

    @model_validator(mode="after")
    def validate_file_name(self) -> NotificationPayload:
        if self.file_url is not None and self.file_name is None:
            raise ValueError("file_name is required with file_url")
        return self


class DeliveryResult(StrictModel):
    used_fallback: bool = False


class LangBotBot(StrictModel):
    uuid: str
    name: str
    adapter: str


class SubmitVideoInput(StrictModel):
    prompt: str = Field(min_length=1, max_length=2000)
    length: int = Field(default=22, ge=1, le=3600)
    steps: int = Field(default=20, ge=1, le=100)
    width: int = Field(default=768, ge=64, le=2048)
    height: int = Field(default=448, ge=64, le=2048)

    @field_validator("prompt")
    @classmethod
    def normalize_prompt(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("video prompt is required")
        if any(ord(character) < 32 and character not in "\n\r\t" for character in normalized):
            raise ValueError("video prompt contains invalid control characters")
        return normalized


class VideoOutput(StrictModel):
    url: str = Field(min_length=1, max_length=2048)
    filename: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

    @field_validator("url")
    @classmethod
    def validate_video_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.casefold().endswith(".mp4")
        ):
            raise ValueError("video output must be an absolute HTTPS MP4 URL")
        return value


class VideoSubmission(StrictModel):
    job: JobRecord
    queue_count: int = Field(ge=1)


class ProbeResult(StrictModel):
    snapshot: dict[str, Any]
    report: str = Field(min_length=1, max_length=50000)
    image_base64: str | None = Field(default=None, max_length=16 * 1024 * 1024)
    guardian_snapshot: dict[str, Any] | None = None
    captured_at: datetime | None = None

    @model_validator(mode="after")
    def validate_guardian_snapshot_time(self) -> ProbeResult:
        if (self.guardian_snapshot is None) != (self.captured_at is None):
            raise ValueError("guardian_snapshot and captured_at must be supplied together")
        if self.captured_at is not None and self.captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        return self


class OutboxPayload(StrictModel):
    notification: NotificationPayload
    coalesce_key: str | None = Field(
        default=None,
        alias="coalesceKey",
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    )
    delivered_snapshot: dict[str, Any] | None = Field(
        default=None, alias="deliveredSnapshot"
    )
