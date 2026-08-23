"""Stable public and persistence contracts for the scheduler service."""

from __future__ import annotations

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


class OutboxPayload(StrictModel):
    notification: NotificationPayload
    delivered_snapshot: dict[str, Any] | None = Field(
        default=None, alias="deliveredSnapshot"
    )
