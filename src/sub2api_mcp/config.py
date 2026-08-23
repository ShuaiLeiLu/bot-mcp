"""Validated runtime configuration loaded only from environment inputs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

Scope = Literal[
    "sub2api:read",
    "sub2api:write",
    "sub2api:admin",
    "sub2api:actor",
]


class AccessTokenConfig(BaseModel):
    """One opaque API token and its fixed authorization scopes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    token: SecretStr = Field(min_length=32, max_length=512)
    scopes: frozenset[Scope] = Field(min_length=1)


def _default_core_root() -> Path:
    return Path(__file__).resolve().parents[2] / "core"


class Settings(BaseSettings):
    """Deployment-neutral settings for the MCP process."""

    model_config = SettingsConfigDict(
        env_prefix="SUB2API_MCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    host: str = Field(default="127.0.0.1", min_length=1, max_length=255)
    port: int = Field(default=5310, ge=1, le=65535)
    database_path: Path = Path("data/sub2api-mcp.db")
    legacy_core_root: Path = Field(default_factory=_default_core_root)

    access_tokens: list[AccessTokenConfig] = Field(min_length=1)
    sub2api_admin_key: SecretStr = Field(min_length=16, max_length=2048)
    sub2api_timeout_seconds: int = Field(default=10, ge=1, le=30)

    scheduler_enabled: bool = False
    probe_interval_seconds: int = Field(default=60, ge=10, le=86400)
    scheduler_lease_seconds: int = Field(default=120, ge=30, le=3600)
    quiet_hours_enabled: bool = False
    quiet_hours_start: str = "23:00"
    quiet_hours_end: str = "08:00"

    recovery_enabled: bool = False
    recovery_window_start: str = "02:00"
    recovery_window_end: str = "05:00"
    recovery_max_accounts_per_run: int = Field(default=5, ge=1, le=20)

    channel_account_sweep_enabled: bool = False
    channel_account_sweep_max_accounts: int = Field(default=1000, ge=1, le=1000)
    log_account_guard_enabled: bool = False
    log_error_threshold: int = Field(default=3, ge=1, le=1000)
    log_slow_first_token_threshold: int = Field(default=3, ge=1, le=1000)
    slow_first_token_ms: int = Field(default=30000, ge=1, le=600000)
    log_window_minutes: int = Field(default=30, ge=1, le=1440)

    langbot_base_url: str | None = None
    langbot_api_key: SecretStr | None = None
    langbot_allow_http: bool = False
    langbot_timeout_seconds: int = Field(default=15, ge=1, le=120)

    video_enabled: bool = False
    video_api_url: str = "https://h3.fzypod.com:9090/v1/video/generations"
    video_length: int = Field(default=22, ge=1, le=3600)
    video_width: int = Field(default=768, ge=64, le=2048)
    video_height: int = Field(default=448, ge=64, le=2048)
    video_steps: int = Field(default=20, ge=1, le=100)
    video_max_pending: int = Field(default=20, ge=1, le=100)
    video_concurrency: int = Field(default=2, ge=1, le=8)

    actor_bridge_enabled: bool = False
    actor_bridge_secret: SecretStr | None = None
    actor_replay_window_seconds: int = Field(default=300, ge=30, le=900)

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator(
        "langbot_base_url",
        "langbot_api_key",
        "actor_bridge_secret",
        mode="before",
    )
    @classmethod
    def blank_optional_values_are_unset(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def validate_cross_field_settings(self) -> Settings:
        token_names = [item.name for item in self.access_tokens]
        token_values = [item.token.get_secret_value() for item in self.access_tokens]
        if len(token_names) != len(set(token_names)):
            raise ValueError("access token names must be unique")
        if len(token_values) != len(set(token_values)):
            raise ValueError("access token values must be unique")

        if (self.langbot_base_url is None) != (self.langbot_api_key is None):
            raise ValueError("langbot_base_url and langbot_api_key must be configured together")
        if self.langbot_base_url is not None:
            parsed = urlsplit(self.langbot_base_url.strip().rstrip("/"))
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError("langbot_base_url must be an absolute HTTP(S) URL")
            if parsed.username or parsed.password or parsed.query or parsed.fragment:
                raise ValueError("langbot_base_url cannot contain credentials, query, or fragment")
            if parsed.scheme == "http" and not self.langbot_allow_http:
                raise ValueError("set LANGBOT_ALLOW_HTTP=true to use an HTTP LangBot URL")
            self.langbot_base_url = self.langbot_base_url.strip().rstrip("/")

        video_url = urlsplit(self.video_api_url.strip().rstrip("/"))
        if video_url.scheme != "https" or not video_url.hostname:
            raise ValueError("video_api_url must be an absolute HTTPS URL")
        if video_url.username or video_url.password or video_url.query or video_url.fragment:
            raise ValueError("video_api_url cannot contain credentials, query, or fragment")
        self.video_api_url = self.video_api_url.strip().rstrip("/")

        if self.actor_bridge_enabled and (
            self.actor_bridge_secret is None
            or len(self.actor_bridge_secret.get_secret_value()) < 32
        ):
            raise ValueError("actor_bridge_secret must contain at least 32 characters")
        for field_name in (
            "quiet_hours_start",
            "quiet_hours_end",
            "recovery_window_start",
            "recovery_window_end",
        ):
            try:
                datetime.strptime(getattr(self, field_name), "%H:%M")
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{field_name} must use HH:MM") from exc
        return self


def load_settings() -> Settings:
    """Load required settings from the configured environment sources."""

    return Settings()  # pyright: ignore[reportCallIssue]
