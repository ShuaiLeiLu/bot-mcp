from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from sub2api_mcp.config import AccessTokenConfig, Scope, Settings


def _token() -> AccessTokenConfig:
    return AccessTokenConfig(
        name="langbot",
        token=SecretStr("a" * 32),
        scopes=frozenset[Scope]({"sub2api:read", "sub2api:write", "sub2api:admin"}),
    )


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "access_tokens": [_token()],
        "sub2api_admin_key": "k" * 32,
        "database_path": tmp_path / "state.db",
    }
    values.update(overrides)
    return Settings.model_validate(values)


def test_settings_are_deployment_neutral_and_safe_by_default(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    assert settings.host == "127.0.0.1"
    assert settings.port == 5310
    assert "sub2api-scheduler-mcp:*" in settings.allowed_hosts
    assert settings.scheduler_enabled is False
    assert settings.langbot_base_url is None
    assert settings.database_path == tmp_path / "state.db"


def test_settings_require_at_least_one_access_token(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="access_tokens"):
        _settings(tmp_path, access_tokens=[])


def test_http_langbot_url_requires_explicit_opt_in(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="LANGBOT_ALLOW_HTTP"):
        _settings(
            tmp_path,
            langbot_base_url="http://langbot.internal:5300",
            langbot_api_key="l" * 32,
        )


def test_http_langbot_url_is_allowed_when_explicitly_configured(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        langbot_base_url="http://langbot.internal:5300",
        langbot_api_key="l" * 32,
        langbot_allow_http=True,
    )

    assert str(settings.langbot_base_url).rstrip("/") == "http://langbot.internal:5300"


def test_actor_bridge_requires_a_long_secret(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="actor_bridge_secret"):
        _settings(
            tmp_path,
            actor_bridge_enabled=True,
            actor_bridge_secret="short",
        )


def test_secret_values_are_redacted_from_repr() -> None:
    token = _token()

    assert "a" * 32 not in repr(token)


def test_invalid_daily_window_clock_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="recovery_window_start"):
        _settings(tmp_path, recovery_window_start="25:99")


def test_blank_optional_environment_values_are_treated_as_unset(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        langbot_base_url="",
        langbot_api_key="",
        actor_bridge_secret="",
    )

    assert settings.langbot_base_url is None
    assert settings.langbot_api_key is None
    assert settings.actor_bridge_secret is None
