from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from probe import MonitorDataError


RECOVERY_TIMEZONE = "Asia/Shanghai"
_CLOCK_PATTERN = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")
_CONTROL_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
_BUCKET_LABELS = {
    "error": "错误",
    "temporary": "临时不可调度",
    "closed": "关闭",
}
_RESULT_LABELS = {
    "recovered": "已恢复正常",
    "test_failed": "测试失败，未调整",
    "recovery_failed": "测试成功，但恢复失败",
}


def _positive_id_text(value: Any, field: str) -> str:
    if isinstance(value, bool):
        raise MonitorDataError(f"invalid {field}")
    text = str(value).strip()
    if not text.isdigit() or len(text) > 20 or int(text) <= 0:
        raise MonitorDataError(f"invalid {field}")
    return text


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MonitorDataError(f"invalid {field}")
    return value


def _parse_clock(value: str, field: str) -> tuple[int, str]:
    text = str(value or "").strip()
    if _CLOCK_PATTERN.fullmatch(text) is None:
        raise ValueError(f"invalid {field}")
    hour, minute = (int(part) for part in text.split(":"))
    return hour * 60 + minute, text


def normalize_daily_window(
    start: str,
    end: str,
    *,
    field_name: str,
) -> tuple[str, str]:
    start_minute, normalized_start = _parse_clock(start, f"{field_name} start")
    end_minute, normalized_end = _parse_clock(end, f"{field_name} end")
    if start_minute == end_minute:
        raise ValueError(f"{field_name} start and end must differ")
    return normalized_start, normalized_end


def normalize_recovery_window(start: str, end: str) -> tuple[str, str]:
    _, normalized_start = _parse_clock(start, "recovery window start")
    _, normalized_end = _parse_clock(end, "recovery window end")
    return normalized_start, normalized_end


def normalize_quiet_hours(start: str, end: str) -> tuple[str, str]:
    return normalize_daily_window(start, end, field_name="quiet hours")


@dataclass(frozen=True, slots=True)
class ActiveDailyWindow:
    window_id: str
    ends_at: datetime


def active_daily_window(
    now: datetime,
    start: str,
    end: str,
    *,
    field_name: str,
    timezone_name: str = RECOVERY_TIMEZONE,
) -> ActiveDailyWindow | None:
    if now.tzinfo is None:
        raise ValueError(f"{field_name} time must be timezone-aware")
    normalized_start, normalized_end = normalize_daily_window(
        start,
        end,
        field_name=field_name,
    )
    start_minute, _ = _parse_clock(normalized_start, f"{field_name} start")
    end_minute, _ = _parse_clock(normalized_end, f"{field_name} end")
    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid {field_name} timezone") from exc

    local_now = now.astimezone(zone)
    current_minute = local_now.hour * 60 + local_now.minute
    window_date = local_now.date()
    if start_minute < end_minute:
        if not start_minute <= current_minute < end_minute:
            return None
        end_date = window_date
    else:
        if current_minute < end_minute:
            window_date -= timedelta(days=1)
        elif current_minute < start_minute:
            return None
        end_date = window_date + timedelta(days=1)
    end_hour, end_minute_value = (int(part) for part in normalized_end.split(":"))
    end_local = datetime.combine(
        end_date,
        datetime_time(end_hour, end_minute_value),
        tzinfo=zone,
    )
    return ActiveDailyWindow(
        window_id=f"{window_date.isoformat()}/{normalized_start}-{normalized_end}",
        ends_at=end_local.astimezone(timezone.utc),
    )


def active_recovery_window(
    now: datetime,
    start: str,
    end: str,
    *,
    timezone_name: str = RECOVERY_TIMEZONE,
) -> ActiveDailyWindow | None:
    if now.tzinfo is None:
        raise ValueError("recovery window time must be timezone-aware")
    normalized_start, normalized_end = normalize_recovery_window(start, end)
    start_minute, _ = _parse_clock(normalized_start, "recovery window start")
    end_minute, _ = _parse_clock(normalized_end, "recovery window end")
    if start_minute == end_minute:
        try:
            zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("invalid recovery window timezone") from exc
        local_now = now.astimezone(zone)
        start_hour, start_minute_value = (
            int(part) for part in normalized_start.split(":")
        )
        window_date = local_now.date()
        start_local = datetime.combine(
            window_date,
            datetime_time(start_hour, start_minute_value),
            tzinfo=zone,
        )
        if local_now < start_local:
            window_date -= timedelta(days=1)
        end_local = datetime.combine(
            window_date + timedelta(days=1),
            datetime_time(start_hour, start_minute_value),
            tzinfo=zone,
        )
        return ActiveDailyWindow(
            window_id=(
                f"{window_date.isoformat()}/{normalized_start}-{normalized_end}"
            ),
            ends_at=end_local.astimezone(timezone.utc),
        )
    return active_daily_window(
        now,
        normalized_start,
        normalized_end,
        field_name="recovery window",
        timezone_name=timezone_name,
    )


def recovery_window_id(
    now: datetime,
    start: str,
    end: str,
    *,
    timezone_name: str = RECOVERY_TIMEZONE,
) -> str | None:
    """Return the daily window identifier when ``now`` is inside the range."""

    active = active_recovery_window(
        now,
        start,
        end,
        timezone_name=timezone_name,
    )
    return active.window_id if active is not None else None


@dataclass(frozen=True, slots=True)
class RecoveryCandidate:
    account_id: str
    name: str
    bucket: str
    status: str
    schedulable: bool
    auto_pause_on_expired: bool = False
    expires_at: float | None = None

    def is_auto_paused_expired(self, now: datetime) -> bool:
        if now.tzinfo is None:
            raise ValueError("recovery candidate time must be timezone-aware")
        return bool(
            self.auto_pause_on_expired
            and self.expires_at is not None
            and self.expires_at <= now.timestamp()
        )


@dataclass(frozen=True, slots=True)
class RecoveryOutcome:
    account_id: str
    name: str
    bucket: str
    result: str

    def __post_init__(self) -> None:
        _positive_id_text(self.account_id, "recovery outcome account id")
        if self.bucket not in _BUCKET_LABELS or self.result not in _RESULT_LABELS:
            raise ValueError("invalid recovery outcome")


def _future_timestamp(value: Any, field: str, now: datetime) -> bool:
    if value is None:
        return False
    if not isinstance(value, str) or len(value) > 100:
        raise MonitorDataError(f"invalid {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorDataError(f"invalid {field}") from exc
    if parsed.tzinfo is None:
        raise MonitorDataError(f"invalid {field}")
    return parsed > now.astimezone(parsed.tzinfo)


def parse_recovery_account_page(
    payload: Any,
    *,
    expected_page: int,
    now: datetime,
) -> tuple[list[RecoveryCandidate], int]:
    if now.tzinfo is None:
        raise ValueError("recovery parser time must be timezone-aware")
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API recovery account request failed")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise MonitorDataError("recovery account data.items must be a list")
    total = _non_negative_int(data.get("total"), "recovery account total")
    page = _non_negative_int(data.get("page"), "recovery account page")
    page_size = _non_negative_int(data.get("page_size"), "recovery account page_size")
    pages = _non_negative_int(data.get("pages"), "recovery account pages")
    expected_pages = max(1, (total + page_size - 1) // page_size) if page_size else 0
    if (
        page != expected_page
        or page < 1
        or page_size < 1
        or pages != expected_pages
        or page > pages
        or len(data["items"]) > page_size
        or len(data["items"]) > total
    ):
        raise MonitorDataError("invalid recovery account pagination")

    candidates: list[RecoveryCandidate] = []
    for item in data["items"]:
        if not isinstance(item, dict):
            raise MonitorDataError("recovery account item must be an object")
        status = item.get("status")
        if status not in {"active", "error", "disabled", "inactive"}:
            raise MonitorDataError("invalid recovery account status")
        schedulable = item.get("schedulable")
        if not isinstance(schedulable, bool):
            raise MonitorDataError("invalid recovery account schedulable")
        auto_pause = item.get("auto_pause_on_expired")
        if not isinstance(auto_pause, bool):
            raise MonitorDataError("invalid recovery account auto_pause_on_expired")
        expires_at = item.get("expires_at")
        if expires_at is not None and (
            isinstance(expires_at, bool) or not isinstance(expires_at, (int, float))
        ):
            raise MonitorDataError("invalid recovery account expires_at")
        if auto_pause and expires_at is not None and expires_at <= now.timestamp():
            continue
        for field in (
            "rate_limit_reset_at",
            "overload_until",
            "temp_unschedulable_until",
        ):
            _future_timestamp(item.get(field), field, now)
        if status != "error":
            continue
        bucket = "error"

        raw_name = item.get("name")
        if not isinstance(raw_name, str):
            raise MonitorDataError("invalid recovery account name")
        name = _CONTROL_PATTERN.sub(" ", raw_name).strip()[:200]
        if not name:
            raise MonitorDataError("invalid recovery account name")
        candidates.append(
            RecoveryCandidate(
                account_id=_positive_id_text(item.get("id"), "recovery account id"),
                name=name,
                bucket=bucket,
                status=status,
                schedulable=schedulable,
                auto_pause_on_expired=auto_pause,
                expires_at=float(expires_at) if expires_at is not None else None,
            )
        )
    return candidates, pages


def account_test_succeeded(body: bytes) -> bool:
    if not isinstance(body, bytes) or not body:
        raise MonitorDataError("invalid account test response")
    try:
        text = body.decode("utf-8").replace("\r\n", "\n")
    except UnicodeDecodeError as exc:
        raise MonitorDataError("invalid account test response") from exc
    if "\r" in text or not text.endswith("\n\n"):
        raise MonitorDataError("invalid account test framing")

    completed: bool | None = None
    for block in text.split("\n\n")[:-1]:
        if not block:
            continue
        data_lines: list[str] = []
        for raw_line in block.split("\n"):
            line = raw_line
            if not line or line.startswith(":"):
                continue
            if not line.startswith("data:"):
                raise MonitorDataError("invalid account test framing")
            data_lines.append(line.removeprefix("data:").strip())
        if not data_lines:
            continue
        if len(data_lines) != 1:
            raise MonitorDataError("invalid account test event")
        raw_event = data_lines[0]
        try:
            event = json.loads(raw_event)
        except json.JSONDecodeError as exc:
            raise MonitorDataError("invalid account test event") from exc
        if not isinstance(event, dict) or not isinstance(event.get("type"), str):
            raise MonitorDataError("invalid account test event")
        if event["type"] == "error":
            return False
        if event["type"] == "test_complete":
            if not isinstance(event.get("success"), bool) or completed is not None:
                raise MonitorDataError("invalid account test completion")
            completed = event["success"]
    return completed is True


def recovered_account_is_normal(
    payload: Any,
    *,
    expected_account_id: str,
    now: datetime,
) -> bool:
    if now.tzinfo is None:
        raise ValueError("recovery verification time must be timezone-aware")
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API recovered account request failed")
    account = payload.get("data")
    if not isinstance(account, dict):
        raise MonitorDataError("recovered account data must be an object")
    if _positive_id_text(account.get("id"), "recovered account id") != _positive_id_text(
        expected_account_id,
        "expected recovered account id",
    ):
        raise MonitorDataError("recovered account id does not match")
    schedulable = account.get("schedulable")
    if not isinstance(schedulable, bool):
        raise MonitorDataError("invalid recovered account schedulable")
    if account.get("status") != "active" or not schedulable:
        return False
    if any(
        _future_timestamp(account.get(field), field, now)
        for field in (
            "rate_limit_reset_at",
            "overload_until",
            "temp_unschedulable_until",
        )
    ):
        return False
    auto_pause = account.get("auto_pause_on_expired")
    if not isinstance(auto_pause, bool):
        raise MonitorDataError("invalid recovered account auto_pause_on_expired")
    expires_at = account.get("expires_at")
    if expires_at is not None:
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
            raise MonitorDataError("invalid recovered account expires_at")
        if auto_pause and expires_at <= now.timestamp():
            return False
    return True


def format_recovery_outcomes(outcomes: list[RecoveryOutcome]) -> str:
    if not outcomes:
        return ""
    lines = ["账号自动恢复："]
    for outcome in outcomes:
        lines.append(
            f"{outcome.name} (#{outcome.account_id})："
            f"{_BUCKET_LABELS[outcome.bucket]} → {_RESULT_LABELS[outcome.result]}"
        )
    return "\n".join(lines)
