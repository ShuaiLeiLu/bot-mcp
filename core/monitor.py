from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo

from client_errors import MonitorRequestError
from maintenance import MaintenancePolicy
from maintenance_gateway import (
    MaintenanceApiAdapterConfig,
    MaintenanceApiAdapterFactory,
)
from probe import (
    AccountGroupState,
    ChannelHealth,
    ChannelProbe,
    GroupAccountCounts,
    GroupDefinition,
    MonitorDataError,
    ProbeUsageRecord,
    aggregate_group_account_counts,
    build_channel_probes,
    format_status_report,
    parse_group_definitions,
    parse_probe_usage_page,
    resolve_channel_group_ids,
)
from probe import (
    parse_channel_monitor_page as _parse_channel_monitor_page,
)
from recovery import (
    RecoveryCandidate,
    RecoveryOutcome,
    account_test_succeeded,
    normalize_quiet_hours,
    normalize_recovery_window,
    parse_recovery_account_page,
    recovered_account_is_normal,
)
from video import VideoGenerationClient, normalize_video_api_url

_LOGGER = logging.getLogger("sub2api_mcp")

class _RejectRedirectHandler(urllib_request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del newurl
        raise urllib_error.HTTPError(req.full_url, code, msg, headers, fp)


@dataclass(frozen=True, slots=True)
class MonitorConfig:
    admin_key: str = field(repr=False)
    bot_uuid: str = ""
    target_type: str = "person"
    target_id: str = ""
    interval_seconds: int = 60
    periodic_enabled: bool = True
    group_notifications_enabled: bool = True
    quiet_hours_enabled: bool = False
    quiet_hours_start: str = "23:00"
    quiet_hours_end: str = "08:00"
    channel_account_sweep_enabled: bool = False
    channel_account_sweep_max_accounts: int = 1000
    log_account_guard_enabled: bool = False
    log_error_threshold: int = 3
    log_slow_first_token_threshold: int = 3
    recovery_enabled: bool = False
    recovery_admin_id: str = ""
    recovery_window_start: str = "02:00"
    recovery_window_end: str = "05:00"
    recovery_max_accounts_per_run: int = 5
    video_enabled: bool = True
    video_api_url: str = VideoGenerationClient.DEFAULT_ENDPOINT
    video_length: int = VideoGenerationClient.DEFAULT_LENGTH
    video_width: int = VideoGenerationClient.DEFAULT_WIDTH
    video_height: int = VideoGenerationClient.DEFAULT_HEIGHT
    video_steps: int = VideoGenerationClient.DEFAULT_STEPS
    video_timeout_seconds: int = VideoGenerationClient.DEFAULT_TIMEOUT_SECONDS

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> MonitorConfig:
        admin_key = str(values.get("api_key") or values.get("admin_key") or "").strip()
        if not admin_key:
            raise ValueError("Sub2API Admin Key is required")
        target_type = str(values.get("target_type") or "person").strip().lower()
        if target_type not in {"person", "group"}:
            raise ValueError("target_type must be person or group")
        bot_uuid = str(values.get("bot_uuid") or "").strip()
        target_id = str(values.get("target_id") or "").strip()
        if bool(bot_uuid) != bool(target_id):
            raise ValueError("bot_uuid and target_id must be configured together")
        recovery_admin_id = str(values.get("recovery_admin_id") or "").strip()
        if len(recovery_admin_id) > 200 or any(
            ord(character) < 32 for character in recovery_admin_id
        ):
            raise ValueError("invalid recovery_admin_id")
        recovery_window_start, recovery_window_end = normalize_recovery_window(
            values.get("recovery_window_start", "02:00"),
            values.get("recovery_window_end", "05:00"),
        )
        quiet_hours_start, quiet_hours_end = normalize_quiet_hours(
            values.get("quiet_hours_start", "23:00"),
            values.get("quiet_hours_end", "08:00"),
        )
        video_api_url = normalize_video_api_url(
            values.get("video_api_url", VideoGenerationClient.DEFAULT_ENDPOINT)
        )

        return cls(
            admin_key=admin_key,
            bot_uuid=bot_uuid,
            target_type=target_type,
            target_id=target_id,
            interval_seconds=_bounded_int(values.get("interval_seconds"), 60, 30, 3600),
            periodic_enabled=values.get("periodic_enabled", True) is not False,
            group_notifications_enabled=(
                values.get("group_notifications_enabled", True) is not False
            ),
            quiet_hours_enabled=values.get("quiet_hours_enabled", False) is True,
            quiet_hours_start=quiet_hours_start,
            quiet_hours_end=quiet_hours_end,
            channel_account_sweep_enabled=(
                values.get("channel_account_sweep_enabled", False) is True
            ),
            channel_account_sweep_max_accounts=_bounded_int(
                values.get("channel_account_sweep_max_accounts"),
                1000,
                1,
                1000,
            ),
            log_account_guard_enabled=(
                values.get("log_account_guard_enabled", False) is True
            ),
            log_error_threshold=_bounded_int(
                values.get("log_error_threshold"),
                3,
                1,
                1000,
            ),
            log_slow_first_token_threshold=_bounded_int(
                values.get("log_slow_first_token_threshold"),
                3,
                1,
                1000,
            ),
            recovery_enabled=values.get("recovery_enabled", False) is True,
            recovery_admin_id=recovery_admin_id,
            recovery_window_start=recovery_window_start,
            recovery_window_end=recovery_window_end,
            recovery_max_accounts_per_run=_bounded_int(
                values.get("recovery_max_accounts_per_run"),
                5,
                1,
                20,
            ),
            video_enabled=values.get("video_enabled", True) is not False,
            video_api_url=video_api_url,
            video_length=_bounded_int(
                values.get("video_length"),
                VideoGenerationClient.DEFAULT_LENGTH,
                1,
                120,
            ),
            video_width=_bounded_int(
                values.get("video_width"),
                VideoGenerationClient.DEFAULT_WIDTH,
                64,
                2048,
            ),
            video_height=_bounded_int(
                values.get("video_height"),
                VideoGenerationClient.DEFAULT_HEIGHT,
                64,
                2048,
            ),
            video_steps=_bounded_int(
                values.get("video_steps"),
                VideoGenerationClient.DEFAULT_STEPS,
                1,
                100,
            ),
            video_timeout_seconds=_bounded_int(
                values.get("video_timeout_seconds"),
                VideoGenerationClient.DEFAULT_TIMEOUT_SECONDS,
                10,
                900,
            ),
        )

    @property
    def has_target(self) -> bool:
        return bool(self.bot_uuid and self.target_id)

    @property
    def recovery_admin_target_id(self) -> str | None:
        if self.recovery_admin_id:
            return self.recovery_admin_id
        if self.target_type == "person" and self.target_id:
            return self.target_id
        return None

    def maintenance_policy(self) -> MaintenancePolicy:
        return MaintenancePolicy(
            channel_account_sweep_enabled=self.channel_account_sweep_enabled,
            channel_account_sweep_max_accounts=self.channel_account_sweep_max_accounts,
            log_account_guard_enabled=self.log_account_guard_enabled,
            log_error_threshold=self.log_error_threshold,
            log_slow_first_token_threshold=self.log_slow_first_token_threshold,
        )


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


@dataclass(frozen=True, slots=True)
class AccountProfile:
    user_id: str
    email: str
    username: str
    status: str
    balance: float


@dataclass(frozen=True, slots=True)
class AccountUsage:
    period: str
    total_cost: float
    total_actual_cost: float
    total_requests: int
    total_tokens: int


def _bounded_text(value: Any, field: str, *, required: bool = False) -> str:
    if value is None:
        if required:
            raise MonitorDataError(f"missing {field}")
        return ""
    text = str(value).strip()
    if required and not text:
        raise MonitorDataError(f"empty {field}")
    return text[:200]


def _optional_number(value: Any, field: str, number_type: type[int | float]):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MonitorDataError(f"invalid {field}")
    return number_type(value)


def normalize_email(value: Any) -> str:
    email = str(value or "").strip().casefold()
    pattern = r"[a-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}@[a-z0-9.-]+\.[a-z]{2,63}"
    if len(email) > 254 or not re.fullmatch(pattern, email):
        raise ValueError("invalid email address")
    return email


def _parse_account_item(item: Any) -> AccountProfile:
    if not isinstance(item, dict):
        raise MonitorDataError("account item must be an object")
    user_id = _bounded_text(item.get("id"), "account id", required=True)
    try:
        email = normalize_email(item.get("email"))
    except ValueError as exc:
        raise MonitorDataError("invalid account email") from exc
    status = _bounded_text(item.get("status"), "account status", required=True).lower()
    balance = _optional_number(item.get("balance"), "account balance", float)
    if balance is None:
        raise MonitorDataError("missing account balance")
    return AccountProfile(
        user_id=user_id,
        email=email,
        username=_bounded_text(item.get("username"), "account username"),
        status=status,
        balance=balance,
    )


def parse_account_search(payload: Any, email: str) -> AccountProfile | None:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API account search failed")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise MonitorDataError("account search data.items must be a list")
    exact_matches = [
        _parse_account_item(item)
        for item in data["items"]
        if isinstance(item, dict) and str(item.get("email") or "").strip().casefold() == email
    ]
    if len(exact_matches) > 1:
        raise MonitorDataError("account search returned duplicate emails")
    return exact_matches[0] if exact_matches else None


def parse_account_profile(payload: Any) -> AccountProfile:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API account request failed")
    return _parse_account_item(payload.get("data"))


def parse_account_usage(payload: Any, period: str) -> AccountUsage:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API account usage request failed")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise MonitorDataError("account usage data must be an object")
    total_cost = _optional_number(data.get("total_cost"), "total_cost", float)
    total_actual_cost = _optional_number(
        data.get("total_actual_cost"),
        "total_actual_cost",
        float,
    )
    total_requests = _optional_number(data.get("total_requests"), "total_requests", int)
    total_tokens = _optional_number(data.get("total_tokens"), "total_tokens", int)
    if (
        total_cost is None
        or total_actual_cost is None
        or total_requests is None
        or total_tokens is None
    ):
        raise MonitorDataError("account usage totals are incomplete")
    return AccountUsage(
        period=period,
        total_cost=total_cost,
        total_actual_cost=total_actual_cost,
        total_requests=total_requests,
        total_tokens=total_tokens,
    )


def _is_valid_sse_data_event(lines: list[bytes]) -> bool:
    data_lines: list[str] = []
    try:
        for raw_line in lines:
            line = raw_line.decode("utf-8").rstrip("\r\n")
            if not line or line.startswith(":"):
                continue
            if not line.startswith("data:"):
                return False
            data_lines.append(line.removeprefix("data:").strip())
        if len(data_lines) != 1:
            return False
        event = json.loads(data_lines[0])
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(event, dict) and isinstance(event.get("type"), str)


class Sub2APIClient:
    """Small fixed-endpoint client that never exposes the Admin Key."""

    API_URL = "https://zhisuanapi.cn/api/v1/admin/channel-monitors"
    # Group identities and scheduler predicates verified against Sub2API 0.1.179.
    # Counts are intentionally recomputed from one account snapshot instead of
    # trusting version-dependent aggregate fields returned by the group endpoint:
    # https://github.com/Wei-Shaw/sub2api/blob/2bc139ab527b4a687546d145dc7bb9063cf14510/backend/internal/repository/group_repo.go#L907-L976
    ADMIN_GROUPS_URL = (
        "https://zhisuanapi.cn/api/v1/admin/groups/all?include_inactive=true"
    )
    # Error membership is paginated and exposes redacted group_ids:
    # https://github.com/Wei-Shaw/sub2api/blob/2bc139ab527b4a687546d145dc7bb9063cf14510/backend/internal/handler/dto/types.go#L196-L313
    # Account tests stream SSE; successful tests are followed by runtime recovery:
    # https://github.com/Wei-Shaw/sub2api/blob/2bc139ab527b4a687546d145dc7bb9063cf14510/backend/internal/handler/admin/account_handler.go#L1090-L1148
    ADMIN_ACCOUNTS_URL = "https://zhisuanapi.cn/api/v1/admin/accounts"
    ADMIN_USAGE_URL = "https://zhisuanapi.cn/api/v1/admin/usage"
    ADMIN_OPS_REQUESTS_URL = "https://zhisuanapi.cn/api/v1/admin/ops/requests"
    ADMIN_USERS_URL = "https://zhisuanapi.cn/api/v1/admin/users"
    # Official real usage endpoint (the per-user /usage endpoint returns placeholder zeros):
    # https://github.com/Wei-Shaw/sub2api/blob/main/backend/internal/handler/admin/usage_handler.go
    ADMIN_USAGE_STATS_URL = "https://zhisuanapi.cn/api/v1/admin/usage/stats"
    USAGE_TIMEZONE = "Asia/Shanghai"
    MAX_RESPONSE_BYTES = 2 * 1024 * 1024
    ACCOUNT_TEST_TIMEOUT_SECONDS = 31
    MAX_CHANNEL_MONITOR_PAGES = 100
    ACCOUNT_SNAPSHOT_PAGE_SIZE = 100
    MAX_ACCOUNT_SNAPSHOT_PAGES = 100
    RECOVERY_ACCOUNT_PAGE_SIZE = 100
    MAX_RECOVERY_ACCOUNT_PAGES = 100
    MAX_USAGE_LOG_PAGES = 100
    MAX_REQUEST_LOG_PAGES = 100
    MAX_MONITOR_BINDING_PAGES = 3

    def __init__(
        self,
        admin_key: str,
        *,
        opener: Callable[..., Any] | None = None,
        timeout_seconds: int = 10,
        now_provider: Callable[[], datetime] | None = None,
        monotonic_provider: Callable[[], float] | None = None,
    ):
        if not admin_key.strip():
            raise ValueError("Sub2API Admin Key is required")
        self._admin_key = admin_key.strip()
        self._opener = opener or urllib_request.build_opener(
            _RejectRedirectHandler()
        ).open
        self._timeout_seconds = max(1, min(int(timeout_seconds), 30))
        self._now_provider = now_provider or (
            lambda: datetime.now(ZoneInfo(self.USAGE_TIMEZONE))
        )
        self._monotonic = monotonic_provider or time.monotonic
        self._maintenance_adapter = MaintenanceApiAdapterFactory.create(
            self,
            MaintenanceApiAdapterConfig(
                accounts_url=self.ADMIN_ACCOUNTS_URL,
                usage_url=self.ADMIN_USAGE_URL,
                request_logs_url=self.ADMIN_OPS_REQUESTS_URL,
                timezone_name=self.USAGE_TIMEZONE,
                account_snapshot_page_size=self.ACCOUNT_SNAPSHOT_PAGE_SIZE,
                max_account_pages=self.MAX_ACCOUNT_SNAPSHOT_PAGES,
                max_usage_pages=self.MAX_USAGE_LOG_PAGES,
                max_request_pages=self.MAX_REQUEST_LOG_PAGES,
                account_test_timeout_seconds=self.ACCOUNT_TEST_TIMEOUT_SECONDS,
            ),
        )

    def _request_body(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        expected_content_type: str | None = None,
    ) -> bytes:
        headers = {"x-api-key": self._admin_key, "Accept": "application/json"}
        headers.update(extra_headers or {})
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
            headers["Content-Type"] = "application/json"
        request = urllib_request.Request(
            url,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            timeout = self._timeout_seconds if timeout_seconds is None else timeout_seconds
            with self._opener(request, timeout=timeout) as response:
                if expected_content_type is not None:
                    response_headers = getattr(response, "headers", None)
                    raw_content_type = (
                        response_headers.get("Content-Type", "")
                        if response_headers is not None
                        else ""
                    )
                    content_type = raw_content_type.split(";", 1)[0].strip().lower()
                    if content_type != expected_content_type:
                        raise MonitorRequestError(
                            "Sub2API returned an unexpected content type"
                        )
                body = response.read(self.MAX_RESPONSE_BYTES + 1)
            if len(body) > self.MAX_RESPONSE_BYTES:
                raise MonitorRequestError("Sub2API response is too large")
            return body
        except MonitorRequestError:
            raise
        except (urllib_error.HTTPError, urllib_error.URLError, TimeoutError, OSError) as exc:
            raise MonitorRequestError("Sub2API request failed") from exc

    def _request_json(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        try:
            body = self._request_body(
                url,
                method=method,
                payload=payload,
                extra_headers=extra_headers,
            )
            return json.loads(body.decode("utf-8"))
        except MonitorRequestError:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise MonitorRequestError("Sub2API returned an invalid response") from exc

    def _request_sse_body_with_first_event(
        self,
        url: str,
        *,
        method: str = "POST",
        payload: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[bytes, int | None]:
        headers = {"x-api-key": self._admin_key, "Accept": "text/event-stream"}
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
            headers["Content-Type"] = "application/json"
        request = urllib_request.Request(url, data=body, headers=headers, method=method)
        started = self._monotonic()
        timeout = self._timeout_seconds if timeout_seconds is None else timeout_seconds
        deadline = started + timeout
        chunks: list[bytes] = []
        first_event_ms: int | None = None
        total = 0
        try:
            with self._opener(request, timeout=timeout) as response:
                response_headers = getattr(response, "headers", None)
                raw_content_type = (
                    response_headers.get("Content-Type", "")
                    if response_headers is not None
                    else ""
                )
                content_type = raw_content_type.split(";", 1)[0].strip().lower()
                if content_type != "text/event-stream":
                    raise MonitorRequestError(
                        "Sub2API returned an unexpected content type"
                )
                measured_blocks = 0
                while True:
                    if self._monotonic() > deadline:
                        raise MonitorRequestError(
                            "Sub2API SSE request exceeded its deadline"
                        )
                    reader = getattr(response, "read1", response.read)
                    chunk = reader(min(4096, self.MAX_RESPONSE_BYTES + 1 - total))
                    observed_at = self._monotonic()
                    if observed_at > deadline:
                        raise MonitorRequestError(
                            "Sub2API SSE request exceeded its deadline"
                        )
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > self.MAX_RESPONSE_BYTES:
                        raise MonitorRequestError("Sub2API response is too large")
                    chunks.append(chunk)
                    normalized = b"".join(chunks).replace(b"\r\n", b"\n")
                    completed_blocks = normalized.split(b"\n\n")[:-1]
                    if first_event_ms is None:
                        for block in completed_blocks[measured_blocks:]:
                            event_lines = block.split(b"\n")
                            if _is_valid_sse_data_event(event_lines):
                                first_event_ms = max(
                                    0,
                                    math.ceil((observed_at - started) * 1000),
                                )
                                break
                    measured_blocks = len(completed_blocks)
            return b"".join(chunks), first_event_ms
        except MonitorRequestError:
            raise
        except (urllib_error.HTTPError, urllib_error.URLError, TimeoutError, OSError) as exc:
            raise MonitorRequestError("Sub2API request failed") from exc

    def fetch_sync(self) -> list[ChannelHealth]:
        try:
            channels, pages, page_size = _parse_channel_monitor_page(
                self._request_json(self.API_URL),
                expected_page=1,
            )
            if pages > self.MAX_CHANNEL_MONITOR_PAGES:
                raise MonitorDataError("too many channel monitor pages")
            seen_ids = {channel.monitor_id for channel in channels}
            if len(seen_ids) != len(channels):
                raise MonitorDataError("duplicate channel monitor")
            for page in range(2, pages + 1):
                query = urllib_parse.urlencode(
                    [("page", page), ("page_size", page_size)]
                )
                page_channels, returned_pages, returned_page_size = (
                    _parse_channel_monitor_page(
                        self._request_json(f"{self.API_URL}?{query}"),
                        expected_page=page,
                    )
                )
                if returned_pages != pages or returned_page_size != page_size:
                    raise MonitorDataError("channel monitor pagination changed")
                for channel in page_channels:
                    if channel.monitor_id in seen_ids:
                        raise MonitorDataError("duplicate channel monitor")
                    seen_ids.add(channel.monitor_id)
                channels.extend(page_channels)
            return channels
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned an invalid response") from exc

    async def fetch(self) -> list[ChannelHealth]:
        return await asyncio.to_thread(self.fetch_sync)

    def fetch_group_account_snapshot_sync(
        self,
    ) -> tuple[list[GroupAccountCounts], list[AccountGroupState]]:
        try:
            groups: list[GroupDefinition] = parse_group_definitions(
                self._request_json(self.ADMIN_GROUPS_URL)
            )
            accounts = self._maintenance_adapter.fetch_account_group_states_sync(
                now=self._now_provider()
            )
            return aggregate_group_account_counts(groups, accounts), accounts
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned invalid group account data") from exc

    def fetch_group_account_counts_sync(self) -> list[GroupAccountCounts]:
        counts, _ = self.fetch_group_account_snapshot_sync()
        return counts

    async def fetch_group_account_counts(self) -> list[GroupAccountCounts]:
        return await asyncio.to_thread(self.fetch_group_account_counts_sync)

    def fetch_known_group_ids_sync(self) -> frozenset[str]:
        try:
            groups = parse_group_definitions(self._request_json(self.ADMIN_GROUPS_URL))
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned invalid group data") from exc
        return frozenset(group.group_id for group in groups)

    async def fetch_known_group_ids(self) -> frozenset[str]:
        return await asyncio.to_thread(self.fetch_known_group_ids_sync)

    def fetch_account_group_states_sync(
        self,
        *,
        now: datetime,
    ) -> list[AccountGroupState]:
        return self._maintenance_adapter.fetch_account_group_states_sync(now=now)

    async def fetch_account_group_states(
        self,
        *,
        now: datetime,
    ) -> list[AccountGroupState]:
        return await self._maintenance_adapter.fetch_account_group_states(now=now)

    def test_account_availability_sync(self, account_id: str):
        return self._maintenance_adapter.test_account_availability_sync(account_id)

    async def test_account_availability(self, account_id: str):
        return await self._maintenance_adapter.test_account_availability(account_id)

    def disable_account_sync(self, account_id: str):
        return self._maintenance_adapter.disable_account_sync(account_id)

    async def disable_account(self, account_id: str):
        return await self._maintenance_adapter.disable_account(account_id)

    def fetch_account_dispatch_state_sync(self, account_id: str):
        return self._maintenance_adapter.fetch_account_dispatch_state_sync(account_id)

    async def fetch_account_dispatch_state(self, account_id: str):
        return await self._maintenance_adapter.fetch_account_dispatch_state(account_id)

    def fetch_account_scheduling_state_sync(self, account_id: str):
        return self._maintenance_adapter.fetch_account_scheduling_state_sync(account_id)

    async def fetch_account_scheduling_state(self, account_id: str):
        return await self._maintenance_adapter.fetch_account_scheduling_state(account_id)

    def write_account_scheduling_field_sync(
        self,
        account_id: str,
        field_name: str,
        value: object,
    ):
        return self._maintenance_adapter.write_account_scheduling_field_sync(
            account_id,
            field_name,
            value,
        )

    async def write_account_scheduling_field(
        self,
        account_id: str,
        field_name: str,
        value: object,
    ):
        return await self._maintenance_adapter.write_account_scheduling_field(
            account_id,
            field_name,
            value,
        )

    def restore_account_sync(
        self,
        account_id: str,
        *,
        now: datetime,
        deadline: datetime | None = None,
    ):
        return self._maintenance_adapter.restore_account_sync(
            account_id,
            now=now,
            deadline=deadline,
        )

    async def restore_account(
        self,
        account_id: str,
        *,
        now: datetime,
        deadline: datetime | None = None,
    ):
        return await self._maintenance_adapter.restore_account(
            account_id,
            now=now,
            deadline=deadline,
        )

    def fetch_recent_usage_logs_sync(self, *, start: datetime, end: datetime):
        return self._maintenance_adapter.fetch_recent_usage_logs_sync(
            start=start,
            end=end,
        )

    async def fetch_recent_usage_logs(self, *, start: datetime, end: datetime):
        return await self._maintenance_adapter.fetch_recent_usage_logs(
            start=start,
            end=end,
        )

    def fetch_recent_request_logs_sync(self, *, start: datetime, end: datetime):
        return self._maintenance_adapter.fetch_recent_request_logs_sync(
            start=start,
            end=end,
        )

    async def fetch_recent_request_logs(self, *, start: datetime, end: datetime):
        return await self._maintenance_adapter.fetch_recent_request_logs(
            start=start,
            end=end,
        )

    def fetch_probe_with_accounts_sync(
        self,
    ) -> tuple[list[ChannelProbe], list[AccountGroupState]]:
        channels = self.fetch_sync()
        groups, accounts = self.fetch_group_account_snapshot_sync()
        try:
            usage_records = self.fetch_probe_usage_records_sync(channels)
        except (MonitorDataError, MonitorRequestError) as exc:
            _LOGGER.warning(
                "probe_group_binding_unavailable errorType=%s",
                type(exc).__name__,
            )
            usage_records = []
        bindings = resolve_channel_group_ids(channels, usage_records)
        return (
            build_channel_probes(
                channels,
                groups,
                group_ids_by_monitor=bindings,
            ),
            accounts,
        )

    def fetch_probe_sync(self) -> list[ChannelProbe]:
        probes, _ = self.fetch_probe_with_accounts_sync()
        return probes

    def fetch_probe_usage_records_sync(
        self,
        channels: list[ChannelHealth],
    ) -> list[ProbeUsageRecord]:
        oldest_by_model: dict[str, datetime] = {}
        for channel in channels:
            if (
                not channel.enabled
                or channel.group_name
                or not channel.model
                or not channel.last_checked_at
            ):
                continue
            try:
                checked_at = datetime.fromisoformat(
                    channel.last_checked_at.replace("Z", "+00:00")
                )
            except ValueError:
                continue
            if checked_at.tzinfo is None:
                continue
            current = oldest_by_model.get(channel.model)
            if current is None or checked_at < current:
                oldest_by_model[channel.model] = checked_at

        zone = ZoneInfo(self.USAGE_TIMEZONE)
        end_date = self._now_provider().astimezone(zone).date().isoformat()
        collected: list[ProbeUsageRecord] = []
        for model, checked_at in oldest_by_model.items():
            cutoff = checked_at - timedelta(seconds=1)
            start_date = cutoff.astimezone(zone).date().isoformat()
            seen: dict[str, tuple[object, ...]] = {}
            previous_created_at: datetime | None = None
            for page in range(1, self.MAX_MONITOR_BINDING_PAGES + 1):
                query = urllib_parse.urlencode(
                    [
                        ("start_date", start_date),
                        ("end_date", end_date),
                        ("timezone", self.USAGE_TIMEZONE),
                        ("model", model),
                        ("page", page),
                        ("page_size", 100),
                        ("sort_by", "created_at"),
                        ("sort_order", "desc"),
                    ]
                )
                records, returned_pages, page_size, item_count = parse_probe_usage_page(
                    self._request_json(f"{self.ADMIN_USAGE_URL}?{query}"),
                    expected_page=page,
                )
                reached_cutoff = False
                for record in records:
                    fingerprint = (
                        record.api_key_id,
                        record.group_id,
                        record.model,
                        record.created_at,
                        record.duration_ms,
                        record.user_agent,
                    )
                    previous = seen.get(record.log_id)
                    if previous is not None:
                        if previous != fingerprint:
                            raise MonitorDataError(
                                "probe usage item changed while paginating"
                            )
                        continue
                    seen[record.log_id] = fingerprint
                    if (
                        previous_created_at is not None
                        and record.created_at > previous_created_at
                    ):
                        raise MonitorDataError("probe usage ordering changed")
                    previous_created_at = record.created_at
                    if record.created_at < cutoff:
                        reached_cutoff = True
                        break
                    collected.append(record)
                if (
                    reached_cutoff
                    or page >= returned_pages
                    or item_count < page_size
                ):
                    break
            else:
                raise MonitorDataError("too many probe usage pages")
        return collected

    async def fetch_probe_with_accounts(
        self,
    ) -> tuple[list[ChannelProbe], list[AccountGroupState]]:
        return await asyncio.to_thread(self.fetch_probe_with_accounts_sync)

    async def fetch_probe(self) -> list[ChannelProbe]:
        return await asyncio.to_thread(self.fetch_probe_sync)

    def fetch_recovery_candidates_sync(
        self,
        *,
        now: datetime,
    ) -> list[RecoveryCandidate]:
        candidates: list[RecoveryCandidate] = []
        seen_ids: set[str] = set()
        expected_pages: int | None = None
        page = 1
        while True:
            query = urllib_parse.urlencode(
                [
                    ("page", page),
                    ("page_size", self.RECOVERY_ACCOUNT_PAGE_SIZE),
                    ("sort_by", "id"),
                    ("sort_order", "asc"),
                ]
            )
            try:
                page_candidates, pages = parse_recovery_account_page(
                    self._request_json(f"{self.ADMIN_ACCOUNTS_URL}?{query}"),
                    expected_page=page,
                    now=now,
                )
            except MonitorDataError as exc:
                raise MonitorRequestError(
                    "Sub2API returned invalid recovery account data"
                ) from exc
            if pages > self.MAX_RECOVERY_ACCOUNT_PAGES:
                raise MonitorRequestError("Sub2API recovery account list is too large")
            if expected_pages is None:
                expected_pages = pages
            elif pages != expected_pages:
                raise MonitorRequestError("Sub2API recovery account pagination changed")
            for candidate in page_candidates:
                if candidate.account_id in seen_ids:
                    raise MonitorRequestError("Sub2API returned duplicate recovery accounts")
                seen_ids.add(candidate.account_id)
                candidates.append(candidate)
            if page >= pages:
                return candidates
            page += 1

    async def fetch_recovery_candidates(
        self,
        *,
        now: datetime,
    ) -> list[RecoveryCandidate]:
        return await asyncio.to_thread(
            self.fetch_recovery_candidates_sync,
            now=now,
        )

    @staticmethod
    def _require_success_envelope(payload: Any) -> None:
        if not isinstance(payload, dict) or payload.get("code") != 0:
            raise MonitorDataError("Sub2API account recovery request failed")

    def test_and_recover_account_sync(
        self,
        candidate: RecoveryCandidate,
        *,
        now: datetime,
        deadline: datetime | None = None,
    ) -> RecoveryOutcome:
        if now.tzinfo is None or (deadline is not None and deadline.tzinfo is None):
            raise ValueError("account recovery times must be timezone-aware")
        if (
            candidate.bucket != "error"
            or candidate.status != "error"
        ):
            return RecoveryOutcome(
                candidate.account_id,
                candidate.name,
                candidate.bucket,
                "test_failed",
            )

        def current_utc() -> datetime:
            current = self._now_provider()
            if current.tzinfo is None:
                raise MonitorDataError("Sub2API recovery clock is invalid")
            return current.astimezone(UTC)

        normalized_deadline = (
            deadline.astimezone(UTC) if deadline is not None else None
        )

        def may_write() -> bool:
            current = current_utc()
            return bool(
                (normalized_deadline is None or current < normalized_deadline)
                and not candidate.is_auto_paused_expired(current)
            )

        account_url = f"{self.ADMIN_ACCOUNTS_URL}/{candidate.account_id}"
        try:
            if not may_write():
                return RecoveryOutcome(
                    candidate.account_id,
                    candidate.name,
                    candidate.bucket,
                    "test_failed",
                )
        except MonitorDataError:
            return RecoveryOutcome(
                candidate.account_id,
                candidate.name,
                candidate.bucket,
                "test_failed",
            )
        try:
            test_body = self._request_body(
                f"{account_url}/test",
                method="POST",
                payload={},
                extra_headers={"Accept": "text/event-stream"},
                timeout_seconds=self.ACCOUNT_TEST_TIMEOUT_SECONDS,
                expected_content_type="text/event-stream",
            )
            if not account_test_succeeded(test_body):
                return RecoveryOutcome(
                    candidate.account_id,
                    candidate.name,
                    candidate.bucket,
                    "test_failed",
                )
        except (MonitorDataError, MonitorRequestError, UnicodeDecodeError):
            return RecoveryOutcome(
                candidate.account_id,
                candidate.name,
                candidate.bucket,
                "test_failed",
            )
        try:
            if not may_write():
                return RecoveryOutcome(
                    candidate.account_id,
                    candidate.name,
                    candidate.bucket,
                    "test_failed",
                )
        except MonitorDataError:
            return RecoveryOutcome(
                candidate.account_id,
                candidate.name,
                candidate.bucket,
                "test_failed",
            )

        if may_write():
            try:
                recovered = self._request_json(
                    f"{account_url}/recover-state",
                    method="POST",
                )
                self._require_success_envelope(recovered)
            except (MonitorDataError, MonitorRequestError):
                pass
        if candidate.status != "active" and may_write():
            try:
                activated = self._request_json(
                    account_url,
                    method="PUT",
                    payload={"status": "active"},
                )
                self._require_success_envelope(activated)
            except (MonitorDataError, MonitorRequestError):
                pass
        if not candidate.schedulable and may_write():
            try:
                scheduled = self._request_json(
                    f"{account_url}/schedulable",
                    method="POST",
                    payload={"schedulable": True},
                )
                self._require_success_envelope(scheduled)
            except (MonitorDataError, MonitorRequestError):
                pass
        try:
            current = self._request_json(account_url)
            is_normal = recovered_account_is_normal(
                current,
                expected_account_id=candidate.account_id,
                now=current_utc(),
            )
        except (MonitorDataError, MonitorRequestError, ValueError):
            is_normal = False

        return RecoveryOutcome(
            candidate.account_id,
            candidate.name,
            candidate.bucket,
            "recovered" if is_normal else "recovery_failed",
        )

    async def test_and_recover_account(
        self,
        candidate: RecoveryCandidate,
        *,
        now: datetime,
        deadline: datetime | None = None,
    ) -> RecoveryOutcome:
        return await asyncio.to_thread(
            self.test_and_recover_account_sync,
            candidate,
            now=now,
            deadline=deadline,
        )

    def find_account_by_email_sync(self, email: str) -> AccountProfile | None:
        normalized_email = normalize_email(email)
        query = urllib_parse.urlencode(
            [("page", 1), ("page_size", 20), ("search", normalized_email)]
        )
        try:
            return parse_account_search(
                self._request_json(f"{self.ADMIN_USERS_URL}?{query}"),
                normalized_email,
            )
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned invalid account data") from exc

    async def find_account_by_email(self, email: str) -> AccountProfile | None:
        return await asyncio.to_thread(self.find_account_by_email_sync, email)

    @staticmethod
    def _normalize_user_id(user_id: str | int) -> str:
        normalized = str(user_id).strip()
        if not normalized.isdigit() or len(normalized) > 20:
            raise ValueError("invalid Sub2API user id")
        return normalized

    def fetch_account_sync(self, user_id: str | int) -> AccountProfile:
        normalized_id = self._normalize_user_id(user_id)
        try:
            return parse_account_profile(
                self._request_json(f"{self.ADMIN_USERS_URL}/{normalized_id}")
            )
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned invalid account data") from exc

    async def fetch_account(self, user_id: str | int) -> AccountProfile:
        return await asyncio.to_thread(self.fetch_account_sync, user_id)

    def fetch_account_usage_sync(self, user_id: str | int, period: str) -> AccountUsage:
        normalized_id = self._normalize_user_id(user_id)
        if period not in {"today", "month"}:
            raise ValueError("invalid Sub2API usage period")

        query: list[tuple[str, str | int]] = [("user_id", normalized_id)]
        if period == "today":
            query.append(("period", "today"))
        else:
            today = self._now_provider().astimezone(
                ZoneInfo(self.USAGE_TIMEZONE)
            ).date()
            query.extend(
                [
                    ("start_date", today.replace(day=1).isoformat()),
                    ("end_date", today.isoformat()),
                ]
            )
        query.extend(
            [
                ("timezone", self.USAGE_TIMEZONE),
                ("nocache", 1),
            ]
        )
        try:
            return parse_account_usage(
                self._request_json(
                    f"{self.ADMIN_USAGE_STATS_URL}?{urllib_parse.urlencode(query)}"
                ),
                period,
            )
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned invalid account usage data") from exc

    async def fetch_account_usage(
        self,
        user_id: str | int,
        period: str,
    ) -> AccountUsage:
        return await asyncio.to_thread(self.fetch_account_usage_sync, user_id, period)

class ChannelMonitorService:
    """Fetch current channel and account-group probe snapshots."""

    def __init__(
        self,
        config: MonitorConfig,
        client: Sub2APIClient,
        send_message: Callable[[str], Awaitable[None] | None],
    ):
        self.config = config
        self.client = client
        self._send_message = send_message

    async def _send(self, message: str) -> None:
        result = self._send_message(message)
        if inspect.isawaitable(result):
            await result

    async def fetch_probe(self) -> list[ChannelProbe]:
        return await self.client.fetch_probe()

    async def fetch_report(self) -> str:
        return format_status_report(await self.fetch_probe())

    async def send_report(self) -> str:
        report = await self.fetch_report()
        await self._send(report)
        return report
