from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib import parse as urllib_parse
from zoneinfo import ZoneInfo

from client_errors import MonitorRequestError
from maintenance import (
    AccountDisableResult,
    AccountDispatchState,
    AccountRestoreResult,
    AccountSchedulingState,
    AccountTestResult,
    RequestLogRecord,
    UsageLogRecord,
)
from probe import (
    AccountGroupState,
    MonitorDataError,
    parse_account_group_state_page,
)
from recovery import account_test_result, recovered_account_is_normal


class MaintenanceRequestPort(Protocol):
    def _request_body(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        expected_content_type: str | None = None,
    ) -> bytes: ...

    def _request_sse_body_with_first_event(
        self,
        url: str,
        *,
        method: str = "POST",
        payload: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[bytes, int | None]: ...

    def _request_json(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class MaintenanceApiAdapterConfig:
    accounts_url: str
    usage_url: str
    request_logs_url: str
    timezone_name: str = "Asia/Shanghai"
    account_snapshot_page_size: int = 100
    max_account_pages: int = 100
    max_usage_pages: int = 100
    max_request_pages: int = 100
    account_test_timeout_seconds: int = 30


def _positive_id_text(value: Any, field: str) -> str:
    if isinstance(value, bool):
        raise MonitorDataError(f"invalid {field}")
    text = str(value or "").strip()
    if not text.isdigit() or int(text) <= 0 or len(text) > 20:
        raise MonitorDataError(f"invalid {field}")
    return text


def _optional_non_negative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MonitorDataError(f"invalid {field}")
    return value


def _future_epoch(value: Any, field: str, now: datetime) -> bool:
    if value is None:
        return False
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise MonitorDataError(f"invalid {field}")
    return value > now.timestamp()


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MonitorDataError(f"invalid {field}")
    return value


def _parse_external_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or len(value) > 100:
        raise MonitorDataError(f"invalid {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorDataError(f"invalid {field}") from exc
    if parsed.tzinfo is None:
        raise MonitorDataError(f"invalid {field}")
    return parsed.astimezone(UTC)


def _parse_admin_items_page(
    payload: Any,
    *,
    expected_page: int,
    field_name: str,
) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError(f"Sub2API {field_name} request failed")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise MonitorDataError(f"Sub2API {field_name} data.items must be a list")
    total = _non_negative_int(data.get("total"), f"{field_name} total")
    page = _non_negative_int(data.get("page"), f"{field_name} page")
    page_size = _non_negative_int(data.get("page_size"), f"{field_name} page_size")
    pages = _non_negative_int(data.get("pages"), f"{field_name} pages")
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
        raise MonitorDataError(f"invalid {field_name} pagination")
    items: list[dict[str, Any]] = []
    for item in data["items"]:
        if not isinstance(item, dict):
            raise MonitorDataError(f"{field_name} item must be an object")
        items.append(item)
    return items, pages


def _validate_time_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("maintenance log times must be timezone-aware")
    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)
    if start_utc >= end_utc:
        raise ValueError("maintenance log start must precede end")
    return start_utc, end_utc


def _log_item_identity(
    item: dict[str, Any],
    *,
    field_name: str,
    fallback_fields: tuple[str, ...],
) -> tuple[str, ...]:
    for key in ("id", "request_id"):
        value = item.get(key)
        if value is None or isinstance(value, bool):
            continue
        text = str(value).strip()
        if text and len(text) <= 256:
            return (key, text)
    fallback = tuple(str(item.get(key))[:256] for key in fallback_fields)
    if not any(value and value != "None" for value in fallback):
        raise MonitorDataError(f"{field_name} item identifier is missing")
    return ("fallback", *fallback)


def _log_item_is_duplicate(
    item: dict[str, Any],
    seen_items: dict[tuple[str, ...], tuple[str, ...]],
    *,
    field_name: str,
    fingerprint_fields: tuple[str, ...],
) -> bool:
    identity = _log_item_identity(
        item,
        field_name=field_name,
        fallback_fields=fingerprint_fields,
    )
    fingerprint = tuple(str(item.get(key))[:256] for key in fingerprint_fields)
    previous = seen_items.get(identity)
    if previous is None:
        seen_items[identity] = fingerprint
        return False
    if previous != fingerprint:
        raise MonitorDataError(f"{field_name} item changed while paginating")
    return True


class MaintenanceApiAdapter:
    """Adapts untrusted Sub2API maintenance responses to domain records."""

    def __init__(
        self,
        request_port: MaintenanceRequestPort,
        config: MaintenanceApiAdapterConfig,
        *,
        clock: Callable[[], datetime] | None = None,
    ):
        self._request_port = request_port
        self._config = config
        self._clock = clock or (lambda: datetime.now(UTC))

    def _fetch_account_group_states_sync(
        self,
        *,
        now: datetime,
    ) -> list[AccountGroupState]:
        states: list[AccountGroupState] = []
        seen_accounts: set[str] = set()
        expected_pages: int | None = None
        for page in range(1, self._config.max_account_pages + 1):
            query = urllib_parse.urlencode(
                [
                    ("page", page),
                    ("page_size", self._config.account_snapshot_page_size),
                    ("sort_by", "id"),
                    ("sort_order", "asc"),
                ]
            )
            page_states, pages = parse_account_group_state_page(
                self._request_port._request_json(
                    f"{self._config.accounts_url}?{query}"
                ),
                expected_page=page,
                now=now,
            )
            if expected_pages is None:
                expected_pages = pages
            elif pages != expected_pages:
                raise MonitorDataError("account snapshot pagination changed")
            for state in page_states:
                if state.account_id in seen_accounts:
                    raise MonitorDataError("duplicate account snapshot")
                seen_accounts.add(state.account_id)
                states.append(state)
            if page >= pages:
                return states
        raise MonitorDataError("too many account snapshot pages")

    def fetch_account_group_states_sync(
        self,
        *,
        now: datetime,
    ) -> list[AccountGroupState]:
        try:
            return self._fetch_account_group_states_sync(now=now)
        except MonitorDataError as exc:
            raise MonitorRequestError("Sub2API returned invalid account data") from exc

    async def fetch_account_group_states(
        self,
        *,
        now: datetime,
    ) -> list[AccountGroupState]:
        return await asyncio.to_thread(
            self.fetch_account_group_states_sync,
            now=now,
        )

    def test_account_availability_sync(self, account_id: str) -> AccountTestResult:
        normalized_id = _positive_id_text(account_id, "account id")
        try:
            body, first_event_ms = self._request_port._request_sse_body_with_first_event(
                f"{self._config.accounts_url}/{normalized_id}/test",
                method="POST",
                payload={},
                timeout_seconds=self._config.account_test_timeout_seconds,
            )
            completed = account_test_result(body)
        except (MonitorRequestError, MonitorDataError, UnicodeDecodeError):
            return AccountTestResult(
                normalized_id,
                success=False,
                definitive_failure=False,
                reason="test_request_unavailable",
            )
        return AccountTestResult(
            normalized_id,
            success=completed is True,
            definitive_failure=completed is False,
            reason=(
                ""
                if completed is True
                else "test_failed"
                if completed is False
                else "test_incomplete"
            ),
            first_event_ms=first_event_ms,
        )

    async def test_account_availability(self, account_id: str) -> AccountTestResult:
        return await asyncio.to_thread(
            self.test_account_availability_sync,
            account_id,
        )

    def disable_account_sync(self, account_id: str) -> AccountDisableResult:
        normalized_id = _positive_id_text(account_id, "account id")
        account_url = f"{self._config.accounts_url}/{normalized_id}"
        try:
            inactive = self._request_port._request_json(
                account_url,
                method="PUT",
                payload={"status": "inactive"},
            )
            _require_success_envelope(inactive)
            scheduled = self._request_port._request_json(
                f"{account_url}/schedulable",
                method="POST",
                payload={"schedulable": False},
            )
            _require_success_envelope(scheduled)
            verified = self._request_port._request_json(account_url)
            if not isinstance(verified, dict) or verified.get("code") != 0:
                raise MonitorDataError("account disable verification failed")
            data = verified.get("data")
            if not isinstance(data, dict):
                raise MonitorDataError("account disable data must be an object")
            if (
                _positive_id_text(data.get("id"), "account id") != normalized_id
                or data.get("status") not in {"inactive", "disabled"}
                or data.get("schedulable") is not False
            ):
                raise MonitorDataError("account disable verification failed")
        except (MonitorRequestError, MonitorDataError):
            try:
                current = self._request_port._request_json(account_url)
                if not isinstance(current, dict) or current.get("code") != 0:
                    raise MonitorDataError("account disable readback failed")
                data = current.get("data")
                if not isinstance(data, dict):
                    raise MonitorDataError("account disable readback data is invalid")
                if _positive_id_text(data.get("id"), "account id") != normalized_id:
                    raise MonitorDataError("account disable readback identity mismatch")
                status = data.get("status")
                schedulable = data.get("schedulable")
                if status not in {"active", "error", "inactive", "disabled"} or not isinstance(
                    schedulable, bool
                ):
                    raise MonitorDataError("account disable readback state is invalid")
            except (MonitorRequestError, MonitorDataError):
                return AccountDisableResult(
                    normalized_id,
                    success=False,
                    reason="disable_state_uncertain",
                    state_uncertain=True,
                )
            if schedulable is False and status in {"inactive", "disabled"}:
                return AccountDisableResult(
                    normalized_id,
                    success=True,
                    reason="partial_disable_verified",
                )
            if schedulable is False or status in {"inactive", "disabled"}:
                return AccountDisableResult(
                    normalized_id,
                    success=False,
                    reason="disable_state_uncertain",
                    state_uncertain=True,
                )
            return AccountDisableResult(
                normalized_id,
                success=False,
                reason="disable_request_failed",
            )
        return AccountDisableResult(normalized_id, success=True)

    async def disable_account(self, account_id: str) -> AccountDisableResult:
        return await asyncio.to_thread(self.disable_account_sync, account_id)

    def fetch_account_dispatch_state_sync(
        self,
        account_id: str,
    ) -> AccountDispatchState:
        normalized_id = _positive_id_text(account_id, "account id")
        try:
            payload = self._request_port._request_json(
                f"{self._config.accounts_url}/{normalized_id}"
            )
            if not isinstance(payload, dict) or payload.get("code") != 0:
                raise MonitorDataError("account state readback failed")
            data = payload.get("data")
            if not isinstance(data, dict):
                raise MonitorDataError("account state data is invalid")
            if _positive_id_text(data.get("id"), "account id") != normalized_id:
                raise MonitorDataError("account state identity mismatch")
            status = data.get("status")
            schedulable = data.get("schedulable")
            auto_pause = data.get("auto_pause_on_expired", False)
            if not isinstance(auto_pause, bool):
                raise MonitorDataError("account auto-pause state is invalid")
            expires_at = data.get("expires_at")
            if expires_at is not None and (
                isinstance(expires_at, bool)
                or not isinstance(expires_at, (int, float))
                or expires_at < 0
            ):
                raise MonitorDataError("account expiry is invalid")
            now = self._clock().astimezone(UTC)
            expired = bool(
                auto_pause
                and expires_at is not None
                and expires_at <= now.timestamp()
            )
            temporary_unavailable = any(
                _future_epoch(data.get(field), field, now)
                for field in (
                    "rate_limit_reset_at",
                    "overload_until",
                    "temp_unschedulable_until",
                )
            )
            if status not in {"active", "error", "inactive", "disabled"} or not isinstance(
                schedulable, bool
            ):
                raise MonitorDataError("account dispatch state is invalid")
        except (MonitorRequestError, MonitorDataError):
            return AccountDispatchState(normalized_id, success=False)
        return AccountDispatchState(
            normalized_id,
            success=True,
            status=status,
            schedulable=schedulable,
            expired=expired,
            temporary_unavailable=temporary_unavailable,
        )

    async def fetch_account_dispatch_state(
        self,
        account_id: str,
    ) -> AccountDispatchState:
        return await asyncio.to_thread(
            self.fetch_account_dispatch_state_sync,
            account_id,
        )

    def fetch_account_scheduling_state_sync(
        self,
        account_id: str,
    ) -> AccountSchedulingState:
        """Read the official account-level scheduling fields without mutation.

        Source: https://github.com/Wei-Shaw/sub2api/blob/
        aa2c4e8d136b13553ac7bae3d76c25715333a554/backend/internal/handler/admin/
        account_handler.go#L133-L153
        """
        normalized_id = _positive_id_text(account_id, "account id")
        try:
            payload = self._request_port._request_json(
                f"{self._config.accounts_url}/{normalized_id}"
            )
            if not isinstance(payload, dict) or payload.get("code") != 0:
                raise MonitorDataError("account scheduling state readback failed")
            data = payload.get("data")
            if not isinstance(data, dict):
                raise MonitorDataError("account scheduling state data is invalid")
            if _positive_id_text(data.get("id"), "account id") != normalized_id:
                raise MonitorDataError("account scheduling state identity mismatch")
            status = data.get("status")
            schedulable = data.get("schedulable")
            priority = data.get("priority")
            load_factor = data.get("load_factor")
            concurrency = data.get("concurrency")
            if status not in {"active", "error", "inactive", "disabled"}:
                raise MonitorDataError("account scheduling status is invalid")
            if not isinstance(schedulable, bool):
                raise MonitorDataError("account scheduling flag is invalid")
            if (
                isinstance(priority, bool)
                or not isinstance(priority, int)
                or not 0 <= priority <= 1_000_000
            ):
                raise MonitorDataError("account priority is invalid")
            if load_factor is not None and (
                isinstance(load_factor, bool)
                or not isinstance(load_factor, int)
                or not 0 <= load_factor <= 10_000
            ):
                raise MonitorDataError("account load factor is invalid")
            if (
                isinstance(concurrency, bool)
                or not isinstance(concurrency, int)
                or not 0 <= concurrency <= 1_000_000
            ):
                raise MonitorDataError("account concurrency is invalid")
            effective_load_factor = (
                load_factor
                if load_factor is not None and load_factor > 0
                else max(1, concurrency)
            )
        except (MonitorRequestError, MonitorDataError):
            return AccountSchedulingState(normalized_id, success=False)
        return AccountSchedulingState(
            normalized_id,
            success=True,
            status=status,
            schedulable=schedulable,
            priority=priority,
            load_factor=load_factor,
            concurrency=concurrency,
            effective_load_factor=effective_load_factor,
        )

    async def fetch_account_scheduling_state(
        self,
        account_id: str,
    ) -> AccountSchedulingState:
        return await asyncio.to_thread(
            self.fetch_account_scheduling_state_sync,
            account_id,
        )

    def restore_account_sync(
        self,
        account_id: str,
        *,
        now: datetime,
        deadline: datetime | None = None,
    ) -> AccountRestoreResult:
        normalized_id = _positive_id_text(account_id, "account id")
        if now.tzinfo is None or (deadline is not None and deadline.tzinfo is None):
            raise ValueError("account restore times must be timezone-aware")
        account_url = f"{self._config.accounts_url}/{normalized_id}"

        def deadline_active() -> bool:
            return deadline is None or self._clock().astimezone(UTC) < deadline.astimezone(UTC)

        def classify_readback(payload: Any) -> AccountRestoreResult:
            try:
                if recovered_account_is_normal(
                    payload,
                    expected_account_id=normalized_id,
                    now=now,
                ):
                    return AccountRestoreResult(normalized_id, success=True)
                if not isinstance(payload, dict) or payload.get("code") != 0:
                    raise MonitorDataError("account restore readback failed")
                data = payload.get("data")
                if not isinstance(data, dict):
                    raise MonitorDataError("account restore readback data is invalid")
                if _positive_id_text(data.get("id"), "account id") != normalized_id:
                    raise MonitorDataError("account restore readback identity mismatch")
                status = data.get("status")
                schedulable = data.get("schedulable")
                if status in {"inactive", "disabled"} and schedulable is False:
                    return AccountRestoreResult(
                        normalized_id,
                        success=False,
                        reason="restore_not_applied",
                    )
            except (MonitorDataError, ValueError):
                pass
            return AccountRestoreResult(
                normalized_id,
                success=False,
                reason="restore_state_uncertain",
                state_uncertain=True,
            )

        def readback_after_failure() -> AccountRestoreResult:
            try:
                payload = self._request_port._request_json(account_url)
            except (MonitorRequestError, MonitorDataError):
                return AccountRestoreResult(
                    normalized_id,
                    success=False,
                    reason="restore_state_uncertain",
                    state_uncertain=True,
                )
            return classify_readback(payload)

        try:
            if not deadline_active():
                return AccountRestoreResult(
                    normalized_id,
                    success=False,
                    reason="restore_deadline_expired",
                )
            # A failed credential-bearing write can still have reached the upstream.
            activated = self._request_port._request_json(
                account_url,
                method="PUT",
                payload={"status": "active"},
            )
            _require_success_envelope(activated)
            if not deadline_active():
                return readback_after_failure()
            scheduled = self._request_port._request_json(
                f"{account_url}/schedulable",
                method="POST",
                payload={"schedulable": True},
            )
            _require_success_envelope(scheduled)
            if not deadline_active():
                return readback_after_failure()
            verified = self._request_port._request_json(account_url)
            return classify_readback(verified)
        except (MonitorRequestError, MonitorDataError, ValueError):
            return readback_after_failure()

    async def restore_account(
        self,
        account_id: str,
        *,
        now: datetime,
        deadline: datetime | None = None,
    ) -> AccountRestoreResult:
        return await asyncio.to_thread(
            self.restore_account_sync,
            account_id,
            now=now,
            deadline=deadline,
        )

    def fetch_recent_usage_logs_sync(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[UsageLogRecord]:
        start_utc, end_utc = _validate_time_range(start, end)
        zone = ZoneInfo(self._config.timezone_name)
        records: list[UsageLogRecord] = []
        start_date = start_utc.astimezone(zone).date().isoformat()
        end_date = end_utc.astimezone(zone).date().isoformat()
        seen_items: dict[tuple[str, ...], tuple[str, ...]] = {}
        previous_created_at: datetime | None = None
        for page in range(1, self._config.max_usage_pages + 1):
            query = urllib_parse.urlencode(
                [
                    ("start_date", start_date),
                    ("end_date", end_date),
                    ("timezone", self._config.timezone_name),
                    ("page", page),
                    ("page_size", 100),
                    ("sort_by", "created_at"),
                    ("sort_order", "desc"),
                ]
            )
            items, returned_pages = _parse_admin_items_page(
                self._request_port._request_json(
                    f"{self._config.usage_url}?{query}"
                ),
                expected_page=page,
                field_name="usage logs",
            )
            for item in items:
                if _log_item_is_duplicate(
                    item,
                    seen_items,
                    field_name="usage logs",
                    fingerprint_fields=(
                        "account_id",
                        "created_at",
                        "first_token_ms",
                        "duration_ms",
                    ),
                ):
                    continue
                account_id = _positive_id_text(item.get("account_id"), "usage account id")
                created_at = _parse_external_datetime(item.get("created_at"), "usage created_at")
                if previous_created_at is not None and created_at > previous_created_at:
                    raise MonitorDataError("usage log ordering changed")
                previous_created_at = created_at
                if created_at < start_utc:
                    return records
                if created_at >= end_utc:
                    continue
                records.append(
                    UsageLogRecord(
                        account_id=account_id,
                        created_at=created_at,
                        first_token_ms=_optional_non_negative_int(
                            item.get("first_token_ms"),
                            "usage first_token_ms",
                        ),
                        duration_ms=_optional_non_negative_int(
                            item.get("duration_ms"),
                            "usage duration_ms",
                        ),
                    )
                )
            # Sub2API defaults exact_total=false, so total/pages may grow while
            # walking a busy log. Stop on the time boundary or a short/final page
            # and deduplicate shifted rows instead of trusting a stable total.
            # Source: https://github.com/Wei-Shaw/sub2api/blob/main/backend/internal/handler/admin/usage_handler.go#L62-L72
            if page >= returned_pages or len(items) < 100:
                return records
        raise MonitorDataError("too many usage log pages")

    async def fetch_recent_usage_logs(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[UsageLogRecord]:
        return await asyncio.to_thread(
            self.fetch_recent_usage_logs_sync,
            start=start,
            end=end,
        )

    def fetch_recent_request_logs_sync(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[RequestLogRecord]:
        start_utc, end_utc = _validate_time_range(start, end)
        records: list[RequestLogRecord] = []
        seen_items: dict[tuple[str, ...], tuple[str, ...]] = {}
        previous_created_at: datetime | None = None
        for page in range(1, self._config.max_request_pages + 1):
            query = urllib_parse.urlencode(
                [
                    ("time_range", "30m"),
                    ("kind", "all"),
                    ("page", page),
                    ("page_size", 100),
                    ("sort", "created_at_desc"),
                ]
            )
            items, returned_pages = _parse_admin_items_page(
                self._request_port._request_json(
                    f"{self._config.request_logs_url}?{query}"
                ),
                expected_page=page,
                field_name="request logs",
            )
            for item in items:
                if _log_item_is_duplicate(
                    item,
                    seen_items,
                    field_name="request logs",
                    fingerprint_fields=(
                        "account_id",
                        "created_at",
                        "kind",
                        "status_code",
                        "phase",
                    ),
                ):
                    continue
                raw_account_id = item.get("account_id")
                account_id = (
                    None
                    if raw_account_id is None
                    else _positive_id_text(raw_account_id, "request account id")
                )
                created_at = _parse_external_datetime(
                    item.get("created_at"),
                    "request created_at",
                )
                if previous_created_at is not None and created_at > previous_created_at:
                    raise MonitorDataError("request log ordering changed")
                previous_created_at = created_at
                if created_at < start_utc:
                    return records
                if created_at >= end_utc:
                    continue
                kind = str(item.get("kind") or "").strip().lower()
                if kind not in {"success", "error"}:
                    raise MonitorDataError("invalid request kind")
                status_code = _optional_non_negative_int(
                    item.get("status_code"),
                    "request status_code",
                )
                records.append(
                    RequestLogRecord(
                        account_id=account_id,
                        created_at=created_at,
                        kind=kind,
                        status_code=status_code,
                        phase=str(item.get("phase") or "").strip().lower()[:200],
                    )
                )
            if page >= returned_pages or len(items) < 100:
                return records
        raise MonitorDataError("too many request log pages")

    async def fetch_recent_request_logs(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[RequestLogRecord]:
        return await asyncio.to_thread(
            self.fetch_recent_request_logs_sync,
            start=start,
            end=end,
        )


def _require_success_envelope(payload: Any) -> None:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API account maintenance request failed")


class MaintenanceApiAdapterFactory:
    @staticmethod
    def create(
        request_port: MaintenanceRequestPort,
        config: MaintenanceApiAdapterConfig,
    ) -> MaintenanceApiAdapter:
        return MaintenanceApiAdapter(request_port, config)
