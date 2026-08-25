from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from email.message import Message
from urllib import error as urllib_error
from urllib.parse import parse_qs, urlsplit

import pytest
from client_errors import MonitorRequestError
from maintenance_gateway import MaintenanceApiAdapter, MaintenanceApiAdapterConfig
from monitor import Sub2APIClient
from probe import MonitorDataError


class DynamicPaginationPort:
    def __init__(self, pages: dict[int, dict[str, object]]) -> None:
        self.pages = pages

    def _request_json(self, url: str, **_: object) -> dict[str, object]:
        page = int(parse_qs(urlsplit(url).query)["page"][0])
        return self.pages[page]

    def _request_body(self, url: str, **_: object) -> bytes:
        raise AssertionError(f"unexpected body request: {url}")

    def _request_sse_body_with_first_event(
        self, url: str, **_: object
    ) -> tuple[bytes, int | None]:
        raise AssertionError(f"unexpected SSE request: {url}")


class AccountMutationPort:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, object]] = []

    def _request_body(self, url: str, **_: object) -> bytes:
        raise AssertionError(f"unexpected body request: {url}")

    def _request_sse_body_with_first_event(
        self, url: str, **_: object
    ) -> tuple[bytes, int | None]:
        self.requests.append((url, "POST", {}))
        return b'data: {"type":"test_complete","success":true}\n\n', 1_234

    def _request_json(self, url: str, **kwargs: object) -> dict[str, object]:
        method = str(kwargs.get("method", "GET"))
        payload = kwargs.get("payload")
        self.requests.append((url, method, payload))
        if method == "GET":
            return {
                "code": 0,
                "data": {
                    "id": 42,
                    "status": "active",
                    "schedulable": True,
                    "auto_pause_on_expired": False,
                    "expires_at": None,
                    "rate_limit_reset_at": None,
                    "overload_until": None,
                    "temp_unschedulable_until": None,
                },
            }
        return {"code": 0}


class StreamingResponse(io.BytesIO):
    headers = {"Content-Type": "text/event-stream; charset=utf-8"}

    def __enter__(self) -> StreamingResponse:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class DripStreamingResponse:
    headers = {"Content-Type": "text/event-stream"}

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    def __enter__(self) -> DripStreamingResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, _: int) -> bytes:
        return self.read1(_)

    def read1(self, _: int) -> bytes:
        return self.chunks.pop(0) if self.chunks else b""


class PartialDisablePort(AccountMutationPort):
    def __init__(self, *, readback_available: bool) -> None:
        super().__init__()
        self.readback_available = readback_available

    def _request_json(self, url: str, **kwargs: object) -> dict[str, object]:
        method = str(kwargs.get("method", "GET"))
        payload = kwargs.get("payload")
        self.requests.append((url, method, payload))
        if method == "PUT":
            raise MonitorRequestError("simulated status write failure")
        if method == "GET":
            if not self.readback_available:
                raise MonitorRequestError("simulated readback failure")
            return {
                "code": 0,
                "data": {
                    "id": 42,
                    "status": "active",
                    "schedulable": False,
                },
            }
        return {"code": 0}


class PartialRestorePort(AccountMutationPort):
    def _request_json(self, url: str, **kwargs: object) -> dict[str, object]:
        method = str(kwargs.get("method", "GET"))
        payload = kwargs.get("payload")
        self.requests.append((url, method, payload))
        if url.endswith("/schedulable"):
            raise MonitorRequestError("simulated scheduling write failure")
        if method == "GET":
            return {
                "code": 0,
                "data": {
                    "id": 42,
                    "status": "active",
                    "schedulable": False,
                    "auto_pause_on_expired": False,
                    "expires_at": None,
                    "rate_limit_reset_at": None,
                    "overload_until": None,
                    "temp_unschedulable_until": None,
                },
            }
        return {"code": 0}


class SchedulingMutationPort:
    def __init__(
        self,
        *,
        state: dict[str, object],
        readback_updates: dict[str, object] | None = None,
        fail_write: bool = False,
    ) -> None:
        self.state = dict(state)
        self.readback_updates = readback_updates
        self.fail_write = fail_write
        self.requests: list[tuple[str, str, object]] = []
        self.write_seen = False

    def _request_body(self, url: str, **_: object) -> bytes:
        raise AssertionError(f"unexpected body request: {url}")

    def _request_sse_body_with_first_event(
        self,
        url: str,
        **_: object,
    ) -> tuple[bytes, int | None]:
        raise AssertionError(f"unexpected SSE request: {url}")

    def _request_json(self, url: str, **kwargs: object) -> dict[str, object]:
        method = str(kwargs.get("method", "GET"))
        payload = kwargs.get("payload")
        self.requests.append((url, method, payload))
        if method == "GET":
            if self.write_seen and self.readback_updates is not None:
                return {"code": 0, "data": {**self.state, **self.readback_updates}}
            return {"code": 0, "data": self.state}
        self.write_seen = True
        if self.fail_write:
            raise MonitorRequestError("simulated scheduling write failure")
        return {"code": 0, "data": {"id": 42}}


def _adapter(pages: dict[int, dict[str, object]]) -> MaintenanceApiAdapter:
    return MaintenanceApiAdapter(
        DynamicPaginationPort(pages),
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )


def test_account_test_records_first_valid_sse_data_event_latency() -> None:
    port = AccountMutationPort()
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.test_account_availability_sync("42")

    assert result.success is True
    assert result.first_event_ms == 1_234


def test_account_test_without_completion_is_indeterminate() -> None:
    port = AccountMutationPort()
    port._request_sse_body_with_first_event = lambda *_args, **_kwargs: (  # type: ignore[method-assign]
        b'data: {"type":"progress"}\n\n',
        100,
    )
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.test_account_availability_sync("42")

    assert result.success is False
    assert result.definitive_failure is False
    assert result.reason == "test_incomplete"


@pytest.mark.parametrize(
    ("account_overrides", "expired", "temporary_unavailable"),
    [
        (
            {
                "auto_pause_on_expired": True,
                "expires_at": datetime(2026, 8, 25, 1, tzinfo=UTC).timestamp(),
            },
            True,
            False,
        ),
        (
            {
                "rate_limit_reset_at": datetime(2026, 8, 25, 3, tzinfo=UTC).timestamp(),
            },
            False,
            True,
        ),
        (
            {
                "overload_until": datetime(2026, 8, 25, 3, tzinfo=UTC).timestamp(),
            },
            False,
            True,
        ),
        (
            {
                "temp_unschedulable_until": datetime(
                    2026, 8, 25, 3, tzinfo=UTC
                ).timestamp(),
            },
            False,
            True,
        ),
    ],
)
def test_dispatch_state_exposes_expiry_and_temporary_protection(
    account_overrides: dict[str, object],
    expired: bool,
    temporary_unavailable: bool,
) -> None:
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    port = AccountMutationPort()
    account = {
        "id": 42,
        "status": "error",
        "schedulable": False,
        "auto_pause_on_expired": False,
        "expires_at": None,
        "rate_limit_reset_at": None,
        "overload_until": None,
        "temp_unschedulable_until": None,
        **account_overrides,
    }
    port._request_json = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
        "code": 0,
        "data": account,
    }
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
        clock=lambda: now,
    )

    state = adapter.fetch_account_dispatch_state_sync("42")

    assert state.success is True
    assert state.expired is expired
    assert state.temporary_unavailable is temporary_unavailable


@pytest.mark.parametrize(
    "account_data",
    [
        {
            "id": 43,
            "status": "active",
            "schedulable": True,
            "priority": 50,
            "load_factor": 9,
            "concurrency": 3,
        },
        {
            "id": 42,
            "status": "active",
            "schedulable": True,
            "load_factor": 9,
            "concurrency": 3,
        },
        {
            "id": 42,
            "status": "active",
            "schedulable": True,
            "priority": True,
            "load_factor": 9,
            "concurrency": 3,
        },
        {
            "id": 42,
            "status": "active",
            "schedulable": True,
            "priority": 50,
            "load_factor": 10_001,
            "concurrency": 3,
        },
    ],
)
def test_scheduling_state_wrong_identity_or_malformed_fields_fail_closed(
    account_data: dict[str, object],
) -> None:
    port = AccountMutationPort()
    port._request_json = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
        "code": 0,
        "data": account_data,
    }
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/api/v1/admin/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    state = adapter.fetch_account_scheduling_state_sync("42")

    assert state.success is False


def test_scheduling_state_uses_official_account_route_and_effective_load_fallback() -> None:
    port = AccountMutationPort()
    port._request_json = lambda url, **kwargs: (  # type: ignore[method-assign]
        port.requests.append((url, str(kwargs.get("method", "GET")), kwargs.get("payload")))
        or {
            "code": 0,
            "data": {
                "id": 42,
                "status": "active",
                "schedulable": True,
                "priority": 50,
                "load_factor": None,
                "concurrency": 4,
            },
        }
    )
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/api/v1/admin/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    state = adapter.fetch_account_scheduling_state_sync("42")

    assert state.success is True
    assert state.account_id == "42"
    assert state.priority == 50
    assert state.load_factor is None
    assert state.concurrency == 4
    assert state.effective_load_factor == 4
    assert port.requests == [
        (
            "https://sub2api.example/api/v1/admin/accounts/42",
            "GET",
            None,
        )
    ]


def test_sub2api_redirect_response_is_rejected_before_json_parsing() -> None:
    def redirecting_opener(*_args: object, **_kwargs: object) -> object:
        raise urllib_error.HTTPError(
            "https://zhisuanapi.cn/api/v1/admin/accounts/42",
            302,
            "Found",
            Message(),
            None,
        )

    client = Sub2APIClient("admin-key", opener=redirecting_opener)

    with pytest.raises(MonitorRequestError):
        client._request_json(  # pyright: ignore[reportPrivateUsage]
            "https://zhisuanapi.cn/api/v1/admin/accounts/42"
        )


@pytest.mark.parametrize(
    ("field_name", "desired", "method", "suffix", "payload"),
    [
        ("priority", 52, "PUT", "", {"priority": 52}),
        ("load_factor", 20, "PUT", "", {"load_factor": 20}),
        (
            "schedulable",
            False,
            "POST",
            "/schedulable",
            {"schedulable": False},
        ),
    ],
)
def test_scheduling_writer_uses_one_official_field_and_exact_readback(
    field_name: str,
    desired: int | bool,
    method: str,
    suffix: str,
    payload: dict[str, object],
) -> None:
    state: dict[str, object] = {
        "id": 42,
        "status": "active",
        "schedulable": True,
        "priority": 50,
        "load_factor": 10,
        "concurrency": 4,
    }
    port = SchedulingMutationPort(
        state=state,
        readback_updates={field_name: desired},
    )
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/api/v1/admin/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.write_account_scheduling_field_sync("42", field_name, desired)

    account_url = "https://sub2api.example/api/v1/admin/accounts/42"
    assert result.success is True
    assert result.before_value == state[field_name]
    assert result.verified_value == desired
    assert port.requests == [
        (account_url, "GET", None),
        (account_url + suffix, method, payload),
        (account_url, "GET", None),
    ]


def test_scheduling_writer_fails_on_readback_mismatch_and_never_writes_another_field() -> None:
    port = SchedulingMutationPort(
        state={
            "id": 42,
            "status": "active",
            "schedulable": True,
            "priority": 50,
            "load_factor": 10,
            "concurrency": 4,
        },
        readback_updates={"priority": 51},
    )
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/api/v1/admin/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.write_account_scheduling_field_sync("42", "priority", 52)

    assert result.success is False
    assert result.reason == "scheduling_write_verification_failed"
    assert result.verified_value == 51
    assert len(port.requests) == 3
    assert sum(request[1] != "GET" for request in port.requests) == 1


def test_scheduling_writer_blocks_manual_pause_and_invalid_values_without_mutation() -> None:
    manual = SchedulingMutationPort(
        state={
            "id": 42,
            "status": "active",
            "schedulable": False,
            "priority": 50,
            "load_factor": 10,
            "concurrency": 4,
        }
    )
    adapter = MaintenanceApiAdapter(
        manual,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/api/v1/admin/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    blocked = adapter.write_account_scheduling_field_sync("42", "priority", 52)
    invalid = adapter.write_account_scheduling_field_sync("42", "load_factor", 10_001)

    assert blocked.success is False
    assert blocked.reason == "manual_pause"
    assert invalid.success is False
    assert invalid.reason == "invalid_scheduling_write"
    assert manual.requests == [
        ("https://sub2api.example/api/v1/admin/accounts/42", "GET", None)
    ]


def test_scheduling_writer_transport_failure_is_failed_and_state_uncertain() -> None:
    port = SchedulingMutationPort(
        state={
            "id": 42,
            "status": "error",
            "schedulable": True,
            "priority": 50,
            "load_factor": 10,
            "concurrency": 4,
        },
        fail_write=True,
    )
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/api/v1/admin/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.write_account_scheduling_field_sync("42", "schedulable", False)

    assert result.success is False
    assert result.state_uncertain is True
    assert result.reason == "scheduling_write_transport_failed"
    assert len(port.requests) == 2


def test_quarantined_account_restore_requires_verified_active_dispatch() -> None:
    port = AccountMutationPort()
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.restore_account_sync(
        "42",
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert result.success is True
    assert port.requests == [
        (
            "https://sub2api.example/accounts/42",
            "PUT",
            {"status": "active"},
        ),
        (
            "https://sub2api.example/accounts/42/schedulable",
            "POST",
            {"schedulable": True},
        ),
        ("https://sub2api.example/accounts/42", "GET", None),
    ]


def test_partial_disable_with_only_dispatch_off_remains_uncertain() -> None:
    adapter = MaintenanceApiAdapter(
        PartialDisablePort(readback_available=True),
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.disable_account_sync("42")

    assert result.success is False
    assert result.state_uncertain is True
    assert result.reason == "disable_state_uncertain"


def test_partial_disable_without_readback_is_uncertain() -> None:
    adapter = MaintenanceApiAdapter(
        PartialDisablePort(readback_available=False),
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
    )

    result = adapter.disable_account_sync("42")

    assert result.success is False
    assert result.state_uncertain is True


def test_quarantined_account_restore_stops_after_deadline() -> None:
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    port = AccountMutationPort()
    adapter = MaintenanceApiAdapter(
        port,
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
        clock=lambda: now,
    )

    result = adapter.restore_account_sync(
        "42",
        now=now,
        deadline=now,
    )

    assert result.success is False
    assert result.reason == "restore_deadline_expired"
    assert port.requests == []


def test_partial_restore_preserves_uncertain_crash_evidence() -> None:
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    adapter = MaintenanceApiAdapter(
        PartialRestorePort(),
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
        clock=lambda: now,
    )

    result = adapter.restore_account_sync(
        "42",
        now=now,
        deadline=now + timedelta(seconds=30),
    )

    assert result.success is False
    assert result.state_uncertain is True
    assert result.reason == "restore_state_uncertain"


def test_sub2api_streaming_probe_measures_before_the_response_completes() -> None:
    ticks = iter((10.0, 10.1, 10.25, 10.3, 10.4))
    response = StreamingResponse(
        b'data: {"type":"progress"}\n\n'
        b'data: {"type":"test_complete","success":true}\n\n'
    )
    client = Sub2APIClient(
        "admin-key",
        opener=lambda *_args, **_kwargs: response,
        monotonic_provider=lambda: next(ticks),
    )

    body, first_event_ms = client._request_sse_body_with_first_event(  # pyright: ignore[reportPrivateUsage]
        "https://zhisuanapi.cn/api/v1/admin/accounts/42/test",
        method="POST",
        payload={},
        timeout_seconds=30,
    )

    assert first_event_ms == 250
    assert body.endswith(b'"success":true}\n\n')


def test_sub2api_streaming_probe_uses_ceiling_at_threshold_boundary() -> None:
    ticks = iter((10.0, 20.0, 40.0004, 40.5, 40.6))
    response = StreamingResponse(
        b'data: {"type":"test_complete","success":true}\n\n'
    )
    client = Sub2APIClient(
        "admin-key",
        opener=lambda *_args, **_kwargs: response,
        monotonic_provider=lambda: next(ticks),
    )

    _, first_event_ms = client._request_sse_body_with_first_event(  # pyright: ignore[reportPrivateUsage]
        "https://zhisuanapi.cn/api/v1/admin/accounts/42/test",
        method="POST",
        payload={},
        timeout_seconds=31,
    )

    assert first_event_ms == 30_001


def test_sub2api_streaming_probe_allows_exactly_thirty_seconds() -> None:
    ticks = iter((10.0, 20.0, 40.0, 40.5, 40.6))
    response = StreamingResponse(
        b'data: {"type":"test_complete","success":true}\n\n'
    )
    client = Sub2APIClient(
        "admin-key",
        opener=lambda *_args, **_kwargs: response,
        monotonic_provider=lambda: next(ticks),
    )

    _, first_event_ms = client._request_sse_body_with_first_event(  # pyright: ignore[reportPrivateUsage]
        "https://zhisuanapi.cn/api/v1/admin/accounts/42/test",
        method="POST",
        payload={},
        timeout_seconds=31,
    )

    assert first_event_ms == 30_000


def test_sub2api_streaming_probe_has_an_absolute_deadline() -> None:
    ticks = iter((0.0, 0.2, 0.4, 0.6, 1.1))
    response = DripStreamingResponse(
        [
            b": heartbeat\n\n",
            b": heartbeat\n\n",
            b'data: {"type":"test_complete","success":true}\n\n',
        ]
    )
    client = Sub2APIClient(
        "admin-key",
        opener=lambda *_args, **_kwargs: response,
        monotonic_provider=lambda: next(ticks),
    )

    with pytest.raises(MonitorRequestError):
        client._request_sse_body_with_first_event(  # pyright: ignore[reportPrivateUsage]
            "https://zhisuanapi.cn/api/v1/admin/accounts/42/test",
            method="POST",
            payload={},
            timeout_seconds=1,
        )


def _page(
    page: int,
    *,
    total: int,
    pages: int,
    items: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "code": 0,
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "page_size": 100,
            "pages": pages,
        },
    }


def test_usage_logs_tolerate_growing_approximate_pagination_and_deduplicate() -> None:
    end = datetime(2026, 8, 23, 9, 0, tzinfo=UTC)
    start = end - timedelta(minutes=30)
    recent = (end - timedelta(minutes=1)).isoformat()
    old = (start - timedelta(seconds=1)).isoformat()
    first_page: list[dict[str, object]] = [
        {
            "id": item_id,
            "account_id": 42,
            "created_at": recent,
            "first_token_ms": 100,
            "duration_ms": 200,
        }
        for item_id in range(1, 101)
    ]
    duplicate = dict(first_page[-1])
    older_item: dict[str, object] = {
        "id": 101,
        "account_id": 42,
        "created_at": old,
        "first_token_ms": 100,
        "duration_ms": 200,
    }
    adapter = _adapter(
        {
            1: _page(1, total=101, pages=2, items=first_page),
            2: _page(2, total=201, pages=3, items=[duplicate, older_item]),
        }
    )

    records = adapter.fetch_recent_usage_logs_sync(start=start, end=end)

    assert len(records) == 100


def test_request_logs_tolerate_growing_approximate_pagination_and_deduplicate() -> None:
    end = datetime(2026, 8, 23, 9, 0, tzinfo=UTC)
    start = end - timedelta(minutes=30)
    recent = (end - timedelta(minutes=1)).isoformat()
    old = (start - timedelta(seconds=1)).isoformat()
    first_page: list[dict[str, object]] = [
        {
            "request_id": f"request-{item_id}",
            "account_id": 42,
            "created_at": recent,
            "kind": "error",
            "status_code": 502,
            "phase": "upstream",
        }
        for item_id in range(1, 101)
    ]
    duplicate = dict(first_page[-1])
    older_item: dict[str, object] = {
        "request_id": "request-101",
        "account_id": 42,
        "created_at": old,
        "kind": "error",
        "status_code": 502,
        "phase": "upstream",
    }
    adapter = _adapter(
        {
            1: _page(1, total=101, pages=2, items=first_page),
            2: _page(2, total=201, pages=3, items=[duplicate, older_item]),
        }
    )

    records = adapter.fetch_recent_request_logs_sync(start=start, end=end)

    assert len(records) == 100


def test_usage_logs_reject_conflicting_duplicate_identifier() -> None:
    end = datetime(2026, 8, 23, 9, 0, tzinfo=UTC)
    start = end - timedelta(minutes=30)
    recent = (end - timedelta(minutes=1)).isoformat()
    first_page: list[dict[str, object]] = [
        {
            "id": item_id,
            "account_id": 42,
            "created_at": recent,
            "first_token_ms": 100,
            "duration_ms": 200,
        }
        for item_id in range(1, 101)
    ]
    conflicting_duplicate = {
        **first_page[-1],
        "account_id": 99,
    }
    adapter = _adapter(
        {
            1: _page(1, total=101, pages=2, items=first_page),
            2: _page(2, total=201, pages=3, items=[conflicting_duplicate]),
        }
    )

    with pytest.raises(MonitorDataError, match="changed while paginating"):
        adapter.fetch_recent_usage_logs_sync(start=start, end=end)
