from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
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
    ticks = iter((10.0, 20.0, 40.0004))
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


def test_sub2api_streaming_probe_has_an_absolute_deadline() -> None:
    ticks = iter((0.0, 0.2, 0.5, 0.8, 1.1))
    response = StreamingResponse(
        b': heartbeat\n\n'
        b': heartbeat\n\n'
        b'data: {"type":"test_complete","success":true}\n\n'
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
