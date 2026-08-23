from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
from maintenance_gateway import MaintenanceApiAdapter, MaintenanceApiAdapterConfig
from probe import MonitorDataError


class DynamicPaginationPort:
    def __init__(self, pages: dict[int, dict[str, object]]) -> None:
        self.pages = pages

    def _request_json(self, url: str, **_: object) -> dict[str, object]:
        page = int(parse_qs(urlsplit(url).query)["page"][0])
        return self.pages[page]

    def _request_body(self, url: str, **_: object) -> bytes:
        raise AssertionError(f"unexpected body request: {url}")


def _adapter(pages: dict[int, dict[str, object]]) -> MaintenanceApiAdapter:
    return MaintenanceApiAdapter(
        DynamicPaginationPort(pages),
        MaintenanceApiAdapterConfig(
            accounts_url="https://sub2api.example/accounts",
            usage_url="https://sub2api.example/usage",
            request_logs_url="https://sub2api.example/requests",
        ),
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
