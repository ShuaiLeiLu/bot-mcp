from __future__ import annotations

import json
from collections.abc import Mapping
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

from monitor import Sub2APIClient


class FakeResponse:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.body = json.dumps(payload).encode()
        self.headers = {"Content-Type": "application/json"}

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, _: int) -> bytes:
        return self.body


def test_fetch_probe_binds_arbitrary_channel_name_through_api_key_usage_group() -> None:
    checked_at = "2026-08-23T10:00:00Z"
    channel_payload: dict[str, object] = {
        "code": 0,
        "data": {
            "items": [
                {
                    "id": 21,
                    "name": "完全无关的显示名称",
                    "provider": "openai",
                    "primary_model": "gpt-test",
                    "primary_status": "operational",
                    "primary_latency_ms": 2700,
                    "availability_7d": 99.8,
                    "last_checked_at": checked_at,
                    "enabled": True,
                    "group_name": "",
                }
            ],
            "total": 1,
            "page": 1,
            "page_size": 20,
            "pages": 1,
        },
    }
    groups_payload: dict[str, object] = {
        "code": 0,
        "data": [{"id": 47, "name": "team 特惠"}],
    }
    accounts_payload: dict[str, object] = {
        "code": 0,
        "data": {
            "items": [
                {
                    "id": 9001,
                    "name": "账号甲",
                    "status": "active",
                    "schedulable": True,
                    "auto_pause_on_expired": False,
                    "expires_at": None,
                    "rate_limit_reset_at": None,
                    "overload_until": None,
                    "temp_unschedulable_until": None,
                    "group_ids": [47],
                }
            ],
            "total": 1,
            "page": 1,
            "page_size": 100,
            "pages": 1,
        },
    }
    usage_payload: dict[str, object] = {
        "code": 0,
        "data": {
            "items": [
                {
                    "id": 501,
                    "api_key_id": 202,
                    "group_id": 47,
                    "model": "gpt-test",
                    "created_at": "2026-08-23T10:00:00.700000Z",
                    "duration_ms": 2680,
                    "user_agent": "Go-http-client/1.1",
                }
            ],
            "total": 1,
            "page": 1,
            "page_size": 100,
            "pages": 1,
        },
    }

    def opener(request: Request, timeout: int) -> FakeResponse:
        del timeout
        path = urlsplit(request.full_url).path
        if path.endswith("/channel-monitors"):
            return FakeResponse(channel_payload)
        if path.endswith("/groups/all"):
            return FakeResponse(groups_payload)
        if path.endswith("/accounts"):
            return FakeResponse(accounts_payload)
        if path.endswith("/usage"):
            query = parse_qs(urlsplit(request.full_url).query)
            assert query["model"] == ["gpt-test"]
            return FakeResponse(usage_payload)
        raise AssertionError(f"unexpected URL: {request.full_url}")

    probes = Sub2APIClient("admin-value", opener=opener).fetch_probe_sync()

    assert probes[0].accounts is not None
    assert probes[0].accounts.group_id == "47"
    assert probes[0].accounts.available_count == 1
