from __future__ import annotations

import json
from datetime import UTC, datetime
from types import TracebackType
from typing import Self, cast
from urllib.request import Request

from monitor import Sub2APIClient
from recovery import RecoveryCandidate, parse_recovery_account_page


class _FakeResponse:
    def __init__(self, body: bytes, content_type: str = "application/json"):
        self.body = body
        self.headers = {"Content-Type": content_type}

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, exc, traceback
        return False

    def read(self, size: int) -> bytes:
        return self.body[:size]


def _account(
    account_id: int,
    *,
    status: str,
    schedulable: bool,
) -> dict[str, object]:
    return {
        "id": account_id,
        "name": f"账号{account_id}",
        "status": status,
        "schedulable": schedulable,
        "rate_limit_reset_at": None,
        "overload_until": None,
        "temp_unschedulable_until": None,
        "auto_pause_on_expired": False,
        "expires_at": None,
    }


def test_error_account_is_recoverable_even_when_dispatch_is_off() -> None:
    now = datetime(2026, 8, 24, 4, 0, tzinfo=UTC)
    payload = {
        "code": 0,
        "data": {
            "items": [
                _account(995, status="active", schedulable=False),
                _account(997, status="error", schedulable=False),
                _account(999, status="inactive", schedulable=False),
            ],
            "total": 3,
            "page": 1,
            "page_size": 100,
            "pages": 1,
        },
    }

    candidates, pages = parse_recovery_account_page(
        payload,
        expected_page=1,
        now=now,
    )

    assert pages == 1
    assert [(item.account_id, item.status, item.schedulable) for item in candidates] == [
        ("997", "error", False)
    ]


def test_successful_error_probe_restores_status_and_dispatch() -> None:
    candidate = RecoveryCandidate("997", "错误账号", "error", "error", False)
    account_url = Sub2APIClient.ADMIN_ACCOUNTS_URL + "/997"
    normal_account = {
        "code": 0,
        "data": {
            "id": 997,
            "status": "active",
            "schedulable": True,
            "rate_limit_reset_at": None,
            "overload_until": None,
            "temp_unschedulable_until": None,
            "auto_pause_on_expired": False,
            "expires_at": None,
        },
    }
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def opener(request: Request, timeout: float) -> _FakeResponse:
        del timeout
        body = (
            cast(dict[str, object], json.loads(cast(bytes, request.data)))
            if request.data
            else None
        )
        calls.append((request.get_method(), request.full_url, body))
        if request.full_url.endswith("/test"):
            return _FakeResponse(
                b'data: {"type":"test_complete","success":true}\n\n',
                "text/event-stream",
            )
        return _FakeResponse(json.dumps(normal_account).encode())

    outcome = Sub2APIClient(
        "admin-value",
        opener=opener,
    ).test_and_recover_account_sync(candidate, now=datetime(2026, 8, 24, 4, 0, tzinfo=UTC))

    assert outcome.result == "recovered"
    assert calls == [
        ("POST", account_url + "/test", {}),
        ("POST", account_url + "/recover-state", None),
        ("PUT", account_url, {"status": "active"}),
        ("POST", account_url + "/schedulable", {"schedulable": True}),
        ("GET", account_url, None),
    ]


def test_non_error_pause_is_never_tested_or_enabled() -> None:
    candidate = RecoveryCandidate("995", "暂停账号", "closed", "active", False)
    calls: list[str] = []

    def opener(request: Request, timeout: float) -> _FakeResponse:
        del timeout
        calls.append(request.full_url)
        return _FakeResponse(b"unexpected")

    outcome = Sub2APIClient(
        "admin-value",
        opener=opener,
    ).test_and_recover_account_sync(candidate, now=datetime(2026, 8, 24, 4, 0, tzinfo=UTC))

    assert outcome.result == "test_failed"
    assert calls == []
