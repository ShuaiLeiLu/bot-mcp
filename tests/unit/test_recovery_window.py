from __future__ import annotations

from datetime import UTC, datetime

from recovery import active_recovery_window, recovery_window_id


def test_equal_recovery_window_is_a_real_twenty_four_hour_window() -> None:
    morning = datetime(2026, 8, 24, 1, 30, tzinfo=UTC)
    evening = datetime(2026, 8, 24, 14, 30, tzinfo=UTC)

    assert recovery_window_id(morning, "00:00", "00:00") == (
        "2026-08-24/00:00-00:00"
    )
    assert recovery_window_id(evening, "00:00", "00:00") == (
        "2026-08-24/00:00-00:00"
    )

    active = active_recovery_window(evening, "00:00", "00:00")

    assert active is not None
    assert active.ends_at == datetime(2026, 8, 24, 16, 0, tzinfo=UTC)


def test_equal_recovery_window_uses_the_latest_daily_boundary() -> None:
    before_beijing_anchor = datetime(2026, 8, 23, 17, 30, tzinfo=UTC)

    active = active_recovery_window(
        before_beijing_anchor,
        "02:00",
        "02:00",
    )

    assert active is not None
    assert active.window_id == "2026-08-23/02:00-02:00"
    assert active.ends_at == datetime(2026, 8, 23, 18, 0, tzinfo=UTC)
