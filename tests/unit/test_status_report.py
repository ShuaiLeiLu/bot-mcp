from __future__ import annotations

from datetime import UTC, datetime

from notification_image import render_status_report_image
from probe import ChannelHealth, ChannelProbe, GroupAccountCounts, format_status_report


def test_status_report_includes_probe_metadata_and_readable_account_layout() -> None:
    probe = ChannelProbe(
        channel=ChannelHealth(
            monitor_id="19",
            name="0.22× 稳定",
            provider="openai",
            model="gpt-5.6",
            status="operational",
            latency_ms=2682,
            availability_7d=99.8,
            last_checked_at="2026-08-23T10:00:00Z",
            enabled=True,
            group_name="",
        ),
        accounts=GroupAccountCounts("36", "稳定渠道【自有+外接】", 5, 3, 0, 2),
    )

    report = format_status_report(
        [probe],
        triggered_at=datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC),
    )

    assert "渠道监控｜共 1 个｜正常 1｜异常 0" in report
    assert "触发时间：2026-08-24 09:02:03（北京时间）" in report
    assert "✅ 0.22× 稳定" in report
    assert "状态：正常｜延迟：2682ms｜7日可用率：99.8%" in report
    assert "探测：openai｜gpt-5.6" in report
    assert "分组：稳定渠道【自有+外接】 (#36)" in report
    assert "账号：可用 3｜错误 2｜临时不可调度 0｜关闭 0" in report


def test_status_image_contains_the_trigger_time() -> None:
    probe = ChannelProbe(
        channel=ChannelHealth(
            monitor_id="19",
            name="0.22× 稳定",
            provider="openai",
            model="gpt-5.6",
            status="operational",
            latency_ms=2682,
            availability_7d=99.8,
            last_checked_at="2026-08-23T10:00:00Z",
            enabled=True,
            group_name="",
        ),
        accounts=GroupAccountCounts("36", "稳定渠道【自有+外接】", 5, 3, 0, 2),
    )

    first = render_status_report_image(
        [probe],
        triggered_at=datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC),
    )
    second = render_status_report_image(
        [probe],
        triggered_at=datetime(2026, 8, 24, 1, 2, 4, tzinfo=UTC),
    )

    assert first.startswith("data:image/png;base64,")
    assert first != second
