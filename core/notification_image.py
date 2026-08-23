from __future__ import annotations

import base64
import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from probe import ChannelProbe


class NotificationImageError(RuntimeError):
    """Raised when a notification image cannot be rendered safely."""


_COUNT_COLORS = {
    "available": ((220, 252, 231), (22, 101, 52)),
    "error": ((254, 226, 226), (153, 27, 27)),
    "temporary": ((254, 243, 199), (146, 64, 14)),
    "closed": ((229, 231, 235), (75, 85, 99)),
}
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
_MAINTENANCE_LABELS = {
    "channel_test_failed": "渠道异常测试失败，已关闭",
    "repeated_errors": "30 分钟内重复错误，已关闭",
    "slow_first_token": "首字延迟超 30 秒，已关闭",
}
_MAX_CHANNEL_ROWS = 200


@dataclass(frozen=True, slots=True)
class _StatusRow:
    name: str
    latency: str
    available: str
    error: str
    temporary: str
    closed: str


@dataclass(frozen=True, slots=True)
class _AdminRow:
    account: str
    original: str
    result: str


def render_status_report_image(probes: Iterable[ChannelProbe]) -> str:
    """Render the channel status report as a PNG data URI."""

    probe_list = list(probes)
    rows = [_status_row(probe) for probe in probe_list[:_MAX_CHANNEL_ROWS]]
    omitted = max(0, len(probe_list) - _MAX_CHANNEL_ROWS)
    return _render_report(rows, (), omitted=omitted)


def render_admin_report_image(
    probes: Iterable[ChannelProbe],
    recovery_outcomes: Sequence[Any],
    maintenance_adjustments: Sequence[Any],
) -> str:
    """Render admin-only account adjustments followed by the status table."""

    probe_list = list(probes)
    rows = [_status_row(probe) for probe in probe_list[:_MAX_CHANNEL_ROWS]]
    admin_rows = [
        _AdminRow(
            account=f"{outcome.name} (#{outcome.account_id})",
            original=_BUCKET_LABELS.get(outcome.bucket, "未知"),
            result=_RESULT_LABELS.get(outcome.result, "处理完成"),
        )
        for outcome in recovery_outcomes
    ]
    admin_rows.extend(
        _AdminRow(
            account=f"{adjustment.account_name} (#{adjustment.account_id})",
            original="账号健康规则",
            result=_MAINTENANCE_LABELS.get(
                adjustment.reason,
                "触发账号健康规则，已关闭",
            ),
        )
        for adjustment in maintenance_adjustments
    )
    omitted = max(0, len(probe_list) - _MAX_CHANNEL_ROWS)
    return _render_report(rows, admin_rows, omitted=omitted)


def _status_row(probe: ChannelProbe) -> _StatusRow:
    channel = probe.channel
    accounts = probe.accounts
    if accounts is None:
        counts = ("--", "--", "--", "--")
    else:
        counts = (
            str(accounts.available_count),
            str(accounts.error_count),
            str(accounts.temporary_unavailable_count),
            str(accounts.closed_count),
        )
    return _StatusRow(
        name=channel.name,
        latency="--" if channel.latency_ms is None else f"{channel.latency_ms}ms",
        available=counts[0],
        error=counts[1],
        temporary=counts[2],
        closed=counts[3],
    )


def _render_report(
    rows: Sequence[_StatusRow],
    admin_rows: Sequence[_AdminRow],
    *,
    omitted: int,
) -> str:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:  # pragma: no cover - depends on runtime packaging
        raise NotificationImageError("Pillow is not installed") from exc

    font_path = _font_path()
    title_font = _load_font(ImageFont, font_path, 34)
    header_font = _load_font(ImageFont, font_path, 22)
    body_font = _load_font(ImageFont, font_path, 24)
    small_font = _load_font(ImageFont, font_path, 20)

    margin = 36
    width = 1536
    title_height = 94
    admin_header_height = 62 if admin_rows else 0
    admin_row_height = 58 * len(admin_rows)
    table_header_height = 70
    row_height = 72
    footer_height = 64 if omitted else 28
    height = (
        margin
        + title_height
        + admin_header_height
        + admin_row_height
        + table_header_height
        + max(1, len(rows)) * row_height
        + footer_height
        + margin
    )

    image = Image.new("RGB", (width, height), (246, 248, 251))
    draw = ImageDraw.Draw(image)
    _rounded_rectangle(draw, (margin, margin, width - margin, height - margin), 20, (255, 255, 255))

    x0 = margin + 24
    x1 = width - margin - 24
    y = margin + 20
    draw.text((x0, y), "智算渠道状态", font=title_font, fill=(15, 23, 42))
    draw.text(
        (x1, y + 12),
        "主动探测结果",
        font=small_font,
        fill=(100, 116, 139),
        anchor="ra",
    )
    y += title_height

    if admin_rows:
        draw.text((x0, y + 12), "账号自动处理（管理员专属）", font=header_font, fill=(15, 23, 42))
        y += admin_header_height
        admin_columns = (x0, x0 + 580, x0 + 890, x1)
        for index, row in enumerate(admin_rows):
            row_y = y + index * 58
            fill = (248, 250, 252) if index % 2 else (241, 245, 249)
            draw.rectangle((x0, row_y, x1, row_y + 58), fill=fill)
            _draw_cell_text(draw, row.account, admin_columns[0], admin_columns[1], row_y, 58, body_font, (30, 41, 59))
            _draw_cell_text(draw, row.original, admin_columns[1], admin_columns[2], row_y, 58, small_font, (71, 85, 105))
            _draw_cell_text(draw, row.result, admin_columns[2], admin_columns[3], row_y, 58, small_font, (153, 27, 27))
        y += admin_row_height + 18

    columns = (
        ("渠道", 470),
        ("延迟", 140),
        ("可用", 130),
        ("错误", 130),
        ("临时不可调度", 220),
        ("关闭", 156),
    )
    boundaries = [x0]
    for _, column_width in columns:
        boundaries.append(boundaries[-1] + column_width)
    draw.rectangle((x0, y, x1, y + table_header_height), fill=(30, 64, 175))
    for index, (label, _) in enumerate(columns):
        _draw_cell_text(
            draw,
            label,
            boundaries[index],
            boundaries[index + 1],
            y,
            table_header_height,
            header_font,
            (255, 255, 255),
            center=index > 0,
        )
    y += table_header_height

    if not rows:
        draw.rectangle((x0, y, x1, y + row_height), fill=(248, 250, 252))
        _draw_cell_text(draw, "暂无启用的渠道探测结果", x0, x1, y, row_height, body_font, (71, 85, 105))
        y += row_height
    else:
        for index, row in enumerate(rows):
            row_y = y + index * row_height
            fill = (255, 255, 255) if index % 2 else (248, 250, 252)
            draw.rectangle((x0, row_y, x1, row_y + row_height), fill=fill)
            _draw_cell_text(draw, row.name, boundaries[0], boundaries[1], row_y, row_height, body_font, (30, 41, 59))
            _draw_cell_text(draw, row.latency, boundaries[1], boundaries[2], row_y, row_height, body_font, (30, 41, 59), center=True)
            for bucket_index, (value, bucket) in enumerate(
                (
                    (row.available, "available"),
                    (row.error, "error"),
                    (row.temporary, "temporary"),
                    (row.closed, "closed"),
                ),
                start=2,
            ):
                _draw_count_cell(draw, value, bucket, boundaries[bucket_index], boundaries[bucket_index + 1], row_y, row_height, body_font)
    y += max(1, len(rows)) * row_height

    if omitted:
        draw.text(
            (x0, y + 18),
            f"其余 {omitted} 个渠道未放入图片，请使用 /zs 查询完整状态。",
            font=small_font,
            fill=(100, 116, 139),
        )

    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    encoded = base64.b64encode(output.getvalue()).decode("ascii")
    if len(encoded) > 4 * 1024 * 1024:
        raise NotificationImageError("notification image is too large")
    return f"data:image/png;base64,{encoded}"


def _font_path() -> str | None:
    configured = os.environ.get("ZHISUAN_NOTIFICATION_FONT", "").strip()
    candidates = [
        configured,
        str(Path(__file__).with_name("assets") / "wqy-microhei.ttc"),
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansSC-Regular.otf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
    ]
    return next((path for path in candidates if path and Path(path).is_file()), None)


def _load_font(font_module: Any, path: str | None, size: int) -> Any:
    if path:
        try:
            return font_module.truetype(path, size=size)
        except OSError:
            pass
    try:
        return font_module.load_default(size=size)
    except TypeError:  # Pillow versions before the size argument was added.
        return font_module.load_default()


def _rounded_rectangle(draw: Any, box: tuple[int, int, int, int], radius: int, fill: tuple[int, int, int]) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill)


def _draw_cell_text(
    draw: Any,
    text: str,
    left: int,
    right: int,
    top: int,
    height: int,
    font: Any,
    fill: tuple[int, int, int],
    *,
    center: bool = False,
) -> None:
    available_width = max(10, right - left - 24)
    text = _truncate(text, font, available_width, draw)
    anchor = "mm" if center else "lm"
    position = ((left + right) // 2, top + height // 2) if center else (left + 12, top + height // 2)
    draw.text(position, text, font=font, fill=fill, anchor=anchor)


def _draw_count_cell(draw: Any, value: str, bucket: str, left: int, right: int, top: int, height: int, font: Any) -> None:
    background, foreground = _COUNT_COLORS[bucket]
    if value == "--":
        background, foreground = (243, 244, 246), (107, 114, 128)
    draw.rounded_rectangle(
        (left + 18, top + 16, right - 18, top + height - 16),
        radius=14,
        fill=background,
    )
    draw.text(((left + right) // 2, top + height // 2), value, font=font, fill=foreground, anchor="mm")


def _truncate(text: str, font: Any, available_width: int, draw: Any) -> str:
    value = str(text)
    if draw.textlength(value, font=font) <= available_width:
        return value
    suffix = "…"
    while value and draw.textlength(value + suffix, font=font) > available_width:
        value = value[:-1]
    return value + suffix if value else suffix
