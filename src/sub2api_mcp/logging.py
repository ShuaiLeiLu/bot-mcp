"""Structured, allowlisted JSON logging for production diagnostics."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

SAFE_FIELDS = frozenset(
    {
        "requestId",
        "jobId",
        "eventId",
        "tool",
        "jobType",
        "eventType",
        "status",
        "statusClass",
        "errorCode",
        "dependency",
        "durationMs",
        "attempt",
        "queueDepth",
        "principal",
        "store",
        "processedRows",
        "deletedRows",
        "databaseBytes",
    }
)


class SafeJsonFormatter(logging.Formatter):
    """Serialize only explicitly allowlisted record attributes."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
        }
        for field in SAFE_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def configure_logging(level: str) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter())
    logger = logging.getLogger("sub2api_mcp")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(level)
    return logger


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str | None = None,
    **fields: object,
) -> None:
    safe_extra = {key: value for key, value in fields.items() if key in SAFE_FIELDS}
    safe_extra["event"] = event
    logger.log(level, message or event, extra=safe_extra)

