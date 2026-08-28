"""Structured, allowlisted JSON logging for production diagnostics."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

_SAFE_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")

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
        "terminalFailures",
        "nextRetrySeconds",
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
        if record.exc_info is not None:
            exception_type, exception, _ = record.exc_info
            if exception_type is not None:
                payload["exceptionType"] = exception_type.__name__
            error_code = getattr(exception, "code", None)
            if isinstance(error_code, str) and _SAFE_ERROR_CODE.fullmatch(error_code):
                payload["errorCode"] = error_code
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
