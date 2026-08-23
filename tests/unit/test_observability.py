from __future__ import annotations

import io
import json
import logging

from sub2api_mcp.logging import SafeJsonFormatter, log_event
from sub2api_mcp.metrics import Metrics


def test_json_logs_include_correlation_fields_and_drop_secrets() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(SafeJsonFormatter())
    logger = logging.getLogger("test-safe-json")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    log_event(
        logger,
        logging.INFO,
        "mcp_call_finished",
        requestId="request-1",
        tool="sub2api_get_status",
        status="ok",
        token="must-not-appear",
        rawEmail="must-not-appear@example.com",
    )

    payload = json.loads(stream.getvalue())
    assert payload["event"] == "mcp_call_finished"
    assert payload["requestId"] == "request-1"
    assert payload["tool"] == "sub2api_get_status"
    assert "must-not-appear" not in stream.getvalue()


def test_metrics_render_red_and_queue_signals() -> None:
    metrics = Metrics.create()
    metrics.mcp_calls.labels(tool="sub2api_get_status", status="ok").inc()
    metrics.job_queue_depth.labels(job_type="VIDEO").set(2)

    output = metrics.render().decode()

    assert 'sub2api_mcp_calls_total{status="ok",tool="sub2api_get_status"} 1.0' in output
    assert 'sub2api_job_queue_depth{job_type="VIDEO"} 2.0' in output

