"""Low-cardinality Prometheus metrics for the MCP service."""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


@dataclass(slots=True)
class Metrics:
    registry: CollectorRegistry
    mcp_calls: Counter
    mcp_duration: Histogram
    upstream_calls: Counter
    upstream_duration: Histogram
    job_transitions: Counter
    job_queue_depth: Gauge
    scheduler_runs: Counter
    outbox_backlog: Gauge
    outbox_oldest_age_seconds: Gauge

    @classmethod
    def create(cls) -> Metrics:
        registry = CollectorRegistry(auto_describe=True)
        return cls(
            registry=registry,
            mcp_calls=Counter(
                "sub2api_mcp_calls_total",
                "MCP tool calls",
                ("tool", "status"),
                registry=registry,
            ),
            mcp_duration=Histogram(
                "sub2api_mcp_call_duration_seconds",
                "MCP tool call duration",
                ("tool",),
                registry=registry,
            ),
            upstream_calls=Counter(
                "sub2api_upstream_calls_total",
                "External dependency calls",
                ("dependency", "status"),
                registry=registry,
            ),
            upstream_duration=Histogram(
                "sub2api_upstream_call_duration_seconds",
                "External dependency duration",
                ("dependency",),
                registry=registry,
            ),
            job_transitions=Counter(
                "sub2api_job_transitions_total",
                "Durable job state transitions",
                ("job_type", "status"),
                registry=registry,
            ),
            job_queue_depth=Gauge(
                "sub2api_job_queue_depth",
                "Queued jobs",
                ("job_type",),
                registry=registry,
            ),
            scheduler_runs=Counter(
                "sub2api_scheduler_runs_total",
                "Scheduler cycle results",
                ("status",),
                registry=registry,
            ),
            outbox_backlog=Gauge(
                "sub2api_outbox_backlog",
                "Undelivered notification count",
                registry=registry,
            ),
            outbox_oldest_age_seconds=Gauge(
                "sub2api_outbox_oldest_age_seconds",
                "Age of the oldest undelivered notification",
                registry=registry,
            ),
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

