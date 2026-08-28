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
    account_quarantines: Gauge
    account_quarantine_probes: Counter
    account_quarantine_transitions: Counter
    guardian_runs: Counter
    guardian_duration: Histogram
    guardian_shared_snapshots: Counter
    guardian_snapshot_age_seconds: Gauge
    guardian_duplicate_observations: Counter
    guardian_traffic_buckets: Counter
    guardian_channel_confidence_min: Gauge
    guardian_channel_confidence_average: Gauge
    guardian_channels_by_freshness: Gauge
    guardian_write_frozen: Counter
    guardian_recovery_probe_requests: Counter
    guardian_recovery_probe_tokens: Counter
    guardian_field_ownership_changes: Counter
    guardian_scheduling_writes: Counter
    guardian_account_recovery_results: Counter
    retention_runs: Counter
    retention_rows: Counter
    database_size_bytes: Gauge
    outbox_backlog: Gauge
    outbox_terminal_failures: Gauge
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
            guardian_runs=Counter(
                "sub2api_guardian_runs_total",
                "Guardian evaluation results",
                ("status", "mode"),
                registry=registry,
            ),
            guardian_duration=Histogram(
                "sub2api_guardian_run_duration_seconds",
                "Guardian evaluation duration",
                ("mode",),
                registry=registry,
            ),
            account_quarantines=Gauge(
                "sub2api_account_quarantines",
                "System-owned account quarantines",
                ("reason",),
                registry=registry,
            ),
            account_quarantine_probes=Counter(
                "sub2api_account_quarantine_probes_total",
                "Account quarantine probes by reason and result",
                ("reason", "result"),
                registry=registry,
            ),
            account_quarantine_transitions=Counter(
                "sub2api_account_quarantine_transitions_total",
                "Account quarantine lifecycle transitions",
                ("reason", "action"),
                registry=registry,
            ),
            guardian_shared_snapshots=Counter(
                "guardian_shared_snapshots_total",
                "Shared Guardian snapshots by processing result",
                ("status",),
                registry=registry,
            ),
            guardian_snapshot_age_seconds=Gauge(
                "guardian_snapshot_age_seconds",
                "Age of the latest shared Guardian snapshot",
                registry=registry,
            ),
            guardian_duplicate_observations=Counter(
                "guardian_duplicate_observations_total",
                "Duplicate Guardian observations discarded",
                ("source",),
                registry=registry,
            ),
            guardian_traffic_buckets=Counter(
                "guardian_traffic_buckets_total",
                "Guardian traffic bucket processing results",
                ("status",),
                registry=registry,
            ),
            guardian_channel_confidence_min=Gauge(
                "guardian_channel_confidence_min",
                "Minimum evidence confidence across Guardian channels",
                registry=registry,
            ),
            guardian_channel_confidence_average=Gauge(
                "guardian_channel_confidence_average",
                "Average evidence confidence across Guardian channels",
                registry=registry,
            ),
            guardian_channels_by_freshness=Gauge(
                "guardian_channels_by_freshness",
                "Guardian channels by evidence freshness state",
                ("state",),
                registry=registry,
            ),
            guardian_write_frozen=Counter(
                "guardian_write_frozen_total",
                "Guardian write recommendations frozen by reason",
                ("reason",),
                registry=registry,
            ),
            guardian_recovery_probe_requests=Counter(
                "guardian_recovery_probe_requests_total",
                "Guardian recovery probe request results",
                ("result",),
                registry=registry,
            ),
            guardian_recovery_probe_tokens=Counter(
                "guardian_recovery_probe_tokens_total",
                "Guardian recovery probe tokens",
                ("priced",),
                registry=registry,
            ),
            guardian_field_ownership_changes=Counter(
                "guardian_field_ownership_changes_total",
                "Guardian field ownership transitions",
                ("from_owner", "to_owner"),
                registry=registry,
            ),
            guardian_scheduling_writes=Counter(
                "guardian_scheduling_writes_total",
                "Verified Guardian scheduling field outcomes",
                ("field", "outcome"),
                registry=registry,
            ),
            guardian_account_recovery_results=Counter(
                "guardian_account_recovery_results_total",
                "Guardian conditional account recovery outcomes",
                ("result",),
                registry=registry,
            ),
            retention_runs=Counter(
                "sub2api_retention_runs_total",
                "Scheduled SQLite retention runs",
                ("status",),
                registry=registry,
            ),
            retention_rows=Counter(
                "sub2api_retention_rows_total",
                "Rows processed by scheduled SQLite retention",
                ("store", "operation"),
                registry=registry,
            ),
            database_size_bytes=Gauge(
                "sub2api_database_size_bytes",
                "SQLite database, WAL, and shared-memory bytes",
                registry=registry,
            ),
            outbox_backlog=Gauge(
                "sub2api_outbox_backlog",
                "Undelivered notification count",
                registry=registry,
            ),
            outbox_terminal_failures=Gauge(
                "sub2api_outbox_terminal_failures",
                "Notifications stopped after a non-retryable delivery failure",
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

