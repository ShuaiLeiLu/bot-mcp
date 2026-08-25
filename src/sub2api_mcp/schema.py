"""SQLite schema for the Sub2API MCP service."""

SCHEMA_VERSION = 3

ACCOUNT_QUARANTINE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS account_quarantines (
    account_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL CHECK (
        reason IN ('SLOW_FIRST_TOKEN', 'CHANNEL_TEST_FAILED')
    ),
    group_ids_json TEXT NOT NULL,
    threshold_ms INTEGER NOT NULL CHECK (threshold_ms > 0),
    observed_count INTEGER NOT NULL CHECK (observed_count > 0),
    quarantined_at TEXT NOT NULL,
    last_probe_at TEXT,
    last_probe_latency_ms INTEGER CHECK (
        last_probe_latency_ms IS NULL OR last_probe_latency_ms >= 0
    ),
    last_probe_result TEXT NOT NULL CHECK (
        last_probe_result IN ('NEVER', 'RECOVERED', 'FAILED', 'SLOW', 'INVALID')
    ),
    updated_at TEXT NOT NULL
)
"""
ACCOUNT_QUARANTINE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_account_quarantines_probe
    ON account_quarantines(last_probe_at, quarantined_at, account_id)
"""
ACCOUNT_QUARANTINE_TABLE_SQL = (
    f"{ACCOUNT_QUARANTINE_TABLE_DDL};{ACCOUNT_QUARANTINE_INDEX_DDL};"
)

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS service_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scheduler_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scheduler_lease (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    result_json TEXT,
    error_code TEXT,
    error_message TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    worker_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs(status, job_type, created_at, job_id);
CREATE TABLE IF NOT EXISTS delivery_targets (
    delivery_target_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    bot_uuid TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    purposes_json TEXT NOT NULL,
    media_policy TEXT NOT NULL,
    required INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notification_outbox (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS notification_deliveries (
    delivery_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL
        REFERENCES notification_outbox(event_id) ON DELETE CASCADE,
    delivery_target_id TEXT NOT NULL
        REFERENCES delivery_targets(delivery_target_id),
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at TEXT,
    next_attempt_at TEXT,
    last_error_code TEXT,
    delivered_at TEXT,
    UNIQUE(event_id, delivery_target_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_claim
    ON notification_deliveries(status, next_attempt_at, lease_expires_at);
CREATE TABLE IF NOT EXISTS account_bindings (
    actor_key TEXT PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    masked_email TEXT NOT NULL,
    bound_at TEXT NOT NULL
);
{ACCOUNT_QUARANTINE_TABLE_SQL}
CREATE TABLE IF NOT EXISTS actor_nonces (
    nonce TEXT PRIMARY KEY,
    expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS probe_snapshots (
    snapshot_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    audit_id TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    action TEXT NOT NULL,
    subject TEXT,
    outcome TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""
