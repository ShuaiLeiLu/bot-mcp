# Operations Runbook

## Health

`GET /healthz` returns `200` only after SQLite migrations, MCP session management,
workers, and the scheduler have started.

## First checks

1. Check `/healthz` and container status.
2. Query `sub2api_get_status` for scheduler state, active jobs, and outbox backlog.
3. Inspect JSON logs by `requestId`, `jobId`, or `eventId`.
4. Inspect `/metrics` with an authorized API key.

## Queue or outbox growth

- Video queue: verify the upstream video endpoint is reachable and has not returned an explicit error.
- Control queue: verify the fixed Sub2API admin endpoints and Admin Key.
- Outbox: verify the LangBot URL/API key, target bot runtime, and target ID.
- Unsupported media: set the target media policy to `AUTO`, `TEXT_ONLY`, or `LINK`.

## Database retention

- Retention runs every 10 minutes in batches of at most 20,000 rows per repository, including
  while Guardian direct scheduling is stopped.
- Account observations are retained for 2 days; Guardian runs and idempotency results for 7 days;
  terminal jobs, successful delivery history, and traffic buckets for 30 days; health samples,
  events, probes, closed recovery episodes, input snapshots, and completed recovery runs for
  90 days; audits for 365 days.
- Open channel-error episodes, running recovery/evaluation jobs, queued or failed deliveries,
  active jobs, field ownership, policy, current channels, and account quarantine state are never
  removed by retention.
- Check `sub2api_retention_runs_total`, `sub2api_retention_rows_total`, and
  `sub2api_database_size_bytes`. A failed cleanup is logged as `guardian_retention_failed` and is
  retried on the next 10-minute interval without stopping scheduling.
- Deletion frees SQLite pages for reuse and checkpoints the WAL; the main database file may not
  immediately shrink on disk. Do not run `VACUUM` against a live service.

## Safe recovery

- Restarting marks `RUNNING` jobs `INTERRUPTED`; non-resumable video jobs are not duplicated.
- SQLite data is under the configured data path or `/data` volume.
- Never delete the database while a worker is running.
- Before rollback, stop the service and back up the database plus the current image/tag.

## Security incident

- Rotate the MCP, Sub2API, LangBot, and actor-bridge secrets independently.
- Search audit events for recent mutations; tokens themselves are never stored there.
- Do not paste raw `.env`, upstream responses, or platform actor IDs into issue reports.
