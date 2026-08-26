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

## Hourly account health checks

- Guardian scoring still consumes the shared channel snapshot; it does not issue an account test
  on every 15-second scan.
- Eligible `active + schedulable=true` accounts are tested at most once per configured interval,
  which defaults to 3,600 seconds. The durable ledger survives restarts.
- All automatic test paths use the account default model with the `hi` prompt. An account marked
  `active + schedulable=false` is treated as a human pause and never tested automatically.
- Query `guardian_get_recovery_status` and inspect `active_check.enabled`,
  `active_check.interval_seconds`, and `active_check.last_run_at` when verifying the schedule.
- Healthy hourly checks do not generate administrator messages. Definitive failures,
  indeterminate results, and verified state changes still use the recovery notification path.

## Slow-first-token protection

- Detection reads the Sub2API usage log for only the latest three minutes. It does not issue an
  extra account test before quarantine.
- An eligible account is quarantined after three log records whose `first_token_ms` is greater
  than 30,000. Human pauses, expired/temporary accounts, and the last usable account in a group
  remain protected.
- A quarantined slow account stays disabled after its first successful probe at or below 30,000
  ms. The result is persisted as `PASSING` with streak `1`; a second consecutive passing probe is
  required before verified restore. `SLOW`, `FAILED`, or `INVALID` resets the streak to zero.
- Inspect `sub2api_list_account_quarantines`: `last_probe_result` and
  `recovery_success_streak` show the current recovery progress.

## Safe recovery

- Restarting marks `RUNNING` jobs `INTERRUPTED`; non-resumable video jobs are not duplicated.
- SQLite data is under the configured data path or `/data` volume.
- Never delete the database while a worker is running.
- Before rollback, stop the service and back up the database plus the current image/tag.

## Security incident

- Rotate the MCP, Sub2API, LangBot, and actor-bridge secrets independently.
- Search audit events for recent mutations; tokens themselves are never stored there.
- Do not paste raw `.env`, upstream responses, or platform actor IDs into issue reports.
