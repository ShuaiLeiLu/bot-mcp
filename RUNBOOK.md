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

## Safe recovery

- Restarting marks `RUNNING` jobs `INTERRUPTED`; non-resumable video jobs are not duplicated.
- SQLite data is under the configured data path or `/data` volume.
- Never delete the database while a worker is running.
- Before rollback, stop the service and back up the database plus the current image/tag.

## Security incident

- Rotate the MCP, Sub2API, LangBot, and actor-bridge secrets independently.
- Search audit events for recent mutations; tokens themselves are never stored there.
- Do not paste raw `.env`, upstream responses, or platform actor IDs into issue reports.

