# Guardian direct scheduling runbook

## What runs and what costs tokens

The Scheduler remains the only periodic collector. Every normal probe reads the Sub2API channel
monitors and account inventory once, then publishes one canonical local SQLite snapshot.
Guardian's 15-second loop consumes that local snapshot exactly once.

An **inventory read is not an account test**. It reads status, group membership, schedulable and
temporary-protection fields but does not call an upstream model. Guardian sends an account test
only in these cases:

1. a new snapshot contains `error`, `disabled`, or `inactive`;
2. a channel first enters a continuous `failed/error` episode, which tests each eligible account
   in its uniquely mapped group once;
3. an administrator confirms a pending recovery job backed by one of those durable conditions.

`active + schedulable=false`, expired accounts, and temporary rate-limit/overload states receive
zero account tests and zero writes.

## Starting and stopping

Use the light Guardian console, REST, or MCP. Start/stop requires `confirm=true`, the current policy
revision, and an idempotency key. `enabled=true` is direct mode; there is no observe or rollout
stage.

Emergency stop sets `enabled=false`. Every field write re-reads that live flag immediately before
entering the writer, so writes not yet started are blocked. A write already in its atomic
read-write-read sequence completes and is audited.

## Scheduling safety

- Monitor IDs are never sent to Sub2API account mutation routes.
- A monitor must uniquely map to one group; an account must belong only to that managed group.
- `load_factor` is bounded to 1–10000 and moves at most the configured relative step.
- `priority` preserves the observed Sub2API baseline (official default 50); smaller is higher.
- Every operation writes one field and performs a separate exact account GET.
- A wrong ID, malformed response, redirect, ownership conflict, cooldown, stale/low-confidence
  evidence, or read-back mismatch fails closed.
- One failed verification stops all remaining writes in that run.
- Account `schedulable` is changed only by conditional recovery after explicit test evidence.

## Recovery behavior

Explicit test success enables the account and verifies `status=active` plus `schedulable=true`.
Definitive test failure disables and verifies. Indeterminate results preserve state. Manual pauses
are never re-enabled. Run, episode, and per-account result keys are durable and restart-safe.

Notifications go to personal `RECOVERY_ADMIN` delivery targets through LangBot. They include
Beijing time, trigger, channel/group context, aggregate counts, and localized safe reasons.

## Useful metrics

- `guardian_scheduling_writes_total{field,outcome}`
- `guardian_account_recovery_results_total{result}`
- `guardian_write_frozen_total{reason}`
- `guardian_shared_snapshots_total{status}`
- `guardian_snapshot_age_seconds`
- `sub2api_outbox_backlog`

Labels are bounded enums; account, channel, group, request, and error-message values are not metric
labels.

## Legacy environment migration

`SUB2API_MCP_RECOVERY_ENABLED`, `SUB2API_MCP_RECOVERY_WINDOW_START`,
`SUB2API_MCP_RECOVERY_WINDOW_END`, and `SUB2API_MCP_RECOVERY_MAX_ACCOUNTS_PER_RUN` are accepted by
Settings for configuration-file compatibility but have no executable recovery path. Remove them
after confirming Guardian ownership. Recovery is controlled only by Guardian policy and durable
evidence.

Legacy policy keys `observe_only`, `auto_apply`, and `rollout` are discarded when old policy JSON
is loaded. Requests that try to mutate them return `DEPRECATED_GUARDIAN_CONTROL`.

## Incident checklist

1. Stop direct scheduling (`enabled=false`).
2. Check the latest run's `writes_failed`, `writeback_reasons`, and account recovery counts.
3. Inspect `/api/guardian/v1/recovery/status` for open episodes and the latest abnormal snapshot.
4. Verify Sub2API admin GET works without redirects and returns matching account IDs.
5. Check notification backlog and the two bounded failure metrics above.
6. Do not manually flip `active+schedulable=false` accounts unless intentionally changing human
   ownership.
7. After correcting the cause, start with the current policy revision and a new idempotency key.
