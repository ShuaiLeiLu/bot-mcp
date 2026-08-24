# Spec: Account Latency Quarantine and Automatic Re-entry

Status: Proposed for approval

Date: 2026-08-24

## Objective

Protect channel quality without exhausting a channel's usable account pool. Accounts with
repeated high first-token latency enter a durable, system-owned latency quarantine. They do not
become ordinary error-recovery candidates and cannot receive traffic while quarantined. The
service continues bounded probes and restores traffic only after latency returns below the
configured threshold.

## Confirmed Requirements

1. Every mapped channel/account group keeps at least one usable account.
2. An account is quarantined for latency only after the existing rolling rule is met:
   first-token latency over `30,000 ms` at least `3` times within `30` minutes.
3. A latency quarantine is stored separately from upstream `error` and human pause state.
4. Quarantined accounts remain non-schedulable and are not passed to ordinary error recovery.
5. Quarantined accounts continue bounded probes every scheduler cycle, with the existing
   maximum of 5 account probes per cycle.
6. A successful probe whose measured first-token latency is at most `30,000 ms` restores both
   active status and scheduling. A failed or still-slow probe leaves the account quarantined.
7. Human-paused accounts are never added to or removed from system quarantine automatically.

## Assumptions

- "Channel" means the normalized account group already mapped to a channel monitor.
- An account belonging to multiple groups may be quarantined only if every one of its groups
  retains at least one other usable account.
- Missing or ambiguous group membership fails closed: the account is not automatically
  quarantined.
- One successful below-threshold probe is sufficient for re-entry, matching the requested
  "latency drops, then enable" behavior.
- Probe failures, malformed responses, timeouts, and missing latency measurements never enable
  an account.

## State Model

```text
AVAILABLE
  |  rolling slow-first-token threshold reached
  |  and every group retains >= 1 other usable account
  v
LATENCY_QUARANTINED (system-owned, scheduling off)
  |  bounded probe fails or first-token > threshold
  +-----------------------------------------------> stay quarantined
  |
  |  probe succeeds and first-token <= threshold
  v
AVAILABLE (status active, scheduling on, quarantine marker removed)

HUMAN_PAUSED -------------------------------> never touched by this state machine
ERROR --------------------------------------> handled only by ordinary error recovery
```

## Persistence

Add a versioned SQLite table for system-owned quarantines:

| Field | Purpose |
|---|---|
| `account_id` | Validated Sub2API account identifier; primary key. |
| `reason` | Stable value `SLOW_FIRST_TOKEN`. |
| `group_ids_json` | Canonical group membership captured at quarantine time. |
| `threshold_ms` | Threshold used for the decision. |
| `observed_count` | Slow observations that triggered quarantine. |
| `quarantined_at` | UTC timestamp. |
| `last_probe_at` | UTC timestamp of the last quarantine probe. |
| `last_probe_latency_ms` | Measured latency, when available. |
| `last_probe_result` | `SLOW`, `FAILED`, or `RECOVERED`. |

The marker is written only after Sub2API disable verification succeeds. It is removed only after
active status plus scheduling-on read-back verification succeeds.

## Minimum-Pool Decision

Before each quarantine mutation:

1. Build the currently usable account count for every group.
2. Process candidates deterministically by account ID.
3. Skip a candidate if disabling it would leave any group below one usable account.
4. Decrement group counts only after a verified successful quarantine mutation.
5. Record a structured `MIN_POOL_PROTECTED` decision for skipped candidates.

## Probe and Re-entry

- Quarantine probes are separate from ordinary `error` recovery outcomes.
- The test endpoint is read as bounded SSE. Time to the first valid `data:` event is the
  first-token measurement.
- The full test must explicitly report success; a fast error event is still a failed probe.
- Re-entry writes are deadline-checked, set account status to active, set scheduling to true,
  and read back both fields.
- Probe selection is bounded, deterministic, and rotates across quarantined accounts.

## Operator Visibility

- `sub2api_get_status` includes the current latency-quarantine count.
- A read-only MCP tool lists quarantines with account IDs, group IDs, timestamps, and last probe
  result; credentials and upstream response bodies remain excluded.
- Administrator notifications distinguish `系统延迟隔离`, `最小池保护`, `继续隔离`, and
  `延迟恢复回池` from ordinary account errors.

## Commands

```text
Focused tests: .\.venv\Scripts\python.exe -m pytest tests/unit -q
Full tests:    .\.venv\Scripts\python.exe -m pytest -q
Lint:          .\.venv\Scripts\python.exe -m ruff check .
Type check:    .\.venv\Scripts\pyright.exe --pythonpath .\.venv\Scripts\python.exe
Audit:         .\.venv\Scripts\python.exe -m pip_audit
```

## Project Structure

- `core/maintenance.py`: minimum-pool quarantine decision.
- `core/maintenance_gateway.py`: measured test and verified disable/enable operations.
- `src/sub2api_mcp/schema.py`: quarantine persistence schema.
- `src/sub2api_mcp/repository.py`: quarantine transactions and bounded listing.
- `src/sub2api_mcp/scheduler.py`: persist quarantine results and run quarantine probes.
- `src/sub2api_mcp/contracts.py`, `service.py`, `tools.py`: safe read-only visibility.
- `tests/unit`, `tests/integration`, `tests/contract`: state, persistence, and MCP guarantees.

## Testing Strategy

- Unit: minimum one usable account per group; multi-group protection; human pause exclusion;
  threshold and re-entry decisions; SSE first-event timing.
- Repository: restart durability; idempotent marker writes; verified removal; bounded pagination.
- Integration: high-latency quarantine, continued slow probe, below-threshold re-entry, and no
  interaction with ordinary error recovery.
- Contract: stable MCP output without credentials or raw upstream bodies.
- Compatibility: the shared parent-plugin suite remains green.

## Boundaries

### Always

- Fail closed on missing group mapping, latency, or explicit test completion.
- Preserve at least one usable account in every group.
- Keep latency quarantine distinct from error recovery and human pause state.
- Verify every Sub2API mutation by read-back.

### Ask First

- Change the 30-second threshold, 3-observation count, 30-minute window, or one-account minimum.
- Increase quarantine probe concurrency or probes per cycle.

### Never

- Probe or enable an account marked as human paused.
- Quarantine the final usable account of any group.
- Treat timeout, network failure, or malformed SSE as recovery.
- Expose credentials or full upstream response bodies.

## Success Criteria

1. A slow candidate that would empty any channel is skipped and remains schedulable.
2. A slow candidate with spare group capacity becomes durably `LATENCY_QUARANTINED`.
3. The same account remains quarantined while probes are slow or fail.
4. A successful probe at or below 30 seconds restores active scheduling and removes the marker.
5. Ordinary error recovery never consumes latency-quarantine records.
6. Human pauses remain unchanged across quarantine and re-entry cycles.
7. Quarantine state and probe history survive process restarts.
8. Focused, full, static, audit, compatibility, and production smoke checks pass.

## Open Questions

None. This proposal uses the existing production thresholds and scheduler cadence.
