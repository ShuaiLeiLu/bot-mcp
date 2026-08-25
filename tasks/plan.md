# Implementation Plan: Guardian Shared Sampling and Controlled Scheduling V2

## Status

- Phase: **Implementation — final release verification**
- Source of truth: `docs/prd-guardian-shared-sampling-v2.md`
- Safety default: Guardian disabled, observe-only, all production writeback disabled
- Branch: `main` (as explicitly required for this delivery)
- Production deployment: automatic deployment is allowed; production writeback remains separately gated

## Overview

Replace Guardian's repeated upstream snapshot reads and equal-count scoring with a durable shared-sampling pipeline. The existing scheduler publishes one canonical channel snapshot per normal probe; Guardian consumes each snapshot exactly once, merges it with de-duplicated minute traffic buckets, computes time-decayed health and confidence, and produces explainable scheduling recommendations. Actual `load_factor`, `priority`, and `schedulable` writes remain independently gated and disabled by default.

## Architecture Decisions

1. The existing scheduler is the only normal channel snapshot collector.
2. Guardian's 15-second loop reads local SQLite and never performs a normal paid account probe in `SHARED` mode.
3. Evidence is stored in 60-second buckets; request volume cannot multiply time weight.
4. Health score and confidence are separate values; stale or low-confidence data freezes writeback.
5. Existing public API fields remain backward-compatible; V2 policy fields are additive with conservative defaults.
6. Database changes are additive, versioned, restart-safe, and covered by migration tests.
7. Manual/upstream ownership outranks automatic state. Only Guardian-owned automatic fuses can recover.
8. Incomplete slices remain unreachable because Guardian and every `autoApply` field default off.

## Dependency Graph

```text
V2 contracts + schema migration
        |
        +--> shared snapshot publication (Scheduler)
        |         |
        |         +--> exactly-once Guardian consumption
        |
        +--> sampling/bucketing/dedup
                  |
                  +--> V2 scoring + confidence + freshness
                              |
                              +--> confidence-aware state machine
                              +--> dimensionless weights + priority
                                          |
                                          +--> field ownership + writeback adapter
                                          +--> recovery probe budget
                                                      |
                                                      +--> service/API/MCP
                                                      +--> UI/explanations
                                                      +--> notifications/metrics
```

## Vertical Slices

### Phase 1: Shared evidence foundation

#### Task 1 — Add V2 policy and evidence contracts

Define additive enums and DTOs for sampling mode, freshness, source identity, time buckets, confidence, rollout stage, and field ownership. Existing V1 policy JSON must continue to validate using defaults.

Acceptance:
- V1 stored policy loads as V2 without mutation permission changes.
- Invalid intervals, thresholds, and `freshSeconds >= expireSeconds` are rejected.
- Default policy keeps Guardian disabled, observe-only, and all writes off.

Likely files: `guardian/contracts.py`, `tests/unit/guardian/test_contracts.py`.

#### Task 2 — Add additive Guardian V2 schema migration

Add shared snapshot, traffic bucket, field ownership, and V2 sample metadata persistence with unique constraints and retention-ready indexes.

Acceptance:
- Empty, V1, and interrupted databases initialize to schema V2.
- Reopening is idempotent and preserves current policy/channels/audits.
- Migration failure rolls back and does not mark the schema upgraded.

Likely files: `guardian/repository.py`, `tests/unit/guardian/test_guardian_repository.py`.

#### Task 3 — Publish canonical shared snapshots

After a successful existing scheduler probe, persist a canonical rich Guardian snapshot with stable hash and source capture time in the same durable database.

Acceptance:
- One successful probe publishes one snapshot without extra upstream calls.
- Re-publishing the same source observation is idempotent.
- Existing status notification behavior remains unchanged.

Likely files: `scheduler.py`, `adapters/sub2api.py`, `repository.py`, `tests/unit/test_scheduler.py`.

#### Task 4 — Consume each shared snapshot exactly once

Change Guardian runs in `SHARED` mode to claim unseen local snapshots rather than calling `guardian_snapshot()`.

Acceptance:
- Repeated 15-second scans consume one snapshot once.
- Restart does not replay consumed snapshots.
- No available snapshot produces a successful no-op, not a failure or zero score.

Likely files: `guardian/engine.py`, `guardian/repository.py`, `tests/unit/guardian/test_engine.py`, `tests/integration/guardian/test_shared_snapshot.py`.

### Checkpoint A — Shared path proven

- Focused and full tests pass.
- A fake adapter proves zero extra Guardian upstream calls.
- Schema upgrade and restart tests pass.
- Human review before traffic ingestion.

### Phase 2: Historical sampling and scoring

#### Task 5 — Build deterministic traffic buckets

Convert validated ops request records into per-channel minute buckets, filter monitor/probe traffic, and de-duplicate request hashes.

Acceptance:
- Duplicate requests and monitor-generated traffic count once or are excluded.
- High request volume changes a bucket mean but not its cross-time weight.
- Unattributed traffic is counted separately and cannot trigger actions.

Likely files: `guardian/sampling.py`, `guardian/contracts.py`, `tests/unit/guardian/test_sampling.py`.

#### Task 6 — Implement time-decayed score, confidence, and freshness

Implement the exact PRD formulas as pure functions, including the published golden calculation and cold-start behavior.

Acceptance:
- Golden values match within `1e-9`.
- No evidence preserves the previous score and sets confidence to zero.
- Fresh/stale/expired and warm-up boundaries are deterministic.

Likely files: `guardian/scoring.py`, `guardian/contracts.py`, `tests/unit/guardian/test_scoring.py`.

#### Task 7 — Integrate evidence ingestion into Guardian runs

Combine shared monitor and traffic evidence into durable buckets and persist score/confidence/freshness per channel.

Acceptance:
- Multiple sources in one bucket use reliability fusion exactly once.
- Channel pages and run results expose score, confidence, freshness, and evidence age.
- Legacy V1 samples are visible but never authorize V2 writeback.

Likely files: `guardian/engine.py`, `guardian/repository.py`, `guardian/contracts.py`, `tests/unit/guardian/test_engine.py`.

### Checkpoint B — Scoring trusted

- Unit, property/invariant, integration, Ruff, and Pyright checks pass.
- Historical replay covers healthy, intermittent error, fatal, latency, and no-data cases.
- Human review of score/confidence traces before state-machine changes.

### Phase 3: Decisions and recommendations

#### Task 8 — Make the state machine confidence- and ownership-aware

Add warm-up/stale states, confidence gates, fatal confirmation, and strict manual/upstream ownership precedence.

Acceptance:
- Low confidence or stale data cannot create a new write recommendation.
- Manual pause/exclusion/fuse and upstream manual disable never auto-recover.
- Fuse, forced-keep, and recovery matrices match the PRD.

Likely files: `guardian/state_machine.py`, `guardian/contracts.py`, `tests/unit/guardian/test_state_machine.py`.

#### Task 9 — Implement V2 weights and priority recommendations

Use dimensionless price/speed signals, reserved frozen budget, capped largest-remainder allocation, health/confidence gates, and health-tier priority offsets.

Acceptance:
- Budget and cap invariants hold or expose explicit unallocated budget.
- Missing price/latency never gives an advantage.
- Small changes and single-step limits produce stable recommendations.

Likely files: `guardian/weights.py`, `guardian/contracts.py`, `tests/unit/guardian/test_weights.py`.

#### Task 10 — Add field ownership and dry-run writeback decisions

Capture baselines, detect out-of-band human changes, and produce audited no-op/write proposals through a disabled-by-default adapter boundary.

Acceptance:
- Human changes transfer ownership and stop Guardian writes for that field.
- Repeated identical recommendations are idempotent.
- Observe-only and disabled adapters cannot mutate Sub2API.

Likely files: `guardian/ownership.py`, `guardian/writeback.py`, `guardian/repository.py`, `tests/unit/guardian/test_ownership.py`, `tests/unit/guardian/test_writeback.py`.

### Checkpoint C — Safe recommendation engine

- State, weight, priority, ownership, and rollback invariant suites pass.
- No production write adapter is enabled.
- Human review of dry-run actions for at least one representative group.

### Phase 4: Recovery, interfaces, and operations

#### Task 11 — Add budgeted recovery probe selection

Select only uniquely mapped Guardian-owned fused channels, enforce interval/request/Token budgets, and persist ledger outcomes.

Acceptance:
- Healthy and human-controlled channels are never selected.
- Budget exhaustion prevents the call and emits one event.
- Three successful probes plus score/confidence/hold gates are required to recover.

Likely files: `guardian/recovery.py`, `guardian/repository.py`, `adapters/sub2api.py`, `tests/unit/guardian/test_recovery.py`.

#### Task 12 — Extend REST and MCP contracts

Expose sampling status, channel explanations, field ownership, probe budget, and rollout stop/advance actions using existing authentication, revision, idempotency, and audit patterns.

Acceptance:
- Read endpoints are additive and paginated where needed.
- Mutations require admin scope, confirmation, revision, idempotency, and audit.
- Existing tool inventory and clients remain compatible.

Likely files: `guardian/api.py`, `guardian/service.py`, `tools.py`, `tests/integration/guardian/test_api.py`, `tests/contract/test_mcp_tools.py`.

#### Task 13 — Update Guardian UI for V2 evidence and controls

Add sampling status, confidence/freshness, explanation, ownership, budget, and rollout panels while keeping the current light responsive design.

Acceptance:
- Every V2 parameter has unit, default, range, and inheritance source.
- Dangerous rollout controls use explicit confirmation and explain blocked prerequisites.
- 390px/1440px browser tests pass with no console errors.

Likely files: `guardian/static/index.html`, `guardian/static/app.js`, `guardian/static/app.css`, `tests/browser/guardian/`.

#### Task 14 — Add V2 notifications, metrics, and retention

Ship structured stale/fuse/recovery/ownership/budget/rollout notifications and bounded cleanup jobs.

Acceptance:
- Messages include Beijing trigger time, score, confidence, source age, action, and reason.
- Status notifications coalesce; maintenance/recovery results remain durable.
- Retention deletes only eligible evidence in bounded batches.

Likely files: `guardian/service.py`, `metrics.py`, `guardian/repository.py`, `tests/unit/guardian/test_notifications.py`, `tests/unit/guardian/test_retention.py`.

### Checkpoint D — Feature complete but writeback off

- Full test, lint, type, audit, Docker build, and browser matrix pass.
- Current channel monitor and WeChat delivery regressions pass.
- `Guardian.enabled=false`, `observeOnly=true`, and all `autoApply=false` in defaults and migrated production policy.
- Product review before merge/deploy.

## Task Sizing and Dependencies

| Task | Depends on | Estimated scope | Expected files |
|---:|---|---|---:|
| 1 | None | S | 2 |
| 2 | 1 | S | 2 |
| 3 | 1–2 | M | 4 |
| 4 | 2–3 | M | 4 |
| 5 | 1 | S | 3 |
| 6 | 1, 5 | S | 3 |
| 7 | 4–6 | M | 4 |
| 8 | 6–7 | S | 3 |
| 9 | 6–7 | S | 3 |
| 10 | 8–9 | M | 5 |
| 11 | 7–8, 10 | M | 4 |
| 12 | 7–11 | M | 5 |
| 13 | 12 | M | 4 |
| 14 | 7–12 | M | 5 |

No task may grow beyond five files without being split and re-reviewed.

## Rollout After Implementation

1. Deploy V2 with Guardian disabled.
2. Back up SQLite and validate additive migration.
3. Enable observe-only shared sampling for at least 24 hours and 100 unique snapshots.
4. Review replay and live recommendation reports.
5. Separately approve one non-core group's `load_factor` gray release.
6. Approve `priority` and finally `schedulable` only after their preceding gates pass.

## Project Definition of Done

Every task must satisfy:

- A failing behavior test exists before implementation.
- Focused tests and the complete test suite pass.
- Ruff and Pyright report no errors.
- External responses and persisted JSON are strictly validated.
- No secret, raw credential, complete request body, or platform identity is logged.
- Defaults remain disabled/observe-only until a documented rollout approval.
- Schema changes are idempotent and restart-tested.
- The slice is one atomic commit and independently revertable.

Final release additionally requires:

- `pip-audit` has no unresolved high-severity findings.
- Docker image builds and health checks pass.
- Browser layout/accessibility checks pass.
- Backup, migration, stop-writeback, and restore drills pass.
- Product explicitly approves merge/deploy and every writeback stage.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Shared snapshot channel mapping is ambiguous | High | Do not score or probe ambiguous channels; expose unattributed evidence. |
| Traffic includes monitor requests | High | Request hash, User-Agent/API-key attribution, and probe-ledger filtering. |
| Schema migration affects live scheduler | High | Additive transaction, backup, disabled feature, restart test, rollback drill. |
| Score looks precise with weak evidence | High | Separate confidence/freshness and gate every action. |
| Weight constraints cannot satisfy budget | Medium | Reserved budget, iterative caps, explicit unallocated budget, group freeze. |
| Human changes are overwritten | High | Per-field ownership and conservative human-change detection. |
| Recovery probing exceeds cost budget | High | Interval, concurrency, request, and Token hard limits. |
| Incomplete UI/API exposes unsafe controls | High | Feature disabled, write adapter absent, prerequisite checks server-side. |

## Open Decisions Before Production Writeback

- Final recovery budget unit: requests, Token, amount, or all three.
- Which non-core group is the first gray-release target.
- Required duration of one complete business peak.
- Whether priority rollout always requires a second explicit approval.
- The stable Sub2API field used for traffic-to-channel attribution.

---

# Approved Addendum Implementation Plan: Account Latency Quarantine

Spec: `tasks/account-latency-quarantine-spec.md`  
Status: **Implementation complete; production rollout pending**

## Dependency Graph

```text
Quarantine schema and repository
        |
        +--> minimum-pool decision
        |         |
        |         +--> verified disable + durable marker
        |
        +--> measured quarantine probe
                  |
                  +--> verified re-entry + marker removal
                  |
                  +--> MCP status/listing + notifications
```

## Architecture Decisions

- SQLite is the authority for system-owned latency quarantine; Sub2API status alone cannot
  distinguish human pause, ordinary error, and automated latency isolation.
- Account-group membership is captured at quarantine time and revalidated before every disable
  and re-entry decision.
- The minimum usable pool is enforced inside the core maintenance decision before any upstream
  mutation. Missing or ambiguous groups fail closed.
- Quarantine probes are a separate scheduler path and never enter ordinary error recovery.
- First-token latency is measured at the first valid SSE `data:` event; total request duration is
  not used as a substitute.
- All new behavior remains disabled until the schema, minimum-pool, re-entry, and visibility
  slices are complete and production backup/migration checks pass.

## Phase 1 — Durable isolation foundation

### Task AQ1 — Add versioned quarantine persistence

Add the strict quarantine record, additive SQLite table, idempotent upsert, bounded listing,
probe-result update, and verified removal operations.

Acceptance:
- Empty/current/restart initialization is additive and idempotent.
- Quarantine reason, group IDs, thresholds, counts, timestamps, and probe result round-trip.
- Invalid account/group IDs and malformed persisted JSON fail closed.

Verification: focused repository migration/restart tests, Ruff, Pyright.  
Dependencies: approved spec.  
Likely files: `schema.py`, `contracts.py`, `repository.py`, `tests/unit/test_repository.py`.  
Scope: M (4 files).

### Task AQ2 — Sweep failed channels and enforce one usable account per group

When a mapped channel enters failed/error state, test all eligible accounts in the resolved group
before mutation. Add a deterministic minimum-pool decision for channel-failure and log-latency
candidates, including multi-group and missing-group fail-closed behavior.

Acceptance:
- The final usable account in any group is never disabled.
- Multi-group candidates require spare capacity in every group.
- Successful disables decrement counts; failed/skipped disables do not.
- At least one explicit successful test is required before disabling any failed account; an
  all-failed sweep emits `NO_HEALTHY_ACCOUNT` and performs no disable mutation.

Verification: RED/GREEN core maintenance matrix and existing compatibility tests.  
Dependencies: AQ1 contract shape only.  
Likely files: `core/maintenance.py`, `tests/unit/test_maintenance_min_pool.py`; parent equivalents.  
Scope: S (2 files per repository).

### Checkpoint AQ-A

- [ ] Schema/restart and minimum-pool matrices pass.
- [ ] No upstream mutation is possible without the minimum-pool decision.
- [ ] Human reviews persisted marker and group semantics.

## Phase 2 — Quarantine and re-entry lifecycle

### Task AQ3 — Persist verified system quarantines

Connect successful slow-first-token and failed-channel maintenance adjustments to the repository
marker in the same control-job flow and emit explicit minimum-pool-protected, no-healthy-account,
and quarantined results.

Acceptance:
- Only a verified successful disable creates a reason-specific marker.
- Slow/channel candidates blocked by minimum pool or an all-failed sweep create no marker or
  disable mutation.
- Restarted services retain marker ownership and never infer it from upstream status.

Verification: scheduler/repository integration tests with fake Sub2API.  
Dependencies: AQ1–AQ2.  
Likely files: `core/maintenance.py`, `adapters/sub2api.py`, `scheduler.py`,
`tests/unit/test_scheduler.py`, `tests/integration/test_sub2api_adapter.py`.  
Scope: M (5 files).

### Task AQ4 — Measure probes and restore quarantined accounts

Add bounded SSE first-event timing, rotating quarantine probe selection, reason-specific
slow/failed updates, and verified active+schedulable re-entry.

Acceptance:
- Missing latency, malformed SSE, timeout, explicit failure, or latency over threshold stays
  quarantined for latency markers.
- Explicit success at/below threshold restores a latency marker; explicit success restores a
  channel-test marker.
- Ordinary error recovery and human-paused accounts never consume quarantine records.

Verification: timing parser unit tests, adapter integration tests, restart/retry test.  
Dependencies: AQ1 and AQ3.  
Likely files: `core/maintenance_gateway.py`, `core/monitor.py`, `adapters/sub2api.py`,
`scheduler.py`, `tests/integration/test_quarantine_reentry.py`.  
Scope: M (5 files).

### Checkpoint AQ-B

- [ ] Quarantine -> slow -> quarantine and quarantine -> fast -> available paths pass.
- [ ] Failure injection proves zero accidental re-entry.
- [ ] Parent shared-core compatibility suite passes.

## Phase 3 — Visibility and rollout

### Task AQ5 — Expose quarantine state safely

Add status count, bounded read-only MCP listing, structured Chinese notifications, metrics, and
operator documentation.

Acceptance:
- Quarantine is visibly distinct from error and human pause.
- MCP results contain no credentials or upstream response bodies.
- Notifications distinguish quarantined, min-pool protected, still slow, failed, and recovered.

Verification: MCP inventory/schema, redaction, notification, and metric tests.  
Dependencies: AQ1–AQ4.  
Likely files: `contracts.py`, `service.py`, `tools.py`, `scheduler.py`,
`tests/contract/test_mcp_tools.py`.  
Scope: M (5 files).

### Task AQ6 — Migrate and roll out production

Back up SQLite and environment config, deploy with the guard disabled, migrate and seed only
verified system-owned slow quarantines, smoke-test probes, then enable the completed guard.

Acceptance:
- Every channel starts and ends with at least one usable account.
- Current qualifying slow accounts are either quarantined or explicitly min-pool protected.
- A controlled fast probe demonstrates automatic re-entry; rollback restores DB/config.

Verification: backup checksum, migration count, health, job/outbox logs, production state report.  
Dependencies: AQ1–AQ5 and product rollout approval.  
Likely files: environment/config only; no committed secrets.  
Scope: operational.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Multi-group account empties another channel | High | Require spare capacity in every captured group. |
| Failed channel has no healthy accounts | High | No disable mutation; emit `NO_HEALTHY_ACCOUNT` and require capacity/account intervention. |
| System quarantine mistaken for human pause | High | Durable local ownership marker; never infer from upstream switch. |
| Buffered test hides first-token latency | High | Measure first valid SSE event, not total duration. |
| Crash between disable and marker write | High | Verified mutation result, immediate durable write, audit event, startup reconciliation alert. |
| Probe cost grows with quarantine count | Medium | Existing cap of 5 per cycle, deterministic rotation, one control worker. |
| Stale group membership | Medium | Revalidate before disable/re-entry; fail closed on ambiguity. |

## Addendum Approval Gate

Implementation starts only after human approval of this plan and the AQ task checklist.

---

# Proposed Addendum Plan: Guardian Direct Scheduling and Conditional Account Recovery

Spec: docs/prd-guardian-account-recovery-unification.md
Status: Plan proposed for review
Branch rule: finish on main; no long-lived feature branch
Production target: direct scheduling, no observe/rollout mode

## Overview

Consolidate every account recovery execution path under Guardian while reusing the existing
60-second channel/account inventory scan. Normal accounts are never periodically tested.
Every new inventory snapshot tests only error/disabled/inactive accounts; a newly failed/error
channel additionally tests every non-paused account in its uniquely mapped group. Guardian also
becomes the real channel scheduler for load_factor, priority and schedulable fields.

## Architecture Decisions

1. Scheduler remains the sole inventory collector and publishes channel plus sanitized account
   observations from the same upstream read.
2. Guardian is the sole owner of RECOVERY jobs and real scheduling writes.
3. active+schedulable=false means manual pause and is a hard no-test/no-write state.
4. error, disabled and inactive accounts are conditionally tested once per canonical snapshot.
5. A channel failed/error transition creates one durable episode and broadens the current
   snapshot selection to every eligible group account.
6. Explicit test success enables and verifies; definitive failure disables and verifies;
   indeterminate results preserve state.
7. Guardian has only enabled/disabled scheduling. Observe mode, rollout stages and partial
   auto-apply switches are removed from operator controls.
8. Every external mutation is current-read -> write -> exact read-back -> audit. One failed
   verification stops the remaining run.

## Dependency Graph

~~~text
Consolidate quarantine lifecycle on main
        |
        v
Contracts + additive schema
        |
        +--> same-probe account inventory publication
        |         |
        |         v
        |    conditional selection + episode idempotency
        |         |
        |         v
        |    Guardian RECOVERY executor + MCP compatibility
        |
        +--> verify Sub2API channel mutation contract
                  |
                  v
             verified channel writer
                  |
                  v
             direct Guardian scheduling
                  |
                  v
          API/MCP/UI + legacy removal
                  |
                  v
          backup, deploy enabled, verify, rollback drill
~~~

## Phase 0 — Consolidate the active prerequisite

### Task GR0A — Review the active quarantine core and adapter delta

Description: Freeze and review the existing core maintenance/probe and Sub2API adapter changes
without adding Guardian behavior.

Acceptance:
- Failed-channel, minimum-pool, SSE timing and verified restore invariants match the approved
  quarantine spec.
- Existing uncommitted changes are preserved byte-for-byte unless a reviewed defect requires a
  focused fix.
- Core/adapter focused tests pass.

Verification:
- uv run pytest tests/unit/test_maintenance_gateway.py
  tests/unit/test_maintenance_min_pool.py tests/integration/test_sub2api_adapter.py -q

Dependencies: None.
Files likely touched: core/maintenance.py, core/maintenance_gateway.py, core/monitor.py,
core/probe.py, src/sub2api_mcp/adapters/sub2api.py.
Estimated scope: M (5 files).

### Task GR0B — Review quarantine persistence and contracts

Description: Review the existing additive schema, quarantine intent/marker transactions and
strict public contracts as one persistence slice.

Acceptance:
- Empty/current/restart migrations are additive and idempotent.
- Intent/marker lifecycle is durable and malformed persisted data fails closed.
- Repository focused tests pass without deleting unrelated assertions.

Verification:
- uv run pytest tests/unit/test_repository.py -q
- uv run ruff check src/sub2api_mcp/contracts.py src/sub2api_mcp/schema.py
  src/sub2api_mcp/repository.py

Dependencies: GR0A contract assumptions.
Files likely touched: src/sub2api_mcp/contracts.py, src/sub2api_mcp/schema.py,
src/sub2api_mcp/repository.py, tests/unit/test_repository.py.
Estimated scope: M (4 files).

### Task GR0C — Review quarantine runtime orchestration

Description: Review scheduler/runtime/service wiring and its focused integration tests, keeping
the existing feature behavior unchanged.

Acceptance:
- One control mutation path and lease protect quarantine disable/re-entry.
- Notifications cannot authorize or roll back a mutation.
- Runtime and scheduler focused tests pass.

Verification:
- uv run pytest tests/unit/test_scheduler.py tests/integration/test_app.py
  tests/contract/test_mcp_tools.py -q

Dependencies: GR0A, GR0B.
Files likely touched: src/sub2api_mcp/scheduler.py, src/sub2api_mcp/app.py,
src/sub2api_mcp/service.py, tests/unit/test_scheduler.py,
tests/integration/test_app.py.
Estimated scope: M (5 files).

### Task GR0D — Consolidate the prerequisite on main

Description: Run the complete gate, create focused save-point commits for reviewed outstanding
changes, merge/fast-forward them into main and remove the feature branch.

Acceptance:
- Full tests, Ruff and Pyright pass before merge.
- No unrelated user change or secret is included in staged diffs.
- Local and remote feature branches are removed only after clean main contains every accepted
  commit.

Verification:
- uv run pytest -q
- uv run ruff check .
- uv run pyright
- git status --short --branch shows clean main

Dependencies: GR0A-GR0C.
Files likely touched: Git metadata only.
Estimated scope: S.

### Checkpoint GR-A — Clean foundation

- [ ] GR0A-GR0D quarantine review, tests and quality gates pass.
- [ ] Main is clean and contains the prerequisite.
- [ ] Production is not changed by plan-only work.

## Phase 1 — Contracts and durable identity

### Task GR1 — Define direct scheduling and conditional recovery contracts

Description: Add strict contracts for account observations, classifications, trigger source,
channel-error episodes, recovery outcomes and the simplified enabled/disabled Guardian policy.
Mark observe/rollout fields as deprecated migration input rather than executable controls.

Acceptance:
- V3 policies load without enabling writes during migration tests.
- New policy has no executable observe or staged rollout path.
- Invalid account states, triggers and oversized group observations fail closed.

Verification:
- uv run pytest tests/unit/guardian/test_contracts.py tests/unit/test_config.py -q
- uv run ruff check src/sub2api_mcp/contracts.py src/sub2api_mcp/guardian/contracts.py
- uv run pyright

Dependencies: GR0D.
Files likely touched: src/sub2api_mcp/contracts.py,
src/sub2api_mcp/guardian/contracts.py, tests/unit/guardian/test_contracts.py,
tests/unit/test_config.py.
Estimated scope: M (4 files).

### Task GR2 — Persist account observations, episodes and result ledger

Description: Add an additive Guardian schema migration for canonical account observations,
channel-error episodes, per-snapshot account idempotency and recovery run/results.

Acceptance:
- Empty, current and interrupted databases migrate idempotently.
- Unique keys prevent the same account being tested twice for one snapshot/episode.
- Restart preserves open episodes and completed account results.

Verification:
- uv run pytest tests/unit/guardian/test_guardian_repository.py -q
- migration rollback/reopen tests
- uv run ruff check src/sub2api_mcp/guardian/repository.py
- uv run pyright

Dependencies: GR1.
Files likely touched: src/sub2api_mcp/guardian/repository.py,
tests/unit/guardian/test_guardian_repository.py.
Estimated scope: S (2 files).

### Checkpoint GR-B — Durable contract

- [ ] Policy compatibility and schema migration pass.
- [ ] Duplicate snapshot/episode writes are impossible.
- [ ] Defaults remain disabled until the direct cutover task.

## Phase 2 — Reuse the existing 60-second inventory

### Task GR3 — Expose sanitized account inventory from the existing probe

Description: Extend the existing probe result with strict account observations collected during
the same Sub2API inventory request already used for group counts. Do not add another list request
or account test.

Acceptance:
- One scheduler probe still performs one account inventory traversal.
- Observations contain only ID, groups, status, schedulable and bounded eligibility flags.
- Credentials, API keys, provider payloads and raw account metadata are absent.

Verification:
- adapter call-count regression proves zero additional inventory requests
- uv run pytest tests/unit/test_probe_enrichment.py
  tests/integration/test_sub2api_adapter.py -q

Dependencies: GR1.
Files likely touched: core/probe.py, src/sub2api_mcp/contracts.py,
src/sub2api_mcp/adapters/sub2api.py, tests/integration/test_sub2api_adapter.py.
Estimated scope: M (4 files).

### Task GR4 — Publish and consume account observations exactly once

Description: Include account observations in the canonical shared snapshot and persist them when
Guardian claims that snapshot. The scheduler remains unaware of recovery decisions.

Acceptance:
- Replayed publication is idempotent by canonical snapshot hash.
- Guardian restart does not re-ingest or re-test the same snapshot.
- A snapshot without valid account observations cannot trigger account tests.

Verification:
- uv run pytest tests/unit/test_scheduler.py tests/unit/guardian/test_engine.py -q
- fake adapter proves no extra upstream call

Dependencies: GR2, GR3.
Files likely touched: src/sub2api_mcp/repository.py,
src/sub2api_mcp/scheduler.py, src/sub2api_mcp/guardian/engine.py,
tests/unit/guardian/test_engine.py.
Estimated scope: M (4 files).

### Checkpoint GR-C — Zero duplicate collection

- [ ] One probe produces one channel+account snapshot.
- [ ] Guardian consumes it exactly once.
- [ ] No account test occurs in this phase.

## Phase 3 — Conditional account selection and verified mutations

### Task GR5 — Implement pure conditional account selection

Description: Build deterministic selection for BAD_ACCOUNT_STATE and CHANNEL_ERROR using only
validated observations and persisted snapshot/episode state.

Acceptance:
- active+schedulable=false is always MANUAL_PAUSE and never selected.
- error/disabled/inactive are selected once per new snapshot unless excluded.
- CHANNEL_ERROR selects all eligible mapped-group accounts once per episode.

Verification:
- uv run pytest tests/unit/guardian/test_account_recovery.py -q
- property matrix for statuses, temporary flags, expiry and duplicate snapshots

Dependencies: GR2, GR4.
Files likely touched: src/sub2api_mcp/guardian/account_recovery.py,
src/sub2api_mcp/guardian/contracts.py,
tests/unit/guardian/test_account_recovery.py.
Estimated scope: M (3 files).

### Task GR6 — Expose typed account test/enable/disable operations

Description: Replace the monolithic legacy recover orchestration with thin typed adapter
operations for current-state read, bounded test, verified enable and verified disable.

Acceptance:
- Success/definitive failure/indeterminate are distinct typed results.
- Every enable/disable is preceded by eligibility re-check and followed by exact read-back.
- Manual pause, expiry, temporary limits and malformed responses produce zero mutation.

Verification:
- uv run pytest tests/unit/test_maintenance_gateway.py
  tests/integration/test_sub2api_adapter.py -q
- request sequence and failure-injection tests

Dependencies: GR1, GR5.
Files likely touched: core/maintenance_gateway.py, core/monitor.py,
src/sub2api_mcp/adapters/sub2api.py,
tests/integration/test_sub2api_adapter.py.
Estimated scope: M (4 files).

### Task GR7 — Execute and persist one Guardian recovery run

Description: Add the Guardian account recovery executor that claims one lease, applies the pure
selection, executes typed account operations sequentially, persists every result and emits one
durable RECOVERY_RESULT notification.

Acceptance:
- One account is tested at most once per snapshot/episode.
- Explicit success enables; definitive failure disables; indeterminate preserves state.
- Notification failure cannot roll back or replay verified account mutations.

Verification:
- uv run pytest tests/unit/guardian/test_account_recovery.py
  tests/integration/guardian/test_account_recovery.py -q
- restart and notification-failure integration tests

Dependencies: GR2, GR5, GR6.
Files likely touched: src/sub2api_mcp/guardian/account_recovery.py,
src/sub2api_mcp/guardian/service.py,
src/sub2api_mcp/guardian/repository.py,
tests/integration/guardian/test_account_recovery.py.
Estimated scope: M (4 files).

### Task GR8 — Transfer RECOVERY job and MCP ownership to Guardian

Description: Register JobType.RECOVERY with Guardian, stop Scheduler from creating or handling
it, and keep sub2api_submit_recovery compatible by processing only the latest abnormal snapshot
or an open channel-error episode.

Acceptance:
- Scheduler publishes snapshots but never creates an automatic RECOVERY job.
- Guardian creates at most one active RECOVERY job per snapshot/episode.
- Existing MCP tool returns the same durable job envelope and cannot test normal accounts.

Verification:
- uv run pytest tests/unit/test_scheduler.py tests/contract/test_mcp_tools.py
  tests/integration/test_app.py -q

Dependencies: GR7.
Files likely touched: src/sub2api_mcp/app.py, src/sub2api_mcp/scheduler.py,
src/sub2api_mcp/service.py, src/sub2api_mcp/tools.py,
tests/contract/test_mcp_tools.py.
Estimated scope: M (5 files).

### Checkpoint GR-D — One recovery owner

- [ ] Normal accounts have zero periodic test calls.
- [ ] Abnormal snapshot and channel-error matrices pass.
- [ ] Scheduler has no recovery execution path.
- [ ] Guardian recovery survives restart and notification failure.

## Phase 4 — Real channel scheduling without observe mode

### Task GR9 — Prove the Sub2API channel mutation contract

Description: Verify the authoritative endpoints and field semantics for channel load_factor,
priority and schedulable using official source/current API contracts, then encode strict
request/response fakes before writing production adapter code.

Acceptance:
- Method, path, request body and success/read-back response are documented for all three fields.
- Redirects, wrong IDs, wrong values and malformed responses fail closed.
- No production mutation is performed during contract discovery.

Verification:
- focused adapter contract tests fail before implementation and pass after typed parsing exists
- docs decision record captures the verified contract

Dependencies: GR0D.
Files likely touched: docs/adr-guardian-channel-writer.md,
src/sub2api_mcp/adapters/sub2api.py,
tests/integration/test_sub2api_adapter.py.
Estimated scope: M (3 files).

### Task GR10 — Implement verified Sub2API channel writer

Description: Implement current-read, one-field write and exact read-back for load_factor,
priority and schedulable behind GuardianFieldWriter.

Acceptance:
- Successful write returns the verified upstream value.
- Any transport, validation or read-back mismatch returns FAILED and performs no follow-up field.
- No credential or full upstream body enters logs/audits.

Verification:
- uv run pytest tests/unit/guardian/test_writeback.py
  tests/integration/test_sub2api_adapter.py -q

Dependencies: GR9.
Files likely touched: src/sub2api_mcp/adapters/sub2api.py,
src/sub2api_mcp/guardian/writeback.py,
tests/unit/guardian/test_writeback.py,
tests/integration/test_sub2api_adapter.py.
Estimated scope: M (4 files).

### Task GR11 — Replace observe/rollout policy with direct start/stop

Description: Remove executable observe, rollout-stage and per-field auto-apply gates. Keep
backward parsing only for migration, and expose one enabled state plus emergency stop.

Acceptance:
- enabled=true authorizes all three Guardian-owned field types.
- enabled=false blocks all new writes and conditional account mutations.
- Legacy observe/rollout mutations return a stable deprecation error.

Verification:
- uv run pytest tests/unit/guardian/test_contracts.py
  tests/unit/guardian/test_writeback.py tests/integration/guardian/test_api.py -q

Dependencies: GR1, GR10.
Files likely touched: src/sub2api_mcp/guardian/contracts.py,
src/sub2api_mcp/guardian/writeback.py,
src/sub2api_mcp/guardian/service.py,
tests/integration/guardian/test_api.py.
Estimated scope: M (4 files).

### Task GR12 — Apply scheduling proposals in the Guardian engine

Description: Convert desired load factor, priority and schedulable values into bounded proposals,
apply them through the verified writer and stop the remaining run on failed verification.

Acceptance:
- Human-owned fields, stale/low-confidence evidence and cooldown block writes.
- Per-run channel cap, relative-step limits and idempotency remain enforced.
- Run result reports proposed/applied/blocked/failed counts and exact safe reasons.

Verification:
- uv run pytest tests/unit/guardian/test_engine.py
  tests/unit/guardian/test_writeback.py -q
- invariant matrix across three field types

Dependencies: GR10, GR11.
Files likely touched: src/sub2api_mcp/guardian/engine.py,
src/sub2api_mcp/guardian/writeback.py,
src/sub2api_mcp/guardian/repository.py,
tests/unit/guardian/test_engine.py.
Estimated scope: M (4 files).

### Checkpoint GR-E — Direct scheduling proven

- [ ] No observe/rollout path remains executable.
- [ ] All three channel fields use read-write-read verification.
- [ ] Emergency stop blocks subsequent writes.
- [ ] Human ownership and confidence/freshness gates pass.

## Phase 5 — Operator interfaces and legacy contraction

### Task GR13 — Add direct scheduling and account recovery APIs/MCP

Description: Expose scheduling start/stop, recovery status, open episodes and manual pending-run
submission with existing admin, revision, idempotency and audit controls.

Acceptance:
- Read endpoints are bounded and redact account identity outside admin contracts.
- Start/stop and manual run require explicit confirmation and idempotency.
- Removed rollout APIs return a documented deprecation error.

Verification:
- uv run pytest tests/integration/guardian/test_api.py
  tests/contract/test_mcp_tools.py -q

Dependencies: GR8, GR11, GR12.
Files likely touched: src/sub2api_mcp/guardian/api.py,
src/sub2api_mcp/guardian/service.py, src/sub2api_mcp/tools.py,
tests/contract/test_mcp_tools.py.
Estimated scope: M (4 files).

### Task GR14 — Update Guardian UI for direct mode

Description: Remove observe/rollout controls and show scheduling start/stop, writer health,
conditional recovery counts, open channel episodes and latest account outcomes.

Acceptance:
- UI never claims scheduling is active when the writer is absent/unhealthy.
- Dangerous start/stop/manual episode controls require explicit confirmation.
- 390px and 1440px layouts have no page overflow or console errors.

Verification:
- node --check src/sub2api_mcp/guardian/static/app.js
- Guardian static/API integration tests
- real browser 390x844 and 1440x900 matrix

Dependencies: GR13.
Files likely touched: src/sub2api_mcp/guardian/static/index.html,
src/sub2api_mcp/guardian/static/app.js,
src/sub2api_mcp/guardian/static/app.css,
tests/integration/guardian/test_api.py.
Estimated scope: M (4 files).

### Task GR15 — Remove legacy Scheduler recovery orchestration

Description: After Guardian tests prove parity, delete Scheduler recovery handler/formatting,
adapter recover() orchestration and old runtime registration. Retain legacy environment values
only as one-time migration inputs.

Acceptance:
- rg finds no automatic Scheduler recovery creation or handler.
- Existing MCP recovery tool is still present and Guardian-owned.
- Old time-window/rotation behavior has no executable path.

Verification:
- full recovery, scheduler, MCP inventory and configuration compatibility tests
- dead-code search and staged diff review

Dependencies: GR8, GR13.
Files likely touched: src/sub2api_mcp/scheduler.py,
src/sub2api_mcp/adapters/sub2api.py, src/sub2api_mcp/app.py,
src/sub2api_mcp/config.py, tests/unit/test_scheduler.py.
Estimated scope: M (5 files).

### Task GR16 — Notifications, metrics and operator documentation

Description: Finalize structured Chinese recovery/scheduling notifications, bounded metrics,
runbook guidance, changelog and environment migration notes.

Acceptance:
- Notifications contain Beijing time, trigger, channel/group, result counts and safe reasons.
- Metrics use only bounded enum labels and expose writer/recovery failures.
- Documentation clearly distinguishes inventory reads from account tests.

Verification:
- notification, observability and redaction tests
- secret scan of staged diff

Dependencies: GR13, GR15.
Files likely touched: src/sub2api_mcp/metrics.py,
src/sub2api_mcp/guardian/service.py, README.md, CHANGELOG.md,
tests/unit/test_observability.py.
Estimated scope: M (5 files).

### Checkpoint GR-F — Feature complete

- [ ] Full account and channel execution paths pass.
- [ ] Scheduler only collects/publishes.
- [ ] Guardian UI/API/MCP report the same owner and state.
- [ ] No duplicate recovery/observe/rollout code remains.

## Phase 6 — Quality gate and direct production cutover

### Task GR17 — Replay and full quality verification

Description: Replay healthy, abnormal-account and channel-error fixtures; run all static,
security, migration and container gates before touching production.

Acceptance:
- Healthy active accounts produce zero account test calls.
- Mixed abnormal/paused account fixtures produce exact expected actions.
- Direct writer failure injection proves stop-on-first-unverified-write.

Verification:
- uv run pytest -q
- uv run ruff check .
- uv run pyright
- uv run pip-audit
- docker build --tag bot-mcp-ci:guardian-direct .
- container health smoke test

Dependencies: GR1-GR16.
Files likely touched: tests/fixtures or replay tests only.
Estimated scope: M.

### Task GR18 — Back up, deploy enabled and verify rollback

Description: Back up production SQLite/environment, deploy the tested release with Guardian
direct scheduling enabled, verify real read-write-read behavior, conditional account testing,
notifications and emergency stop/rollback.

Acceptance:
- Current release SHA, backup checksum and schema version are recorded.
- Production starts enabled with a healthy writer and zero legacy Scheduler recovery jobs.
- A failed health/write gate invokes emergency stop and restores the previous release/database.

Verification:
- container healthy and deployed SHA matches main
- no account tests for active+schedulable accounts across normal snapshots
- error/disabled/inactive and controlled channel-error paths produce expected verified results
- notification queue and structured logs contain no secret/PII

Dependencies: GR17 and explicit final deployment approval.
Files likely touched: production configuration only; no committed secrets.
Estimated scope: Operational.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Active dirty quarantine work is overwritten | High | GR0 is mandatory; no overlapping edits before clean main |
| Manual pause is mistaken for disabled | High | Exact status+schedulable classification with matrix tests |
| Same scan calls account test twice | High | Snapshot/account unique ledger constraint |
| Persistent channel error repeats full sweep | High | Durable channel-error episode identity |
| Channel writer API assumption is wrong | High | GR9 contract proof before writer implementation |
| Direct mode writes a wrong value | High | Current-read, one-field write, exact read-back, stop remainder |
| No observe rollout increases production risk | High | Full replay, backup, emergency stop and release rollback |
| Notification delivery fails | Medium | Durable outbox never participates in mutation transaction |
| Large failing group creates a token burst | Medium | Sequential default, safety ceiling, visible run progress |

## Plan Approval Gate

Implementation begins only after this addendum plan and its checklist are approved. Planning work
does not commit, merge, deploy, change production policy or enable scheduling.
