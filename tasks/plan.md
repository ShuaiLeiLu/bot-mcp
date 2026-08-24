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
