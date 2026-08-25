# Guardian Shared Sampling V2 Task Checklist

Status: **Implementation active — final release verification**

## Phase 1 — Shared evidence foundation

- [x] Task 1: Add V2 policy and evidence contracts.
  - Acceptance: V1 policy compatibility; strict V2 validation; safe disabled defaults.
  - Verify: focused contract tests, Ruff, Pyright.
  - Dependencies: none.
- [x] Task 2: Add additive Guardian V2 schema migration.
  - Acceptance: empty/V1/restart/idempotency/rollback tests.
  - Verify: Guardian repository tests.
  - Dependencies: Task 1.
- [x] Task 3: Publish canonical snapshots from the existing scheduler.
  - Acceptance: no extra upstream call; idempotent publication; notifications unchanged.
  - Verify: scheduler tests and fake adapter call counts.
  - Dependencies: Tasks 1–2.
- [x] Task 4: Consume each shared snapshot exactly once.
  - Acceptance: local-only no-op scans; no restart replay; no zero-score on missing data.
  - Verify: engine and shared-snapshot integration tests.
  - Dependencies: Tasks 2–3.

### Checkpoint A

- [x] Full quality gate passes.
- [x] Zero-extra-upstream-call proof passes.
- [ ] Human review before Phase 2.

## Phase 2 — Historical sampling and scoring

- [x] Task 5: Build deterministic traffic buckets and de-duplication.
  - Acceptance: monitor filtering, request de-dup, volume-neutral time weighting.
  - Verify: sampling unit/property tests.
  - Dependencies: Task 1.
- [x] Task 6: Implement V2 health, confidence, freshness, and cold start.
  - Acceptance: published golden example within `1e-9`; stale/no-data behavior.
  - Verify: scoring unit/property tests.
  - Dependencies: Tasks 1 and 5.
- [x] Task 7: Integrate durable evidence ingestion into Guardian runs.
  - Acceptance: source fusion, persisted confidence, legacy samples blocked from writes.
  - Verify: engine/repository integration tests.
  - Dependencies: Tasks 4–6.

### Checkpoint B

- [ ] Historical replay matrix passes.
- [ ] Human reviews score/confidence traces.

## Phase 3 — Decisions and recommendations

- [x] Task 8: Add confidence-aware, ownership-safe state transitions.
  - Acceptance: stale/low-confidence freeze; fatal confirmation; manual states immutable.
  - Verify: state-machine matrix/property tests.
  - Dependencies: Tasks 6–7.
- [x] Task 9: Implement V2 weights and priority recommendations.
  - Acceptance: budget/cap invariants, missing-data penalty, cooldown/step behavior.
  - Verify: weight property and golden tests.
  - Dependencies: Tasks 6–7.
- [x] Task 10: Add field ownership and disabled dry-run writeback boundary.
  - Acceptance: human takeover, idempotency, observe-only non-mutation.
  - Verify: ownership/writeback tests.
  - Dependencies: Tasks 8–9.

### Checkpoint C

- [ ] Dry-run recommendation report reviewed for a representative group.
- [x] Production writeback adapter remains disabled.

## Phase 4 — Recovery, interfaces, and operations

- [x] Task 11: Add budgeted recovery-probe selection and ledger.
  - Acceptance: only uniquely mapped Guardian fuses; hard budget; 3-success recovery.
  - Verify: recovery and budget tests.
  - Dependencies: Tasks 7–8 and 10.
- [x] Task 12: Extend REST and MCP contracts.
  - Acceptance: additive reads; guarded rollout/ownership mutations; compatibility.
  - Verify: API integration and MCP contract tests.
  - Dependencies: Tasks 7–11.
- [x] Task 13: Update Guardian UI for V2 evidence and controls.
  - Acceptance: parameter/help completeness; confirmations; responsive/a11y/browser checks.
  - Verify: JS syntax, API integration, 390px/1440px browser matrix.
  - Dependencies: Task 12.
- [x] Task 14: Add notifications, metrics, and retention.
  - Acceptance: structured Chinese messages; coalescing rules; bounded cleanup.
  - Verify: notification, metric, and retention tests.
  - Dependencies: Tasks 7–12.

### Checkpoint D — Feature complete, writeback off

- [x] `uv run pytest -q`
- [x] `uv run ruff check .`
- [x] `uv run pyright`
- [x] `uv run pip-audit`
- [x] `docker build --tag bot-mcp-ci:guardian-v2 .`
- [x] Browser and accessibility matrix passes.
- [ ] Backup/migration/stop/restore drill passes.
- [ ] Product approves merge and deployment.

---

# Account Latency Quarantine Task Checklist

Status: **Implementation complete; production rollout pending**

- [x] AQ1: Add durable latency-quarantine schema and repository operations.
  - Acceptance: additive/idempotent migration; strict marker round-trip; restart durability.
  - Verify: repository focused tests, Ruff, Pyright.
  - Dependencies: approved spec.
- [x] AQ2: Add failed-channel full sweep and per-group minimum usable account protection.
  - Acceptance: test all eligible group accounts first; require one success before disables;
    all-failed no-mutation alert; min 1 per group; multi-group safe; unknown mapping fail closed.
  - Verify: core RED/GREEN matrix and parent compatibility tests.
  - Dependencies: AQ1 contract.
- [x] AQ3: Persist only verified reason-specific quarantine mutations.
  - Acceptance: successful latency/channel disable creates marker; minimum-pool/all-failed/
    failed disable does not.
  - Verify: scheduler + fake Sub2API integration tests.
  - Dependencies: AQ1–AQ2.
- [x] AQ4: Add measured reason-specific quarantine probes and verified automatic re-entry.
  - Acceptance: latency marker requires fast success; channel-test marker requires success;
    failure stays isolated; human pause untouched.
  - Verify: SSE timing, retry, restart, and re-entry integration tests.
  - Dependencies: AQ1 and AQ3.
- [x] AQ5: Expose quarantine status, MCP listing, notifications, and metrics.
  - Acceptance: distinct state; bounded/redacted reads; structured Chinese action messages.
  - Verify: contract, redaction, notification, and metric tests.
  - Dependencies: AQ1–AQ4.
- [ ] AQ6: Back up, migrate, seed verified records, deploy disabled, smoke-test, and enable.
  - Acceptance: no zero-account channel; current slow accounts safely classified; rollback tested.
  - Verify: production health, DB/config checksums, job logs, and account-group counts.
  - Dependencies: AQ1–AQ5 and rollout approval.

## Account Quarantine Checkpoints

- [x] AQ-A: persistence and minimum-pool behavior reviewed.
- [x] AQ-B: slow/stay and fast/re-entry lifecycle reviewed.
- [ ] AQ-C: full quality gate and parent compatibility pass.
- [ ] AQ-D: product approves production enablement.

---

# Guardian Direct Scheduling and Conditional Account Recovery Checklist

Status: Plan proposed for approval

- [x] GR0A: Review active quarantine core and adapter changes.
- [x] GR0B: Review quarantine persistence and contracts.
- [x] GR0C: Review quarantine runtime orchestration.
- [x] GR0D: Pass the full gate and consolidate the prerequisite on clean main.
- [x] GR1: Add direct-scheduling and conditional-recovery contracts.
- [x] GR2: Add account observation, error-episode, run and ledger persistence.
- [x] GR3: Expose sanitized account inventory from the existing probe with zero extra list calls.
- [x] GR4: Publish/consume account observations exactly once.
- [x] GR5: Select bad-state accounts and channel-error groups with manual-pause protection.
- [x] GR6: Add typed account test, verified enable and verified disable operations.
- [x] GR7: Execute and persist one Guardian-owned account recovery run.
- [x] GR8: Transfer RECOVERY jobs and compatible MCP submission to Guardian.
- [ ] GR9: Prove the Sub2API channel mutation contract without production writes.
- [ ] GR10: Implement verified load_factor, priority and schedulable writer operations.
- [ ] GR11: Replace observe/rollout controls with direct scheduling start/stop.
- [ ] GR12: Apply bounded scheduling proposals in Guardian.
- [ ] GR13: Add direct scheduling/recovery REST and MCP controls.
- [ ] GR14: Update the light Guardian UI for direct mode.
- [ ] GR15: Remove legacy Scheduler recovery orchestration and periodic window/rotation.
- [ ] GR16: Complete notifications, metrics, runbook and changelog.
- [ ] GR17: Pass replay, full tests, lint, types, audit, Docker and health gates.
- [ ] GR18: Back up, deploy enabled, verify production and complete rollback drill.

## Guardian Direct Scheduling Checkpoints

- [x] GR-A: Quarantine prerequisite is clean on main.
- [ ] GR-B: Contracts/schema migration reviewed.
- [ ] GR-C: Same-probe account inventory proven with zero extra list calls.
- [ ] GR-D: Guardian is the only recovery owner.
- [ ] GR-E: Real channel writer and emergency stop proven.
- [ ] GR-F: API/MCP/UI and legacy removal complete.
- [ ] GR-G: Product approves direct production cutover.
