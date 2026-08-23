# Guardian Scheduler Implementation Plan

Status: Proposed — awaiting PRD approval

Source: `docs/prd-guardian-scheduler.md`

## Phase 0 — Parity research

1. Capture sanitized golden snapshots from read-only reference APIs.
2. Lock the exact scoring formula with golden tests.
3. Fit and validate price/speed/balanced weight allocation.
4. Produce a parity report; block production writeback until thresholds pass.

Checkpoint: scoring exact; weight parity meets PRD §9.3.

## Phase 1 — Domain foundation

1. Add strict policy/sample/state contracts.
2. Add Guardian SQLite migration and repository.
3. Implement classifier and scoring engine.
4. Implement scope resolution and state machine.
5. Implement weight allocator and desired-state planner.

Checkpoint: pure unit tests cover all decision tables.

## Phase 2 — Safe orchestration

1. Extend the Sub2API adapter with validated read and write contracts.
2. Implement dry-run engine and durable run history.
3. Add scheduler lease, queue, cancellation and bounded concurrency.
4. Add idempotent writeback, original snapshots and restore preview.
5. Add metrics, structured events and write audits.

Checkpoint: fake-Sub2API integration tests pass; no production writes.

## Phase 3 — Stable interfaces

1. Implement `/api/guardian/v1` read endpoints.
2. Implement revisioned policy and scoped mutation endpoints.
3. Add Guardian MCP tools with existing scopes.
4. Add contract tests and error-shape tests.

Checkpoint: API/MCP contract tests pass.

## Phase 4 — Management UI

1. Build the shared shell, navigation, responsive layout and in-memory auth.
2. Build overview, groups, channels and live-routing pages.
3. Build probe spend, guide and event pages.
4. Build policy tabs, connection status and notifications page.
5. Add forms, validation, conflict handling, destructive confirmations and a11y.

Checkpoint: browser tests at desktop/mobile widths; zero console errors.

## Phase 5 — Rollout

1. Deploy with every write switch off.
2. Run a 24-hour / 100-cycle observation comparison.
3. Review parity and safety report.
4. Ask for explicit approval before single-group writeback.
5. Exercise restore and rollback.

Checkpoint: product approval for each expansion step.

## Major risks

| Risk | Mitigation |
|---|---|
| Reference weight formula is not public | Golden snapshot fitting; label as own algorithm if parity fails. |
| Automated writes disable good channels | Observe-only default, conservative classifier, min pool, one switch per round. |
| Human pause is accidentally recovered | Separate manual state; invariant tests. |
| UI exposes credentials | In-memory token, same-origin API, no browser storage, strict CSP. |
| Concurrent admin edits overwrite policy | Revision and `If-Match`. |
| High-volume logs exhaust SQLite | Pagination, retention, bounded samples and indexes. |
