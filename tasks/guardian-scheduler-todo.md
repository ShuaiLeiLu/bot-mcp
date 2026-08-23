# Guardian Scheduler Tasks

Status: In progress

- [x] Approve `docs/prd-guardian-scheduler.md` assumptions and open questions.
  - Acceptance: product owner confirms auth, visual parity, multi-upstream scope and observation period.
  - Verify: explicit approval in the task conversation.

- [ ] Capture and sanitize Guardian parity fixtures.
  - Acceptance: at least 20 rounds, with multi-group, missing-price, degraded and fused cases.
  - Verify: fixture secret scan and schema validation.

- [x] Implement contracts and scoring.
  - Acceptance: exact short/long/final scores and event classification.
  - Verify: focused golden tests, Ruff, Pyright.

- [x] Implement scope and state machine.
  - Acceptance: pause/exclude/fuse/degrade/recover/min-pool rules match PRD.
  - Verify: table-driven transition tests.

- [ ] Implement weight allocation.
  - Acceptance: parity thresholds in PRD §9.3.
  - Verify: golden parity report.
  - Current: self-owned candidate algorithm is implemented; third-party parity
    remains blocked on the 20-round sanitized fixture set.

- [x] Implement Guardian persistence.
  - Acceptance: migrations, restart durability, pagination, lease and audits.
  - Verify: repository integration tests.

- [x] Implement dry-run engine.
  - Acceptance: computes complete desired state without writes.
  - Verify: fake Sub2API end-to-end run.

- [ ] Implement idempotent writeback and restore.
  - Acceptance: observe-only default, original snapshot, preview, confirmation and rollback.
  - Verify: failure injection and restore tests.

- [x] Implement REST and MCP contracts.
  - Acceptance: authenticated v1 API, revision conflicts, uniform errors and tool parity.
  - Verify: contract tests.

- [x] Implement the 10-page Guardian UI.
  - Acceptance: all PRD pages/fields/actions, responsive and accessible.
  - Verify: browser tests at 390px and 1440px; no console errors.
  - Verified: responsive overflow, ten-menu navigation, policy tabs, revision
    save and in-memory credential clearing passed in a real Chromium session.

- [x] Complete security and quality review for the observation build.
  - Acceptance: no secret leakage or unbounded operations; audit clean.
  - Verify: pytest, Ruff, Pyright, pip-audit, Docker build, secret scan.
  - Verified: 101 tests, Ruff, Pyright, pip-audit, wheel asset inspection,
    secret scan and `bot-mcp-ci:guardian` image build passed.

- [ ] Run observation rollout.
  - Acceptance: at least 24 hours and 100 cycles; no production writes.
  - Verify: signed parity/safety report.

- [ ] Request explicit production writeback approval.
  - Acceptance: one noncritical group selected with rollback window.
  - Verify: user approval immediately before enabling writes.
