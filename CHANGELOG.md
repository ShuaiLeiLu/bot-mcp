# Changelog

## [Unreleased]

### Added

- Guardian now has strict, backward-compatible contracts for shared sampling, evidence
  reliability, confidence gates, conditional recovery, and per-account field ownership.
- Guardian databases now migrate transactionally to the additive V2 evidence schema while
  preserving V1 policy, channel, sample, run, event, and audit data.
- Each successful existing scheduler probe now publishes one canonical rich Guardian snapshot
  from the same validated response, without making an additional upstream probe request.
- Guardian shared mode now leases and consumes each published snapshot exactly once; repeated
  scans and service restarts become local no-ops until new evidence arrives.
- Guardian V2 traffic sampling now filters monitor requests, rejects conflicting duplicate
  request hashes, keeps unattributed evidence out of decisions, and aggregates traffic into
  deterministic minute buckets with volume-neutral cross-time weight.
- Guardian V2 now computes time-decayed short/long health, independent evidence confidence,
  deterministic freshness, and cold-start state; missing evidence preserves the prior health
  score while confidence falls to zero.
- Guardian runs now persist de-duplicated shared-monitor evidence, fuse matching traffic
  buckets by source reliability, exclude legacy samples from V2 scoring, and expose each
  channel's confidence, freshness, evidence age, warm-up count, and source mix.
- Guardian state decisions now freeze on stale or low-confidence evidence, require trusted
  fatal confirmation, prohibit probes or recovery for human-controlled channels, and allow
  recovery only for Guardian-owned fuses meeting the recovery confidence gate.
- Guardian V2 weight recommendations now use dimensionless price/speed signals, reserve low-
  confidence channel budget, penalize missing signals, enforce integer caps and explicit
  unallocated budget, bound load changes, and keep priority tied only to health tiers.
- Guardian now tracks per-account field baselines and ownership, detects sticky human takeover,
  audits every decision, replays idempotent results, and applies direct writes only while the
  single `enabled` switch and verified account writer both permit them.
- Guardian recovery probing now selects only uniquely mapped Guardian-owned fuses and enforces
  interval, concurrency, per-channel hourly, global daily request, and daily Token budgets with
  durable request, cost, Token, and blocked-attempt accounting.
- Guardian REST and MCP surfaces now expose direct scheduling status/start/stop, sampling status,
  channel score explanations, write ownership, open recovery episodes, and confirmed pending
  recovery submission. Legacy rollout endpoints return a stable deprecation error.
- Conditional recovery reuses the existing inventory, tests only abnormal accounts per new
  snapshot, broadens once for a new failed-channel episode, protects manual pauses, and persists
  verified enable/disable/indeterminate outcomes.
- Normal schedulable accounts now receive one durable active health check per hour. The check is
  de-duplicated across fast Guardian scans and restarts, skips human pauses and protected states,
  and keeps ordinary successful checks silent.
- All system-owned account tests now send the same explicit low-token Sub2API template using the
  account default model, prompt `hi`, and default text mode.
- Direct scheduling resolves unique monitor→group→account mappings and applies bounded
  `load_factor` plus baseline-relative `priority` through current-read, one-field write, exact
  read-back verification. A failed verification stops the remaining run.
- The Guardian console is now a light, responsive operations dashboard with explicit scheduling
  controls and redacted recovery status.
- SQLite retention now runs every ten minutes even when direct scheduling is stopped. It removes
  bounded batches of expired observations, runs, events, recovery history, terminal jobs, and
  successfully delivered notifications while preserving live recovery state, queued/failed
  deliveries, human ownership, current channel state, and recent audit history.

### Changed

- Slow-first-token protection now counts only the latest three minutes of Sub2API usage logs and
  quarantines after exactly three over-30-second observations. Recovery now requires two
  consecutive at-or-below-30-second successful probes, with a restart-safe persisted streak.
- Error accounts whose dispatch switch is off are now probed; a successful probe restores both
  account status and dispatch, while non-error paused accounts remain excluded.
- Equal recovery-window start and end times now represent a true 24-hour window.
- Status reports and Guardian transition notifications now show their trigger time in the
  Asia/Shanghai timezone.
- A new status event now supersedes older undelivered status events for the same target to
  prevent stale notification bursts after a delivery outage.
- Automatic media delivery now uses an atomic media-only first attempt and retries as text
  when a LangBot adapter reports an internal media-send failure; ordinary transient upstream
  failures remain queued for retry.
- Account maintenance and recovery notifications now use structured Chinese copy with a
  Beijing trigger time, account identity, localized reason, and explicit result.
- Retention migrations add time-oriented indexes, checkpoint the WAL after each bounded cleanup,
  and expose cleanup outcome, deleted-row, and database-size metrics.
- The Guardian policy page now exposes the hourly account-check switch and interval instead of
  claiming that all normal active probes are disabled.

### Fixed

- Conditional account recovery now carries the tested pre-mutation state through verified
  enablement, so an upstream `active + schedulable=false` transition created during a successful
  error recovery is completed instead of being misclassified as a human pause. True pre-test
  manual pauses remain immutable.
- Bad-state account tests now use a durable 15-minute cross-snapshot cooldown, preventing the
  same disabled account from being tested and disabled again every scheduler snapshot.
- A validated `error`, `disabled`, or `inactive` account snapshot now remains sufficient evidence
  to run the system account test when the redundant detail re-read is unavailable. A successful
  test immediately completes verified enablement instead of waiting for another state cycle.
- Direct-write limits now count accounts that actually received a verified write. Accounts with
  no change or a cooldown-blocked proposal no longer starve later accounts and groups.

## [0.1.0] - 2026-08-23

### Added

- Deployment-neutral Streamable HTTP MCP service with scoped API-key authentication.
- Durable SQLite scheduler, jobs, leases, bindings, notification outbox, and audit events.
- Channel probing plus existing Sub2API recovery and account-maintenance invariants.
- Durable video generation with queue count, polling, cancellation, and restart safety.
- Platform-neutral delivery through every bot adapter registered in LangBot.
- Signed actor bridge for identity-sensitive bind/unbind/account commands.
- Structured JSON logging, Prometheus metrics, health checks, tests, and hardened Docker files.
- GitHub Actions quality gates, local-only image deployment, immutable server releases, and manual rollback.

### Security

- Secrets are environment-only and excluded from tool results and structured logs.
- Administrator details cannot target group delivery destinations.
- Actor requests use HMAC signatures, a bounded timestamp window, and one-time nonces.
- Credential-bearing upstream HTTP redirects remain disabled.
