# Changelog

## [Unreleased]

### Added

- Guardian V2 now has strict, backward-compatible contracts for shared sampling, evidence
  reliability, confidence gates, rollout stages, recovery budgets, and field ownership; all
  active probing and writeback capabilities remain disabled by default.
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

### Changed

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
