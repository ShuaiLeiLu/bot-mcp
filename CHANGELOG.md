# Changelog

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
