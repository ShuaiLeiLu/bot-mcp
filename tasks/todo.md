# Sub2API Scheduler MCP Service Tasks

- [x] Task 1: Scaffold the independent Python project and validated configuration.
  - Acceptance: own `pyproject.toml`, lockfile, source package, rules, environment template.
  - Verify: config contract tests fail first, then pass; `uv lock --check`.
  - Files: project metadata, `config.py`, contract tests.

- [x] Task 2: Add scoped authentication, request context, logging, and metrics.
  - Acceptance: X-API-Key/Bearer authentication, 401/403 behavior, no secret logging.
  - Verify: auth abuse tests and telemetry tests pass.
  - Files: `auth.py`, `logging.py`, `metrics.py`, tests.

- [x] Task 3: Add versioned SQLite repository.
  - Acceptance: migrations, durable jobs/scheduler/targets/outbox/bindings/audit, parameterized transactions.
  - Verify: empty/restart/concurrency repository tests pass.
  - Files: `contracts.py`, `repository.py`, repository tests.

- [x] Task 4: Add platform-neutral LangBot delivery.
  - Acceptance: discovers arbitrary adapter names, passes person/group targets unchanged, supports media policy fallback.
  - Verify: fake LangBot API integration matrix passes.
  - Files: `adapters/langbot.py`, `delivery.py`, tests.

- [x] Task 5: Add durable job manager and video jobs.
  - Acceptance: bounded queue, two video workers, job polling/cancellation, restart interruption semantics.
  - Verify: job state-machine and fake-video tests pass.
  - Files: `jobs.py`, `adapters/video.py`, tests.

- [x] Task 6: Add Sub2API operations and scheduler.
  - Acceptance: probe/recovery/maintenance reuse parent invariants, scheduler lease, quiet-hour/outbox behavior.
  - Verify: fake core integration tests and relevant parent tests pass.
  - Files: `adapters/sub2api.py`, `scheduler.py`, tests.

- [x] Task 7: Add platform-neutral actor bridge and bindings.
  - Acceptance: signed requests, replay protection, HMAC actor key, one-to-one bindings, masked responses.
  - Verify: spoofing/replay/uniqueness/privacy tests pass.
  - Files: `actor_bridge.py`, binding service, tests.

- [x] Task 8: Add curated MCP tools.
  - Acceptance: deterministic tool list, scopes, stable compact JSON results, no raw identities or secrets.
  - Verify: MCP contract tests pass with in-memory/ASGI client.
  - Files: `tools.py`, `service.py`, contract tests.

- [x] Task 9: Assemble the ASGI application.
  - Acceptance: `/mcp`, `/healthz`, `/metrics`, actor route, correct lifespan/start/stop behavior.
  - Verify: ASGI integration and shutdown tests pass.
  - Files: `app.py`, `__main__.py`, integration tests.

- [x] Task 10: Add deployment-neutral packaging and documentation.
  - Acceptance: non-root/read-only Docker image, Compose template, README/runbook, no host assumption.
  - Verify: full tests/static checks/audit/Docker build and local smoke test pass.
  - Files: Docker/Compose/README/runbook and smoke tests.

- [x] Final review
  - Acceptance: all spec success criteria met; no existing dirty plugin changes included.
  - Verify: code review, secret scan, final `git diff -- mcp-server` inspection.
