# Implementation Plan: Sub2API Scheduler MCP Service

## Overview

Create a deployment-neutral MCP subproject that reuses the existing Sub2API domain clients, persists scheduling state and jobs, exposes a curated authenticated MCP surface, and sends durable notifications through any configured LangBot bot via the common bot-service contract.

## Architecture Decisions

- Keep all new files inside `mcp-server/`; do not rewrite existing dirty plugin files.
- Pin MCP SDK 1.28.1, within LangBot's `<2` compatibility range, to include current security fixes while following its FastMCP stateless JSON mount pattern.
- Use service-layer classes behind MCP tools so protocol code contains no business logic.
- Reuse the parent plugin's validated Sub2API/video modules through an explicit core-root adapter instead of copying them.
- Use SQLite for durable state with one connection per transaction and parameterized SQL.
- Use generic LangBot bot discovery and `person/group + MessageChain`; never branch on adapter names.
- Keep all endpoint addresses and credentials runtime-configured. Do not deploy during this task.

## Dependency Graph

```text
project/lock/config/contracts
        |
        +--> auth + request context
        +--> SQLite repository
                    |
                    +--> job manager
                    +--> delivery target/outbox service
                    +--> scheduler state/lease
        +--> parent-core adapters
        +--> LangBot delivery adapter
                    |
                    +--> scheduler service
                    +--> MCP tool service
                                |
                                +--> FastMCP ASGI app
                                +--> actor bridge / health / metrics
                                +--> Docker and operator docs
```

## Phases

### Phase 1: Foundation

- Task 1: Create project metadata, rules, configuration contracts, and locked dependencies.
- Task 2: Implement API-key authentication, request context, safe JSON logging, and metrics.
- Task 3: Implement SQLite migrations plus scheduler/job/target/outbox repositories.

Checkpoint: focused tests, Ruff, and Pyright pass.

### Phase 2: Core vertical slices

- Task 4: Implement generic LangBot bot discovery/message delivery and all-channel target configuration.
- Task 5: Implement durable jobs and video submission/status/cancellation.
- Task 6: Implement Sub2API probe/recovery/maintenance adapters and scheduler cycle/lease.
- Task 7: Implement platform-neutral actor identity and binding/query bridge.

Checkpoint: fake Sub2API/video/LangBot integration tests pass; parent plugin tests remain green.

### Phase 3: MCP and operations

- Task 8: Register curated MCP tools, permission checks, stable JSON envelopes, and instructions.
- Task 9: Assemble Streamable HTTP ASGI app, session-manager lifespan, health, metrics, and actor route.
- Task 10: Add Dockerfile, Compose template, environment example, operator README, and smoke tests.

Checkpoint: full tests, static checks, dependency audit, Docker build, and local container smoke test pass without contacting production.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Parent plugin files are already dirty | High | Add only `mcp-server/` files; never stage or rewrite unrelated changes |
| Media support differs by adapter | Medium | Generic MessageChain with explicit media policy and text/link fallback |
| MCP calls lack trusted sender identity | High | Admin/service MCP scopes; signed actor bridge for user-specific operations |
| Video API has no resumable job ID | Medium | Durable local job IDs; mark running jobs interrupted on restart, never duplicate automatically |
| Duplicate scheduler execution | High | SQLite lease and single-replica guard |
| Long blocking upstream calls | Medium | Async workers call legacy clients through their existing thread boundaries |
| Secrets leak through logs/errors | High | Allowlisted JSON fields, stable safe errors, redaction tests |

## Verification Commands

```bash
uv sync --frozen --all-extras
uv run pytest tests/unit -q
uv run pytest -q
uv run ruff check .
uv run pyright
uv run pip-audit
docker compose build
```

## Deployment Boundary

No SSH, remote file changes, DNS, Caddy, firewall, or live LangBot registration occurs in this implementation plan. Deployment is a separate later task after the user identifies the target machine.
