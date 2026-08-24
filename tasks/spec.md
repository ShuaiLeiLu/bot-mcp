# Spec: Sub2API Scheduler MCP Service

Status: Approved for implementation; deployment target intentionally undecided

Version: 0.1.0

Date: 2026-08-23

## Assumptions

1. The service lives in `sub2api-channel-monitor/mcp-server/` as an independently runnable Python project with its own dependency manifest, lockfile, tests, Dockerfile, and Compose file.
2. It covers the complete Sub2API scheduling domain: channel probing, account-group counts, snapshot change detection, scheduled recovery, health protection, account binding/query, durable jobs, notification outbox, and video generation.
3. Removed WeChatPad, transfer, and recharge behavior remains removed.
4. LangBot remains an unmodified official deployment. It connects to this service as a remote MCP client.
5. The existing LangBot plugin remains installed during migration and is reduced later to a platform-neutral inbound command/identity bridge. It is not deleted until parity and rollback tests pass.
6. The first production deployment is single-replica. SQLite is the durable store; multi-replica scheduling is out of scope for v0.1.0. The destination host is deliberately not selected during implementation.
7. Delivery supports every messaging-platform bot registered in LangBot through LangBot's common bot service contract. The MCP service never hard-codes a WeChat-, QQ-, Telegram-, Discord-, Slack-, Lark-, DingTalk-, WeCom-, LINE-, KOOK-, Matrix-, Satori-, HTTP-, or WebSocket-specific send path.

## Objective

Build a standalone, durable Sub2API scheduling service that:

- continuously runs the current channel/account monitoring schedule without depending on chat traffic;
- exposes safe, typed MCP tools for inspection and administrative actions;
- preserves the rule that manually paused or otherwise ineligible accounts are never automatically enabled;
- stores jobs, snapshots, scheduler state, bindings, audit records, and pending notifications across restarts;
- lets LangBot, Codex, and other authorized MCP clients use the same scheduling system;
- emits a durable notification outbox whose worker delivers through any configured LangBot bot;
- supports multiple simultaneous delivery targets across different LangBot adapters;
- does not modify LangBot core or its official image.

## Non-goals

- Reintroducing WeChatPad, transfers, recharge, or payment callbacks.
- Replacing LangBot's platform adapters.
- Letting an unauthenticated MCP client access account data or mutations.
- Automatically retrying interrupted video generations after a service restart when the upstream API has not returned a resumable job ID.
- Running more than one active scheduler replica in v0.1.0.
- Exposing raw Sub2API API responses, API keys, full email addresses, or raw platform actor identifiers to an LLM.

## Tech Stack

- Python `>=3.11,<4.0`, matching LangBot.
- `uv` for dependency and lockfile management.
- MCP Python SDK `mcp==1.28.1` on the stable v1 line. This remains inside LangBot's declared `>=1.25,<2` compatibility range and includes the security fixes unavailable in 1.26.0.
- FastMCP with Streamable HTTP at `/mcp`; SSE is not used for new code.
- Starlette/ASGI composition for MCP, health, metrics, and the authenticated actor bridge route.
- Pydantic v2 models for configuration and public input/output contracts.
- SQLite through the standard library with parameterized SQL and explicit migrations.
- `prometheus-client` for bounded-cardinality service metrics.
- Standard-library structured JSON logging; optional OpenTelemetry export when configured.
- pytest, pytest-asyncio, Ruff, and Pyright for verification.

Official references:

- MCP Python SDK v1.28.1: https://github.com/modelcontextprotocol/python-sdk/tree/v1.28.1
- Streamable HTTP server pattern: https://github.com/modelcontextprotocol/python-sdk/blob/v1.28.1/README.md
- LangBot remote MCP configuration: https://docs.langbot.app/zh/usage/mcp/readme
- MCP tool contract: https://modelcontextprotocol.io/specification/2025-11-25/server/tools

LangBot project contracts followed by this subproject:

- `LangBot/src/langbot/pkg/api/mcp/server.py`: curated FastMCP tools, stateless JSON Streamable HTTP, direct service-layer calls.
- `LangBot/src/langbot/pkg/api/mcp/mount.py`: API-key/Bearer authentication before the MCP ASGI app and explicit session-manager lifespan.
- `LangBot/skills/skills/langbot-mcp-ops/SKILL.md`: `/mcp` endpoint, client header format, permission behavior, and MCP/HTTP maintenance rules.
- `LangBot/src/langbot/pkg/api/http/service/bot.py`: platform-neutral bot UUID, `person/group` target, and common `MessageChain` send contract.
- `LangBot/src/langbot/pkg/api/http/controller/groups/platform/bots.py`: authenticated runtime-send HTTP endpoint used by the outbox worker.

## Runtime Architecture

```text
LangBot Local Agent / other MCP client
        |
        | Streamable HTTP + Bearer token
        v
sub2api-mcp (:5310)
        |-- MCP tools
        |-- scheduler loop
        |-- durable job workers
        |-- notification outbox worker
        |-- /healthz and /metrics
        |
        +--> fixed HTTPS Sub2API admin API
        +--> configured, allowlisted HTTPS video API
        +--> LangBot bot-service HTTP API
        +--> SQLite /data/sub2api-mcp.db

LangBot bot service
        |-- resolves bot UUID to its configured platform adapter
        |-- validates a common person/group target
        +-- sends a common MessageChain through any registered adapter

platform-neutral inbound bridge plugin
        |-- receives deterministic /zs commands on any LangBot platform
        |-- derives trusted workspace/bot/adapter/actor identity
        +-- calls the MCP service's authenticated actor endpoint
```

The MCP service owns business state, schedules, outbox claims, and outbound delivery. LangBot owns platform translation. The small inbound bridge is needed only for deterministic commands and trusted sender identity; it contains no scheduling or Sub2API business logic. This keeps proactive behavior outside model-controlled tool calls while supporting every adapter that implements LangBot's common bot contract.

The service follows the MCP conventions already used by this LangBot version:

- FastMCP with `stateless_http=True` and `json_response=True`;
- Streamable HTTP mounted at `/mcp`;
- authentication before the FastMCP ASGI app using `X-API-Key` or `Authorization: Bearer`;
- a curated tool surface that calls the service layer directly;
- compact JSON tool results with stable permissions and redacted secrets;
- the MCP tool inventory and operator documentation updated together.

## Project Structure

```text
mcp-server/
├── AGENTS.md
├── README.md
├── pyproject.toml
├── uv.lock
├── Dockerfile
├── compose.yaml
├── .env.example
├── src/sub2api_mcp/
│   ├── __init__.py
│   ├── __main__.py
│   ├── app.py                 # ASGI assembly and lifespan
│   ├── auth.py                # MCP bearer-token verifier and scopes
│   ├── config.py              # validated environment configuration
│   ├── contracts.py           # public Pydantic input/output schemas
│   ├── errors.py              # stable error codes
│   ├── logging.py             # structured JSON logging
│   ├── metrics.py             # RED/USE metrics
│   ├── repository.py          # SQLite migrations and persistence
│   ├── scheduler.py           # periodic probe/recovery orchestration
│   ├── jobs.py                # durable job state machine and workers
│   ├── outbox.py              # notification enqueue/claim/ack
│   ├── actor_bridge.py        # authenticated, platform-neutral command endpoint
│   ├── tools.py               # MCP tool registration only
│   └── adapters/
│       ├── sub2api.py         # wraps existing validated domain clients
│       ├── video.py           # wraps existing video client
│       └── langbot.py         # generic bot discovery and MessageChain delivery
├── tests/
│   ├── unit/
│   ├── integration/
│   └── contract/
└── tasks/
    ├── spec.md
    ├── plan.md
    └── todo.md
```

The standalone repository carries a reviewed `core/` snapshot of the validated domain modules (`monitor.py`, `probe.py`, `recovery.py`, `maintenance.py`, `maintenance_gateway.py`, `bindings.py`, and `video.py`). CI tests the MCP service against this exact snapshot, and Docker copies it into a private runtime path. Core updates are synchronized deliberately in the same pull request as their compatibility tests.

## MCP Tool Contract

Tool names use lowercase ASCII and underscores for compatibility across MCP clients and function-calling model providers.

All successful tools return a typed object containing:

```json
{
  "ok": true,
  "requestId": "uuid",
  "data": {}
}
```

Expected failures return:

```json
{
  "ok": false,
  "requestId": "uuid",
  "error": {
    "code": "STABLE_MACHINE_CODE",
    "message": "Safe human-readable message",
    "retryable": false
  }
}
```

Validation failures remain protocol-level invalid-argument errors generated from the JSON Schema. Internal stack traces and upstream response bodies are never returned.

### Read tools

| Tool | Purpose | Required scope |
|---|---|---|
| `sub2api_get_status` | Scheduler state, last/next run, job counts, outbox backlog, version | `sub2api:read` |
| `sub2api_probe_channels` | Run a manual read-only channel/group probe and return the normalized snapshot | `sub2api:read` |
| `sub2api_get_job` | Get one durable job by opaque `jobId` | `sub2api:read` |
| `sub2api_list_jobs` | Cursor-paginated jobs filtered by type/status | `sub2api:read` |
| `sub2api_get_bound_account` | Return a masked account summary for an administrator-supplied actor key | `sub2api:read` |
| `sub2api_list_delivery_bots` | Discover all currently configured LangBot bots with secrets redacted | `sub2api:read` |
| `sub2api_list_delivery_targets` | List cursor-paginated platform-neutral delivery targets | `sub2api:read` |

### Mutation tools

| Tool | Purpose | Required scope |
|---|---|---|
| `sub2api_set_scheduler_enabled` | Persistently enable/disable periodic probing; manual tools remain available | `sub2api:admin` |
| `sub2api_submit_recovery` | Submit one recovery run as a durable job | `sub2api:admin` |
| `sub2api_submit_maintenance` | Submit account sweep/log-guard maintenance as a durable job | `sub2api:admin` |
| `sub2api_bind_account` | Create an admin-authorized one-to-one actor/account binding | `sub2api:admin` |
| `sub2api_unbind_account` | Remove the caller-specified actor binding idempotently | `sub2api:admin` |
| `sub2api_submit_video` | Validate prompt/length/steps/resolution, enqueue a video job, and return queue count | `sub2api:write` |
| `sub2api_cancel_job` | Cancel a queued job; running upstream calls may be marked cancelled even if the provider cannot be interrupted | `sub2api:write` |
| `sub2api_upsert_delivery_target` | Create/update a target by bot UUID, purpose, person/group type, target ID, and media policy | `sub2api:admin` |
| `sub2api_delete_delivery_target` | Disable/remove a delivery target idempotently | `sub2api:admin` |
| `sub2api_test_delivery_target` | Send a bounded test message through the configured LangBot adapter | `sub2api:admin` |

MCP Tasks are not a v0.1.0 dependency. Long operations use explicit durable `jobId` submission and polling because client support is not assumed.

## Job State Machine

```text
QUEUED -> RUNNING -> SUCCEEDED
                  -> FAILED
       -> CANCELLED
RUNNING -> CANCELLED
RUNNING -> INTERRUPTED (service restart without resumable upstream job ID)
```

- IDs are random UUIDs and never sequential database IDs.
- Queue limits are enforced before insertion.
- Video defaults preserve the existing limits: maximum 20 pending jobs and 2 concurrent upstream calls.
- The returned video URL must remain HTTPS, same-origin with the configured generation endpoint, under `/outputs/`, and end in `.mp4`.
- Transport failures keep retrying according to the existing behavior; explicit upstream HTTP failures become terminal failures.
- An interrupted non-resumable video job is not silently resubmitted, preventing duplicate paid generation.
- Job list pagination uses opaque cursors and a bounded page size.

## Scheduler Behavior

- Default probe interval remains 60 seconds.
- The first successful probe establishes/delivers a baseline; later notifications are generated only for channel-state or account-count changes, not latency-only changes.
- Snapshot advancement occurs only after the internal delivery worker records a successful LangBot send response for every required target.
- Quiet hours defer outbox delivery while probes, recovery, and maintenance continue.
- Scheduled recovery is opt-in and runs only inside the configured Asia/Shanghai daily window.
- Recovery tests every account in `error` state, including accounts whose scheduling switch was
  turned off as part of entering the error state.
- Manually paused, disabled, inactive, expired, rate-limited, overloaded, or temporarily unschedulable accounts are excluded before every test and write.
- Recovery may restore error state and its scheduling switch only after explicit test success and
  read-back verification. Non-error paused, disabled, inactive, expired, rate-limited,
  overloaded, or temporarily unschedulable accounts remain excluded.
- Maintenance mutations are opt-in, capped, audited, and fail closed when no administrator delivery target is configured.
- The v0.1.0 service enforces a single active scheduler process using a database lease and refuses to run a second scheduler against the same database.

## Platform-neutral Delivery Contract

The service discovers bots from LangBot and sends each outbox item through LangBot's existing bot-service HTTP endpoint. It supplies only the public common contract:

```json
{
  "botUuid": "uuid",
  "targetType": "person",
  "targetId": "adapter-owned opaque id",
  "messageChain": [
    {"type": "Plain", "text": "message"}
  ]
}
```

- `targetType` is exactly `person` or `group`, matching LangBot's public bot-service API.
- `targetId` remains an opaque adapter-owned value; the MCP service does not parse platform-specific ID formats.
- `botUuid` selects any currently configured LangBot adapter. Adapter names are discovered, never allowlisted in the MCP service.
- A delivery target may subscribe to one or more purposes: `STATUS`, `RECOVERY_ADMIN`, `MAINTENANCE_ADMIN`, `VIDEO_RESULT`.
- Multiple targets can subscribe to the same purpose, enabling fan-out across different platforms.
- A target has a media policy: `AUTO`, `TEXT_ONLY`, `IMAGE`, `FILE`, or `LINK`.
- Text is the universal fallback. When an adapter explicitly does not support an image/file/video message, `AUTO` falls back to safe text plus an HTTPS link. Transient platform/network errors retry the original representation instead of being misclassified as unsupported media.
- The common LangBot `MessageChain` wire format is used for text, image, and file payloads. No platform SDK is imported into the MCP service.

Each outbox event has a stable type (`STATUS_CHANGED`, `RECOVERY_RESULT`, `MAINTENANCE_RESULT`, `VIDEO_READY`, `VIDEO_FAILED`), target class, safe payload, attempt count, creation time, and opaque event ID.

- Group events never contain account names, IDs, raw emails, actor IDs, or adjustment details.
- Administrator events may contain account name/ID but never credentials.
- Delivery is at-least-once. The service deduplicates claims by `eventId` and records a per-target delivery result.
- Snapshot delivery state advances only after all required target deliveries succeed; optional targets do not block state advancement.

## Platform-neutral Actor Bridge Contract

Ordinary deterministic commands such as bind/unbind/account query require a trusted sender identity that an external MCP tool call does not carry. The existing plugin is therefore reduced to a generic bridge that works with LangBot's shared event/target entities rather than a WeChat API:

- input identity is derived from Workspace UUID, bot UUID, adapter identifier, launcher type, and launcher ID;
- the raw platform ID is never stored; the service stores a versioned HMAC actor key;
- group/private classification uses LangBot's shared launcher/target type, not a platform-specific chatroom suffix;
- the bridge calls one authenticated internal endpoint with a signed request and short replay window;
- the same bridge component receives commands from every platform adapter that can run a LangBot pipeline/plugin event;
- MCP tools remain administrator/service scoped and cannot accept a caller-supplied raw platform identity as proof of identity.

## Persistence

SQLite tables are versioned through explicit forward-only migration functions:

- `service_metadata`
- `scheduler_state`
- `probe_snapshots`
- `jobs`
- `account_bindings`
- `delivery_targets`
- `notification_outbox`
- `notification_deliveries`
- `audit_events`
- `scheduler_lease`

All SQL is parameterized. Transactions are explicit around job claims, outbox leases, binding uniqueness, scheduler lease updates, and snapshot acknowledgements. The database and generated artifacts live under `/data`; source directories remain read-only in Docker.

## Authentication and Authorization

- The MCP endpoint uses the official SDK token-verifier boundary and `Authorization: Bearer ...` headers.
- Opaque access tokens and their scopes are loaded from environment/configured secret files, never committed or logged.
- Token comparison is constant-time.
- Read, write, admin, and actor-bridge scopes are separate.
- The actor-bridge token cannot call MCP admin tools or select an arbitrary actor.
- LangBot supports configured request headers for remote MCP connections; no LangBot source change is required.
- The outbound LangBot API key requires only the runtime-send permission needed by the existing bot-service endpoint; it is separate from the MCP client key and Sub2API admin key.
- The service binds to `127.0.0.1` by default. Host, port, public URL, LangBot URL, and data path are runtime configuration; the code contains no deployment-host assumption. Binding externally requires an explicit setting and HTTPS reverse proxy.
- Request bodies, prompt length, page size, job count, and concurrency are bounded.
- Redirects are rejected for credential-bearing upstream calls.

## Threat Model

### Trust boundaries

- MCP/actor-bridge HTTP requests: untrusted until token, signature, replay-window, and scope checks pass.
- LLM-generated tool arguments: untrusted even after authentication.
- Sub2API and video API responses: untrusted external input.
- SQLite contents after an unclean shutdown: validated before use.
- Delivery payloads: confidential according to target class.

### Primary abuse cases and controls

| Threat | Control |
|---|---|
| LLM invokes destructive maintenance unexpectedly | Admin scope, explicit mutation tool names, audit event, configured mutation switches, bounded caps |
| Token/API key disclosure | Environment-only secrets, allowlisted structured logs, no response dumps |
| SSRF through configurable URLs | HTTPS-only URL validation, host allowlist, redirect rejection, same-origin video output checks |
| Queue exhaustion | Auth, rate/concurrency caps, maximum pending jobs, bounded request sizes |
| Duplicate recovery or notifications | Scheduler lease, idempotency keys, transactional claims, outbox event IDs |
| Cross-user binding disclosure | Platform-neutral HMAC actor keys, masked emails, one-to-one uniqueness, trusted actor bridge |
| Platform-specific coupling | Runtime bot discovery, common person/group target contract, common MessageChain, no adapter allowlist |
| Unsupported media on a platform | Configured media policy, explicit capability-error classification, safe text/link fallback |
| Group privacy leak | Separate event schemas and contract tests forbidding account details in group payloads |

## Observability

On-call questions:

1. Is the scheduler running, and when did the last successful probe finish?
2. Which external dependency or policy caused a job to fail?
3. Is the job queue or notification outbox growing or aging abnormally?
4. Which authenticated principal requested an account mutation, without exposing its token?

Signals:

- JSON logs with stable event names and `requestId`, `jobId`, or `eventId` where applicable.
- No tokens, API keys, prompts, raw emails, raw actor IDs, or full upstream bodies in logs.
- Prometheus counters/histograms/gauges for MCP calls, upstream calls, job transitions, scheduler runs, queue depth, oldest queue age, outbox backlog, and oldest outbox age.
- Labels are limited to bounded values such as tool name, job type, status class, dependency, and error code.
- `/healthz` reports process/database/scheduler readiness without secrets.
- Optional OpenTelemetry spans cover MCP call -> scheduler/job -> Sub2API/video dependency when an exporter is configured.

## Commands

From `sub2api-channel-monitor/mcp-server`:

```bash
# Install locked development dependencies
uv sync --frozen --all-extras

# Run focused/full tests
uv run pytest tests/unit -q
uv run pytest -q

# Static verification
uv run ruff check .
uv run pyright

# Dependency audit
uv run pip-audit

# Run locally
uv run python -m sub2api_mcp

# Docker verification and start
docker compose build --pull
docker compose up -d
docker compose ps
```

The initial lockfile is generated with `uv lock`; all later CI/deploy installs use `--frozen`.

## Code Style

- Python type hints on every public function and all public contracts.
- `from __future__ import annotations` in source modules.
- Pydantic models at external boundaries; domain internals use dataclasses where simpler.
- Async orchestration with blocking legacy HTTP clients isolated through `asyncio.to_thread` as they are today.
- No generic framework abstraction until at least three real consumers require it.
- Comments/docstrings are English; user-facing text may be Chinese.

Example:

```python
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class SubmitVideoInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field(min_length=1, max_length=2000)
    length: int = Field(default=22, ge=1, le=3600)
    steps: int = Field(default=20, ge=1, le=100)
    width: int = Field(default=768, ge=64, le=2048)
    height: int = Field(default=448, ge=64, le=2048)
```

## Testing Strategy

- Unit tests: configuration, schema validation, state transitions, repository transactions, cursor pagination, quiet hours, recovery eligibility, scope checks, redaction, and group-event privacy.
- Contract tests: deterministic MCP tool list, JSON Schema, success/error envelopes, output schemas, and authorization behavior.
- Integration tests: ASGI MCP client, SQLite restart recovery, scheduler lease, fake Sub2API/video/LangBot servers, outbox claim/deliver/retry, actor signature verification, and metrics/log emission.
- Delivery compatibility tests: arbitrary discovered adapter names are accepted; person/group targets pass through unchanged; media-policy fallback is tested without importing a platform SDK.
- Migration tests: start from an empty database and every historical schema version introduced by this subproject.
- Compatibility tests: the existing parent plugin test suite must remain green whenever shared modules change.
- No test contacts production services; external calls use local fakes.
- Every behavior is implemented with RED -> GREEN -> REFACTOR.

## Boundaries

### Always do

- Preserve unrelated dirty worktree changes.
- Add new behavior behind the separate service boundary before removing plugin behavior.
- Validate all MCP inputs and external responses.
- Keep account mutations opt-in, capped, auditable, and fail-closed.
- Run focused tests after every vertical slice and the full service/parent suites before handoff.
- Keep secrets out of source, fixtures, logs, images, and commits.

### Ask first

- Change an existing plugin database/file format.
- Remove the old plugin scheduler or notification path.
- Deploy to any host or switch a live LangBot instance to the MCP service.
- Expose the MCP service beyond loopback/private Docker networking.
- Change mutation thresholds, recovery eligibility, or group privacy behavior.
- Bypass LangBot's common bot service to call a platform provider directly.

### Never do

- Re-enable manually paused accounts.
- Treat network timeouts or malformed responses as proof an account failed.
- Commit `.env`, API keys, bearer tokens, private keys, generated videos, or SQLite data.
- Log raw credentials, prompts, full email addresses, or raw platform actor IDs.
- Return internal stack traces or upstream response dumps to MCP clients.
- Remove or disable existing tests to make the new service pass.

## Success Criteria

- `mcp-server/` is an independently runnable, locked Python project and builds as a non-root, read-only Docker container with a writable `/data` volume.
- LangBot can connect to `/mcp` with an Authorization header and list the documented tools.
- Missing/invalid tokens receive 401; insufficient scopes cannot execute protected tools.
- Scheduler state, snapshots, jobs, bindings, outbox items, and audit records survive restart.
- A single scheduler lease prevents two instances from executing the same interval concurrently.
- Manual probe returns the same normalized status/account counts as the current plugin.
- Recovery and maintenance preserve every current eligibility and fail-closed invariant, especially manual pauses.
- Video submission immediately returns a job ID and queue count; status polling reaches a terminal result without a 300-second client failure rule.
- Group notification events contain no account identity or adjustment details.
- The service discovers and can target every bot registered in LangBot without an adapter allowlist.
- Text notifications work through the common `person/group + MessageChain` contract; unsupported media follows the configured text/link fallback.
- A successful required-target delivery advances the delivered snapshot; failed/deferred delivery does not.
- Structured logs and metrics can identify scheduler health, dependency errors, queue age, and outbox backlog without exposing secrets or PII.
- Focused, full MCP-service, and existing parent-plugin tests all pass.
- No LangBot core files or official image contents are modified.

## Resolved Decisions

1. MCP tools are administrator/service scoped in v0.1.0. Ordinary user commands on all LangBot platforms continue through the generic actor bridge; arbitrary LLM callers cannot impersonate a user by passing a platform ID.
2. Implementation, tests, container artifacts, and documentation are completed without deploying anywhere. The destination host, network exposure, domain, and reverse proxy are selected only when a later deployment request identifies the target machine.
