# Sub2API Scheduler MCP

## Stack

- Python 3.11+
- uv-managed project with committed `uv.lock`
- MCP Python SDK 1.28.1 / FastMCP Streamable HTTP
- Pydantic v2, Starlette, SQLite, httpx, Prometheus

## Commands

- Install: `uv sync --frozen --all-extras`
- Local test (when the ignored `tests/` workspace exists): `uv run pytest -q`
- Lint: `uv run ruff check .`
- Type check: `uv run pyright`
- Audit: `uv run pip-audit`

## Conventions

- Read local planning files under the ignored `tasks/` directory when they are available.
- Local tests are written before behavior changes and run before pushing.
- Public inputs/outputs use strict Pydantic models with extra fields forbidden.
- MCP tools call the service layer; tools contain no scheduler, SQL, or HTTP logic.
- All SQL is parameterized and wrapped in explicit transactions.
- Logs are structured JSON with allowlisted fields; never log secrets, prompts, emails, or actor IDs.
- Delivery uses LangBot's common bot UUID + person/group + MessageChain contract. Never branch on adapter names.
- Deployment addresses and credentials come only from environment configuration.

## Boundaries

- Never modify LangBot core or its official image.
- Never reintroduce WeChatPad, recharge, transfer, or payment behavior.
- Never automatically re-enable a manually paused account.
- Never contact production services from tests.
- Deploy only through the configured GitHub Actions workflow; never commit deployment credentials.
