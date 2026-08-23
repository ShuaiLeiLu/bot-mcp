# Sub2API Scheduler MCP

## Stack

- Python 3.11+
- uv-managed project with committed `uv.lock`
- MCP Python SDK 1.28.1 / FastMCP Streamable HTTP
- Pydantic v2, Starlette, SQLite, httpx, Prometheus

## Commands

- Install: `uv sync --frozen --all-extras`
- Test: `uv run pytest -q`
- Focused test: `uv run pytest tests/unit -q`
- Lint: `uv run ruff check .`
- Type check: `uv run pyright`
- Audit: `uv run pip-audit`

## Conventions

- Read `tasks/spec.md`, `tasks/plan.md`, and `tasks/todo.md` before changes.
- Tests are written before behavior changes.
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
- Do not deploy until the user provides a target machine in a later request.
