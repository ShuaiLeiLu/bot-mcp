"""Command-line entrypoint for the MCP service."""

from __future__ import annotations

import uvicorn

from .bootstrap import bootstrap_legacy_core
from .config import load_settings


def main() -> None:
    settings = load_settings()
    bootstrap_legacy_core(settings.legacy_core_root)
    from .app import build_runtime, create_app

    app = create_app(build_runtime(settings))
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
