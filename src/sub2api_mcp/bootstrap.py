"""Locate the existing plugin domain modules before importing runtime adapters."""

from __future__ import annotations

import sys
from pathlib import Path

REQUIRED_CORE_FILES = (
    "bindings.py",
    "client_errors.py",
    "maintenance.py",
    "maintenance_gateway.py",
    "monitor.py",
    "notification_image.py",
    "probe.py",
    "recovery.py",
    "video.py",
)


def bootstrap_legacy_core(core_root: Path) -> Path:
    resolved = core_root.expanduser().resolve()
    missing = [name for name in REQUIRED_CORE_FILES if not (resolved / name).is_file()]
    if missing:
        raise RuntimeError(f"Sub2API core root is missing required modules: {', '.join(missing)}")
    root_text = str(resolved)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
    return resolved

