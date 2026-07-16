#!/usr/bin/env python
"""CLI compatibility entrypoint for the canonical Python watchdog core."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.app.watchdog import (
    DEFAULT_CHILD_PID_FILE,
    _parse_fatal_exit_codes,
    build_command,
    run_live_watchdog,
)


def main() -> int:
    return run_live_watchdog(project_root=PROJECT_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
