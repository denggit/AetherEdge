#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

exec "${LIVE_PYTHON_BIN:-python}" -u "$PROJECT_ROOT/scripts/run_live.py" "$@"
