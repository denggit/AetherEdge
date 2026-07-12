"""Install the SQLite runtime-state guard in Python test subprocesses."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


if os.environ.get("AETHER_PYTEST_SQLITE_GUARD") == "1":
    repo_root = os.environ.get("AETHER_PYTEST_REPO_ROOT")
    pytest_root = os.environ.get("AETHER_PYTEST_ALLOWED_TEMP_ROOT")
    if not repo_root or not pytest_root:
        raise RuntimeError("pytest SQLite guard environment is incomplete")

    resolved_pytest_root = str(Path(pytest_root).resolve())
    tempfile.tempdir = resolved_pytest_root
    os.environ["TMPDIR"] = resolved_pytest_root
    os.environ["TEMP"] = resolved_pytest_root
    os.environ["TMP"] = resolved_pytest_root

    repo_parent = str(Path(repo_root).resolve())
    if repo_parent not in sys.path:
        sys.path.insert(0, repo_parent)
    from tests._support.runtime_state_guard import install_sqlite_guard

    install_sqlite_guard(repo_root=repo_root, pytest_root=pytest_root)
