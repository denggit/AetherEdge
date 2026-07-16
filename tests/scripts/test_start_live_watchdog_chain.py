from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
START_SCRIPT = PROJECT_ROOT / "scripts" / "start_live_watchdog.sh"
WATCHDOG_ENTRY = PROJECT_ROOT / "scripts" / "watchdog_live.py"


def test_shell_wrapper_uses_only_the_canonical_python_watchdog_chain() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "scripts/watchdog_live.py" in source
    assert "tools/run_live.py" not in source
    assert "WATCHDOG_PID_FILE" in source
    assert "LIVE_PID_FILE" in source
    assert '"$WATCHDOG_SCRIPT"' in source
    assert "nohup" in source


def test_python_watchdog_entry_calls_app_core() -> None:
    source = WATCHDOG_ENTRY.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "src.app.watchdog"
        for alias in node.names
    }
    main = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "main"
    )

    assert "run_live_watchdog" in imports
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "run_live_watchdog"
        for node in ast.walk(main)
    )
