from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PLUGIN_ROOT = PROJECT_ROOT / "strategies" / "eth_portfolio_v1"
PLUGIN_PREFIX = "strategies.eth_portfolio_v1"


def test_eth_portfolio_v1_does_not_cross_strategy_plugin_boundaries() -> None:
    violations: list[str] = []

    for path in sorted(PLUGIN_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = (alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                modules = (node.module,)
            else:
                continue

            for module in modules:
                if (
                    module.startswith("strategies.")
                    and module != PLUGIN_PREFIX
                    and not module.startswith(f"{PLUGIN_PREFIX}.")
                ):
                    relative_path = path.relative_to(PROJECT_ROOT)
                    violations.append(f"{relative_path}:{node.lineno} imports {module}")

    assert not violations, "cross-plugin strategy imports found:\n" + "\n".join(violations)
