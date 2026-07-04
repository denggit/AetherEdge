from __future__ import annotations

import ast
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_ROOT = PROJECT_ROOT / "strategies" / "eth_portfolio_v1"
PLUGIN_PREFIX = "strategies.eth_portfolio_v1"
FORBIDDEN_PLUGIN_IMPORTS = (
    "strategies.eth_lf_portfolio_v8",
    "strategies.eth_lf_portfolio_v10b",
)


def _python_files() -> tuple[Path, ...]:
    return tuple(sorted(PLUGIN_ROOT.rglob("*.py")))


def test_v1_python_sources_do_not_reference_v8_or_v10b() -> None:
    for path in _python_files():
        source = path.read_text(encoding="utf-8")
        for forbidden in FORBIDDEN_PLUGIN_IMPORTS:
            assert forbidden not in source, f"{path.relative_to(PROJECT_ROOT)} references {forbidden}"


def test_v1_strategy_imports_are_self_contained() -> None:
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported_modules = [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        ]
        imported_modules.extend(
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        )

        cross_plugin_imports = [
            module
            for module in imported_modules
            if module.startswith("strategies.")
            and module != PLUGIN_PREFIX
            and not module.startswith(f"{PLUGIN_PREFIX}.")
        ]
        assert not cross_plugin_imports, (
            f"{path.relative_to(PROJECT_ROOT)} imports other strategy plugins: "
            f"{cross_plugin_imports}"
        )


def test_v1_config_has_independent_identity() -> None:
    config = json.loads((PLUGIN_ROOT / "config.json").read_text(encoding="utf-8"))

    assert config["strategy_id"] == "eth_portfolio_v1"
    assert config["strategy_version"] == "V1"
    assert config["display_name"] == "ETH Portfolio V1"
