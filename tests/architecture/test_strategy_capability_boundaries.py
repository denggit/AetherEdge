from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = ROOT / "src" / "runtime"
RUNNER = RUNTIME_ROOT / "runner.py"
PORTS = ROOT / "src" / "strategy" / "ports.py"
CAPABILITIES = RUNTIME_ROOT / "strategy_capabilities.py"
REQUIREMENTS = RUNTIME_ROOT / "requirements.py"


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_removed_conflicting_strategy_protocols_have_no_references() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for root in (ROOT / "src", ROOT / "strategies")
        for path in root.rglob("*.py")
    )

    assert "StrategyRuntimeStateProvider" not in source
    assert "MarketFeatureStrategyPort" not in source


def test_runtime_state_capabilities_are_three_independent_protocols() -> None:
    class_names = {
        node.name
        for node in _tree(PORTS).body
        if isinstance(node, ast.ClassDef)
    }

    assert {
        "StrategyIdentityProvider",
        "StrategyPendingWorkProvider",
        "StrategyStartupPreviewProvider",
    } <= class_names


def test_capability_validation_is_first_startup_operation() -> None:
    runner_class = next(
        node
        for node in _tree(RUNNER).body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    startup = next(
        node
        for node in runner_class.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_startup"
    )
    first = startup.body[0]

    assert isinstance(first, ast.Expr)
    assert isinstance(first.value, ast.Call)
    assert isinstance(first.value.func, ast.Attribute)
    assert first.value.func.attr == "_strategy_capabilities"


def test_capability_validator_has_no_concrete_strategy_dependency() -> None:
    imports: set[str] = set()
    for node in ast.walk(_tree(CAPABILITIES)):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)

    assert not any(
        module == "strategies" or module.startswith("strategies.")
        for module in imports
    )


def test_capability_manifest_parser_never_uses_permissive_bool_parser() -> None:
    parser = next(
        node
        for node in _tree(REQUIREMENTS).body
        if isinstance(node, ast.FunctionDef) and node.name == "_capabilities"
    )

    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_bool"
        for node in ast.walk(parser)
    )


def test_all_formal_strategy_manifests_have_exact_version_one_schema() -> None:
    manifest_fields = {
        "manifest_version",
        "strategy_id",
        "position_snapshots",
        "recovery_status",
        "market_features",
        "range_speed_history",
        "startup_preview",
        "pending_work",
    }
    config_paths = (
        ROOT / "strategies" / "eth_lf_portfolio_v8" / "config.json",
        ROOT / "strategies" / "eth_lf_portfolio_v10b" / "config.json",
        ROOT / "strategies" / "eth_portfolio_v1" / "config.json",
    )

    for path in config_paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        manifest = data["runtime_requirements"]["capabilities"]
        assert set(manifest) == manifest_fields
        assert manifest["manifest_version"] == 1

    empty_source = (
        ROOT / "strategies" / "empty_strategy.py"
    ).read_text(encoding="utf-8")
    assert '"manifest_version": 1' in empty_source
