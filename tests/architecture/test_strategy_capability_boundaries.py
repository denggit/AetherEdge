from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = ROOT / "src" / "runtime"
RUNNER = RUNTIME_ROOT / "runner.py"
PORTS = ROOT / "src" / "strategy" / "ports.py"
CAPABILITIES = RUNTIME_ROOT / "strategy_capabilities.py"


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
