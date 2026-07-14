from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LEGACY_FILES = (
    tuple((PROJECT_ROOT / "strategies" / "eth_portfolio_v1").rglob("*.py"))
    + (
        PROJECT_ROOT / "src" / "runtime" / "runner.py",
        PROJECT_ROOT / "src" / "runtime" / "strategy_host.py",
        PROJECT_ROOT / "src" / "runtime" / "signal_execution_service.py",
        PROJECT_ROOT / "src" / "runtime" / "orders.py",
    )
    + tuple((PROJECT_ROOT / "src" / "planner").rglob("*.py"))
    + tuple((PROJECT_ROOT / "src" / "order_management").rglob("*.py"))
)
TARGET_NAMES = {"StrategyDecision", "VirtualSleeveTarget", "StrategyTargetPosition"}
TARGET_IMPORT_PREFIXES = ("src.strategy.targets", "strategy.targets")
TARGET_FORBIDDEN_IMPORT_PREFIXES = (
    "src.runtime",
    "src.planner",
    "src.order_management",
    "src.platform.execution",
    "src.platform.exchanges.okx",
    "src.platform.exchanges.binance",
)
TARGET_FORBIDDEN_EXECUTION_NAMES = {
    "LiveOrderIntentFactory",
    "RuntimeSignalExecutionService",
    "MultiExchangeOrderCoordinator",
    "OrderIntent",
    "place_order",
    "place_market_order",
    "place_stop",
    "cancel_order",
}


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imports(tree: ast.AST) -> set[str]:
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def _referenced_names(tree: ast.AST) -> set[str]:
    return {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
    } | {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
    }


def _starts_with_any(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == prefix or module.startswith(prefix + ".") for prefix in prefixes)


def test_legacy_portfolio_runtime_planner_and_order_management_are_target_free() -> None:
    assert LEGACY_FILES
    missing = [path for path in LEGACY_FILES if not path.is_file()]
    assert missing == []

    violations: list[str] = []
    for path in sorted(set(LEGACY_FILES)):
        tree = _tree(path)
        for module in sorted(_imports(tree)):
            if _starts_with_any(module, TARGET_IMPORT_PREFIXES):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} imports {module}")
        for name in sorted(TARGET_NAMES.intersection(_referenced_names(tree))):
            violations.append(f"{path.relative_to(PROJECT_ROOT)} references {name}")

    assert violations == []


def test_target_core_remains_execution_agnostic_when_package_is_introduced() -> None:
    target_root = PROJECT_ROOT / "src" / "strategy" / "targets"
    target_files = tuple(target_root.rglob("*.py")) if target_root.is_dir() else ()
    violations: list[str] = []

    for path in target_files:
        tree = _tree(path)
        for module in sorted(_imports(tree)):
            if _starts_with_any(module, TARGET_FORBIDDEN_IMPORT_PREFIXES):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} imports {module}")
        for name in sorted(TARGET_FORBIDDEN_EXECUTION_NAMES.intersection(_referenced_names(tree))):
            violations.append(f"{path.relative_to(PROJECT_ROOT)} references {name}")

    # This assertion is meaningful before the package exists because the legacy
    # isolation test above always scans every currently live execution module.
    assert violations == []

