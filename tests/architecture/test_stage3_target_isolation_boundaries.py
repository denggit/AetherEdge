from __future__ import annotations

import ast
import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TARGET_ROOT = PROJECT_ROOT / "src" / "strategy" / "targets"
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
    + tuple((PROJECT_ROOT / "src" / "signals").rglob("*.py"))
    + tuple(
        path
        for path in (PROJECT_ROOT / "src" / "strategy").rglob("*.py")
        if TARGET_ROOT not in path.parents
    )
)
TARGET_NAMES = {"StrategyDecision", "VirtualSleeveTarget", "StrategyTargetPosition"}
TARGET_IMPORT_PREFIXES = ("src.strategy.targets", "strategy.targets")
TARGET_FORBIDDEN_IMPORT_PREFIXES = (
    "src.runtime",
    "src.planner",
    "src.order_management",
    "src.reconcile",
    "src.platform",
    "strategies.eth_portfolio_v1",
)
TARGET_FORBIDDEN_EXECUTION_NAMES = {
    "OrderCoordinator",
    "MultiExchangeOrderCoordinator",
    "LiveOrderIntentFactory",
    "RuntimeSignalExecutionService",
    "OrderIntent",
    "OrderJournal",
    "PositionPlan",
    "PositionPlanStore",
    "SqlitePositionPlanStore",
    "create_exchange_client",
    "create_execution_client",
    "place_order",
    "place_market_order",
    "place_stop",
    "place_stop_market_order",
    "place_stop_loss_for_position",
    "cancel_order",
    "cancel_algo_order",
    "_execute_signals",
    "OKX",
    "Binance",
}
TARGET_FORBIDDEN_ENDPOINT_FRAGMENTS = ("/api/v5", "fapi", "dapi", "api/v3")


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(PROJECT_ROOT).with_suffix("").parts)


def _imports(tree: ast.AST, module_name: str) -> set[str]:
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                relative_name = "." * node.level + (node.module or "")
                package = module_name.rpartition(".")[0]
                base_module = importlib.util.resolve_name(relative_name, package)
            else:
                base_module = node.module or ""
            if base_module:
                imports.add(base_module)
                imports.update(
                    f"{base_module}.{alias.name}"
                    for alias in node.names
                    if alias.name != "*"
                )
        elif isinstance(node, ast.Call) and node.args:
            first_arg = node.args[0]
            if not isinstance(first_arg, ast.Constant) or not isinstance(first_arg.value, str):
                continue
            is_import_module = (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "importlib"
                and node.func.attr == "import_module"
            )
            is_dunder_import = (
                isinstance(node.func, ast.Name) and node.func.id == "__import__"
            )
            if is_import_module or is_dunder_import:
                imports.add(first_arg.value)
    return imports


def _referenced_names(tree: ast.AST) -> set[str]:
    names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
    } | {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
    }
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(alias.name for alias in node.names)
    return names


def _starts_with_any(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == prefix or module.startswith(prefix + ".") for prefix in prefixes)


def _target_violations(tree: ast.AST, label: str, module_name: str) -> list[str]:
    violations: list[str] = []
    for module in sorted(_imports(tree, module_name)):
        if _starts_with_any(module, TARGET_FORBIDDEN_IMPORT_PREFIXES):
            violations.append(f"{label} imports {module}")
    for name in sorted(TARGET_FORBIDDEN_EXECUTION_NAMES.intersection(_referenced_names(tree))):
        violations.append(f"{label} references {name}")
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        for fragment in TARGET_FORBIDDEN_ENDPOINT_FRAGMENTS:
            if fragment in node.value:
                violations.append(f"{label} contains endpoint fragment {fragment}")
    return violations


def test_legacy_portfolio_runtime_planner_and_order_management_are_target_free() -> None:
    assert LEGACY_FILES
    missing = [path for path in LEGACY_FILES if not path.is_file()]
    assert missing == []

    violations: list[str] = []
    for path in sorted(set(LEGACY_FILES)):
        tree = _tree(path)
        for module in sorted(_imports(tree, _module_name(path))):
            if _starts_with_any(module, TARGET_IMPORT_PREFIXES):
                violations.append(f"{path.relative_to(PROJECT_ROOT)} imports {module}")
        for name in sorted(TARGET_NAMES.intersection(_referenced_names(tree))):
            violations.append(f"{path.relative_to(PROJECT_ROOT)} references {name}")

    assert violations == []


def test_target_core_remains_execution_agnostic_when_package_is_introduced() -> None:
    target_files = tuple(TARGET_ROOT.rglob("*.py")) if TARGET_ROOT.is_dir() else ()
    violations: list[str] = []

    for path in target_files:
        violations.extend(
            _target_violations(
                _tree(path),
                str(path.relative_to(PROJECT_ROOT)),
                _module_name(path),
            )
        )

    # This assertion is meaningful before the package exists because the legacy
    # isolation test above always scans every currently live execution module.
    assert violations == []


def test_target_violation_rule_rejects_facades_execution_store_and_endpoint() -> None:
    forbidden_sources = (
        ("from src.platform import ExchangeName", "src.platform"),
        ("from src import platform", "src.platform"),
        ("from ...platform import ExchangeName", "src.platform"),
        ("from ...reconcile import ReconcileReport", "src.reconcile"),
        (
            "from ...order_management.position_plan import store",
            "src.order_management",
        ),
        ('import importlib\nimportlib.import_module("src.platform")', "src.platform"),
        ('__import__("src.order_management")', "src.order_management"),
        ('endpoint = "/api/v5/trade/order"', "/api/v5"),
    )

    for index, (source, expected_module) in enumerate(forbidden_sources):
        tree = ast.parse(source, filename=f"synthetic-{index}.py")
        violations = _target_violations(
            tree,
            f"synthetic-{index}.py",
            "src.strategy.targets.synthetic",
        )
        assert any(expected_module in violation for violation in violations), (
            source,
            violations,
        )
