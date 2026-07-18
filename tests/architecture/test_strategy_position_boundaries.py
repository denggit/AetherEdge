from __future__ import annotations

import ast
from pathlib import Path

from tests.runtime_surface_ast import runtime_surface_class


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = PROJECT_ROOT / "src" / "runtime"
STRATEGY_POSITIONS = PROJECT_ROOT / "src" / "strategy" / "positions.py"
RUNTIME_RESOLVER = PROJECT_ROOT / "src" / "runtime" / "strategy_positions.py"
NEW_PRODUCTION_FILES = (STRATEGY_POSITIONS, RUNTIME_RESOLVER)
RAW_ADAPTER_PREFIXES = (
    "src.platform.exchanges.okx",
    "src.platform.exchanges.binance",
)


def test_strategy_position_model_has_no_forbidden_layer_dependencies() -> None:
    imports = _imports(STRATEGY_POSITIONS)
    forbidden_prefixes = (
        "strategies",
        "src.runtime",
        "src.order_management",
        *RAW_ADAPTER_PREFIXES,
    )

    assert not _has_import_prefix(imports, forbidden_prefixes)


def test_runtime_position_resolver_has_no_concrete_strategy_or_raw_adapter_dependency() -> None:
    imports = _imports(RUNTIME_RESOLVER)

    assert not _has_import_prefix(
        imports,
        ("strategies", "src.order_management", *RAW_ADAPTER_PREFIXES),
    )


def test_runtime_never_reads_strategy_position_state() -> None:
    violations: list[str] = []
    for path in RUNTIME_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                continue
            if node.func.id != "getattr" or len(node.args) < 2:
                continue
            attribute_name = node.args[1]
            if isinstance(attribute_name, ast.Constant) and attribute_name.value == "position":
                violations.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}")

    assert violations == []


def test_runtime_position_resolver_uses_public_provider_only() -> None:
    source = RUNTIME_RESOLVER.read_text(encoding="utf-8")
    tree = ast.parse(source)
    private_reads = [
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "strategy"
        and node.attr in {"config", "position"}
    ]

    assert private_reads == []
    assert "legacy_" not in source


def test_runtime_has_no_loaded_first_active_strategy_helpers() -> None:
    violations: list[str] = []
    for path in RUNTIME_ROOT.rglob("*.py"):
        if path == RUNTIME_RESOLVER:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name in {"_first_active_position", "_first_active_plan"}:
                    violations.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}")
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id in {"_first_active_position", "_first_active_plan"}:
                    violations.append(f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}")

    assert violations == []


def test_generic_stop_protection_paths_do_not_use_single_active_helpers() -> None:
    tree = runtime_surface_class(PROJECT_ROOT / "src")
    generic_paths = {
        "_validate_recovery_protection_postcondition",
        "_validate_post_execution_stop_protection",
        "_verify_stop_order_results",
    }
    violations: list[str] = []
    matching_helper_calls: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name not in generic_paths:
            continue
        loaded_names = {
            child.id
            for child in ast.walk(node)
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load)
        }
        loaded_names.update(
            child.attr
            for child in ast.walk(node)
            if isinstance(child, ast.Attribute) and isinstance(child.ctx, ast.Load)
        )
        if {
            "single_active_or_none_for_legacy",
            "_single_active_exchange_position_or_none_for_legacy",
        } & loaded_names:
            violations.append(node.name)
        if "_exchange_positions_matching_strategy_position" in loaded_names:
            matching_helper_calls.add(node.name)

    assert violations == []
    assert matching_helper_calls == generic_paths


def test_position_foundation_has_no_exchange_endpoint_fragments() -> None:
    source = _combined_source().lower()
    forbidden_fragments = (
        "/api/" + "v5",
        "fa" + "pi",
        "da" + "pi",
        "api/" + "v3",
    )

    for fragment in forbidden_fragments:
        assert fragment not in source


def test_position_foundation_has_no_concrete_plugin_names() -> None:
    source = _combined_source().lower()
    forbidden_names = (
        "eth_" + "portfolio_v1",
        "eth_lf_" + "portfolio_v8",
        "eth_lf_" + "portfolio_v10b",
    )

    for name in forbidden_names:
        assert name not in source


def _combined_source() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in NEW_PRODUCTION_FILES)


def _has_import_prefix(imports: set[str], prefixes: tuple[str, ...]) -> bool:
    return any(
        imported == prefix or imported.startswith(f"{prefix}.")
        for imported in imports
        for prefix in prefixes
    )


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports
