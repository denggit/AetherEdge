from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = ROOT / "src" / "runtime"
STRATEGY_HOST = RUNTIME_ROOT / "strategy_host.py"

CALLBACK_ALLOWLIST = {
    "on_start": {"src/runtime/strategy_host.py"},
    "on_kline": {"src/runtime/strategy_host.py"},
    "on_ticker": {"src/runtime/strategy_host.py"},
    "on_trade": {"src/runtime/strategy_host.py"},
    "on_order_book": {"src/runtime/strategy_host.py"},
    "on_account_event": {"src/runtime/strategy_host.py"},
    "on_account_snapshot": {"src/runtime/strategy_host.py"},
    "on_order_results": {"src/runtime/strategy_host.py"},
    "on_market_feature": {"src/runtime/market_features.py"},
    "recover": {"src/runtime/recovery/service.py"},
}

FORBIDDEN_EXCHANGE_IMPORTS = (
    "src.platform.exchanges.okx",
    "src.platform.exchanges.binance",
    "src.platform.data.websocket.okx",
    "src.platform.data.websocket.binance",
    "src.platform.account.websocket.okx",
    "src.platform.account.websocket.binance",
)

# These are generic runtime collaborators whose method names happen to match
# controlled Strategy callback names. Keep each exception exact so a new
# reference still requires an architecture review.
NON_STRATEGY_CALLBACK_REFERENCES = {
    ("recover", "src/runtime/runner.py", 1830),
    ("on_trade", "src/runtime/runner.py", 3282),
    # Generic Trade Feature Builder callback, not a Strategy callback.
    ("on_trade", "src/runtime/feature_pipeline.py", 103),
}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted(RUNTIME_ROOT.rglob("*.py")))


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _callback_references(tree: ast.AST) -> tuple[tuple[str, int], ...]:
    callbacks = set(CALLBACK_ALLOWLIST)
    found: set[tuple[str, int]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in callbacks:
            if (
                isinstance(node.value, ast.Attribute)
                and node.value.attr == "_strategy_host"
            ):
                continue
            found.add((node.attr, node.lineno))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in callbacks
        ):
            found.add((str(node.args[1].value), node.lineno))
    return tuple(sorted(found, key=lambda item: (item[1], item[0])))


def _all_runtime_callback_references() -> set[tuple[str, str, int]]:
    references: set[tuple[str, str, int]] = set()
    for path in _runtime_files():
        relative_path = _relative(path)
        for callback, line in _callback_references(_tree(path)):
            references.add((callback, relative_path, line))
    return references


def _stale_non_strategy_callback_references(
    exceptions: set[tuple[str, str, int]],
    discovered: set[tuple[str, str, int]],
) -> set[tuple[str, str, int]]:
    return exceptions - discovered


def _imports(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _module_is_or_below(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(f"{prefix}.")


def test_direct_strategy_callback_locations_match_explicit_allowlist() -> None:
    actual = {name: set() for name in CALLBACK_ALLOWLIST}
    violations: list[str] = []
    for callback, relative_path, line in sorted(_all_runtime_callback_references()):
        reference = (callback, relative_path, line)
        if reference in NON_STRATEGY_CALLBACK_REFERENCES:
            continue
        actual[callback].add(relative_path)
        if relative_path not in CALLBACK_ALLOWLIST[callback]:
            violations.append(
                f"callback={callback} path={relative_path} line={line}"
            )

    assert violations == [], "Unexpected Strategy callback references:\n" + "\n".join(
        violations
    )
    assert actual == CALLBACK_ALLOWLIST


def test_non_strategy_callback_exceptions_are_live_and_exact() -> None:
    discovered = _all_runtime_callback_references()
    stale = _stale_non_strategy_callback_references(
        NON_STRATEGY_CALLBACK_REFERENCES,
        discovered,
    )

    assert stale == set(), (
        "Stale NON_STRATEGY_CALLBACK_REFERENCES entries:\n"
        + "\n".join(
            f"callback={callback} path={path} line={line}"
            for callback, path, line in sorted(stale)
        )
    )


def test_stale_exception_detection_checks_callback_path_and_line() -> None:
    live = ("on_trade", "src/runtime/feature_pipeline.py", 103)
    missing_line = ("on_trade", "src/runtime/feature_pipeline.py", 104)
    missing_path = ("on_trade", "src/runtime/missing.py", 103)
    discovered = _all_runtime_callback_references()

    assert live in discovered
    assert _stale_non_strategy_callback_references(
        {live, missing_line, missing_path},
        discovered,
    ) == {missing_line, missing_path}


def test_runtime_has_no_concrete_strategy_imports() -> None:
    violations = []
    for path in _runtime_files():
        for module in _imports(path):
            if _module_is_or_below(module, "strategies"):
                violations.append((_relative(path), module))

    assert violations == []


def test_runtime_has_no_concrete_exchange_client_imports() -> None:
    violations = []
    for path in _runtime_files():
        for module in _imports(path):
            if any(
                _module_is_or_below(module, prefix)
                for prefix in FORBIDDEN_EXCHANGE_IMPORTS
            ):
                violations.append((_relative(path), module))

    assert violations == []


def test_strategy_host_has_no_execution_or_business_implementation_imports() -> None:
    forbidden_prefixes = (
        "strategies",
        "src.reconcile",
        *FORBIDDEN_EXCHANGE_IMPORTS,
    )
    violations = []
    for module in _imports(STRATEGY_HOST):
        if any(
            _module_is_or_below(module, prefix)
            for prefix in forbidden_prefixes
        ):
            violations.append(module)
        if (
            _module_is_or_below(module, "src.order_management")
            and module != "src.order_management.models"
        ):
            violations.append(module)

    assert violations == []


def test_callback_scanner_recognizes_attribute_calls_references_and_getattr() -> None:
    tree = ast.parse(
        """
strategy.on_trade(event)
handler = strategy.on_order_results
callback = getattr(strategy, "recover", None)
ordinary = strategy.metadata
"""
    )

    assert _callback_references(tree) == (
        ("on_trade", 2),
        ("on_order_results", 3),
        ("recover", 4),
    )
