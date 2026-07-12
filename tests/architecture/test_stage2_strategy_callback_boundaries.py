from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_ROOT = ROOT / "src" / "runtime"

CALLBACK_ALLOWLIST = {
    "on_start": {"src/runtime/runner.py"},
    "on_kline": {"src/runtime/runner.py"},
    "on_ticker": {"src/runtime/runner.py"},
    "on_trade": {"src/runtime/runner.py"},
    "on_order_book": {"src/runtime/runner.py"},
    "on_account_event": {"src/runtime/runner.py"},
    "on_account_snapshot": {"src/runtime/runner.py"},
    "on_order_results": {"src/runtime/runner.py"},
    "on_market_feature": {"src/runtime/market_features.py"},
    "recover": {"src/runtime/recovery/service.py"},
}

FORBIDDEN_STRATEGY_IMPORTS = (
    "strategies.eth_portfolio_v1",
    "strategies.eth_lf_portfolio_v8",
    "strategies.eth_lf_portfolio_v10b",
)
FORBIDDEN_EXCHANGE_IMPORTS = (
    "src.platform.exchanges.okx",
    "src.platform.exchanges.binance",
)


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted(RUNTIME_ROOT.rglob("*.py")))


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _callback_references(path: Path) -> set[str]:
    callbacks = set(CALLBACK_ALLOWLIST)
    found: set[str] = set()
    for node in ast.walk(_tree(path)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in callbacks
        ):
            found.add(str(node.args[1].value))
    return found


def _imports(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_direct_strategy_callback_locations_match_explicit_allowlist() -> None:
    actual = {name: set() for name in CALLBACK_ALLOWLIST}
    for path in _runtime_files():
        for callback in _callback_references(path):
            actual[callback].add(_relative(path))

    assert actual == CALLBACK_ALLOWLIST


def test_runtime_has_no_concrete_strategy_imports() -> None:
    violations = []
    for path in _runtime_files():
        for module in _imports(path):
            if module.startswith(FORBIDDEN_STRATEGY_IMPORTS):
                violations.append((_relative(path), module))

    assert violations == []


def test_runtime_has_no_concrete_exchange_client_imports() -> None:
    violations = []
    for path in _runtime_files():
        for module in _imports(path):
            if module.startswith(FORBIDDEN_EXCHANGE_IMPORTS):
                violations.append((_relative(path), module))

    assert violations == []
