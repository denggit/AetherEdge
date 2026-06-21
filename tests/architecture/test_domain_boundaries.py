from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
STRATEGIES = ROOT / "strategies"


def _python_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return [path for path in folder.rglob("*.py") if "__pycache__" not in path.parts]


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _violations(folder: Path, forbidden_prefixes: tuple[str, ...]) -> list[tuple[str, str]]:
    leaks: list[tuple[str, str]] = []
    for path in _python_files(folder):
        for module in _imported_modules(path):
            if module.startswith(forbidden_prefixes):
                leaks.append((str(path.relative_to(ROOT)), module))
    return leaks


def test_platform_does_not_depend_on_business_domains_or_strategies():
    leaks = _violations(
        SRC / "platform",
        (
            "src.market_data",
            "src.order_management",
            "src.runtime",
            "strategies",
        ),
    )
    assert leaks == []


def test_market_data_does_not_depend_on_strategy_or_order_management():
    leaks = _violations(
        SRC / "market_data",
        (
            "strategies",
            "src.order_management",
        ),
    )
    assert leaks == []


def test_order_management_does_not_depend_on_strategy_or_market_data():
    leaks = _violations(
        SRC / "order_management",
        (
            "strategies",
            "src.market_data",
        ),
    )
    assert leaks == []


def test_runtime_does_not_import_exchange_adapters_directly():
    leaks = _violations(
        SRC / "runtime",
        (
            "src.platform.exchanges.okx.client",
            "src.platform.exchanges.binance.client",
        ),
    )
    assert leaks == []


def test_strategies_do_not_import_exchange_adapters_directly():
    leaks = _violations(
        STRATEGIES,
        (
            "src.platform.exchanges.okx.client",
            "src.platform.exchanges.binance.client",
        ),
    )
    assert leaks == []
