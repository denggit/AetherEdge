from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path

from src.runtime.services import RuntimeServiceBundle, RuntimeServices


ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "src" / "runtime"
PLATFORM = ROOT / "src" / "platform"
TRADE_FEATURES = ROOT / "src" / "market_data" / "trade_features"
TRADE_FEATURE_STORE = (
    ROOT / "src" / "market_data" / "storage" / "trade_feature_store.py"
)


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tree(path: Path) -> ast.Module:
    return ast.parse(_source(path), filename=str(path))


def _imports(path: Path) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def test_runtime_has_no_magic_owner_or_dynamic_component_scanner() -> None:
    runner = _source(RUNTIME / "runner.py")
    component_base = _source(RUNTIME / "components" / "base.py")
    formal_runtime = "\n".join(
        (runner, component_base)
    )
    for token in (
        "__getattribute__",
        "__getattr__",
        "__setattr__",
        "_runtime_component_override",
        "_COMPATIBILITY_COMPONENT_METHODS",
        "_compatibility_component_methods",
        "service_dependencies",
        "_owner",
    ):
        assert token not in formal_runtime
    assert "def _bind_component_ports(" in runner
    assert "RuntimeContext()" in runner


def test_runtime_services_are_grouped_and_mapping_compat_is_external() -> None:
    assert {item.name for item in fields(RuntimeServiceBundle)} == {
        "market",
        "execution",
        "account",
        "persistence",
        "recovery",
        "lifecycle",
        "range",
    }
    for method in ("get", "__getitem__", "__setitem__", "__contains__"):
        assert method not in RuntimeServices.__dict__
    compat = _source(RUNTIME / "compat" / "services.py")
    assert "class LegacyRuntimeServiceView" in compat
    assert "def __getitem__" in compat


def test_market_event_models_have_one_definition_and_no_conversion_layer() -> None:
    duplicate_definitions: list[str] = []
    for path in PLATFORM.rglob("*.py"):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in {
                "Kline",
                "Ticker",
                "Trade",
            }:
                duplicate_definitions.append(
                    f"{path.relative_to(ROOT).as_posix()}:{node.name}"
                )
    assert duplicate_definitions == []

    platform_source = "\n".join(
        _source(path) for path in PLATFORM.rglob("*.py")
    )
    for name in (
        "market_kline_from_exchange",
        "market_ticker_from_exchange",
        "market_trade_from_exchange",
    ):
        assert name not in platform_source

    models = _source(PLATFORM / "data" / "models.py")
    for name in (
        "MarketKline",
        "MarketTicker",
        "MarketTrade",
        "MarketOrderBook",
        "MarketOrderBookL2",
        "MarketFullOrderBook",
        "MarketOpenInterest",
        "OrderBookLevel",
    ):
        assert f"class {name}" in models


def test_market_data_ports_do_not_reflect_over_fetch_trade_signatures() -> None:
    data_source = "\n".join(
        _source(path) for path in (PLATFORM / "data").rglob("*.py")
    )
    assert "inspect.signature" not in data_source
    assert 'getattr(client, "fetch_trades"' not in data_source
    config_model = _source(PLATFORM / "exchanges" / "models.py")
    assert "Deprecated strategy-tool compatibility" in config_model
    assert "config_loader" not in _imports(
        PLATFORM / "exchanges" / "models.py"
    )
    formal_sources = "\n".join(
        _source(path)
        for root in (ROOT / "src", ROOT / "scripts", ROOT / "tools")
        for path in root.rglob("*.py")
        if path != PLATFORM / "exchanges" / "models.py"
    )
    assert "ExchangeConfig.from_env" not in formal_sources


def test_trade_feature_coverage_dependency_direction_is_explicit() -> None:
    repository = TRADE_FEATURES / "coverage_repository.py"
    service = TRADE_FEATURES / "coverage_service.py"
    calendar = TRADE_FEATURES / "okx_archive_calendar.py"

    assert "src.market_data.storage" not in _imports(repository)
    assert "coverage_service" not in "\n".join(_imports(repository))
    assert {
        "src.market_data.trade_features.coverage_repository",
        "src.market_data.trade_features.okx_archive_calendar",
    } <= _imports(service)
    calendar_source = _source(calendar)
    for forbidden in ("sqlite", "Store", "src.runtime"):
        assert forbidden not in calendar_source

    store_tree = _tree(TRADE_FEATURE_STORE)
    store_class = next(
        node
        for node in store_tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "SqliteTradeFeatureStore"
    )
    store_methods = {
        node.name
        for node in store_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert "coverage_scan" not in store_methods
    assert "range_footprint_coverage_summary" not in store_methods
    store_imports = _imports(TRADE_FEATURE_STORE)
    assert (
        "src.market_data.trade_features.coverage_service"
        not in store_imports
    )
    assert (
        "src.market_data.trade_features.okx_archive_calendar"
        not in store_imports
    )


def test_coverage_audit_uses_repository_port_not_sqlite_connection() -> None:
    coverage_source = _source(TRADE_FEATURES / "coverage.py")
    assert "._connect(" not in coverage_source
    assert "TradeFeatureCoverageService(" in coverage_source
