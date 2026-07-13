from __future__ import annotations

import ast
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STRATEGY_PORT = PROJECT_ROOT / "src" / "strategy" / "market_features.py"
RUNTIME_DISPATCHER = PROJECT_ROOT / "src" / "runtime" / "market_features.py"
TRADE_FEATURE_PIPELINE = PROJECT_ROOT / "src" / "runtime" / "feature_pipeline.py"
RUNNER = PROJECT_ROOT / "src" / "runtime" / "runner.py"
NEW_SOURCE_FILES = (
    STRATEGY_PORT,
    RUNTIME_DISPATCHER,
    TRADE_FEATURE_PIPELINE,
)


def _imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    ]
    modules.extend(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    return tuple(modules)


def test_strategy_observer_port_has_only_allowed_dependencies() -> None:
    imports = _imports(STRATEGY_PORT)

    assert not any(module.startswith("src.runtime") for module in imports)
    assert not any(module.startswith("src.order_management") for module in imports)
    assert not any(module.startswith("src.platform.exchanges.okx") for module in imports)
    assert not any(module.startswith("src.platform.exchanges.binance") for module in imports)
    assert not any(module.startswith("strategies") for module in imports)


def test_runtime_dispatcher_has_no_concrete_or_execution_dependencies() -> None:
    imports = _imports(RUNTIME_DISPATCHER)

    assert not any(module.startswith("strategies") for module in imports)
    assert not any(module.startswith("src.order_management") for module in imports)
    assert not any(module.startswith("src.reconcile") for module in imports)
    assert not any(module.startswith("src.platform.exchanges.okx") for module in imports)
    assert not any(module.startswith("src.platform.exchanges.binance") for module in imports)
    assert not any(
        module.startswith("src.platform.data.websocket.okx") for module in imports
    )
    assert not any(
        module.startswith("src.platform.data.websocket.binance") for module in imports
    )
    assert not any(
        module.startswith("src.platform.account.websocket.okx") for module in imports
    )
    assert not any(
        module.startswith("src.platform.account.websocket.binance")
        for module in imports
    )


def test_market_feature_pipeline_has_one_definition_and_no_runtime_side_effects() -> None:
    definitions: list[str] = []
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(
            isinstance(node, ast.ClassDef) and node.name == "MarketFeaturePipeline"
            for node in ast.walk(tree)
        ):
            definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/market_features.py"]

    dispatcher_tree = ast.parse(
        RUNTIME_DISPATCHER.read_text(encoding="utf-8"),
        filename=str(RUNTIME_DISPATCHER),
    )
    pipeline = next(
        node
        for node in ast.walk(dispatcher_tree)
        if isinstance(node, ast.ClassDef) and node.name == "MarketFeaturePipeline"
    )
    forbidden = {
        "_execute_signals",
        "coordinator",
        "sync",
        "sync_service",
        "persistence",
        "persist",
        "alerts",
        "execute",
    }
    used_boundaries = {
        node.id
        for node in ast.walk(pipeline)
        if isinstance(node, ast.Name) and node.id in forbidden
    }
    used_boundaries.update(
        node.attr
        for node in ast.walk(pipeline)
        if isinstance(node, ast.Attribute) and node.attr in forbidden
    )
    assert used_boundaries == set()


def test_runner_uses_only_market_feature_pipeline_boundary() -> None:
    tree = ast.parse(RUNNER.read_text(encoding="utf-8"), filename=str(RUNNER))
    dispatcher_imports = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "src.runtime.market_features"
        for alias in node.names
    ]

    assert dispatcher_imports == ["MarketFeaturePipeline"]
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "MarketFeaturePipeline"
        for node in ast.walk(tree)
    )
    assert not any(
        isinstance(node, ast.Name)
        and node.id
        in {"dispatch_market_feature_event", "resolve_market_feature_observers"}
        for node in ast.walk(tree)
    )
    assert not any(
        isinstance(node, ast.Attribute) and node.attr == "on_market_feature"
        for node in ast.walk(tree)
    )


def test_runner_owns_feature_bookkeeping_and_trade_pipeline_emitter() -> None:
    tree = ast.parse(RUNNER.read_text(encoding="utf-8"), filename=str(RUNNER))
    runner_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    assert any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "process_market_feature"
        for node in runner_class.body
    )

    trade_pipeline_calls = [
        node
        for node in ast.walk(runner_class)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "TradeDerivedFeaturePipeline"
    ]
    assert len(trade_pipeline_calls) == 1
    emit_keywords = [
        keyword.value
        for keyword in trade_pipeline_calls[0].keywords
        if keyword.arg == "emit_feature"
    ]
    assert len(emit_keywords) == 1
    assert isinstance(emit_keywords[0], ast.Attribute)
    assert isinstance(emit_keywords[0].value, ast.Name)
    assert emit_keywords[0].value.id == "self"
    assert emit_keywords[0].attr == "process_market_feature"


def test_trade_feature_pipeline_has_no_business_or_execution_dependencies() -> None:
    imports = _imports(TRADE_FEATURE_PIPELINE)

    assert not any(module.startswith("strategies") for module in imports)
    assert not any(module.startswith("src.order_management") for module in imports)
    assert not any(module.startswith("src.reconcile") for module in imports)
    assert not any(module.startswith("src.platform.exchanges.okx") for module in imports)
    assert not any(module.startswith("src.platform.exchanges.binance") for module in imports)

    tree = ast.parse(
        TRADE_FEATURE_PIPELINE.read_text(encoding="utf-8"),
        filename=str(TRADE_FEATURE_PIPELINE),
    )
    forbidden_calls = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr in {"on_market_feature", "_execute_signals", "execute"}
    }
    assert forbidden_calls == set()


def test_trade_feature_pipeline_has_one_static_builder_on_trade_boundary() -> None:
    tree = ast.parse(
        TRADE_FEATURE_PIPELINE.read_text(encoding="utf-8"),
        filename=str(TRADE_FEATURE_PIPELINE),
    )
    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_feed_trade"
    )
    direct_on_trade = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "on_trade"
    ]
    helper_on_trade = [
        node
        for node in ast.walk(helper)
        if isinstance(node, ast.Attribute) and node.attr == "on_trade"
    ]

    assert len(direct_on_trade) == 1
    assert helper_on_trade == direct_on_trade
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func is direct_on_trade[0]
        for node in ast.walk(helper)
    )
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        for node in ast.walk(helper)
    )
    assert not any(isinstance(node, ast.BinOp) for node in ast.walk(helper))

    forbidden_names = {"eval", "exec", "__getattribute__", "methodcaller"}
    bypasses = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    bypasses.update(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    assert bypasses == set()


def test_new_public_boundary_sources_have_no_strategy_specific_vocabulary() -> None:
    forbidden = re.compile(
        r"eth_portfolio_v1|eth_lf_portfolio_v8|eth_lf_portfolio_v10b|"
        r"low_sweep|\blf\b|\bmf\b|\bhf\b|iceberg",
        re.IGNORECASE,
    )

    for path in NEW_SOURCE_FILES:
        assert forbidden.search(path.read_text(encoding="utf-8")) is None


def test_runtime_has_only_one_direct_market_feature_dispatch_module() -> None:
    runtime_root = PROJECT_ROOT / "src" / "runtime"
    violations: list[str] = []

    for path in sorted(runtime_root.rglob("*.py")):
        if path == RUNTIME_DISPATCHER:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            direct_attribute = (
                isinstance(node, ast.Attribute)
                and node.attr == "on_market_feature"
            )
            dynamic_lookup = (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "on_market_feature"
            )
            if direct_attribute or dynamic_lookup:
                violations.append(str(path.relative_to(PROJECT_ROOT)))
                break

    assert violations == []
