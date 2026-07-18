from __future__ import annotations

import ast
from pathlib import Path

from tests.runtime_surface_ast import runtime_surface_class

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
RUNTIME_ROOT = SOURCE_ROOT / "runtime"
RUNNER = RUNTIME_ROOT / "runner.py"
RUNTIME_CONFIG = RUNTIME_ROOT / "config.py"

GENERIC_MODULES = (
    RUNTIME_ROOT / "strategy_host.py",
    RUNTIME_ROOT / "feature_pipeline.py",
    RUNTIME_ROOT / "market_features.py",
    RUNTIME_ROOT / "persistence_service.py",
    RUNTIME_ROOT / "health_state.py",
    RUNTIME_ROOT / "heartbeat.py",
    RUNTIME_ROOT / "sync_lifecycle.py",
    RUNTIME_ROOT / "sync_services.py",
    RUNTIME_ROOT / "shutdown_coordinator.py",
    RUNTIME_ROOT / "startup_phase_coordinator.py",
    RUNTIME_ROOT / "recovery_coordinator.py",
    RUNTIME_ROOT / "reconciliation_coordinator.py",
    RUNTIME_ROOT / "signal_execution_service.py",
)

SERVICE_FIELDS = {
    "strategy_host": "_strategy_host",
    "market_feature_pipeline": "_market_feature_pipeline",
    "sync_lifecycle": "_sync_lifecycle",
    "sync_service_registry": "_sync_service_registry",
    "signal_execution_service": "_signal_execution_service",
    "recovery_coordinator": "_recovery_coordinator",
    "reconciliation_coordinator": "_reconciliation_coordinator",
    "runtime_persistence_service": "_runtime_persistence_service",
    "trade_derived_feature_pipeline": "_trade_derived_feature_pipeline",
    "market_data_persistence": "_market_data_persistence",
    "runtime_health_state": "_runtime_health_state",
    "heartbeat_service": "_heartbeat_service",
    "shutdown_coordinator": "_shutdown_coordinator",
    "startup_phase_coordinator": "_startup_phase_coordinator",
}


def test_generic_runtime_config_contains_no_market_module_configuration() -> None:
    config_class = _class(RUNTIME_CONFIG, "LiveRuntimeConfig")
    fields = {
        node.target.id
        for node in config_class.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
    }
    assert not {name for name in fields if name.startswith("range_")}
    assert "market_data_db_path" not in fields
    assert "degraded_fast_margin" not in fields

UNIQUE_CLASSES = {
    "StrategyHost": "src/runtime/strategy_host.py",
    "TradeDerivedFeaturePipeline": "src/runtime/feature_pipeline.py",
    "MarketFeaturePipeline": "src/runtime/market_features.py",
    "RuntimePersistenceService": "src/runtime/persistence_service.py",
    "RuntimeMarketDataPersistence": "src/runtime/market_data_persistence.py",
    "RuntimeHealthState": "src/runtime/health_state.py",
    "RuntimeHeartbeatService": "src/runtime/heartbeat.py",
    "RuntimeSyncLifecycle": "src/runtime/sync_lifecycle.py",
    "RuntimeSyncServiceRegistry": "src/runtime/sync_services.py",
    "RuntimeShutdownCoordinator": "src/runtime/shutdown_coordinator.py",
    "RuntimeStartupPhaseCoordinator": (
        "src/runtime/startup_phase_coordinator.py"
    ),
    "RuntimeRecoveryCoordinator": "src/runtime/recovery_coordinator.py",
    "RuntimeReconciliationCoordinator": (
        "src/runtime/reconciliation_coordinator.py"
    ),
    "RuntimeSignalExecutionService": (
        "src/runtime/signal_execution_service.py"
    ),
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _class(path: Path, name: str) -> ast.ClassDef:
    if path == RUNNER and name == "LiveRuntimeRunner":
        return runtime_surface_class(SOURCE_ROOT)
    return next(
        node
        for node in _tree(path).body
        if isinstance(node, ast.ClassDef) and node.name == name
    )


def _methods(class_node: ast.ClassDef) -> dict[str, ast.AST]:
    return {
        node.name: node
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _imports(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _calls(node: ast.AST, attribute: str) -> list[ast.Call]:
    return [
        child
        for child in ast.walk(node)
        if isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and child.func.attr == attribute
    ]


def _is_none_return(statement: ast.stmt, name: str) -> bool:
    return (
        isinstance(statement, ast.If)
        and isinstance(statement.test, ast.Compare)
        and isinstance(statement.test.left, ast.Name)
        and statement.test.left.id == name
        and len(statement.test.ops) == 1
        and isinstance(statement.test.ops[0], ast.Is)
        and len(statement.test.comparators) == 1
        and isinstance(statement.test.comparators[0], ast.Constant)
        and statement.test.comparators[0].value is None
        and len(statement.body) == 1
        and isinstance(statement.body[0], ast.Return)
    )


def _assert_acyclic(graph: dict[str, set[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str, path: tuple[str, ...]) -> None:
        if node in visiting:
            cycle_start = path.index(node)
            cycle = path[cycle_start:] + (node,)
            raise AssertionError(
                "runtime generic dependency cycle: "
                + " -> ".join(cycle)
            )
        if node in visited:
            return

        visiting.add(node)
        for dependency in sorted(graph[node]):
            visit(dependency, path + (node,))
        visiting.remove(node)
        visited.add(node)

    for node in sorted(graph):
        visit(node, ())


def test_final_runner_delegate_methods_remain_thin_and_single_call() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))

    startup = methods["_startup"]
    assert len(startup.body) == 4
    assert ast.unparse(startup.body[0]) == "self._strategy_capabilities()"
    assert len(
        [
            call
            for call in _calls(startup, "execute")
            if ast.unparse(call.func.value) == "self._startup_phase_coordinator"
        ]
    ) == 1
    assert sum(
        isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeStartupPhasePlan"
        for node in ast.walk(startup)
    ) == 1

    recovery = methods["_run_recovery"]
    reconciliation = methods["_run_reconciliation"]
    signals = methods["_execute_signals"]
    expected = (
        (recovery, "self._recovery_coordinator", "RuntimeRecoveryPlan"),
        (
            reconciliation,
            "self._reconciliation_coordinator",
            "RuntimeReconciliationPlan",
        ),
        (
            signals,
            "self._signal_execution_service",
            "RuntimeSignalExecutionPlan",
        ),
    )
    for method, owner, plan_name in expected:
        assert len(method.body) == 1
        delegate_calls = [
            call
            for call in _calls(method, "execute")
            if ast.unparse(call.func.value) == owner
        ]
        assert len(delegate_calls) == 1
        assert sum(
            isinstance(node, ast.Call)
            and ast.unparse(node.func) == plan_name
            for node in ast.walk(method)
        ) == 1
        assert not any(
            isinstance(node, (ast.For, ast.AsyncFor, ast.Try, ast.TryStar))
            for node in ast.walk(method)
        )

    signal_names = {
        node.id
        for node in ast.walk(signals)
        if isinstance(node, ast.Name)
    }
    signal_attrs = {
        node.attr
        for node in ast.walk(signals)
        if isinstance(node, ast.Attribute)
    }
    assert "dry_run" not in signal_attrs
    assert "_has_account_config_entry_block" not in signal_attrs
    assert "_has_unresolved_follower_close" not in signal_attrs
    assert "SignalAction" not in signal_names
    assert "_intent_factory" not in signal_attrs
    assert "_get_order_coordinator" not in signal_attrs


def test_shutdown_health_and_sync_delegation_stays_frozen() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    final = methods["_run_finally_shutdown"]
    explicit = methods["_explicit_stop_shutdown"]
    final_execute = _calls(final, "execute")
    explicit_execute = _calls(explicit, "execute")
    assert len(final_execute) == 1
    assert len(explicit_execute) == 1
    final_steps = final_execute[0].args[0]
    explicit_steps = explicit_execute[0].args[0]
    assert isinstance(final_steps, ast.Tuple)
    assert isinstance(explicit_steps, ast.Tuple)
    assert [ast.unparse(item) for item in final_steps.elts] == [
        "self._stop_market_data_modules",
        "self._stop_sync_tasks",
        "self._stop_producers",
        "self._stop_live_persistence_writer",
        "self.context.alerts.stop",
    ]
    assert [ast.unparse(item) for item in explicit_steps.elts] == [
        "self._stop_market_data_modules",
        "self._stop_producers",
        "self._stop_live_persistence_writer",
    ]

    set_health = methods["_set_health"]
    updates = [
        call
        for call in _calls(set_health, "update")
        if ast.unparse(call.func.value) == "self._runtime_health_state"
    ]
    assert len(updates) == 1
    assert not any(
        isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeHealth"
        for node in ast.walk(set_health)
    )

    start_sync = methods["_start_sync_tasks"]
    stop_sync = methods["_stop_sync_tasks"]
    assert len(_calls(start_sync, "start")) == 1
    assert len(_calls(stop_sync, "stop")) == 1
    for method in (start_sync, stop_sync):
        attrs = {
            node.attr
            for node in ast.walk(method)
            if isinstance(node, ast.Attribute)
        }
        assert {"create_task", "cancel", "gather"}.isdisjoint(attrs)


def test_final_service_injection_keys_and_fields_are_frozen() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    dependency_fields = {
        node.attr
        for node in ast.walk(initializer)
        if isinstance(node, ast.Attribute)
        and ast.unparse(node.value) == "self.runtime_services"
    }
    assigned_fields = {
        ast.unparse(target)
        for node in ast.walk(initializer)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Attribute)
    }
    assert set(SERVICE_FIELDS) <= dependency_fields
    assert {
        f"self.{field}" for field in SERVICE_FIELDS.values()
    } <= assigned_fields

    execute_receivers = {
        ast.unparse(call.func.value)
        for call in ast.walk(initializer)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr
        in {
            "execute",
            "start",
            "stop",
            "recover",
            "reconcile_and_apply",
            "sync_once",
            "run_periodic",
            "read_previous",
        }
    }
    assert execute_receivers == set()


def test_generic_runtime_dependency_direction_is_acyclic_and_adapter_free() -> None:
    module_names = {
        path: f"src.runtime.{path.stem}" for path in GENERIC_MODULES
    }
    generic_module_names = set(module_names.values())
    coordinator_modules = {
        "src.runtime.shutdown_coordinator",
        "src.runtime.startup_phase_coordinator",
        "src.runtime.recovery_coordinator",
        "src.runtime.reconciliation_coordinator",
        "src.runtime.signal_execution_service",
    }
    for path in GENERIC_MODULES:
        imports = _imports(path)
        assert "src.runtime.runner" not in imports
        assert not any(module.startswith("strategies") for module in imports)
        assert not any(
            module.startswith(
                (
                    "src.platform.exchanges.okx",
                    "src.platform.exchanges.binance",
                    "src.platform.data.websocket.okx",
                    "src.platform.data.websocket.binance",
                    "src.platform.account.websocket.okx",
                    "src.platform.account.websocket.binance",
                )
            )
            for module in imports
        )
        own = module_names[path]
        assert imports.isdisjoint(coordinator_modules)
        assert own not in imports

    graph = {
        module_names[path]: _imports(path) & generic_module_names
        for path in GENERIC_MODULES
    }
    _assert_acyclic(graph)


def test_generic_dependency_cycle_detector_rejects_cycles() -> None:
    _assert_acyclic(
        {
            "a": {"b"},
            "b": {"c"},
            "c": set(),
        }
    )

    cyclic_graphs = (
        ({"a": {"b"}, "b": {"a"}}, "a -> b -> a"),
        (
            {"a": {"b"}, "b": {"c"}, "c": {"a"}},
            "a -> b -> c -> a",
        ),
        ({"a": {"a"}}, "a -> a"),
    )
    for graph, cycle in cyclic_graphs:
        with pytest.raises(
            AssertionError,
            match=f"runtime generic dependency cycle: {cycle}",
        ):
            _assert_acyclic(graph)


def test_strategy_callback_receivers_remain_explicit_and_not_strategy_direct() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    controlled = {
        "on_start",
        "on_kline",
        "on_ticker",
        "on_trade",
        "on_order_book",
        "on_account_event",
        "on_account_snapshot",
        "on_order_results",
        "on_market_feature",
        "recover",
    }
    references = {
        (method.name, node.attr, ast.unparse(node.value))
        for method in methods.values()
        for node in ast.walk(method)
        if isinstance(node, ast.Attribute) and node.attr in controlled
    }
    assert references == {
        ("_invoke_recovery_service", "recover", "service"),
        ("_call_on_start", "on_start", "self._strategy_host"),
        (
            "_process_account_event",
            "on_account_event",
            "self._strategy_host",
        ),
        (
            "_on_account_snapshot_synced",
            "on_account_snapshot",
            "self._strategy_host",
        ),
        (
            "_process_order_result_feedback",
            "on_order_results",
            "self._strategy_host",
        ),
    }
    assert not any(
        receiver.startswith("self.context.strategy")
        for _, _, receiver in references
    )


def test_optional_strategy_callback_early_returns_precede_side_effects() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))

    on_start = methods["_call_on_start"]
    assert _is_none_return(on_start.body[1], "signals")
    assert isinstance(on_start.body[0], ast.Assign)
    assert ast.unparse(on_start.body[0].value).startswith(
        "await self._strategy_host.on_start("
    )
    assert isinstance(on_start.body[2], ast.Assign)
    assert ast.unparse(on_start.body[2].targets[0]) == (
        "self.stats.on_start_called"
    )
    assert _calls(on_start.body[3], "info")
    assert _calls(on_start.body[4], "_execute_signals")

    account_event = methods["_process_account_event"]
    signals_index = next(
        index
        for index, statement in enumerate(account_event.body)
        if isinstance(statement, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "signals"
            for target in statement.targets
        )
    )
    assert signals_index > 0
    assert "save_account_event" in ast.unparse(
        ast.Module(body=account_event.body[:signals_index], type_ignores=[])
    )
    assert _is_none_return(account_event.body[signals_index + 1], "signals")
    assert _calls(
        account_event.body[signals_index + 2],
        "_execute_signals",
    )

    snapshot = methods["_on_account_snapshot_synced"]
    callback_index = next(
        index
        for index, statement in enumerate(snapshot.body)
        if isinstance(statement, ast.Assign)
        and any(
            isinstance(target, ast.Name)
            and target.id == "callback_called"
            for target in statement.targets
        )
    )
    cache_prefix = ast.unparse(
        ast.Module(body=snapshot.body[:callback_index], type_ignores=[])
    )
    assert "self._last_snapshots" in cache_prefix
    assert "self._last_snapshot" in cache_prefix
    callback_guard = snapshot.body[callback_index + 1]
    assert isinstance(callback_guard, ast.If)
    assert isinstance(callback_guard.test, ast.UnaryOp)
    assert isinstance(callback_guard.test.op, ast.Not)
    assert isinstance(callback_guard.test.operand, ast.Name)
    assert callback_guard.test.operand.id == "callback_called"
    assert len(callback_guard.body) == 1
    assert isinstance(callback_guard.body[0], ast.Return)
    logging_suffix = ast.unparse(
        ast.Module(body=snapshot.body[callback_index + 2 :], type_ignores=[])
    )
    assert "_last_account_snapshot_log_state" in logging_suffix
    assert "_last_account_snapshot_log_ms" in logging_suffix

    order_results = methods["_process_order_result_feedback"]
    assert isinstance(order_results.body[0], ast.Assign)
    assert ast.unparse(order_results.body[0].value).startswith(
        "await self._strategy_host.on_order_results("
    )
    assert _is_none_return(order_results.body[1], "follow_up")
    assert "follow_up_count" in ast.unparse(order_results.body[2])
    assert _calls(order_results.body[3], "info")
    assert _calls(order_results.body[3], "debug")

    assert not any(
        isinstance(node, ast.Attribute) and node.attr == "_strategy"
        for method_name in (
            "_call_on_start",
            "_process_account_event",
            "_on_account_snapshot_synced",
            "_process_order_result_feedback",
        )
        for node in ast.walk(methods[method_name])
    )


def test_all_signal_sources_converge_on_one_runner_entrypoint() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    callers = {
        name
        for name, method in methods.items()
        if _calls(method, "_execute_signals")
    }
    assert callers == {
        "process_market_event",
        "process_market_feature",
        "_execute_recovery_stop_signals",
        "_execute_recovery_other_signals",
        "_call_on_start",
        "_evaluate_startup_catchup_once",
        "_periodic_follower_close_check",
        "_process_account_event",
    }

    intent_creators = {
        name
        for name, method in methods.items()
        for call in ast.walk(method)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "create"
        and ast.unparse(call.func.value) == "self._intent_factory"
    }
    order_executors = {
        name
        for name, method in methods.items()
        for call in ast.walk(method)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "execute"
        and "_get_order_coordinator()" in ast.unparse(call.func.value)
    }
    assert intent_creators == {"_create_signal_execution_intent"}
    assert order_executors == {"_execute_signal_execution_intent"}


def test_stage2_runtime_component_classes_have_one_definition_each() -> None:
    discovered = {name: [] for name in UNIQUE_CLASSES}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in discovered:
                discovered[node.name].append(
                    path.relative_to(PROJECT_ROOT).as_posix()
                )
    assert discovered == {
        name: [path] for name, path in UNIQUE_CLASSES.items()
    }


def test_stage2_has_no_risk_engine_type_key_or_dependency() -> None:
    forbidden_class_names = {
        "RiskEngine",
        "PortfolioRiskCoordinator",
        "PositionSizingEngine",
        "TradeApprovalEngine",
    }
    forbidden_keys = {
        "risk_engine",
        "portfolio_risk_coordinator",
        "position_sizing_engine",
        "trade_approval_engine",
    }
    found_classes: set[str] = set()
    found_keys: set[str] = set()
    found_imports: set[str] = set()
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = _tree(path)
        found_classes.update(
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ClassDef)
            and node.name in forbidden_class_names
        )
        found_keys.update(
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value in forbidden_keys
        )
        found_imports.update(
            module
            for module in _imports(path)
            if module.endswith("risk_engine")
            or ".risk_engine." in module
        )
    assert found_classes == set()
    assert found_keys == set()
    assert found_imports == set()
