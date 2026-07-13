from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
PERSISTENCE = SOURCE_ROOT / "runtime" / "persistence.py"
PERSISTENCE_SERVICE = SOURCE_ROOT / "runtime" / "persistence_service.py"
MARKET_DATA_PERSISTENCE = (
    SOURCE_ROOT / "runtime" / "market_data_persistence.py"
)
RUNNER = SOURCE_ROOT / "runtime" / "runner.py"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imports(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_background_write_classes_have_one_definition() -> None:
    definitions = {"BackgroundWriteItem": [], "BackgroundWriteQueue": []}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in definitions:
                definitions[node.name].append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == {
        "BackgroundWriteItem": ["src/runtime/persistence.py"],
        "BackgroundWriteQueue": ["src/runtime/persistence.py"],
    }


def test_runtime_persistence_service_has_one_definition() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimePersistenceService"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/persistence_service.py"]


def test_runtime_market_data_persistence_has_one_definition() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimeMarketDataPersistence"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/market_data_persistence.py"]


def test_runner_keeps_aliases_without_private_class_definitions() -> None:
    tree = _tree(RUNNER)
    aliases = {
        target.id: node.value.id
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Name)
        for target in node.targets
        if target.id in {"_BackgroundWriteItem", "_BackgroundWriteQueue"}
    }

    assert aliases == {
        "_BackgroundWriteItem": "BackgroundWriteItem",
        "_BackgroundWriteQueue": "BackgroundWriteQueue",
    }
    assert not any(
        isinstance(node, ast.ClassDef)
        and node.name in {"_BackgroundWriteItem", "_BackgroundWriteQueue"}
        for node in ast.walk(tree)
    )


def test_persistence_module_has_only_generic_dependencies() -> None:
    imports = _imports(PERSISTENCE)
    allowed = {
        "__future__",
        "queue",
        "threading",
        "dataclasses",
        "typing",
        "collections.abc",
        "src.utils.log",
    }

    assert imports <= allowed
    assert "asyncio" not in imports


def test_persistence_module_has_no_runtime_or_business_vocabulary() -> None:
    tree = _tree(PERSISTENCE)
    forbidden_names = {
        "AppAlert",
        "AppContext",
        "RangeCheckpointWriter",
        "RangeRepairJournalWriter",
        "SQLite",
        "Sqlite",
        "_execute_signals",
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
    used_names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used_names.update(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    forbidden_text = (
        "database path",
        "exchange",
        "order journal",
        "position plan",
        "strategy callback",
    )
    string_violations = {
        text
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        for text in forbidden_text
        if text in node.value.lower()
    }

    assert used_names == set()
    assert string_violations == set()


def test_persistence_service_has_only_lifecycle_dependencies() -> None:
    imports = _imports(PERSISTENCE_SERVICE)
    allowed = {
        "__future__",
        "asyncio",
        "inspect",
        "dataclasses",
        "typing",
        "collections.abc",
        "src.runtime.persistence",
    }

    assert imports <= allowed


def test_persistence_service_has_no_runtime_business_dependencies() -> None:
    tree = _tree(PERSISTENCE_SERVICE)
    forbidden_names = {
        "AppAlert",
        "AppContext",
        "StateStore",
        "OrderJournal",
        "PositionPlanStore",
        "RangeCheckpointWriter",
        "RangeRepairJournalWriter",
        "SQLite",
        "Sqlite",
        "Strategy",
        "_execute_signals",
        "on_market_feature",
        "on_trade",
        "recover",
    }
    used_names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used_names.update(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    forbidden_import_prefixes = (
        "src.app",
        "src.market_data",
        "src.order_management",
        "src.reconcile",
        "src.platform",
        "src.strategy",
        "src.signals",
        "strategies",
    )

    assert used_names == set()
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in _imports(PERSISTENCE_SERVICE)
        for prefix in forbidden_import_prefixes
    )


def test_market_data_persistence_has_only_gateway_dependencies() -> None:
    assert _imports(MARKET_DATA_PERSISTENCE) <= {
        "__future__",
        "collections.abc",
        "typing",
        "src.market_data.models",
        "src.platform.data.models",
        "src.runtime.persistence_service",
    }


def test_market_data_persistence_has_no_runtime_or_business_ownership() -> None:
    tree = _tree(MARKET_DATA_PERSISTENCE)
    forbidden_names = {
        "LiveRuntimeRunner",
        "AppContext",
        "AppAlert",
        "Strategy",
        "SqliteKlineStore",
        "SqliteRangeBarStore",
        "SqliteRangeCheckpointStore",
        "OrderJournal",
        "PositionPlanStore",
        "RangeCheckpointWriter",
        "RangeRepairJournalWriter",
        "logger",
        "metrics",
        "_execute_signals",
        "process_market_feature",
    }
    used_names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used_names.update(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    forbidden_import_prefixes = (
        "src.app",
        "src.order_management",
        "src.reconcile",
        "src.platform.exchanges",
        "src.signals",
        "src.strategy",
        "strategies",
    )

    assert used_names == set()
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in _imports(MARKET_DATA_PERSISTENCE)
        for prefix in forbidden_import_prefixes
    )


def test_market_data_persistence_holds_only_explicit_dependencies() -> None:
    gateway_class = next(
        node
        for node in _tree(MARKET_DATA_PERSISTENCE).body
        if isinstance(node, ast.ClassDef)
        and node.name == "RuntimeMarketDataPersistence"
    )
    initializer = next(
        node
        for node in gateway_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    assigned_attributes = {
        target.attr
        for node in ast.walk(initializer)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    }

    assert assigned_attributes == {
        "_persistence_service",
        "_kline_store_provider",
        "_range_bar_store_provider",
        "_completed_aggregate_store_provider",
        "_exchange",
        "_clock_ms",
    }


def test_runner_market_data_persistence_wrappers_only_delegate() -> None:
    runner_class = next(
        node
        for node in _tree(RUNNER).body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    methods = {
        node.name: node
        for node in runner_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    wrapper_names = {
        "_persist_closed_kline",
        "_persist_range_bar",
        "_persist_completed_range_aggregate",
    }
    forbidden_calls = {"save", "save_completed_aggregate", "submit"}

    for name in wrapper_names:
        method = methods[name]
        assert not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in forbidden_calls
            for node in ast.walk(method)
        )


def test_market_data_descriptions_are_owned_only_by_gateway() -> None:
    expected_by_method = {
        "persist_closed_kline": "closed_kline",
        "persist_range_bar": "range_bar",
        "persist_completed_range_aggregate": "completed_range_aggregate",
    }
    gateway_class = next(
        node
        for node in _tree(MARKET_DATA_PERSISTENCE).body
        if isinstance(node, ast.ClassDef)
        and node.name == "RuntimeMarketDataPersistence"
    )
    gateway_methods = {
        node.name: node
        for node in gateway_class.body
        if isinstance(node, ast.FunctionDef)
    }
    runner_class = next(
        node
        for node in _tree(RUNNER).body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    runner_methods = {
        node.name: node
        for node in runner_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    for method_name, expected_description in expected_by_method.items():
        method = gateway_methods[method_name]
        description_assignments = [
            node
            for node in ast.walk(method)
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "description"
                for target in node.targets
            )
        ]
        assert len(description_assignments) == 1
        assert isinstance(description_assignments[0].value, ast.Constant)
        assert description_assignments[0].value.value == expected_description

        submit_calls = [
            node
            for node in ast.walk(method)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "submit"
        ]
        assert len(submit_calls) == 1
        description_keywords = [
            keyword.value
            for keyword in submit_calls[0].keywords
            if keyword.arg == "description"
        ]
        assert len(description_keywords) == 1
        assert isinstance(description_keywords[0], ast.Name)
        assert description_keywords[0].id == "description"

    descriptions = set(expected_by_method.values())
    for method_name in {
        "_persist_closed_kline",
        "_persist_range_bar",
        "_persist_completed_range_aggregate",
    }:
        assert not any(
            isinstance(node, ast.Constant) and node.value in descriptions
            for node in ast.walk(runner_methods[method_name])
        )


def test_gateway_rejection_path_uses_submitted_description() -> None:
    gateway_class = next(
        node
        for node in _tree(MARKET_DATA_PERSISTENCE).body
        if isinstance(node, ast.ClassDef)
        and node.name == "RuntimeMarketDataPersistence"
    )
    gateway_methods = {
        node.name: node
        for node in gateway_class.body
        if isinstance(node, ast.FunctionDef)
    }

    for method_name in {
        "persist_closed_kline",
        "persist_range_bar",
        "persist_completed_range_aggregate",
    }:
        method = gateway_methods[method_name]
        rejection_branches = [
            node
            for node in ast.walk(method)
            if isinstance(node, ast.If)
            and {
                name.id
                for name in ast.walk(node.test)
                if isinstance(name, ast.Name)
            }
            >= {"accepted", "on_rejected"}
        ]
        assert len(rejection_branches) == 1
        rejection_calls = [
            node
            for node in ast.walk(rejection_branches[0])
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "on_rejected"
        ]
        assert len(rejection_calls) == 1
        assert len(rejection_calls[0].args) == 1
        assert isinstance(rejection_calls[0].args[0], ast.Name)
        assert rejection_calls[0].args[0].id == "description"


def test_runner_owns_rejection_warning_and_wrappers_pass_callback() -> None:
    runner_class = next(
        node
        for node in _tree(RUNNER).body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    methods = {
        node.name: node
        for node in runner_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    rejection_handler = methods["_on_live_persistence_write_rejected"]
    metrics_calls = [
        node
        for node in ast.walk(rejection_handler)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "metrics"
    ]
    warning_calls = [
        node
        for node in ast.walk(rejection_handler)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "logger"
        and node.func.attr == "warning"
    ]

    assert len(metrics_calls) == 1
    assert len(warning_calls) == 1
    assert isinstance(warning_calls[0].args[0], ast.Constant)
    assert warning_calls[0].args[0].value == (
        "Live persistence write dropped | "
        "description=%s pending=%s dropped=%s"
    )

    for method_name in {
        "_persist_closed_kline",
        "_persist_range_bar",
        "_persist_completed_range_aggregate",
    }:
        rejection_keywords = [
            keyword.value
            for call in ast.walk(methods[method_name])
            if isinstance(call, ast.Call)
            for keyword in call.keywords
            if keyword.arg == "on_rejected"
        ]
        assert len(rejection_keywords) == 1
        callback = rejection_keywords[0]
        assert isinstance(callback, ast.Attribute)
        assert isinstance(callback.value, ast.Name)
        assert callback.value.id == "self"
        assert callback.attr == "_on_live_persistence_write_rejected"


def test_gateway_has_no_logging_metrics_alerts_or_signal_execution() -> None:
    tree = _tree(MARKET_DATA_PERSISTENCE)
    forbidden_attributes = {
        "metrics",
        "debug",
        "info",
        "warning",
        "error",
        "exception",
        "_execute_signals",
        "process_market_feature",
    }

    assert not any(
        isinstance(node, ast.Attribute) and node.attr in forbidden_attributes
        for node in ast.walk(tree)
    )
    assert not any(
        isinstance(node, ast.Name) and node.id in {"logger", "AppAlert"}
        for node in ast.walk(tree)
    )


def test_gateway_excludes_error_handlers_checkpoint_and_repair_writers() -> None:
    gateway_tree = _tree(MARKET_DATA_PERSISTENCE)
    runner_tree = _tree(RUNNER)
    gateway_methods = {
        node.name
        for node in ast.walk(gateway_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    runner_methods = {
        node.name
        for node in ast.walk(runner_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    error_handlers = {
        "_on_closed_kline_persist_error",
        "_on_range_bar_persist_error",
        "_on_completed_range_aggregate_persist_error",
    }
    excluded_writer_names = {
        "RangeCheckpointWriter",
        "RangeRepairJournalWriter",
        "_get_range_checkpoint_writer",
        "_append_range_repair_trade",
        "_finalize_range_repair_journal",
    }
    gateway_names = {
        node.id for node in ast.walk(gateway_tree) if isinstance(node, ast.Name)
    } | gateway_methods

    assert error_handlers <= runner_methods
    assert error_handlers.isdisjoint(gateway_methods)
    assert excluded_writer_names.isdisjoint(gateway_names)


def test_runner_delegates_item_construction_submit_and_stop_to_service() -> None:
    tree = _tree(RUNNER)
    runner_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    methods = {
        node.name: node
        for node in runner_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"BackgroundWriteItem", "_BackgroundWriteItem"}
        for node in ast.walk(runner_class)
    )

    submit_calls = [
        node.func
        for node in ast.walk(methods["_submit_live_persistence_write"])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "submit"
    ]
    assert len(submit_calls) == 1
    assert isinstance(submit_calls[0].value, ast.Name)
    assert submit_calls[0].value.id == "service"

    stop_calls = [
        node.func
        for node in ast.walk(methods["_stop_live_persistence_writer"])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "stop"
    ]
    assert len(stop_calls) == 1
    assert isinstance(stop_calls[0].value, ast.Call)
    assert isinstance(stop_calls[0].value.func, ast.Attribute)
    assert stop_calls[0].value.func.attr == "_get_runtime_persistence_service"


def test_runtime_persistence_wrappers_remain_in_runner() -> None:
    runner_class = next(
        node
        for node in _tree(RUNNER).body
        if isinstance(node, ast.ClassDef) and node.name == "LiveRuntimeRunner"
    )
    methods = {
        node.name
        for node in runner_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert {
        "_get_live_persistence_writer",
        "_submit_live_persistence_write",
        "_stop_live_persistence_writer",
        "_emit_alert_threadsafe",
        "_maybe_log_live_data_path_stats",
    } <= methods


def test_independent_range_writers_were_not_moved() -> None:
    persistence_names = {
        node.id for node in ast.walk(_tree(PERSISTENCE)) if isinstance(node, ast.Name)
    }
    runner_names = {
        node.id for node in ast.walk(_tree(RUNNER)) if isinstance(node, ast.Name)
    }

    assert "RangeCheckpointWriter" not in persistence_names
    assert "RangeRepairJournalWriter" not in persistence_names
    assert "RangeCheckpointWriter" in runner_names
    assert "RangeRepairJournalWriter" in runner_names
