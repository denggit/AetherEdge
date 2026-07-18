from __future__ import annotations

import ast
from pathlib import Path

from tests.runtime_surface_ast import runtime_surface_class


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
SYNC_SERVICES = SOURCE_ROOT / "runtime" / "sync_services.py"
SYNC_LIFECYCLE = SOURCE_ROOT / "runtime" / "sync_lifecycle.py"
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


def _self_assignments(method: ast.AST) -> set[str]:
    assigned = {
        target.attr
        for node in ast.walk(method)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    }
    assigned.update(
        node.target.attr
        for node in ast.walk(method)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Attribute)
        and isinstance(node.target.value, ast.Name)
        and node.target.value.id == "self"
    )
    return assigned


def test_runtime_sync_service_registry_has_one_definition() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimeSyncServiceRegistry"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/sync_services.py"]


def test_registry_has_only_generic_dependencies_and_no_asyncio() -> None:
    assert _imports(SYNC_SERVICES) <= {
        "__future__",
        "collections.abc",
        "typing",
    }
    assert "asyncio" not in _imports(SYNC_SERVICES)
    forbidden_prefixes = (
        "src.runtime.runner",
        "src.runtime.account_sync",
        "src.order_management",
        "src.market_data",
        "src.platform",
        "src.strategy",
        "src.signals",
        "strategies",
    )
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in _imports(SYNC_SERVICES)
        for prefix in forbidden_prefixes
    )


def test_registry_owns_only_account_and_order_service_slots() -> None:
    registry_class = _class(
        SYNC_SERVICES,
        "RuntimeSyncServiceRegistry",
    )
    initializer = _methods(registry_class)["__init__"]
    forbidden_names = {
        "LiveRuntimeRunner",
        "AppContext",
        "Strategy",
        "requirements",
        "services",
        "stop_event",
        "RequestThrottle",
        "SyncExchangeContext",
        "AccountStateSyncService",
        "OrderStateSyncService",
        "AppAlert",
        "PositionPlanStore",
        "task",
        "tasks",
        "sync_type",
        "priority",
        "poll_enabled",
        "poll_when_position_enabled",
    }
    used = {
        node.id
        for node in ast.walk(registry_class)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used.update(
        node.attr
        for node in ast.walk(registry_class)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    forbidden_strings = {
        "post_submit",
        "post_order_account",
    }
    strings = {
        node.value
        for node in ast.walk(registry_class)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    assert _self_assignments(initializer) == {
        "_account_service",
        "_order_service",
    }
    assert used == set()
    assert forbidden_strings.isdisjoint(strings)


def test_registry_exposes_read_only_service_slots_without_calls() -> None:
    registry_class = _class(
        SYNC_SERVICES,
        "RuntimeSyncServiceRegistry",
    )
    methods = _methods(registry_class)

    for property_name, slot in (
        ("account_service", "_account_service"),
        ("order_service", "_order_service"),
    ):
        method = methods[property_name]
        assert len(method.decorator_list) == 1
        assert isinstance(method.decorator_list[0], ast.Name)
        assert method.decorator_list[0].id == "property"
        assert len(method.body) == 1
        assert isinstance(method.body[0], ast.Return)
        assert ast.unparse(method.body[0].value) == f"self.{slot}"
        assert not any(
            isinstance(node, ast.Call) for node in ast.walk(method)
        )
        assert _self_assignments(method) == set()

    setters = {
        ast.unparse(decorator)
        for node in registry_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Attribute)
        and decorator.attr == "setter"
    }
    assert setters.isdisjoint(
        {"account_service.setter", "order_service.setter"}
    )


def test_registry_getters_only_invoke_their_factory() -> None:
    methods = _methods(
        _class(SYNC_SERVICES, "RuntimeSyncServiceRegistry")
    )
    for method_name, slot in (
        ("get_account", "_account_service"),
        ("get_order", "_order_service"),
    ):
        method = methods[method_name]
        calls = [node for node in ast.walk(method) if isinstance(node, ast.Call)]
        assert len(calls) == 1
        assert isinstance(calls[0].func, ast.Name)
        assert calls[0].func.id == "factory"
        assert _self_assignments(method) == {slot}


def test_runner_init_syncs_compatibility_fields_from_selected_registry() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    assignments = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Assign) and len(node.targets) == 1
    ]

    registry_assignment = next(
        node
        for node in assignments
        if ast.unparse(node.targets[0]) == "self._sync_service_registry"
    )
    registry_writeback = next(
        node
        for node in assignments
        if ast.unparse(node.targets[0])
        == "self.runtime_services.sync_service_registry"
    )
    for field, property_name in (
        ("_account_sync_service", "account_service"),
        ("_order_sync_service", "order_service"),
    ):
        candidates = [
            node
            for node in assignments
            if ast.unparse(node.targets[0]) == f"self.{field}"
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "getattr"
        ]
        assert len(candidates) == 1
        assignment = candidates[0]
        assert [ast.unparse(argument) for argument in assignment.value.args] == [
            "self._sync_service_registry",
            repr(property_name),
            "None",
        ]
        assert registry_assignment.lineno < registry_writeback.lineno
        assert registry_writeback.lineno < assignment.lineno

    forbidden_constructor_calls = {
        "get_account",
        "get_order",
        "_build_account_sync_service",
        "_build_order_sync_service",
    }
    called_attributes = {
        node.func.attr
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
    }
    assert forbidden_constructor_calls.isdisjoint(called_attributes)

    overwritten_service_keys = {
        target.slice.value
        for node in assignments
        for target in node.targets
        if isinstance(target, ast.Subscript)
        and ast.unparse(target.value) == "self.services"
        and isinstance(target.slice, ast.Constant)
        and target.slice.value in {
            "account_sync_service",
            "order_sync_service",
        }
    }
    assert overwritten_service_keys == set()


def test_runner_owns_exact_account_and_order_service_builders() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    expected = {
        "_build_account_sync_service": (
            "AccountStateSyncService",
            {
                "contexts": "self._get_sync_contexts()",
                "config": "self.requirements.account_state",
                "alert_sink": "self.context.alerts",
                "throttle": "self._request_sync_throttle",
                "snapshot_callback": "self._on_account_snapshot_synced",
            },
        ),
        "_build_order_sync_service": (
            "OrderStateSyncService",
            {
                "contexts": "self._get_sync_contexts()",
                "config": "self.requirements.order_state",
                "alert_sink": "self.context.alerts",
                "throttle": "self._request_sync_throttle",
                "active_check": "self._order_sync_active",
                "position_plan_store": "self._get_position_plan_store()",
            },
        ),
    }

    for method_name, (constructor_name, expected_keywords) in expected.items():
        calls = [
            node
            for node in ast.walk(methods[method_name])
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == constructor_name
        ]
        assert len(calls) == 1
        actual_keywords = {
            keyword.arg: ast.unparse(keyword.value)
            for keyword in calls[0].keywords
        }
        assert actual_keywords == expected_keywords


def test_legacy_getters_delegate_registry_and_sync_compatibility_fields() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    expected = {
        "_get_account_sync_service": (
            "get_account",
            "_build_account_sync_service",
            "_account_sync_service",
        ),
        "_get_order_sync_service": (
            "get_order",
            "_build_order_sync_service",
            "_order_sync_service",
        ),
    }

    for method_name, (getter_name, builder_name, field_name) in expected.items():
        method = methods[method_name]
        calls = [
            node
            for node in ast.walk(method)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == getter_name
        ]
        assert len(calls) == 1
        assert ast.unparse(calls[0].func.value) == (
            "self._sync_service_registry"
        )
        assert [ast.unparse(argument) for argument in calls[0].args] == [
            f"self.{builder_name}"
        ]
        assert field_name in _self_assignments(method)


def test_runner_retains_sync_context_callbacks_and_service_call_paths() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    assert {
        "_get_sync_contexts",
        "_resolved_account_config_env",
        "_build_account_sync_service",
        "_build_order_sync_service",
        "_order_sync_active",
        "_on_account_snapshot_synced",
        "_periodic_follower_close_check",
        "_periodic_feature_readiness_refresh",
        "_start_sync_tasks",
        "_stop_sync_tasks",
    } <= set(methods)

    start_attributes = {
        node.attr
        for node in ast.walk(methods["_start_sync_tasks"])
        if isinstance(node, ast.Attribute)
    }
    assert {
        "_get_account_sync_service",
        "_get_order_sync_service",
    } <= start_attributes

    signal_sync_methods = (
        methods["_run_post_submit_order_sync"],
        methods["_run_post_order_account_sync"],
    )
    execute_attributes = {
        node.attr
        for method in signal_sync_methods
        for node in ast.walk(method)
        if isinstance(node, ast.Attribute)
    }
    execute_strings = {
        node.value
        for method in signal_sync_methods
        for node in ast.walk(method)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert {
        "post_submit_sync_enabled",
        "post_order_sync_enabled",
        "_get_order_sync_service",
        "_get_account_sync_service",
        "sync_once",
    } <= execute_attributes
    assert {"post_submit", "post_order_account"} <= execute_strings


def test_sync_lifecycle_does_not_own_registry_or_concrete_services() -> None:
    lifecycle_tree = _tree(SYNC_LIFECYCLE)
    forbidden_names = {
        "RuntimeSyncServiceRegistry",
        "AccountStateSyncService",
        "OrderStateSyncService",
        "_sync_service_registry",
        "_account_sync_service",
        "_order_sync_service",
    }
    used = {
        node.id
        for node in ast.walk(lifecycle_tree)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used.update(
        node.attr
        for node in ast.walk(lifecycle_tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )

    assert used == set()
