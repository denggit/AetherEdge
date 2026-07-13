from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
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


def test_runtime_sync_lifecycle_has_one_definition() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimeSyncLifecycle"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/sync_lifecycle.py"]


def test_sync_lifecycle_has_only_generic_async_dependencies() -> None:
    assert _imports(SYNC_LIFECYCLE) <= {
        "__future__",
        "asyncio",
        "collections.abc",
        "typing",
    }
    forbidden_prefixes = (
        "src.app",
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
        for module in _imports(SYNC_LIFECYCLE)
        for prefix in forbidden_prefixes
    )


def test_sync_lifecycle_owns_only_task_list_without_business_vocabulary() -> None:
    lifecycle_class = _class(SYNC_LIFECYCLE, "RuntimeSyncLifecycle")
    initializer = _methods(lifecycle_class)["__init__"]
    assigned_attributes = {
        target.attr
        for node in ast.walk(initializer)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    }
    assigned_attributes.update(
        node.target.attr
        for node in ast.walk(initializer)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Attribute)
        and isinstance(node.target.value, ast.Name)
        and node.target.value.id == "self"
    )
    forbidden_names = {
        "LiveRuntimeRunner",
        "AppContext",
        "AppAlert",
        "Strategy",
        "AccountStateSyncService",
        "OrderStateSyncService",
        "RuntimeHeartbeatService",
        "requirements",
        "services",
        "stop_event",
        "alerts",
        "_execute_signals",
        "PositionPlanStore",
    }
    used = {
        node.id
        for node in ast.walk(lifecycle_class)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used.update(
        node.attr
        for node in ast.walk(lifecycle_class)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )

    assert assigned_attributes == {"_tasks"}
    assert used == set()


def test_lifecycle_start_uses_create_task_without_gather_or_task_name() -> None:
    start = _methods(_class(SYNC_LIFECYCLE, "RuntimeSyncLifecycle"))["start"]
    create_calls = [
        node
        for node in ast.walk(start)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "create_task"
    ]

    assert len(create_calls) == 1
    assert len(create_calls[0].args) == 1
    assert create_calls[0].keywords == []
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "gather"
        for node in ast.walk(start)
    )


def test_lifecycle_stop_cancels_task_list_and_gathers_exceptions() -> None:
    stop = _methods(_class(SYNC_LIFECYCLE, "RuntimeSyncLifecycle"))["stop"]
    cancel_calls = [
        node
        for node in ast.walk(stop)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "cancel"
    ]
    gather_calls = [
        node
        for node in ast.walk(stop)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "gather"
    ]

    assert len(cancel_calls) == 1
    assert isinstance(cancel_calls[0].func.value, ast.Name)
    assert cancel_calls[0].func.value.id == "task"
    assert len(gather_calls) == 1
    return_exceptions = next(
        keyword.value
        for keyword in gather_calls[0].keywords
        if keyword.arg == "return_exceptions"
    )
    assert isinstance(return_exceptions, ast.Constant)
    assert return_exceptions.value is True
    assert not any(
        isinstance(node, ast.Attribute) and node.attr in {"wait_for", "timeout"}
        for node in ast.walk(stop)
    )


def test_runner_sync_wrappers_delegate_lifecycle_mechanics() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    start = methods["_start_sync_tasks"]
    stop = methods["_stop_sync_tasks"]

    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "create_task"
        for node in ast.walk(start)
    )
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"cancel", "gather"}
        for node in ast.walk(stop)
    )
    start_calls = [
        node
        for node in ast.walk(start)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "start"
    ]
    stop_calls = [
        node
        for node in ast.walk(stop)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "stop"
    ]
    assert len(start_calls) == 1
    assert len(stop_calls) == 1


def test_runner_keeps_sync_task_selection_and_business_methods() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    start = methods["_start_sync_tasks"]
    used_attributes = {
        node.attr for node in ast.walk(start) if isinstance(node, ast.Attribute)
    }

    assert {
        "poll_enabled",
        "poll_when_position_enabled",
        "_get_account_sync_service",
        "_get_order_sync_service",
        "_periodic_follower_close_check",
        "_heartbeat_service",
        "_get_startup_feature_backfill_providers",
        "_periodic_feature_readiness_refresh",
        "_stop_event",
    } <= used_attributes
    assert {
        "_get_account_sync_service",
        "_get_order_sync_service",
        "_get_sync_contexts",
        "_periodic_follower_close_check",
        "_periodic_feature_readiness_refresh",
        "_on_account_snapshot_synced",
        "_build_unresolved_follower_close_signals",
    } <= set(methods)


def test_post_submit_and_post_order_sync_remain_in_signal_execution() -> None:
    execute_signals = _methods(_class(RUNNER, "LiveRuntimeRunner"))[
        "_execute_signals"
    ]
    attributes = {
        node.attr
        for node in ast.walk(execute_signals)
        if isinstance(node, ast.Attribute)
    }
    strings = {
        node.value
        for node in ast.walk(execute_signals)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    assert {
        "post_submit_sync_enabled",
        "post_order_sync_enabled",
        "_get_order_sync_service",
        "_get_account_sync_service",
        "sync_once",
    } <= attributes
    assert {"post_submit", "post_order_account"} <= strings
