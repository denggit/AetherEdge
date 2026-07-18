from __future__ import annotations

import ast
from pathlib import Path

from tests.runtime_surface_ast import runtime_surface_class


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
SHUTDOWN_COORDINATOR = SOURCE_ROOT / "runtime" / "shutdown_coordinator.py"
RUNNER = SOURCE_ROOT / "runtime" / "runner.py"
SYNC_LIFECYCLE = SOURCE_ROOT / "runtime" / "sync_lifecycle.py"
HEALTH_STATE = SOURCE_ROOT / "runtime" / "health_state.py"
HEARTBEAT = SOURCE_ROOT / "runtime" / "heartbeat.py"


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


def _assignment(method: ast.AST, target: str) -> ast.Assign:
    return next(
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and ast.unparse(node.targets[0]) == target
    )


def _calls(node: ast.AST, attribute: str) -> list[ast.Call]:
    return [
        child
        for child in ast.walk(node)
        if isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and child.func.attr == attribute
    ]


def _coordinator_steps(
    method: ast.AST,
    *,
    receiver: str = "self._shutdown_coordinator",
) -> list[str]:
    execute_calls = [
        call
        for call in _calls(method, "execute")
        if ast.unparse(call.func.value) == receiver
    ]
    assert len(execute_calls) == 1
    call = execute_calls[0]
    assert len(call.args) == 1
    assert isinstance(call.args[0], ast.Tuple)
    assert call.keywords == []
    return [ast.unparse(element) for element in call.args[0].elts]


def test_shutdown_coordinator_has_one_definition() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimeShutdownCoordinator"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/shutdown_coordinator.py"]


def test_shutdown_coordinator_has_only_generic_dependencies() -> None:
    assert _imports(SHUTDOWN_COORDINATOR) <= {
        "__future__",
        "collections.abc",
        "typing",
    }
    forbidden_prefixes = (
        "asyncio",
        "time",
        "src.runtime.runner",
        "src.runtime.heartbeat",
        "src.runtime.health_state",
        "src.runtime.sync_lifecycle",
        "src.runtime.persistence",
        "src.app",
        "src.market_data",
        "src.order_management",
        "src.platform",
        "src.strategy",
        "src.signals",
    )
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in _imports(SHUTDOWN_COORDINATOR)
        for prefix in forbidden_prefixes
    )


def test_coordinator_is_stateless_and_execute_is_only_for_await() -> None:
    coordinator = _class(
        SHUTDOWN_COORDINATOR,
        "RuntimeShutdownCoordinator",
    )
    assert not any(
        isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
            for target in (
                node.targets if isinstance(node, ast.Assign) else [node.target]
            )
        )
        for node in ast.walk(coordinator)
    )

    execute = _methods(coordinator)["execute"]
    assert isinstance(execute, ast.AsyncFunctionDef)
    assert len(execute.body) == 1
    loop = execute.body[0]
    assert isinstance(loop, ast.For)
    assert ast.unparse(loop.target) == "step"
    assert ast.unparse(loop.iter) == "steps"
    assert loop.orelse == []
    assert len(loop.body) == 1
    expression = loop.body[0]
    assert isinstance(expression, ast.Expr)
    assert isinstance(expression.value, ast.Await)
    call = expression.value.value
    assert isinstance(call, ast.Call)
    assert ast.unparse(call.func) == "step"
    assert call.args == []
    assert call.keywords == []
    assert not any(
        isinstance(node, (ast.Try, ast.TryStar))
        for node in ast.walk(execute)
    )


def test_coordinator_contains_no_business_or_concurrency_names() -> None:
    tree = _tree(SHUTDOWN_COORDINATOR)
    forbidden = {
        "asyncio",
        "gather",
        "create_task",
        "wait_for",
        "timeout",
        "logger",
        "alerts",
        "health",
        "stop_event",
        "heartbeat",
        "persistence",
        "producer",
        "writer",
        "tasks",
        "services",
        "runner",
    }
    used = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id in forbidden
    }
    used.update(
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden
    )
    assert used == set()


def test_runner_selects_and_writes_back_one_shutdown_coordinator() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    injected = _assignment(initializer, "injected_shutdown_coordinator")
    selected = _assignment(initializer, "self._shutdown_coordinator")
    writeback = _assignment(
        initializer,
        "self.runtime_services.shutdown_coordinator",
    )

    assert ast.unparse(injected.value) == (
        "self.runtime_services.shutdown_coordinator"
    )
    assert isinstance(selected.value, ast.IfExp)
    assert ast.unparse(selected.value.test) == (
        "injected_shutdown_coordinator is not None"
    )
    assert ast.unparse(selected.value.body) == (
        "injected_shutdown_coordinator"
    )
    assert ast.unparse(selected.value.orelse) == (
        "RuntimeShutdownCoordinator()"
    )
    assert ast.unparse(writeback.value) == "self._shutdown_coordinator"
    assert injected.lineno < selected.lineno < writeback.lineno

    factories = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeShutdownCoordinator"
    ]
    assert len(factories) == 1
    assert sum(
        1
        for node in ast.walk(_class(RUNNER, "LiveRuntimeRunner"))
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeShutdownCoordinator"
    ) == 1
    assert _calls(initializer, "execute") == []


def test_run_finally_only_delegates_to_final_shutdown_helper() -> None:
    run = _methods(_class(RUNNER, "LiveRuntimeRunner"))["run"]
    try_node = next(node for node in run.body if isinstance(node, ast.Try))
    assert len(try_node.finalbody) == 1
    statement = try_node.finalbody[0]
    assert isinstance(statement, ast.Expr)
    assert isinstance(statement.value, ast.Await)
    assert ast.unparse(statement.value.value) == (
        "self._run_finally_shutdown()"
    )


def test_final_shutdown_helper_has_one_range_owned_stop() -> None:
    method = _methods(_class(RUNNER, "LiveRuntimeRunner"))[
        "_run_finally_shutdown"
    ]
    assert len(method.body) == 1
    assert _coordinator_steps(method) == [
        "self._stop_market_data_modules",
        "self._stop_sync_tasks",
        "self._stop_producers",
        "self._stop_live_persistence_writer",
        "self.context.alerts.stop",
    ]
    assert not any(
        isinstance(node, (ast.Lambda, ast.Try, ast.TryStar))
        for node in ast.walk(method)
    )


def test_explicit_stop_has_distinct_three_step_helper_and_outer_order() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    helper = methods["_explicit_stop_shutdown"]
    assert _coordinator_steps(helper, receiver="coordinator") == [
        "self._stop_market_data_modules",
        "self._stop_producers",
        "self._stop_live_persistence_writer",
    ]

    stop = methods["stop"]
    assert [ast.unparse(statement) for statement in stop.body] == [
        "self._stop_event.set()",
        "await self._explicit_stop_shutdown()",
        "self._set_health(RuntimePhase.STOPPED, healthy=True)",
        "return self._health",
    ]
    assert "_stop_sync_tasks" not in ast.unparse(helper)
    assert "alerts.stop" not in ast.unparse(helper)
    assert "_heartbeat_service" not in ast.unparse(helper)


def test_final_and_explicit_shutdown_sequences_are_not_merged() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    final_steps = _coordinator_steps(methods["_run_finally_shutdown"])
    explicit_steps = _coordinator_steps(
        methods["_explicit_stop_shutdown"],
        receiver="coordinator",
    )
    assert final_steps != explicit_steps
    assert final_steps == [
        explicit_steps[0],
        "self._stop_sync_tasks",
        *explicit_steps[1:],
        "self.context.alerts.stop",
    ]


def test_runner_has_no_heartbeat_stop_and_sync_lifecycle_keeps_task_cleanup() -> None:
    runner_class = _class(RUNNER, "LiveRuntimeRunner")
    heartbeat_stops = [
        node
        for node in ast.walk(runner_class)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "stop"
        and ast.unparse(node.func.value) == "self._heartbeat_service"
    ]
    assert heartbeat_stops == []

    lifecycle = _methods(_class(SYNC_LIFECYCLE, "RuntimeSyncLifecycle"))
    stop = lifecycle["stop"]
    assert _calls(stop, "cancel")
    gathers = [
        node
        for node in ast.walk(stop)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "asyncio.gather"
    ]
    assert len(gathers) == 1
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in gathers[0].keywords
    } == {"return_exceptions": "True"}


def test_health_state_and_heartbeat_do_not_depend_on_coordinator() -> None:
    for path in (HEALTH_STATE, HEARTBEAT):
        assert not any(
            module == "src.runtime.shutdown_coordinator"
            or module.startswith("src.runtime.shutdown_coordinator.")
            for module in _imports(path)
        )
        assert not any(
            isinstance(node, ast.Name)
            and node.id == "RuntimeShutdownCoordinator"
            for node in ast.walk(_tree(path))
        )
