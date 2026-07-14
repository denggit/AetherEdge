from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
RUNNER = SOURCE_ROOT / "runtime" / "runner.py"
HEARTBEAT = SOURCE_ROOT / "runtime" / "heartbeat.py"
HEALTH_STATE = SOURCE_ROOT / "runtime" / "health_state.py"
SYNC_LIFECYCLE = SOURCE_ROOT / "runtime" / "sync_lifecycle.py"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


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


def _assignment(method: ast.AST, target: str) -> ast.Assign:
    return next(
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and ast.unparse(node.targets[0]) == target
    )


def test_runtime_heartbeat_service_has_one_unchanged_definition_boundary() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimeHeartbeatService"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/heartbeat.py"]
    assert {
        "RuntimeHeartbeat",
        "RuntimeHeartbeatStore",
        "RuntimeHeartbeatService",
    } <= {
        node.name
        for node in _tree(HEARTBEAT).body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }


def test_runner_constructor_selects_and_writes_back_one_heartbeat_service() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    injected = _assignment(initializer, "injected_heartbeat_service")
    selected = _assignment(initializer, "self._heartbeat_service")
    writeback = _assignment(
        initializer,
        "self.services['heartbeat_service']",
    )
    health_compatibility = _assignment(initializer, "self._health")

    assert ast.unparse(injected.value) == (
        "self.services.get('heartbeat_service')"
    )
    assert isinstance(selected.value, ast.IfExp)
    assert ast.unparse(selected.value.test) == (
        "injected_heartbeat_service is not None"
    )
    assert ast.unparse(selected.value.body) == "injected_heartbeat_service"
    assert ast.unparse(selected.value.orelse) == "RuntimeHeartbeatService()"
    assert ast.unparse(writeback.value) == "self._heartbeat_service"
    assert health_compatibility.lineno < injected.lineno
    assert injected.lineno < selected.lineno < writeback.lineno

    default_factories = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeHeartbeatService"
    ]
    assert len(default_factories) == 1
    assert default_factories[0].args == []
    assert default_factories[0].keywords == []


def test_runner_constructor_has_no_heartbeat_business_calls_or_store_access() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    forbidden_calls = {
        "start",
        "read_previous",
        "note_market_event",
        "note_closed_bar",
        "write_now",
        "run_periodic",
    }
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in forbidden_calls
        for node in ast.walk(initializer)
    )
    assert not any(
        isinstance(node, ast.Attribute) and node.attr == "store"
        for node in ast.walk(initializer)
    )


def test_runner_retains_only_one_heartbeat_compatibility_field() -> None:
    runner_class = _class(RUNNER, "LiveRuntimeRunner")
    assigned = {
        target.attr
        for node in ast.walk(runner_class)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
        and "heartbeat" in target.attr
    }
    assert assigned == {"_heartbeat_service"}


def test_startup_owns_single_heartbeat_start_with_exact_id_and_order() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    startup = methods["_startup"]
    heartbeat_wrapper = methods["_start_runtime_heartbeat"]
    start_calls = [
        call
        for call in _calls(heartbeat_wrapper, "start")
        if ast.unparse(call.func.value) == "self._heartbeat_service"
    ]
    assert len(start_calls) == 1
    start_call = start_calls[0]
    assert start_call.args == []
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in start_call.keywords
    } == {
        "runtime_id": (
            "f'{self.app_config.strategy}::{self.app_config.symbol}'"
        )
    }

    plans = [
        node
        for node in ast.walk(startup)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeStartupPhasePlan"
    ]
    assert len(plans) == 1
    plan_callbacks = {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in plans[0].keywords
    }
    assert plan_callbacks["start_heartbeat"] == (
        "self._start_runtime_heartbeat"
    )
    assert plan_callbacks["start_range_speed_background_services"] == (
        "self._start_range_speed_background_services"
    )
    assert plan_callbacks["enter_running"] == (
        "self._enter_startup_running"
    )

    all_runner_starts = [
        call
        for call in ast.walk(_class(RUNNER, "LiveRuntimeRunner"))
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "start"
        and ast.unparse(call.func.value) == "self._heartbeat_service"
    ]
    assert len(all_runner_starts) == 1
    assert all_runner_starts[0].lineno == start_call.lineno


def test_sync_tasks_own_single_unconditional_heartbeat_periodic_factory() -> None:
    method = _methods(_class(RUNNER, "LiveRuntimeRunner"))[
        "_start_sync_tasks"
    ]
    heartbeat_calls = [
        call
        for call in _calls(method, "run_periodic")
        if ast.unparse(call.func.value) == "self._heartbeat_service"
    ]
    assert len(heartbeat_calls) == 1
    heartbeat_call = heartbeat_calls[0]
    assert [ast.unparse(arg) for arg in heartbeat_call.args] == [
        "self._stop_event"
    ]
    assert heartbeat_call.keywords == []
    assert not any(
        heartbeat_call in list(ast.walk(node))
        for node in ast.walk(method)
        if isinstance(node, ast.If)
    )
    assert len(_calls(method, "start")) == 1
    assert ast.unparse(_calls(method, "start")[0].func.value) == (
        "self._sync_lifecycle"
    )
    assert not any(
        isinstance(node, ast.Call)
        and ast.unparse(node.func) == "asyncio.create_task"
        for node in ast.walk(method)
    )

    account = _calls(method, "_get_account_sync_service")[0]
    order = _calls(method, "_get_order_sync_service")[0]
    follower = _calls(method, "_periodic_follower_close_check")[0]
    readiness = _calls(method, "_periodic_feature_readiness_refresh")[0]
    assert (
        account.lineno
        < order.lineno
        < follower.lineno
        < heartbeat_call.lineno
        < readiness.lineno
    )


def test_market_event_notes_time_before_health_and_strategy_processing() -> None:
    method = _methods(_class(RUNNER, "LiveRuntimeRunner"))[
        "process_market_event"
    ]
    notes = _calls(method, "note_market_event")
    assert len(notes) == 1
    assert [ast.unparse(arg) for arg in notes[0].args] == ["event_ms"]
    health = _calls(method, "_set_health")
    strategy = _calls(method, "_call_strategy_market_event")
    execute = _calls(method, "_execute_signals")
    assert len(health) == len(strategy) == len(execute) == 1
    assert notes[0].lineno < health[0].lineno
    assert notes[0].lineno < strategy[0].lineno < execute[0].lineno


def test_closed_bar_note_is_guarded_and_precedes_feature_dispatch() -> None:
    method = _methods(_class(RUNNER, "LiveRuntimeRunner"))[
        "process_market_feature"
    ]
    notes = _calls(method, "note_closed_bar")
    dispatch = _calls(method, "dispatch")
    assert len(notes) == len(dispatch) == 1
    assert [ast.unparse(arg) for arg in notes[0].args] == ["open_ms"]
    assert notes[0].lineno < dispatch[0].lineno

    parent_if = next(
        node
        for node in ast.walk(method)
        if isinstance(node, ast.If)
        and notes[0] in list(ast.walk(node))
        and "event.type_value == 'closed_kline'" in ast.unparse(node.test)
    )
    nested_guard = next(
        node
        for node in ast.walk(parent_if)
        if isinstance(node, ast.If)
        and notes[0] in list(ast.walk(node))
        and ast.unparse(node.test) == "isinstance(open_ms, int)"
    )
    assert nested_guard is not None


def test_startup_catchup_reads_previous_heartbeat_exactly_once() -> None:
    method = _methods(_class(RUNNER, "LiveRuntimeRunner"))[
        "_evaluate_startup_catchup_once"
    ]
    reads = [
        call
        for call in _calls(method, "read_previous")
        if ast.unparse(call.func.value) == "self._heartbeat_service"
    ]
    assert len(reads) == 1
    assert reads[0].args == []
    assert reads[0].keywords == []


def test_runner_has_no_explicit_heartbeat_stop_call() -> None:
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

    stop_sync = _methods(runner_class)["_stop_sync_tasks"]
    stop_calls = _calls(stop_sync, "stop")
    assert len(stop_calls) == 1
    assert ast.unparse(stop_calls[0].func.value) == "self._sync_lifecycle"


def test_sync_lifecycle_and_health_state_do_not_depend_on_heartbeat() -> None:
    assert not any(
        module == "src.runtime.heartbeat"
        or module.startswith("src.runtime.heartbeat.")
        for module in _imports(SYNC_LIFECYCLE) | _imports(HEALTH_STATE)
    )
    forbidden = {"RuntimeHeartbeat", "RuntimeHeartbeatService"}
    for path in (SYNC_LIFECYCLE, HEALTH_STATE):
        used = {
            node.id
            for node in ast.walk(_tree(path))
            if isinstance(node, ast.Name) and node.id in forbidden
        }
        assert used == set()


def test_run_finally_and_explicit_stop_order_remain_unchanged() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    run = methods["run"]
    try_node = next(node for node in run.body if isinstance(node, ast.Try))
    assert len(try_node.finalbody) == 1
    assert ast.unparse(try_node.finalbody[0]) == (
        "await self._run_finally_shutdown()"
    )

    stop = methods["stop"]
    assert [ast.unparse(statement) for statement in stop.body] == [
        "self._stop_event.set()",
        "await self._explicit_stop_shutdown()",
        "self._set_health(RuntimePhase.STOPPED, healthy=True)",
        "return self._health",
    ]
