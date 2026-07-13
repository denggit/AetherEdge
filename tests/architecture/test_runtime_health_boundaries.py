from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
HEALTH_STATE = SOURCE_ROOT / "runtime" / "health_state.py"
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


def _self_assignments(node: ast.AST) -> set[str]:
    assigned = {
        target.attr
        for child in ast.walk(node)
        if isinstance(child, ast.Assign)
        for target in child.targets
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    }
    assigned.update(
        child.target.attr
        for child in ast.walk(node)
        if isinstance(child, ast.AnnAssign)
        and isinstance(child.target, ast.Attribute)
        and isinstance(child.target.value, ast.Name)
        and child.target.value.id == "self"
    )
    return assigned


def _attribute_calls(node: ast.AST, name: str) -> list[ast.Call]:
    return [
        child
        for child in ast.walk(node)
        if isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and child.func.attr == name
    ]


def test_runtime_health_state_has_one_definition() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "RuntimeHealthState"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert definitions == ["src/runtime/health_state.py"]


def test_health_state_has_only_model_and_generic_dependencies() -> None:
    assert _imports(HEALTH_STATE) <= {
        "__future__",
        "collections.abc",
        "typing",
        "src.runtime.models",
    }
    forbidden_prefixes = (
        "asyncio",
        "time",
        "src.runtime.runner",
        "src.runtime.heartbeat",
        "src.app",
        "src.market_data",
        "src.order_management",
        "src.platform",
        "src.strategy",
        "src.signals",
    )
    assert not any(
        module == prefix or module.startswith(f"{prefix}.")
        for module in _imports(HEALTH_STATE)
        for prefix in forbidden_prefixes
    )


def test_health_state_owns_only_current_snapshot_and_no_runtime_services() -> None:
    state_class = _class(HEALTH_STATE, "RuntimeHealthState")
    assert _self_assignments(state_class) == {"_current"}

    forbidden_names = {
        "LiveRuntimeRunner",
        "AppContext",
        "AppConfig",
        "LiveRuntimeConfig",
        "Strategy",
        "RuntimeHeartbeatService",
        "stop_event",
        "alerts",
        "stats",
        "services",
        "clock",
        "logger",
        "asyncio",
        "time",
    }
    used = {
        node.id
        for node in ast.walk(state_class)
        if isinstance(node, ast.Name) and node.id in forbidden_names
    }
    used.update(
        node.attr
        for node in ast.walk(state_class)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_names
    )
    assert used == set()

    phase_members = {
        node.attr
        for node in ast.walk(state_class)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "RuntimePhase"
    }
    assert phase_members == set()
    assert not any(isinstance(node, ast.Raise) for node in ast.walk(state_class))
    phase_comparisons = [
        node
        for node in ast.walk(state_class)
        if isinstance(node, ast.Compare)
        and any(
            isinstance(child, ast.Name) and child.id == "phase"
            or isinstance(child, ast.Attribute)
            and isinstance(child.value, ast.Name)
            and child.value.id == "RuntimePhase"
            for child in ast.walk(node)
        )
    ]
    assert phase_comparisons == []


def test_health_state_current_is_read_only_and_update_replaces_snapshot() -> None:
    methods = _methods(_class(HEALTH_STATE, "RuntimeHealthState"))
    current = methods["current"]
    assert [ast.unparse(item) for item in current.decorator_list] == [
        "property"
    ]
    assert len(current.body) == 1
    assert isinstance(current.body[0], ast.Return)
    assert ast.unparse(current.body[0].value) == "self._current"

    setters = {
        ast.unparse(decorator)
        for node in _class(HEALTH_STATE, "RuntimeHealthState").body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Attribute)
        and decorator.attr == "setter"
    }
    assert "current.setter" not in setters

    update = methods["update"]
    runtime_health_calls = [
        node
        for node in ast.walk(update)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "RuntimeHealth"
    ]
    assert len(runtime_health_calls) == 1
    assert _self_assignments(update) == {"_current"}
    assert isinstance(update.body[-1], ast.Return)
    assert ast.unparse(update.body[-1].value) == "updated"


def test_runner_constructs_and_exposes_health_state_without_updating_it() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    assignments = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Assign) and len(node.targets) == 1
    ]
    initial_assignment = next(
        node
        for node in assignments
        if ast.unparse(node.targets[0]) == "initial_health"
    )
    assert isinstance(initial_assignment.value, ast.Call)
    assert ast.unparse(initial_assignment.value.func) == "RuntimeHealth"
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in initial_assignment.value.keywords
    } == {
        "phase": "RuntimePhase.CREATED",
        "warmup_complete": "not self.runtime_config.warmup_enabled",
        "caught_up": "not self.runtime_config.warmup_enabled",
        "metadata": (
            "{'runtime_mode': self.runtime_config.mode.value, "
            "'strategy': self.app_config.strategy}"
        ),
    }

    state_assignment = next(
        node
        for node in assignments
        if ast.unparse(node.targets[0]) == "self._runtime_health_state"
    )
    state_factories = [
        node
        for node in ast.walk(state_assignment.value)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeHealthState"
    ]
    assert len(state_factories) == 1
    assert [ast.unparse(arg) for arg in state_factories[0].args] == [
        "initial_health"
    ]
    assert "injected_health_state" in ast.unparse(state_assignment.value)

    writeback = next(
        node
        for node in assignments
        if ast.unparse(node.targets[0])
        == "self.services['runtime_health_state']"
    )
    compatibility = next(
        node
        for node in assignments
        if ast.unparse(node.targets[0]) == "self._health"
    )
    assert ast.unparse(writeback.value) == "self._runtime_health_state"
    assert ast.unparse(compatibility.value) == (
        "self._runtime_health_state.current"
    )
    assert initial_assignment.lineno < state_assignment.lineno
    assert state_assignment.lineno < writeback.lineno < compatibility.lineno
    assert _attribute_calls(initializer, "update") == []
    assert _attribute_calls(initializer, "start") == []


def test_runner_set_health_is_a_single_exact_delegate() -> None:
    method = _methods(_class(RUNNER, "LiveRuntimeRunner"))["_set_health"]
    assert len(method.body) == 1
    assignment = method.body[0]
    assert isinstance(assignment, ast.Assign)
    assert [ast.unparse(target) for target in assignment.targets] == [
        "self._health"
    ]
    assert isinstance(assignment.value, ast.Call)
    call = assignment.value
    assert ast.unparse(call.func) == "self._runtime_health_state.update"
    assert [ast.unparse(argument) for argument in call.args] == ["phase"]
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in call.keywords
    } == {
        "healthy": "healthy",
        "warmup_complete": "warmup_complete",
        "caught_up": "caught_up",
        "last_market_event_time_ms": "last_market_event_time_ms",
        "error": "error",
        "metadata": "metadata",
    }
    assert sum(
        1
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "self._runtime_health_state.update"
    ) == 1
    assert not any(
        isinstance(node, ast.Call) and ast.unparse(node.func) == "RuntimeHealth"
        for node in ast.walk(method)
    )
    forbidden = {"logger", "alerts", "_heartbeat_service"}
    assert not any(
        isinstance(node, ast.Name) and node.id in forbidden
        or isinstance(node, ast.Attribute) and node.attr in forbidden
        for node in ast.walk(method)
    )


def test_runner_retains_health_call_ownership_and_compatibility_reader() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    call_counts = {
        name: len(_attribute_calls(method, "_set_health"))
        for name, method in methods.items()
        if _attribute_calls(method, "_set_health")
    }
    assert call_counts == {
        "run": 2,
        "start": 1,
        "stop": 1,
        "process_market_event": 1,
        "_startup": 3,
        "_check_startup_feature_backfills": 1,
        "_check_strategy_position_mode_requirements": 1,
        "_record_feature_backfill_result": 1,
        "_record_order_results": 2,
    }

    health = methods["health"]
    assert isinstance(health, ast.AsyncFunctionDef)
    assert len(health.body) == 1
    assert isinstance(health.body[0], ast.Return)
    assert ast.unparse(health.body[0].value) == "self._health"


def test_market_health_throttle_and_heartbeat_calls_remain_in_runner() -> None:
    runner_class = _class(RUNNER, "LiveRuntimeRunner")
    methods = _methods(runner_class)
    market = methods["process_market_event"]
    market_source = ast.unparse(market)
    assert "time.time()" in market_source
    assert "self._last_trade_health_update_ms" in market_source
    assert "should_update_health" in market_source
    assert len(_attribute_calls(market, "note_market_event")) == 1
    assert len(_attribute_calls(market, "_set_health")) == 1

    heartbeat_calls = {
        node.func.attr
        for node in ast.walk(runner_class)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr
        in {
            "note_market_event",
            "note_closed_bar",
            "read_previous",
            "start",
            "run_periodic",
        }
    }
    assert {
        "note_market_event",
        "note_closed_bar",
        "read_previous",
        "start",
        "run_periodic",
    } <= heartbeat_calls


def test_run_start_and_stop_keep_existing_health_and_shutdown_order() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))

    start = methods["start"]
    assert len(start.body) == 2
    assert ast.unparse(start.body[0]).startswith(
        "self._set_health(RuntimePhase.RUNNING"
    )
    assert ast.unparse(start.body[1]) == "return self._health"

    stop = methods["stop"]
    stop_statements = [ast.unparse(statement) for statement in stop.body]
    assert stop_statements == [
        "self._stop_event.set()",
        "await self._stop_range_speed_background_services()",
        "await self._stop_producers()",
        "await self._stop_live_persistence_writer()",
        "await self._stop_range_repair_journal_writer()",
        "self._set_health(RuntimePhase.STOPPED, healthy=True)",
        "return self._health",
    ]

    run = methods["run"]
    try_node = next(node for node in run.body if isinstance(node, ast.Try))
    stopped_call = next(
        node
        for node in ast.walk(try_node)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "self._set_health"
        and node.args
        and ast.unparse(node.args[0]) == "RuntimePhase.STOPPED"
    )
    error_call = next(
        node
        for node in ast.walk(try_node)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "self._set_health"
        and node.args
        and ast.unparse(node.args[0]) == "RuntimePhase.ERROR"
    )
    assert stopped_call.lineno < next(
        node.lineno
        for node in ast.walk(try_node)
        if isinstance(node, ast.Return)
        and ast.unparse(node.value) == "self.stats"
    )
    alert_emit = next(
        node
        for node in ast.walk(try_node)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "self.context.alerts.emit"
    )
    assert error_call.lineno < alert_emit.lineno

    assert [
        ast.unparse(statement.value.value.func)
        for statement in try_node.finalbody
        if isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Await)
        and isinstance(statement.value.value, ast.Call)
    ] == [
        "self._stop_range_speed_background_services",
        "self._stop_sync_tasks",
        "self._stop_producers",
        "self._stop_live_persistence_writer",
        "self._stop_range_repair_journal_writer",
        "self._stop_range_checkpoint_writer",
        "self.context.alerts.stop",
    ]
