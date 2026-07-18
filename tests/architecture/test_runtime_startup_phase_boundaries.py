from __future__ import annotations

import ast
from pathlib import Path

from tests.runtime_surface_ast import runtime_surface_class


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
STARTUP_COORDINATOR = SOURCE_ROOT / "runtime" / "startup_phase_coordinator.py"
RUNNER = SOURCE_ROOT / "runtime" / "runner.py"
SHUTDOWN_COORDINATOR = SOURCE_ROOT / "runtime" / "shutdown_coordinator.py"
HEALTH_STATE = SOURCE_ROOT / "runtime" / "health_state.py"
HEARTBEAT = SOURCE_ROOT / "runtime" / "heartbeat.py"
SYNC_LIFECYCLE = SOURCE_ROOT / "runtime" / "sync_lifecycle.py"
PERSISTENCE = SOURCE_ROOT / "runtime" / "persistence.py"
PERSISTENCE_SERVICE = SOURCE_ROOT / "runtime" / "persistence_service.py"
RANGE_SPEED_RUNTIME = (
    SOURCE_ROOT / "runtime" / "market_data" / "range_speed_runtime.py"
)


PLAN_FIELDS = [
    "initialize_rangebar_trust_window",
    "enter_warming_up",
    "bootstrap_account_config",
    "check_position_mode",
    "run_warmup",
    "warmup_range_speed_history",
    "handle_range_speed_history_result",
    "check_feature_backfills",
    "enter_catching_up",
    "run_recovery",
    "run_post_recovery_checks",
    "run_reconciliation",
    "call_strategy_on_start",
    "evaluate_startup_catchup",
    "finish_range_speed_warmup",
    "start_heartbeat",
    "start_range_speed_background_services",
    "enter_running",
]


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


def test_startup_coordinator_and_plan_have_single_definitions() -> None:
    definitions: dict[str, list[str]] = {
        "RuntimeStartupPhaseCoordinator": [],
        "RuntimeStartupPhasePlan": [],
    }
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in definitions:
                definitions[node.name].append(
                    path.relative_to(PROJECT_ROOT).as_posix()
                )

    assert definitions == {
        "RuntimeStartupPhaseCoordinator": [
            "src/runtime/startup_phase_coordinator.py"
        ],
        "RuntimeStartupPhasePlan": [
            "src/runtime/startup_phase_coordinator.py"
        ],
    }


def test_startup_coordinator_module_has_only_generic_dependencies() -> None:
    assert _imports(STARTUP_COORDINATOR) <= {
        "__future__",
        "collections.abc",
        "dataclasses",
        "typing",
    }
    forbidden = {
        "asyncio",
        "time",
        "logging",
        "src.runtime.runner",
        "src.runtime.models",
        "src.runtime.heartbeat",
        "src.runtime.health_state",
        "src.runtime.recovery",
        "src.runtime.startup_catchup",
        "src.app",
        "src.market_data",
        "src.order_management",
        "src.platform",
        "src.strategy",
        "src.signals",
    }
    assert forbidden.isdisjoint(_imports(STARTUP_COORDINATOR))


def test_plan_is_frozen_dataclass_with_callback_fields_only() -> None:
    plan = _class(STARTUP_COORDINATOR, "RuntimeStartupPhasePlan")
    assert len(plan.decorator_list) == 1
    decorator = plan.decorator_list[0]
    assert isinstance(decorator, ast.Call)
    assert ast.unparse(decorator.func) == "dataclass"
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in decorator.keywords
    } == {"frozen": "True"}
    assert [
        node.target.id
        for node in plan.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
    ] == PLAN_FIELDS
    assert not any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for node in plan.body
    )


def test_coordinator_is_stateless_and_executes_exact_sequence() -> None:
    coordinator = _class(
        STARTUP_COORDINATOR,
        "RuntimeStartupPhaseCoordinator",
    )
    assert not any(
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
        for node in ast.walk(coordinator)
    )
    execute = _methods(coordinator)["execute"]
    assert [ast.unparse(statement) for statement in execute.body] == [
        "plan.initialize_rangebar_trust_window()",
        "plan.enter_warming_up()",
        "await plan.bootstrap_account_config()",
        "await plan.check_position_mode()",
        "await plan.run_warmup()",
        "loaded_range_speed_history = await plan.warmup_range_speed_history()",
        "plan.handle_range_speed_history_result(loaded_range_speed_history)",
        "await plan.check_feature_backfills()",
        "plan.enter_catching_up()",
        "snapshots = await plan.run_recovery()",
        "await plan.run_post_recovery_checks(snapshots)",
        "await plan.run_reconciliation(snapshots)",
        "first_snapshot = snapshots[0]",
        "await plan.call_strategy_on_start(first_snapshot)",
        "await plan.evaluate_startup_catchup(first_snapshot)",
        "await plan.finish_range_speed_warmup()",
        "plan.start_heartbeat()",
        "plan.start_range_speed_background_services()",
        "plan.enter_running()",
        "return snapshots",
    ]
    assert not any(
        isinstance(node, (ast.Try, ast.TryStar, ast.Raise))
        for node in ast.walk(execute)
    )


def test_coordinator_has_no_concurrency_or_business_rules() -> None:
    tree = _tree(STARTUP_COORDINATOR)
    forbidden = {
        "asyncio",
        "gather",
        "create_task",
        "RuntimePhase",
        "AppConfig",
        "Heartbeat",
        "logger",
        "alerts",
        "new_entries_blocked",
        "OKX",
        "Binance",
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
    assert not any(isinstance(node, ast.If) for node in ast.walk(tree))


def test_runner_selects_and_writes_back_one_startup_coordinator() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    injected = _assignment(initializer, "injected_startup_phase_coordinator")
    selected = _assignment(initializer, "self._startup_phase_coordinator")
    writeback = _assignment(
        initializer,
        "self.runtime_services.startup_phase_coordinator",
    )
    assert ast.unparse(injected.value) == (
        "self.runtime_services.startup_phase_coordinator"
    )
    assert isinstance(selected.value, ast.IfExp)
    assert ast.unparse(selected.value.test) == (
        "injected_startup_phase_coordinator is not None"
    )
    assert ast.unparse(selected.value.body) == (
        "injected_startup_phase_coordinator"
    )
    assert ast.unparse(selected.value.orelse) == (
        "RuntimeStartupPhaseCoordinator()"
    )
    assert ast.unparse(writeback.value) == "self._startup_phase_coordinator"
    factories = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeStartupPhaseCoordinator"
    ]
    assert len(factories) == 1
    assert _calls(initializer, "execute") == []


def test_runner_startup_only_logs_builds_plan_and_delegates() -> None:
    startup = _methods(_class(RUNNER, "LiveRuntimeRunner"))["_startup"]
    assert len(startup.body) == 4
    assert ast.unparse(startup.body[0]) == "self._strategy_capabilities()"
    assert ast.unparse(startup.body[1]) == (
        "logger.info('Live runtime startup phase started')"
    )
    assert ast.unparse(startup.body[3]) == (
        "logger.info('Live runtime startup phase completed')"
    )
    execute_calls = [
        call
        for call in _calls(startup, "execute")
        if ast.unparse(call.func.value) == "self._startup_phase_coordinator"
    ]
    assert len(execute_calls) == 1
    assert len(execute_calls[0].args) == 1
    plan = execute_calls[0].args[0]
    assert isinstance(plan, ast.Call)
    assert ast.unparse(plan.func) == "RuntimeStartupPhasePlan"
    assert [keyword.arg for keyword in plan.keywords] == PLAN_FIELDS
    assert not any(isinstance(node, ast.Lambda) for node in ast.walk(startup))
    allowed_call_functions = {
        "logger.info",
        "self._strategy_capabilities",
        "self._startup_phase_coordinator.execute",
        "RuntimeStartupPhasePlan",
    }
    assert {
        ast.unparse(node.func)
        for node in ast.walk(startup)
        if isinstance(node, ast.Call)
    } <= allowed_call_functions


def test_runner_wrappers_retain_health_business_values() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    expected = {
        "_enter_startup_warming_up": (
            "RuntimePhase.WARMING_UP",
            {"healthy": "True"},
        ),
        "_enter_startup_catching_up": (
            "RuntimePhase.CATCHING_UP",
            {"healthy": "True", "warmup_complete": "True"},
        ),
        "_enter_startup_running": (
            "RuntimePhase.RUNNING",
            {
                "healthy": "True",
                "warmup_complete": "True",
                "caught_up": "True",
            },
        ),
    }
    for name, (phase, keywords) in expected.items():
        calls = _calls(methods[name], "_set_health")
        assert len(calls) == 1
        assert [ast.unparse(arg) for arg in calls[0].args] == [phase]
        assert {
            keyword.arg: ast.unparse(keyword.value)
            for keyword in calls[0].keywords
        } == keywords


def test_range_speed_component_and_runner_retain_business_conditions() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    range_result = methods["_handle_startup_range_speed_history_result"]
    assert len(_calls(range_result, "warn_if_insufficient")) == 1

    speed_methods = _methods(
        _class(RANGE_SPEED_RUNTIME, "RangeSpeedWarmup")
    )
    warning_method = speed_methods["warn_if_insufficient"]
    conditions = [
        node for node in ast.walk(warning_method) if isinstance(node, ast.If)
    ]
    assert len(conditions) == 1
    assert ast.unparse(conditions[0].test) == (
        "self.min_periods > 0 and loaded < self.min_periods"
    )
    warnings = _calls(warning_method, "warning")
    assert len(warnings) == 1
    assert ast.unparse(warnings[0].func.value) == "logger"

    post_recovery = methods["_run_startup_post_recovery_checks"]
    blocked = [node for node in ast.walk(post_recovery) if isinstance(node, ast.If)]
    assert len(blocked) == 1
    assert ast.unparse(blocked[0].test) == (
        "self._account_config_new_entries_blocked"
    )
    assert len(_calls(post_recovery, "_recheck_account_config_after_recovery")) == 1


def test_runner_retains_heartbeat_id_and_business_implementations() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    heartbeat = methods["_start_runtime_heartbeat"]
    starts = [
        call
        for call in _calls(heartbeat, "start")
        if ast.unparse(call.func.value) == "self._heartbeat_service"
    ]
    assert len(starts) == 1
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in starts[0].keywords
    } == {
        "runtime_id": "f'{self.app_config.strategy}::{self.app_config.symbol}'"
    }
    assert {
        "_run_recovery",
        "_run_reconciliation",
        "_call_on_start",
        "_evaluate_startup_catchup_once",
        "_bootstrap_account_config_if_enabled",
        "_run_warmup",
    } <= set(methods)


def test_run_and_shutdown_order_remain_runner_owned() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    run = methods["run"]
    try_node = next(node for node in run.body if isinstance(node, ast.Try))
    startup = _calls(try_node, "_startup")[0]
    producers = _calls(try_node, "_start_producers")[0]
    sync_tasks = _calls(try_node, "_start_sync_tasks")[0]
    consume = _calls(try_node, "_consume_market_events")[0]
    assert startup.lineno < producers.lineno < sync_tasks.lineno < consume.lineno
    assert len(try_node.finalbody) == 1
    assert ast.unparse(try_node.finalbody[0]) == (
        "await self._run_finally_shutdown()"
    )


def test_other_runtime_boundaries_do_not_depend_on_startup_coordinator() -> None:
    for path in (
        SHUTDOWN_COORDINATOR,
        HEALTH_STATE,
        HEARTBEAT,
        SYNC_LIFECYCLE,
        PERSISTENCE,
        PERSISTENCE_SERVICE,
    ):
        assert not any(
            module == "src.runtime.startup_phase_coordinator"
            or module.startswith("src.runtime.startup_phase_coordinator.")
            for module in _imports(path)
        )
