from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
RUNTIME_ROOT = SOURCE_ROOT / "runtime"
RECOVERY_COORDINATOR = RUNTIME_ROOT / "recovery_coordinator.py"
RECOVERY_SERVICE = RUNTIME_ROOT / "recovery" / "service.py"
RUNNER = RUNTIME_ROOT / "runner.py"
STARTUP_COORDINATOR = RUNTIME_ROOT / "startup_phase_coordinator.py"

PLAN_FIELDS = [
    "resolve_service",
    "fallback_snapshots",
    "invoke_service",
    "record_run",
    "validate_report",
    "partition_signals",
    "capture_failure_counts",
    "execute_stop_signals",
    "validate_stop_execution",
    "validate_post_execution_protection",
    "execute_other_signals",
    "finalize_report",
]


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


def test_recovery_coordinator_and_plan_have_single_definitions() -> None:
    definitions: dict[str, list[str]] = {
        "RuntimeRecoveryCoordinator": [],
        "RuntimeRecoveryPlan": [],
    }
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in definitions:
                definitions[node.name].append(
                    path.relative_to(PROJECT_ROOT).as_posix()
                )

    assert definitions == {
        "RuntimeRecoveryCoordinator": ["src/runtime/recovery_coordinator.py"],
        "RuntimeRecoveryPlan": ["src/runtime/recovery_coordinator.py"],
    }


def test_recovery_coordinator_has_only_generic_dependencies() -> None:
    assert _imports(RECOVERY_COORDINATOR) <= {
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
        "src.runtime.recovery",
        "src.runtime.models",
        "src.runtime.health_state",
        "src.runtime.startup_phase_coordinator",
        "src.order_management",
        "src.reconcile",
        "src.platform",
        "src.strategy",
        "src.signals",
    }
    assert forbidden.isdisjoint(_imports(RECOVERY_COORDINATOR))


def test_plan_is_frozen_dataclass_with_callback_fields_only() -> None:
    plan = _class(RECOVERY_COORDINATOR, "RuntimeRecoveryPlan")
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


def test_coordinator_is_stateless_and_executes_exact_flow() -> None:
    coordinator = _class(
        RECOVERY_COORDINATOR,
        "RuntimeRecoveryCoordinator",
    )
    assert not any(
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
        for node in ast.walk(coordinator)
    )
    execute = _methods(coordinator)["execute"]
    assert [ast.unparse(statement) for statement in execute.body] == [
        "service = plan.resolve_service()",
        "if service is None:\n    return plan.fallback_snapshots()",
        "report = await plan.invoke_service(service)",
        "plan.record_run()",
        "plan.validate_report(report)",
        "stop_signals, other_signals = plan.partition_signals(report)",
        "if stop_signals:\n    failure_counts = plan.capture_failure_counts()\n    await plan.execute_stop_signals(stop_signals)\n    plan.validate_stop_execution(failure_counts)\n    await plan.validate_post_execution_protection()",
        "if other_signals:\n    await plan.execute_other_signals(other_signals)",
        "return plan.finalize_report(report)",
    ]
    assert not any(
        isinstance(node, (ast.Try, ast.TryStar, ast.Raise))
        for node in ast.walk(execute)
    )


def test_coordinator_has_no_concurrency_logging_or_business_concepts() -> None:
    tree = _tree(RECOVERY_COORDINATOR)
    forbidden = {
        "asyncio",
        "gather",
        "create_task",
        "RuntimeRecoveryService",
        "RecoveryReport",
        "PlatformSnapshot",
        "TradeSignal",
        "SignalAction",
        "LiveRuntimeError",
        "Strategy",
        "AppConfig",
        "OKX",
        "Binance",
        "failed_intents",
        "partial_failures",
        "logger",
        "alerts",
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


def test_runner_selects_and_writes_back_one_recovery_coordinator() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    injected = _assignment(initializer, "injected_recovery_coordinator")
    selected = _assignment(initializer, "self._recovery_coordinator")
    writeback = _assignment(
        initializer,
        "self.services['recovery_coordinator']",
    )
    assert ast.unparse(injected.value) == (
        "self.services.get('recovery_coordinator')"
    )
    assert isinstance(selected.value, ast.IfExp)
    assert ast.unparse(selected.value.test) == (
        "injected_recovery_coordinator is not None"
    )
    assert ast.unparse(selected.value.body) == "injected_recovery_coordinator"
    assert ast.unparse(selected.value.orelse) == "RuntimeRecoveryCoordinator()"
    assert ast.unparse(writeback.value) == "self._recovery_coordinator"
    factories = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeRecoveryCoordinator"
    ]
    assert len(factories) == 1
    assert _calls(initializer, "execute") == []
    assert _calls(initializer, "_get_recovery_service") == []


def test_runner_run_recovery_only_builds_plan_and_delegates() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    run_recovery = methods["_run_recovery"]
    assert len(run_recovery.body) == 1
    execute_calls = [
        call
        for call in _calls(run_recovery, "execute")
        if ast.unparse(call.func.value) == "self._recovery_coordinator"
    ]
    assert len(execute_calls) == 1
    plan = execute_calls[0].args[0]
    assert isinstance(plan, ast.Call)
    assert ast.unparse(plan.func) == "RuntimeRecoveryPlan"
    assert [keyword.arg for keyword in plan.keywords] == PLAN_FIELDS
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in plan.keywords
    } == {
        "resolve_service": "self._get_recovery_service",
        "fallback_snapshots": "self._recovery_fallback_snapshots",
        "invoke_service": "self._invoke_recovery_service",
        "record_run": "self._record_recovery_run",
        "validate_report": "self._validate_runtime_recovery_report",
        "partition_signals": "self._partition_recovery_signals",
        "capture_failure_counts": "self._capture_recovery_failure_counts",
        "execute_stop_signals": "self._execute_recovery_stop_signals",
        "validate_stop_execution": "self._validate_recovery_stop_execution",
        "validate_post_execution_protection": (
            "self._validate_post_execution_stop_protection"
        ),
        "execute_other_signals": "self._execute_recovery_other_signals",
        "finalize_report": "self._finalize_recovery_report",
    }
    assert not any(isinstance(node, ast.Lambda) for node in ast.walk(run_recovery))


def test_runner_retains_service_construction_and_recovery_business_rules() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    get_service = methods["_get_recovery_service"]
    source = ast.unparse(get_service)
    assert "RuntimeRecoveryService" in source
    assert "RecoveryExchangeContext" in source
    assert "zip(accounts, clients, strict=False)" in source
    assert "state_store=self.context.state_store" in source
    assert "self._get_order_journal()" in source
    assert "self._get_position_plan_store()" in source

    required = {
        "_recovery_fallback_snapshots",
        "_invoke_recovery_service",
        "_record_recovery_run",
        "_validate_runtime_recovery_report",
        "_partition_recovery_signals",
        "_capture_recovery_failure_counts",
        "_execute_recovery_stop_signals",
        "_validate_recovery_stop_execution",
        "_validate_post_execution_stop_protection",
        "_execute_recovery_other_signals",
        "_finalize_recovery_report",
        "_validate_recovery_protection_postcondition",
        "_execute_signals",
        "_run_reconciliation",
    }
    assert required <= set(methods)
    partition = ast.unparse(methods["_partition_recovery_signals"])
    assert "SignalAction.PLACE_STOP_LOSS_LONG" in partition
    assert "SignalAction.PLACE_STOP_LOSS_SHORT" in partition
    finalize = ast.unparse(methods["_finalize_recovery_report"])
    assert "self._last_snapshots" in finalize
    assert "self._last_snapshot" in finalize


def test_recovery_service_definition_and_startup_boundary_remain_owned() -> None:
    definitions = []
    for path in sorted(RUNTIME_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name == "RuntimeRecoveryService":
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())
    assert definitions == ["src/runtime/recovery/service.py"]

    startup = _methods(_class(RUNNER, "LiveRuntimeRunner"))["_startup"]
    plans = [
        node
        for node in ast.walk(startup)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeStartupPhasePlan"
    ]
    assert len(plans) == 1
    callbacks = {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in plans[0].keywords
    }
    assert callbacks["run_recovery"] == "self._run_recovery"
    assert "run_reconciliation" not in PLAN_FIELDS


def test_other_runtime_boundaries_do_not_depend_on_recovery_coordinator() -> None:
    paths = (
        STARTUP_COORDINATOR,
        RUNTIME_ROOT / "shutdown_coordinator.py",
        RUNTIME_ROOT / "health_state.py",
        RUNTIME_ROOT / "heartbeat.py",
        RUNTIME_ROOT / "sync_lifecycle.py",
        RUNTIME_ROOT / "sync_services.py",
        RUNTIME_ROOT / "persistence.py",
        RUNTIME_ROOT / "persistence_service.py",
    )
    for path in paths:
        assert not any(
            module == "src.runtime.recovery_coordinator"
            or module.startswith("src.runtime.recovery_coordinator.")
            for module in _imports(path)
        )
