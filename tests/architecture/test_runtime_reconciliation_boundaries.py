from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
RUNTIME_ROOT = SOURCE_ROOT / "runtime"
RECONCILIATION_COORDINATOR = (
    RUNTIME_ROOT / "reconciliation_coordinator.py"
)
RECOVERY_COORDINATOR = RUNTIME_ROOT / "recovery_coordinator.py"
STARTUP_COORDINATOR = RUNTIME_ROOT / "startup_phase_coordinator.py"
RUNNER = RUNTIME_ROOT / "runner.py"

PLAN_FIELDS = [
    "resolve_service",
    "validate_snapshots",
    "begin_reconciliation",
    "apply_legacy_adoptions",
    "invoke_service",
    "handle_report",
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


def test_reconciliation_coordinator_and_plan_have_single_definitions() -> None:
    definitions: dict[str, list[str]] = {
        "RuntimeReconciliationCoordinator": [],
        "RuntimeReconciliationPlan": [],
    }
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in definitions:
                definitions[node.name].append(
                    path.relative_to(PROJECT_ROOT).as_posix()
                )

    assert definitions == {
        "RuntimeReconciliationCoordinator": [
            "src/runtime/reconciliation_coordinator.py"
        ],
        "RuntimeReconciliationPlan": [
            "src/runtime/reconciliation_coordinator.py"
        ],
    }


def test_reconciliation_coordinator_has_only_generic_dependencies() -> None:
    assert _imports(RECONCILIATION_COORDINATOR) <= {
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
        "src.runtime.recovery_coordinator",
        "src.order_management.reconciliation",
        "src.platform",
        "src.strategy",
        "src.signals",
    }
    assert forbidden.isdisjoint(_imports(RECONCILIATION_COORDINATOR))


def test_plan_is_frozen_dataclass_with_callback_fields_only() -> None:
    plan = _class(RECONCILIATION_COORDINATOR, "RuntimeReconciliationPlan")
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
        RECONCILIATION_COORDINATOR,
        "RuntimeReconciliationCoordinator",
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
        "if service is None:\n    return",
        "plan.validate_snapshots(snapshots)",
        "plan.begin_reconciliation(snapshots)",
        "plan.apply_legacy_adoptions(service)",
        "report = await plan.invoke_service(service, snapshots)",
        "plan.handle_report(report)",
    ]
    assert not any(
        isinstance(node, (ast.Try, ast.TryStar, ast.Raise))
        for node in ast.walk(execute)
    )


def test_coordinator_has_no_concurrency_logging_or_business_concepts() -> None:
    tree = _tree(RECONCILIATION_COORDINATOR)
    forbidden = {
        "asyncio",
        "gather",
        "create_task",
        "LiveStateReconciliationService",
        "PlatformSnapshot",
        "LiveRuntimeError",
        "ReconciliationAction",
        "ReconciliationVerdict",
        "PositionPlan",
        "Strategy",
        "AppConfig",
        "OKX",
        "Binance",
        "legacy_adoptions",
        "stale_plans_closed",
        "fake_order_refs",
        "unresolved_follower_positions",
        "alerts",
        "logger",
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


def test_runner_selects_and_writes_back_one_reconciliation_coordinator() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    injected = _assignment(
        initializer,
        "injected_reconciliation_coordinator",
    )
    selected = _assignment(initializer, "self._reconciliation_coordinator")
    writeback = _assignment(
        initializer,
        "self.services['reconciliation_coordinator']",
    )
    assert ast.unparse(injected.value) == (
        "self.services.get('reconciliation_coordinator')"
    )
    assert isinstance(selected.value, ast.IfExp)
    assert ast.unparse(selected.value.test) == (
        "injected_reconciliation_coordinator is not None"
    )
    assert ast.unparse(selected.value.body) == (
        "injected_reconciliation_coordinator"
    )
    assert ast.unparse(selected.value.orelse) == (
        "RuntimeReconciliationCoordinator()"
    )
    assert ast.unparse(writeback.value) == "self._reconciliation_coordinator"
    factories = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeReconciliationCoordinator"
    ]
    assert len(factories) == 1
    assert _calls(initializer, "execute") == []
    assert _calls(initializer, "_get_reconciliation_service") == []
    assert _calls(initializer, "_get_position_plan_store") == []
    assert _calls(initializer, "_get_order_journal") == []


def test_runner_run_reconciliation_only_builds_plan_and_delegates() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    run_reconciliation = methods["_run_reconciliation"]
    assert len(run_reconciliation.body) == 1
    execute_calls = [
        call
        for call in _calls(run_reconciliation, "execute")
        if ast.unparse(call.func.value) == "self._reconciliation_coordinator"
    ]
    assert len(execute_calls) == 1
    assert ast.unparse(execute_calls[0].args[0]) == "snapshots"
    plan = execute_calls[0].args[1]
    assert isinstance(plan, ast.Call)
    assert ast.unparse(plan.func) == "RuntimeReconciliationPlan"
    assert [keyword.arg for keyword in plan.keywords] == PLAN_FIELDS
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in plan.keywords
    } == {
        "resolve_service": "self._get_reconciliation_service",
        "validate_snapshots": (
            "self._validate_startup_reconciliation_snapshots"
        ),
        "begin_reconciliation": "self._log_startup_reconciliation_begin",
        "apply_legacy_adoptions": (
            "self._apply_startup_legacy_stop_adoptions"
        ),
        "invoke_service": "self._invoke_startup_reconciliation_service",
        "handle_report": "self._handle_startup_reconciliation_report",
    }
    assert not any(
        isinstance(node, (ast.Lambda, ast.Try, ast.TryStar))
        for node in ast.walk(run_reconciliation)
    )


def test_runner_retains_service_construction_and_business_rules() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    get_service = ast.unparse(methods["_get_reconciliation_service"])
    assert "LiveStateReconciliationService" in get_service
    assert "position_plan_store=self._get_position_plan_store()" in get_service
    assert "order_journal=self._get_order_journal()" in get_service
    assert "state_store=self.context.state_store" in get_service
    assert "alert_sink=self.context.alerts" in get_service

    validate = ast.unparse(
        methods["_validate_startup_reconciliation_snapshots"]
    )
    assert "expected = len(self.app_config.exchanges)" in validate
    assert "sorted" in validate
    assert "LiveRuntimeError" in validate

    legacy = ast.unparse(methods["_apply_startup_legacy_stop_adoptions"])
    assert "StrategyStopAdoptionProvider" in legacy
    assert "pending_stop_adoptions" in legacy
    assert "clear_pending_stop_adoptions" in legacy
    assert "ReconciliationAction" in legacy
    assert "int(time.time() * 1000)" in legacy
    assert "service._apply_actions" in legacy

    report = ast.unparse(methods["_handle_startup_reconciliation_report"])
    for token in (
        "stale_plans_closed",
        "fake_order_refs_found",
        "unresolved_follower_positions",
        "self.context.alerts.emit",
        "report.verdict.value",
        "LiveRuntimeError",
        "pass_with_cleanup",
    ):
        assert token in report


def test_service_definition_and_startup_entrypoint_remain_owned() -> None:
    definitions = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if (
                isinstance(node, ast.ClassDef)
                and node.name == "LiveStateReconciliationService"
            ):
                definitions.append(path.relative_to(PROJECT_ROOT).as_posix())
    assert definitions == [
        "src/order_management/reconciliation/service.py"
    ]

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
    assert callbacks["run_reconciliation"] == "self._run_reconciliation"


def test_other_runtime_boundaries_do_not_depend_on_coordinator() -> None:
    paths = (
        RECOVERY_COORDINATOR,
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
            module == "src.runtime.reconciliation_coordinator"
            or module.startswith("src.runtime.reconciliation_coordinator.")
            for module in _imports(path)
        )
