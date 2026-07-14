from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src"
RUNTIME_ROOT = SOURCE_ROOT / "runtime"
SIGNAL_EXECUTION = RUNTIME_ROOT / "signal_execution_service.py"
RUNNER = RUNTIME_ROOT / "runner.py"

REQUEST_FIELDS = [
    "signals",
    "source",
    "event_time_ms",
    "metadata",
    "feedback_depth",
]
PLAN_FIELDS = [
    "prepare_signal",
    "create_intent",
    "execute_intent",
    "post_submit_sync",
    "handle_results",
    "post_order_sync",
    "process_feedback",
    "build_feedback_request",
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


def _ann_fields(class_node: ast.ClassDef) -> list[str]:
    return [
        node.target.id
        for node in class_node.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
    ]


def test_service_request_and_plan_have_single_definitions() -> None:
    names = {
        "RuntimeSignalExecutionService": [],
        "RuntimeSignalExecutionRequest": [],
        "RuntimeSignalExecutionPlan": [],
    }
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.ClassDef) and node.name in names:
                names[node.name].append(
                    path.relative_to(PROJECT_ROOT).as_posix()
                )
    expected = ["src/runtime/signal_execution_service.py"]
    assert names == {name: expected for name in names}


def test_signal_execution_module_has_only_generic_dependencies() -> None:
    assert _imports(SIGNAL_EXECUTION) <= {
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
        "src.runtime.orders",
        "src.runtime.sync_services",
        "src.order_management",
        "src.platform",
        "src.strategy",
        "src.signals",
    }
    assert forbidden.isdisjoint(_imports(SIGNAL_EXECUTION))


def test_request_and_plan_are_frozen_dataclasses_with_exact_fields() -> None:
    request = _class(SIGNAL_EXECUTION, "RuntimeSignalExecutionRequest")
    plan = _class(SIGNAL_EXECUTION, "RuntimeSignalExecutionPlan")
    for class_node, expected_fields in (
        (request, REQUEST_FIELDS),
        (plan, PLAN_FIELDS),
    ):
        assert len(class_node.decorator_list) == 1
        decorator = class_node.decorator_list[0]
        assert isinstance(decorator, ast.Call)
        assert ast.unparse(decorator.func) == "dataclass"
        assert {
            keyword.arg: ast.unparse(keyword.value)
            for keyword in decorator.keywords
        } == {"frozen": "True"}
        assert _ann_fields(class_node) == expected_fields
        assert not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            for node in class_node.body
        )
    request_defaults = {
        node.target.id: ast.unparse(node.value)
        for node in request.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.value is not None
    }
    assert request_defaults == {"metadata": "None", "feedback_depth": "0"}


def test_service_is_stateless_and_execute_has_exact_depth_first_flow() -> None:
    service = _class(SIGNAL_EXECUTION, "RuntimeSignalExecutionService")
    assert not any(
        isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign))
        and any(
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
            for target in (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
            )
        )
        for node in ast.walk(service)
    )
    execute = _methods(service)["execute"]
    assert len(execute.body) == 1
    loop = execute.body[0]
    assert isinstance(loop, ast.For)
    assert ast.unparse(loop.target) == "signal"
    assert ast.unparse(loop.iter) == "request.signals"
    assert [ast.unparse(statement) for statement in loop.body] == [
        "if not plan.prepare_signal(signal, request):\n    continue",
        "intent = plan.create_intent(signal, request)",
        "results = await plan.execute_intent(intent)",
        "await plan.post_submit_sync(signal, request)",
        "plan.handle_results(signal, results)",
        "await plan.post_order_sync(signal, request)",
        "follow_up = await plan.process_feedback(signal, results, request)",
        "if follow_up:\n    feedback_request = plan.build_feedback_request(signal, follow_up, request)\n    if feedback_request is not None:\n        await self.execute(feedback_request, plan)",
    ]
    recursive = [
        call
        for call in _calls(execute, "execute")
        if ast.unparse(call.func.value) == "self"
    ]
    assert len(recursive) == 1
    assert [ast.unparse(arg) for arg in recursive[0].args] == [
        "feedback_request",
        "plan",
    ]
    assert not any(
        isinstance(node, (ast.Try, ast.TryStar))
        for node in ast.walk(execute)
    )


def test_service_has_no_concurrency_logging_or_business_concepts() -> None:
    tree = _tree(SIGNAL_EXECUTION)
    forbidden = {
        "asyncio",
        "gather",
        "create_task",
        "TradeSignal",
        "SignalAction",
        "ExchangeOrderResult",
        "OrderIntent",
        "AppAlert",
        "LiveRuntimeError",
        "RuntimePhase",
        "OKX",
        "Binance",
        "dry_run",
        "feedback_depth_limit",
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


def test_runner_selects_and_writes_back_one_signal_execution_service() -> None:
    initializer = _methods(_class(RUNNER, "LiveRuntimeRunner"))["__init__"]
    injected = _assignment(initializer, "injected_signal_execution_service")
    selected = _assignment(initializer, "self._signal_execution_service")
    writeback = _assignment(
        initializer,
        "self.services['signal_execution_service']",
    )
    assert ast.unparse(injected.value) == (
        "self.services.get('signal_execution_service')"
    )
    assert isinstance(selected.value, ast.IfExp)
    assert ast.unparse(selected.value.test) == (
        "injected_signal_execution_service is not None"
    )
    assert ast.unparse(selected.value.body) == (
        "injected_signal_execution_service"
    )
    assert ast.unparse(selected.value.orelse) == "RuntimeSignalExecutionService()"
    assert ast.unparse(writeback.value) == "self._signal_execution_service"
    factories = [
        node
        for node in ast.walk(initializer)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "RuntimeSignalExecutionService"
    ]
    assert len(factories) == 1
    assert _calls(initializer, "execute") == []
    assert _calls(initializer, "_get_order_coordinator") == []
    assert _calls(initializer, "_get_order_sync_service") == []
    assert _calls(initializer, "_get_account_sync_service") == []
    assert _calls(initializer, "_get_order_journal") == []
    assert _calls(initializer, "_get_position_plan_store") == []


def test_runner_execute_signals_signature_and_delegate_are_exact() -> None:
    execute = _methods(_class(RUNNER, "LiveRuntimeRunner"))["_execute_signals"]
    assert [arg.arg for arg in execute.args.args] == ["self", "signals"]
    assert [arg.arg for arg in execute.args.kwonlyargs] == [
        "source",
        "event_time_ms",
        "metadata",
        "feedback_depth",
    ]
    assert [
        None if value is None else ast.unparse(value)
        for value in execute.args.kw_defaults
    ] == [None, None, "None", "0"]
    assert len(execute.body) == 1
    calls = [
        call
        for call in _calls(execute, "execute")
        if ast.unparse(call.func.value) == "self._signal_execution_service"
    ]
    assert len(calls) == 1
    request, plan = calls[0].args
    assert isinstance(request, ast.Call)
    assert ast.unparse(request.func) == "RuntimeSignalExecutionRequest"
    assert [keyword.arg for keyword in request.keywords] == REQUEST_FIELDS
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in request.keywords
    } == {field: field for field in REQUEST_FIELDS}
    assert isinstance(plan, ast.Call)
    assert ast.unparse(plan.func) == "RuntimeSignalExecutionPlan"
    assert [keyword.arg for keyword in plan.keywords] == PLAN_FIELDS
    assert {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in plan.keywords
    } == {
        "prepare_signal": "self._prepare_signal_execution",
        "create_intent": "self._create_signal_execution_intent",
        "execute_intent": "self._execute_signal_execution_intent",
        "post_submit_sync": "self._run_post_submit_order_sync",
        "handle_results": "self._handle_signal_execution_results",
        "post_order_sync": "self._run_post_order_account_sync",
        "process_feedback": "self._process_signal_execution_feedback",
        "build_feedback_request": "self._build_signal_feedback_request",
    }
    assert not any(
        isinstance(node, (ast.Lambda, ast.Try, ast.TryStar))
        for node in ast.walk(execute)
    )


def test_runner_retains_all_signal_execution_business_rules() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    prepare = ast.unparse(methods["_prepare_signal_execution"])
    for token in (
        "self.stats.signals_seen += 1",
        "self.app_config.dry_run",
        "self.stats.dry_run_actions += 1",
        "self._has_account_config_entry_block()",
        "self._has_unresolved_follower_close()",
        "follower_recovery_topup",
        "SignalAction.OPEN_LONG",
        "SignalAction.OPEN_SHORT",
        "self.context.alerts.emit",
    ):
        assert token in prepare

    create = ast.unparse(methods["_create_signal_execution_intent"])
    assert "self._intent_factory.create" in create
    execute_intent = ast.unparse(methods["_execute_signal_execution_intent"])
    assert "self._get_order_coordinator().execute(intent)" in execute_intent

    handle = ast.unparse(methods["_handle_signal_execution_results"])
    positions = [
        handle.index(name)
        for name in (
            "_record_order_results",
            "_save_order_results",
            "_check_follower_close_failure",
        )
    ]
    assert positions == sorted(positions)

    post_order = ast.unparse(methods["_run_post_order_account_sync"])
    for action in (
        "SignalAction.OPEN_LONG",
        "SignalAction.OPEN_SHORT",
        "SignalAction.CLOSE_LONG",
        "SignalAction.CLOSE_SHORT",
    ):
        assert action in post_order
    assert "PLACE_STOP_LOSS" not in post_order

    feedback = ast.unparse(methods["_build_signal_feedback_request"])
    assert "request.feedback_depth >= 5" in feedback
    assert "order_result_feedback" in feedback
    assert "'parent_source': request.source" in feedback
    assert "request.feedback_depth + 1" in feedback


def test_existing_business_methods_and_recovery_entrypoints_remain_runner_owned() -> None:
    methods = _methods(_class(RUNNER, "LiveRuntimeRunner"))
    required = {
        "_record_order_results",
        "_save_order_results",
        "_check_follower_close_failure",
        "_process_order_result_feedback",
        "_has_account_config_entry_block",
        "_has_unresolved_follower_close",
        "_get_order_coordinator",
        "_get_order_sync_service",
        "_get_account_sync_service",
        "_validate_order_results_before_journal",
        "_verify_stop_order_results",
    }
    assert required <= set(methods)
    for name in (
        "_execute_recovery_stop_signals",
        "_execute_recovery_other_signals",
    ):
        assert len(_calls(methods[name], "_execute_signals")) == 1


def test_other_runtime_modules_do_not_depend_on_signal_execution_service() -> None:
    paths = (
        RUNTIME_ROOT / "recovery_coordinator.py",
        RUNTIME_ROOT / "reconciliation_coordinator.py",
        RUNTIME_ROOT / "startup_phase_coordinator.py",
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
            module == "src.runtime.signal_execution_service"
            or module.startswith("src.runtime.signal_execution_service.")
            for module in _imports(path)
        )
