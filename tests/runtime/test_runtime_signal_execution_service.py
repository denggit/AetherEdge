from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError, fields
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from src.app import AppConfig
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime.components import signal_execution as signal_execution_component
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner, LiveRuntimeStats
from src.runtime.signal_execution_service import (
    RuntimeSignalExecutionPlan,
    RuntimeSignalExecutionRequest,
    RuntimeSignalExecutionService,
)
from src.signals.models import SignalAction


PLAN_FIELDS = (
    "prepare_signal",
    "create_intent",
    "execute_intent",
    "post_submit_sync",
    "handle_results",
    "post_order_sync",
    "process_feedback",
    "build_feedback_request",
)


def _runner(*, signal_execution_service=None) -> LiveRuntimeRunner:
    config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    services = {
        "project_env_config": ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
    }
    if signal_execution_service is not None:
        services["signal_execution_service"] = signal_execution_service
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(strategy=object()),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )


def _request(
    signals,
    *,
    source: str = "test",
    event_time_ms: int | None = 123,
    metadata=None,
    feedback_depth: int = 0,
) -> RuntimeSignalExecutionRequest:
    return RuntimeSignalExecutionRequest(
        signals=signals,
        source=source,
        event_time_ms=event_time_ms,
        metadata=metadata,
        feedback_depth=feedback_depth,
    )


def _plan(
    events: list[str],
    *,
    follow_up=(),
    feedback_request=None,
    overrides: dict[str, object] | None = None,
) -> tuple[RuntimeSignalExecutionPlan, dict[str, object]]:
    received: dict[str, object] = {}
    intent = object()
    results = [object()]

    def prepare(signal, request) -> bool:
        events.append(f"prepare:{signal}")
        received["prepared_signal"] = signal
        received["prepared_request"] = request
        return True

    def create(signal, request):
        events.append(f"create:{signal}")
        received["created_signal"] = signal
        received["created_request"] = request
        return intent

    async def execute(value):
        events.append("execute")
        received["intent"] = value
        return results

    async def post_submit(signal, request) -> None:
        events.append(f"post_submit:{signal}")
        received["post_submit_request"] = request

    def handle(signal, value) -> None:
        events.append(f"handle:{signal}")
        received["handled_results"] = value

    async def post_order(signal, request) -> None:
        events.append(f"post_order:{signal}")
        received["post_order_request"] = request

    async def feedback(signal, value, request):
        events.append(f"feedback:{signal}")
        received["feedback_results"] = value
        received["feedback_request"] = request
        return follow_up

    def build(signal, value, request):
        events.append(f"build:{signal}")
        received["built_follow_up"] = value
        received["builder_request"] = request
        return feedback_request

    values = {
        "prepare_signal": prepare,
        "create_intent": create,
        "execute_intent": execute,
        "post_submit_sync": post_submit,
        "handle_results": handle,
        "post_order_sync": post_order,
        "process_feedback": feedback,
        "build_feedback_request": build,
    }
    values.update(overrides or {})
    return RuntimeSignalExecutionPlan(**values), {
        **received,
        "received": received,
        "intent_object": intent,
        "results_object": results,
    }


def _signal(
    action: SignalAction,
    *,
    metadata=None,
):
    return SimpleNamespace(action=action, metadata=metadata or {})


def test_service_stateless_and_request_plan_are_frozen_with_exact_fields() -> None:
    signals = [object()]
    metadata = {"feature_type": "test"}
    request = RuntimeSignalExecutionRequest(
        signals=signals,
        source="source",
        event_time_ms=7,
        metadata=metadata,
    )
    plan, _ = _plan([])

    assert vars(RuntimeSignalExecutionService()) == {}
    assert tuple(field.name for field in fields(request)) == (
        "signals",
        "source",
        "event_time_ms",
        "metadata",
        "feedback_depth",
    )
    assert tuple(field.name for field in fields(plan)) == PLAN_FIELDS
    assert request.signals is signals
    assert request.metadata is metadata
    assert request.feedback_depth == 0
    assert RuntimeSignalExecutionRequest([], "source", None).metadata is None
    with pytest.raises(FrozenInstanceError):
        request.feedback_depth = 1  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        plan.prepare_signal = Mock()  # type: ignore[misc]


@pytest.mark.asyncio
async def test_empty_signals_returns_none_without_callbacks() -> None:
    events: list[str] = []
    plan, _ = _plan(events)

    result = await RuntimeSignalExecutionService().execute(_request([]), plan)

    assert result is None
    assert events == []


@pytest.mark.asyncio
async def test_single_signal_exact_flow_preserves_all_identities() -> None:
    events: list[str] = []
    signal = object()
    request = _request([signal])
    plan, state = _plan(events)
    received = state["received"]

    result = await RuntimeSignalExecutionService().execute(request, plan)

    assert result is None
    assert events == [
        f"prepare:{signal}",
        f"create:{signal}",
        "execute",
        f"post_submit:{signal}",
        f"handle:{signal}",
        f"post_order:{signal}",
        f"feedback:{signal}",
    ]
    assert received["prepared_signal"] is signal
    assert received["created_signal"] is signal
    assert received["intent"] is state["intent_object"]
    assert received["handled_results"] is state["results_object"]
    assert received["feedback_results"] is state["results_object"]
    assert received["prepared_request"] is request
    assert received["created_request"] is request
    assert received["post_submit_request"] is request
    assert received["post_order_request"] is request
    assert received["feedback_request"] is request


@pytest.mark.asyncio
async def test_two_signals_stay_in_input_order() -> None:
    events: list[str] = []
    plan, _ = _plan(events)

    await RuntimeSignalExecutionService().execute(_request(["first", "second"]), plan)

    prepare_events = [event for event in events if event.startswith("prepare:")]
    assert prepare_events == ["prepare:first", "prepare:second"]
    assert events.index("feedback:first") < events.index("prepare:second")


@pytest.mark.asyncio
async def test_prepare_false_skips_every_remaining_callback() -> None:
    events: list[str] = []

    def reject(signal, request) -> bool:
        events.append(f"reject:{signal}")
        return False

    plan, _ = _plan(events, overrides={"prepare_signal": reject})

    await RuntimeSignalExecutionService().execute(_request(["skip", "next"]), plan)

    assert events == ["reject:skip", "reject:next"]


@pytest.mark.asyncio
async def test_builder_receives_follow_up_identity_and_none_stops_recursion() -> None:
    events: list[str] = []
    follow_up = [object()]
    plan, state = _plan(events, follow_up=follow_up, feedback_request=None)

    await RuntimeSignalExecutionService().execute(_request(["root"]), plan)

    assert state["received"]["built_follow_up"] is follow_up
    assert events[-1] == "build:root"
    assert events.count("execute") == 1


@pytest.mark.asyncio
async def test_feedback_is_depth_first_before_next_original_signal() -> None:
    events: list[str] = []
    follow_up = ["child"]
    child_request = _request(follow_up, source="order_result_feedback", feedback_depth=1)

    async def feedback(signal, results, request):
        events.append(f"feedback:{signal}")
        return follow_up if signal == "root" else ()

    def build(signal, value, request):
        events.append(f"build:{signal}")
        assert value is follow_up
        return child_request

    plan, _ = _plan(
        events,
        overrides={"process_feedback": feedback, "build_feedback_request": build},
    )
    service = RuntimeSignalExecutionService()

    await service.execute(_request(["root", "sibling"]), plan)

    assert events.index("prepare:child") < events.index("prepare:sibling")
    child_prepare_index = events.index("prepare:child")
    assert events[child_prepare_index].endswith("child")


@pytest.mark.asyncio
async def test_callback_exception_propagates_and_stops_later_steps() -> None:
    events: list[str] = []
    error = RuntimeError("execution failed")

    async def fail(_intent):
        events.append("failed")
        raise error

    plan, _ = _plan(events, overrides={"execute_intent": fail})

    with pytest.raises(RuntimeError) as raised:
        await RuntimeSignalExecutionService().execute(_request(["signal"]), plan)

    assert raised.value is error
    assert events == ["prepare:signal", "create:signal", "failed"]


@pytest.mark.asyncio
async def test_cancelled_error_propagates_unchanged() -> None:
    cancelled = asyncio.CancelledError("cancelled")

    async def cancel(_intent):
        raise cancelled

    plan, _ = _plan([], overrides={"execute_intent": cancel})
    with pytest.raises(asyncio.CancelledError) as raised:
        await RuntimeSignalExecutionService().execute(_request([object()]), plan)
    assert raised.value is cancelled


def test_injected_service_has_priority_without_default_construction(
    monkeypatch,
) -> None:
    service = SimpleNamespace(execute=AsyncMock())
    default_factory = Mock()
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeSignalExecutionService",
        default_factory,
    )

    runner = _runner(signal_execution_service=service)

    default_factory.assert_not_called()
    service.execute.assert_not_called()
    assert runner._signal_execution_service is service
    assert runner.services["signal_execution_service"] is service
    assert runner._order_coordinator is None
    assert runner._account_sync_service is None
    assert runner._order_sync_service is None
    assert runner._order_journal is None
    assert runner._position_plan_store is None


def test_default_service_created_once_written_back_and_not_executed(
    monkeypatch,
) -> None:
    service = SimpleNamespace(execute=AsyncMock())
    factory = Mock(return_value=service)
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeSignalExecutionService",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with()
    service.execute.assert_not_called()
    assert runner._signal_execution_service is service
    assert runner.services["signal_execution_service"] is service
    assert runner._order_coordinator is None
    assert runner._account_sync_service is None
    assert runner._order_sync_service is None
    assert runner._order_journal is None
    assert runner._position_plan_store is None


@pytest.mark.asyncio
async def test_runner_builds_exact_request_plan_and_delegates_once() -> None:
    captured = []

    class Service:
        async def execute(self, request, plan) -> None:
            captured.append((request, plan))

    runner = _runner(signal_execution_service=Service())
    signals = [object()]
    metadata = {"feature_type": "test"}

    result = await runner._execute_signals(
        signals,
        source="source",
        event_time_ms=88,
        metadata=metadata,
        feedback_depth=3,
    )

    assert result is None
    assert len(captured) == 1
    request, plan = captured[0]
    assert request.signals is signals
    assert request.source == "source"
    assert request.event_time_ms == 88
    assert request.metadata is metadata
    assert request.feedback_depth == 3
    expected = {
        "prepare_signal": runner._prepare_signal_execution,
        "create_intent": runner._create_signal_execution_intent,
        "execute_intent": runner._execute_signal_execution_intent,
        "post_submit_sync": runner._run_post_submit_order_sync,
        "handle_results": runner._handle_signal_execution_results,
        "post_order_sync": runner._run_post_order_account_sync,
        "process_feedback": runner._process_signal_execution_feedback,
        "build_feedback_request": runner._build_signal_feedback_request,
    }
    assert all(getattr(plan, name) == callback for name, callback in expected.items())


def test_prepare_dry_run_precedes_guards_and_keeps_exact_stats_log(
    monkeypatch,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.stats = LiveRuntimeStats()
    runner.app_config = SimpleNamespace(dry_run=True)
    runner._has_account_config_entry_block = Mock(side_effect=AssertionError)
    runner._has_unresolved_follower_close = Mock(side_effect=AssertionError)
    logger = Mock()
    monkeypatch.setattr(signal_execution_component, "logger", logger)
    signal = _signal(SignalAction.OPEN_LONG)
    request = _request([signal], source="dry", event_time_ms=9)

    assert runner._prepare_signal_execution(signal, request) is False

    assert runner.stats.signals_seen == 1
    assert runner.stats.dry_run_actions == 1
    runner._has_account_config_entry_block.assert_not_called()
    runner._has_unresolved_follower_close.assert_not_called()
    logger.info.assert_called_once_with(
        "Dry-run signal skipped | action=%s source=%s event_time_ms=%s",
        "open_long",
        "dry",
        9,
    )


def test_prepare_account_config_block_keeps_warning_and_alert(monkeypatch) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.stats = LiveRuntimeStats()
    runner.app_config = SimpleNamespace(dry_run=False)
    runner._has_account_config_entry_block = Mock(return_value=True)
    runner._has_unresolved_follower_close = Mock()
    emit = Mock()
    runner.context = SimpleNamespace(alerts=SimpleNamespace(emit=emit))
    logger = Mock()
    monkeypatch.setattr(signal_execution_component, "logger", logger)
    signal = _signal(SignalAction.OPEN_SHORT)
    request = _request([signal], source="account")

    assert runner._prepare_signal_execution(signal, request) is False

    assert runner.stats.signals_seen == 1
    runner._has_unresolved_follower_close.assert_not_called()
    logger.warning.assert_called_once_with(
        "Blocking new entry — account config not verified due to existing exposure | action=%s source=%s",
        "open_short",
        "account",
    )
    alert = emit.call_args.args[0]
    assert alert.subject == "AetherEdge entry blocked: account config unverified"
    assert alert.severity == "warning"
    assert "reason=account_config_existing_exposure" in alert.content


def test_prepare_unresolved_follower_block_and_topup_exception(monkeypatch) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.stats = LiveRuntimeStats()
    runner.app_config = SimpleNamespace(dry_run=False)
    runner._has_account_config_entry_block = Mock(return_value=False)
    runner._has_unresolved_follower_close = Mock(return_value=True)
    emit = Mock()
    runner.context = SimpleNamespace(alerts=SimpleNamespace(emit=emit))
    logger = Mock()
    monkeypatch.setattr(signal_execution_component, "logger", logger)
    blocked = _signal(SignalAction.OPEN_LONG)
    request = _request([blocked], source="follower")

    assert runner._prepare_signal_execution(blocked, request) is False
    alert = emit.call_args.args[0]
    assert alert.subject == (
        "AetherEdge entry blocked due to unresolved follower close"
    )
    assert "reason=unresolved_follower_close_after_master_close" in alert.content

    topup = _signal(
        SignalAction.OPEN_LONG,
        metadata={"execution_purpose": "follower_recovery_topup"},
    )
    logger.reset_mock()
    assert runner._prepare_signal_execution(topup, request) is True
    logger.info.assert_called_once_with(
        "Executing signal | action=%s source=%s event_time_ms=%s",
        "open_long",
        "follower",
        123,
    )


def test_create_intent_keeps_signal_and_request_arguments() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    intent = object()
    runner._intent_factory = SimpleNamespace(create=Mock(return_value=intent))
    signal = object()
    metadata = {"parent_source": "root"}
    request = _request(
        [signal],
        source="feedback",
        event_time_ms=17,
        metadata=metadata,
    )

    returned = runner._create_signal_execution_intent(signal, request)

    assert returned is intent
    runner._intent_factory.create.assert_called_once_with(
        signal,
        source="feedback",
        event_time_ms=17,
        metadata=metadata,
    )
    assert runner._intent_factory.create.call_args.kwargs["metadata"] is metadata


@pytest.mark.asyncio
async def test_execute_intent_passes_identity_to_lazy_coordinator() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    intent = object()
    results = [object()]
    coordinator = SimpleNamespace(execute=AsyncMock(return_value=results))
    runner._get_order_coordinator = Mock(return_value=coordinator)

    returned = await runner._execute_signal_execution_intent(intent)

    assert returned is results
    coordinator.execute.assert_awaited_once_with(intent)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", (False, True))
async def test_post_submit_sync_condition_log_and_arguments(
    monkeypatch,
    enabled: bool,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.requirements = SimpleNamespace(
        order_state=SimpleNamespace(post_submit_sync_enabled=enabled)
    )
    sync = SimpleNamespace(sync_once=AsyncMock())
    runner._get_order_sync_service = Mock(return_value=sync)
    logger = Mock()
    monkeypatch.setattr(signal_execution_component, "logger", logger)
    signal = _signal(SignalAction.CLOSE_LONG)
    request = _request([signal], source="submit")

    await runner._run_post_submit_order_sync(signal, request)

    if enabled:
        logger.info.assert_called_once_with(
            "Post-submit order sync started | action=%s source=%s",
            "close_long",
            "submit",
        )
        sync.sync_once.assert_awaited_once_with(
            sync_type="post_submit",
            priority=True,
        )
    else:
        logger.info.assert_not_called()
        runner._get_order_sync_service.assert_not_called()


def test_result_handlers_keep_exact_three_step_order_and_identity() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    parent = Mock()
    runner._record_order_results = Mock()
    runner._save_order_results = Mock()
    runner._check_follower_close_failure = Mock()
    parent.attach_mock(runner._record_order_results, "record")
    parent.attach_mock(runner._save_order_results, "save")
    parent.attach_mock(runner._check_follower_close_failure, "follower")
    signal = object()
    results = [object()]

    runner._handle_signal_execution_results(signal, results)

    assert parent.mock_calls == [
        call.record(results),
        call.save(signal, results),
        call.follower(signal, results),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "enabled", "expected"),
    [
        (SignalAction.OPEN_LONG, True, True),
        (SignalAction.OPEN_SHORT, True, True),
        (SignalAction.CLOSE_LONG, True, True),
        (SignalAction.CLOSE_SHORT, True, True),
        (SignalAction.PLACE_STOP_LOSS_LONG, True, False),
        (SignalAction.OPEN_LONG, False, False),
    ],
)
async def test_post_order_account_sync_exact_action_set(
    action: SignalAction,
    enabled: bool,
    expected: bool,
) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.requirements = SimpleNamespace(
        account_state=SimpleNamespace(post_order_sync_enabled=enabled)
    )
    sync = SimpleNamespace(sync_once=AsyncMock())
    runner._get_account_sync_service = Mock(return_value=sync)

    await runner._run_post_order_account_sync(
        _signal(action),
        _request([]),
    )

    if expected:
        sync.sync_once.assert_awaited_once_with(
            sync_type="post_order_account",
            priority=True,
        )
    else:
        runner._get_account_sync_service.assert_not_called()


@pytest.mark.asyncio
async def test_feedback_wrapper_keeps_all_arguments_and_result_identity() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    follow_up = [object()]
    runner._process_order_result_feedback = AsyncMock(return_value=follow_up)
    signal = object()
    results = [object()]
    request = _request([], source="root", event_time_ms=44)

    returned = await runner._process_signal_execution_feedback(
        signal,
        results,
        request,
    )

    assert returned is follow_up
    runner._process_order_result_feedback.assert_awaited_once_with(
        signal=signal,
        results=results,
        source="root",
        event_time_ms=44,
    )


def test_feedback_depth_four_builds_depth_five_request_with_identity() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    follow_up = [object()]
    signal = _signal(SignalAction.CLOSE_LONG)
    request = _request(
        [signal],
        source="root",
        event_time_ms=55,
        feedback_depth=4,
    )

    built = runner._build_signal_feedback_request(signal, follow_up, request)

    assert built is not None
    assert built.signals is follow_up
    assert built.source == "order_result_feedback"
    assert built.event_time_ms == 55
    assert built.metadata == {"parent_source": "root"}
    assert built.feedback_depth == 5


def test_feedback_depth_five_blocks_with_exact_log_and_alert(monkeypatch) -> None:
    runner = object.__new__(LiveRuntimeRunner)
    emit = Mock()
    runner.context = SimpleNamespace(alerts=SimpleNamespace(emit=emit))
    logger = Mock()
    monkeypatch.setattr(signal_execution_component, "logger", logger)
    signal = _signal(SignalAction.CLOSE_SHORT)
    request = _request([signal], source="feedback", feedback_depth=5)

    assert runner._build_signal_feedback_request(signal, [object()], request) is None

    logger.error.assert_called_once_with(
        "Order result feedback depth exceeded | action=%s source=%s",
        "close_short",
        "feedback",
    )
    alert = emit.call_args.args[0]
    assert alert.subject == "AetherEdge order feedback recursion blocked"
    assert alert.content == "action=close_short source=feedback"
    assert alert.severity == "error"
