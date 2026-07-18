from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.app import AppConfig, AppContext
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime import runner as runner_module
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.services import DEFAULT_RUNTIME_SERVICE
from src.runtime.signal_execution_service import (
    RuntimeSignalExecutionPlan,
    RuntimeSignalExecutionRequest,
    RuntimeSignalExecutionService,
)


class _Alerts:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.emitted = []

    def start(self) -> None:
        self.calls.append("alerts.start")

    async def stop(self) -> None:
        self.calls.append("alerts.stop")

    def emit(self, alert) -> None:
        self.emitted.append(alert)
        if alert.subject == "AetherEdge live runtime error":
            self.calls.append("error.alert")


def _runner(
    *,
    calls: list[str] | None = None,
    services: dict | None = None,
) -> LiveRuntimeRunner:
    calls = calls if calls is not None else []
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
    injected = dict(services or {})
    injected.setdefault(
        "project_env_config",
        ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
    )
    injected.setdefault(
        "runtime_requirements",
        StrategyRuntimeRequirements.from_mapping(
            {
                "capabilities": {
                    "manifest_version": 1,
                    "strategy_id": "characterization-test",
                    "position_snapshots": False,
                    "recovery_status": False,
                    "market_features": False,
                    "range_speed_history": False,
                    "startup_preview": False,
                    "pending_work": False,
                }
            }
        ),
    )
    context = AppContext(
        data=SimpleNamespace(exchange=ExchangeName.OKX, symbol=config.symbol),
        execution=SimpleNamespace(
            exchange=ExchangeName.OKX,
            symbol=config.symbol,
        ),
        state_store=SimpleNamespace(),
        strategy=SimpleNamespace(
            strategy_identity=lambda: "characterization-test"
        ),
        planner=SimpleNamespace(),
        alerts=_Alerts(calls),
    )
    return LiveRuntimeRunner(
        app_config=config,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=injected,
    )


def _async_step(calls: list[str], name: str, *, result=None, error=None):
    async def step(*args, **kwargs):
        calls.append(name)
        if error is not None:
            raise error
        return result

    return step


def test_all_final_service_keys_preserve_injected_identity_and_are_lazy() -> None:
    calls: list[str] = []
    account_sync = SimpleNamespace(sync_once=AsyncMock(), run_periodic=Mock())
    order_sync = SimpleNamespace(sync_once=AsyncMock(), run_periodic=Mock())
    registry = SimpleNamespace(
        account_service=account_sync,
        order_service=order_sync,
    )
    strategy_host = SimpleNamespace()
    market_pipeline = SimpleNamespace()
    sync_lifecycle = SimpleNamespace(start=Mock(), stop=AsyncMock())
    signal_service = SimpleNamespace(execute=AsyncMock())
    recovery_coordinator = SimpleNamespace(execute=AsyncMock())
    reconciliation_coordinator = SimpleNamespace(execute=AsyncMock())
    persistence_service = SimpleNamespace()
    trade_pipeline = SimpleNamespace()
    market_persistence = SimpleNamespace()
    health = object()
    health_state = SimpleNamespace(current=health, update=Mock())
    heartbeat = SimpleNamespace(
        start=Mock(),
        read_previous=Mock(),
        run_periodic=Mock(),
    )
    shutdown = SimpleNamespace(execute=AsyncMock())
    startup = SimpleNamespace(execute=AsyncMock())
    recovery_service = SimpleNamespace(recover=AsyncMock())
    reconciliation_service = SimpleNamespace(reconcile_and_apply=AsyncMock())
    order_coordinator = SimpleNamespace(execute=AsyncMock())
    services = {
        "strategy_host": strategy_host,
        "market_feature_pipeline": market_pipeline,
        "sync_lifecycle": sync_lifecycle,
        "sync_service_registry": registry,
        "signal_execution_service": signal_service,
        "recovery_coordinator": recovery_coordinator,
        "reconciliation_coordinator": reconciliation_coordinator,
        "runtime_persistence_service": persistence_service,
        "trade_derived_feature_pipeline": trade_pipeline,
        "market_data_persistence": market_persistence,
        "runtime_health_state": health_state,
        "heartbeat_service": heartbeat,
        "shutdown_coordinator": shutdown,
        "startup_phase_coordinator": startup,
        "recovery_service": recovery_service,
        "reconciliation_service": reconciliation_service,
        "order_coordinator": order_coordinator,
    }

    runner = _runner(calls=calls, services=services)

    identities = {
        "strategy_host": runner._strategy_host,
        "market_feature_pipeline": runner._market_feature_pipeline,
        "sync_lifecycle": runner._sync_lifecycle,
        "sync_service_registry": runner._sync_service_registry,
        "signal_execution_service": runner._signal_execution_service,
        "recovery_coordinator": runner._recovery_coordinator,
        "reconciliation_coordinator": runner._reconciliation_coordinator,
        "runtime_persistence_service": runner._runtime_persistence_service,
        "trade_derived_feature_pipeline": runner._trade_derived_feature_pipeline,
        "market_data_persistence": runner._market_data_persistence,
        "runtime_health_state": runner._runtime_health_state,
        "heartbeat_service": runner._heartbeat_service,
        "shutdown_coordinator": runner._shutdown_coordinator,
        "startup_phase_coordinator": runner._startup_phase_coordinator,
    }
    for key, component in identities.items():
        assert component is services[key]
        assert runner.services[key] is component

    for mock in (
        signal_service.execute,
        recovery_coordinator.execute,
        reconciliation_coordinator.execute,
        shutdown.execute,
        startup.execute,
        heartbeat.start,
        heartbeat.read_previous,
        heartbeat.run_periodic,
        sync_lifecycle.start,
        sync_lifecycle.stop,
        recovery_service.recover,
        reconciliation_service.reconcile_and_apply,
        order_coordinator.execute,
        account_sync.sync_once,
        order_sync.sync_once,
    ):
        mock.assert_not_called()


def test_default_construction_preserves_expensive_service_laziness() -> None:
    runner = _runner()

    assert runner._recovery_service is DEFAULT_RUNTIME_SERVICE
    assert runner._reconciliation_service is DEFAULT_RUNTIME_SERVICE
    assert runner._order_coordinator is None
    assert runner._order_journal is None
    assert runner._position_plan_store is None
    assert runner._account_sync_service is None
    assert runner._order_sync_service is None
    assert runner._execution_clients is None
    assert runner._account_clients is None


@pytest.mark.asyncio
async def test_normal_run_final_call_graph(monkeypatch) -> None:
    calls: list[str] = []
    runner = _runner(calls=calls)
    monkeypatch.setattr(runner, "_startup", _async_step(calls, "startup"))
    monkeypatch.setattr(
        runner,
        "_start_producers",
        lambda: calls.append("producers") or [],
    )
    monkeypatch.setattr(
        runner,
        "_start_sync_tasks",
        lambda: calls.append("sync_tasks") or [],
    )
    monkeypatch.setattr(
        runner,
        "_consume_market_events",
        _async_step(calls, "consume"),
    )
    monkeypatch.setattr(
        runner,
        "_set_health",
        lambda phase, **kwargs: calls.append(f"health.{phase.value}"),
    )
    monkeypatch.setattr(
        runner,
        "_run_finally_shutdown",
        _async_step(calls, "final_shutdown"),
    )

    result = await runner.run(max_market_events=1)

    assert result is runner.stats
    assert calls == [
        "alerts.start",
        "startup",
        "producers",
        "sync_tasks",
        "consume",
        "health.stopped",
        "final_shutdown",
    ]


@pytest.mark.asyncio
async def test_error_run_final_call_graph_and_original_exception(monkeypatch) -> None:
    calls: list[str] = []
    error = RuntimeError("business failed")
    runner = _runner(calls=calls)
    monkeypatch.setattr(runner, "_startup", _async_step(calls, "startup"))
    monkeypatch.setattr(
        runner,
        "_start_producers",
        lambda: calls.append("producers") or [],
    )
    monkeypatch.setattr(
        runner,
        "_start_sync_tasks",
        lambda: calls.append("sync_tasks") or [],
    )
    monkeypatch.setattr(
        runner,
        "_consume_market_events",
        _async_step(calls, "consume", error=error),
    )
    monkeypatch.setattr(
        runner,
        "_set_health",
        lambda phase, **kwargs: calls.append(f"health.{phase.value}"),
    )
    monkeypatch.setattr(
        runner,
        "_run_finally_shutdown",
        _async_step(calls, "final_shutdown"),
    )
    logger = Mock()
    logger.exception.side_effect = lambda *args: calls.append("error.log")
    monkeypatch.setattr(runner_module, "logger", logger)

    with pytest.raises(RuntimeError) as raised:
        await runner.run(max_market_events=1)

    assert raised.value is error
    assert calls == [
        "alerts.start",
        "startup",
        "producers",
        "sync_tasks",
        "consume",
        "health.error",
        "error.log",
        "error.alert",
        "final_shutdown",
    ]


@pytest.mark.asyncio
async def test_startup_final_call_graph_and_snapshot_identity(monkeypatch) -> None:
    calls: list[str] = []
    runner = _runner(calls=calls)
    snapshots = (object(), object())
    first = snapshots[0]

    monkeypatch.setattr(
        runner,
        "_initialize_rangebar_trust_window",
        lambda: calls.append("trust_window"),
    )
    monkeypatch.setattr(
        runner,
        "_enter_startup_warming_up",
        lambda: calls.append("WARMING_UP"),
    )
    monkeypatch.setattr(
        runner,
        "_bootstrap_account_config_if_enabled",
        _async_step(calls, "account_config"),
    )
    monkeypatch.setattr(
        runner,
        "_check_strategy_position_mode_requirements",
        _async_step(calls, "position_mode"),
    )
    monkeypatch.setattr(runner, "_run_warmup", _async_step(calls, "warmup"))
    monkeypatch.setattr(
        runner,
        "_warmup_range_speed_history",
        _async_step(calls, "range_speed", result=9),
    )
    monkeypatch.setattr(
        runner,
        "_handle_startup_range_speed_history_result",
        lambda loaded: calls.append(f"range_speed_result:{loaded}"),
    )
    monkeypatch.setattr(
        runner,
        "_check_startup_feature_backfills",
        _async_step(calls, "feature_backfill"),
    )
    monkeypatch.setattr(
        runner,
        "_enter_startup_catching_up",
        lambda: calls.append("CATCHING_UP"),
    )
    monkeypatch.setattr(
        runner,
        "_run_recovery",
        _async_step(calls, "recovery", result=snapshots),
    )

    async def post_recovery(received) -> None:
        assert received is snapshots
        calls.append("post_recovery")

    async def reconciliation(received) -> None:
        assert received is snapshots
        calls.append("reconciliation")

    async def on_start(received) -> None:
        assert received is first
        calls.append("strategy_on_start")

    async def catchup(received) -> None:
        assert received is first
        calls.append("startup_catchup")

    monkeypatch.setattr(runner, "_run_startup_post_recovery_checks", post_recovery)
    monkeypatch.setattr(runner, "_run_reconciliation", reconciliation)
    monkeypatch.setattr(runner, "_call_on_start", on_start)
    monkeypatch.setattr(runner, "_evaluate_startup_catchup_once", catchup)
    monkeypatch.setattr(
        runner,
        "_finish_range_speed_warmup_after_catchup",
        _async_step(calls, "finish_range_speed"),
    )
    monkeypatch.setattr(
        runner,
        "_start_runtime_heartbeat",
        lambda: calls.append("heartbeat"),
    )
    monkeypatch.setattr(
        runner,
        "_start_range_speed_background_services",
        lambda: calls.append("background_services"),
    )
    monkeypatch.setattr(
        runner,
        "_enter_startup_running",
        lambda: calls.append("RUNNING"),
    )

    await runner._startup()

    assert calls == [
        "trust_window",
        "WARMING_UP",
        "account_config",
        "position_mode",
        "warmup",
        "range_speed",
        "range_speed_result:9",
        "feature_backfill",
        "CATCHING_UP",
        "recovery",
        "post_recovery",
        "reconciliation",
        "strategy_on_start",
        "startup_catchup",
        "finish_range_speed",
        "heartbeat",
        "background_services",
        "RUNNING",
    ]


@pytest.mark.asyncio
async def test_signal_execution_and_feedback_are_depth_first() -> None:
    calls: list[str] = []
    service = RuntimeSignalExecutionService()

    def prepare(signal, request) -> bool:
        calls.append(f"{signal}.prepare")
        return True

    def create(signal, request):
        calls.append(f"{signal}.intent")
        return f"{signal}.intent_object"

    async def execute(intent):
        signal = intent.removesuffix(".intent_object")
        calls.append(f"{signal}.order_execute")
        return (f"{signal}.result",)

    async def post_submit(signal, request) -> None:
        calls.append(f"{signal}.post_submit")

    def handle(signal, results) -> None:
        calls.extend(
            (
                f"{signal}.result_record",
                f"{signal}.result_save",
                f"{signal}.follower_check",
            )
        )

    async def post_order(signal, request) -> None:
        calls.append(f"{signal}.post_order")

    async def feedback(signal, results, request):
        calls.append(f"{signal}.feedback")
        return ("child",) if signal == "root" else ()

    def build(signal, follow_up, request):
        calls.append(f"{signal}.feedback_request")
        return RuntimeSignalExecutionRequest(
            signals=follow_up,
            source="order_result_feedback",
            event_time_ms=request.event_time_ms,
            metadata={"parent_source": request.source},
            feedback_depth=request.feedback_depth + 1,
        )

    plan = RuntimeSignalExecutionPlan(
        prepare_signal=prepare,
        create_intent=create,
        execute_intent=execute,
        post_submit_sync=post_submit,
        handle_results=handle,
        post_order_sync=post_order,
        process_feedback=feedback,
        build_feedback_request=build,
    )

    await service.execute(
        RuntimeSignalExecutionRequest(
            signals=("root", "sibling"),
            source="root_source",
            event_time_ms=100,
        ),
        plan,
    )

    assert calls == [
        "root.prepare",
        "root.intent",
        "root.order_execute",
        "root.post_submit",
        "root.result_record",
        "root.result_save",
        "root.follower_check",
        "root.post_order",
        "root.feedback",
        "root.feedback_request",
        "child.prepare",
        "child.intent",
        "child.order_execute",
        "child.post_submit",
        "child.result_record",
        "child.result_save",
        "child.follower_check",
        "child.post_order",
        "child.feedback",
        "sibling.prepare",
        "sibling.intent",
        "sibling.order_execute",
        "sibling.post_submit",
        "sibling.result_record",
        "sibling.result_save",
        "sibling.follower_check",
        "sibling.post_order",
        "sibling.feedback",
    ]


@pytest.mark.asyncio
async def test_final_and_explicit_shutdown_keep_distinct_sequences(
    monkeypatch,
) -> None:
    calls: list[str] = []
    runner = _runner(calls=calls)

    for name in (
        "_stop_market_data_modules",
        "_stop_sync_tasks",
        "_stop_producers",
        "_stop_live_persistence_writer",
    ):
        monkeypatch.setattr(
            runner,
            name,
            _async_step(calls, name.removeprefix("_")),
        )

    await runner._run_finally_shutdown()
    assert calls == [
        "stop_market_data_modules",
        "stop_sync_tasks",
        "stop_producers",
        "stop_live_persistence_writer",
        "alerts.stop",
    ]

    calls.clear()
    stop_state = {"set": False}
    runner._stop_event = SimpleNamespace(
        set=lambda: (stop_state.__setitem__("set", True), calls.append("stop_event")),
        is_set=lambda: stop_state["set"],
    )
    stopped = RuntimeHealth(phase=RuntimePhase.STOPPED)

    def set_health(phase, **kwargs) -> None:
        assert stop_state["set"] is True
        calls.append(f"health.{phase.value}")
        runner._health = stopped

    monkeypatch.setattr(runner, "_set_health", set_health)

    returned = await runner.stop()

    assert returned is stopped
    assert calls == [
        "stop_event",
        "stop_market_data_modules",
        "stop_producers",
        "stop_live_persistence_writer",
        "health.stopped",
    ]
