from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.app import AppConfig, AppContext
from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.order_management import ExchangeOrderResult, OrderIntent
from src.platform import ExchangeName, OrderSide, OrderStatus
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.config import ProjectEnvConfig
from src.platform.data.models import MarketTrade, TradeSide
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime import runner as runner_module
from src.signals import SignalAction, TradeSignal


class FakeAlerts:
    def __init__(self, calls: list[str] | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.emitted = []

    def start(self) -> None:
        self.calls.append("alerts.start")

    async def stop(self) -> None:
        self.calls.append("alerts.stop")

    def emit(self, alert) -> None:
        self.emitted.append(alert)


class FakeStateStore:
    def __init__(self, calls: list[str] | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.saved_account_events = []
        self.saved_orders = []

    def save_account_event(self, event) -> None:
        self.calls.append("state_store.save_account_event")
        self.saved_account_events.append(event)

    def save_order(self, order, *, is_stop_order=False) -> None:
        self.calls.append("state_store.save_order")
        self.saved_orders.append((order, is_stop_order))


class FakeStrategy:
    raw_trade_callbacks_enabled = True
    observer_id = "characterization-test"
    enabled = True

    def __init__(self, calls: list[str] | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.market_signals = ()
        self.feature_signals = ()
        self.account_signals = ()
        self.feedback_signal = None
        self.feedback_results = []

    async def on_start(self, snapshot):
        self.calls.append("strategy.on_start")
        return ()

    async def on_kline(self, event):
        return ()

    async def on_ticker(self, event):
        return ()

    async def on_trade(self, event):
        self.calls.append("strategy.on_trade")
        return self.market_signals

    async def on_order_book(self, event):
        return ()

    async def on_market_feature(self, event):
        self.calls.append("strategy.on_market_feature")
        return self.feature_signals

    def market_feature_observers(self):
        return (self,)

    async def on_account_event(self, event):
        self.calls.append("strategy.on_account_event")
        return self.account_signals

    async def on_order_results(self, *, signal, results, source, event_time_ms):
        self.calls.append("strategy.on_order_results")
        self.feedback_results.append((signal, results, source, event_time_ms))
        return () if self.feedback_signal is None else (self.feedback_signal,)

    def strategy_identity(self) -> str:
        return "characterization-test"

    def runtime_requirements(self):
        return {
            "capabilities": {
                "manifest_version": 1,
                "strategy_id": "characterization-test",
                "position_snapshots": False,
                "recovery_status": False,
                "market_features": True,
                "range_speed_history": False,
                "startup_preview": False,
                "pending_work": False,
            },
            "trades": {
                "enabled": True,
                "stream_enabled": True,
            },
        }


def _runner(
    strategy: FakeStrategy | None = None,
    *,
    calls: list[str] | None = None,
    services: dict | None = None,
) -> LiveRuntimeRunner:
    calls = calls if calls is not None else []
    strategy = strategy or FakeStrategy(calls)
    config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=False,
        enable_email_alerts=False,
    )
    context = AppContext(
        data=SimpleNamespace(exchange=ExchangeName.OKX, symbol=config.symbol),
        execution=SimpleNamespace(exchange=ExchangeName.OKX, symbol=config.symbol),
        state_store=FakeStateStore(calls),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=FakeAlerts(calls),
    )
    injected = dict(services or {})
    injected.setdefault(
        "project_env_config",
        ProjectEnvConfig(
            values={}, source_files=(), env_file=Path(".env"), example_file=None
        ),
    )
    return LiveRuntimeRunner(
        app_config=config,
        app_context=context,
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services=injected,
    )


def _async_stage(calls: list[str], name: str, result=None):
    async def stage(*args, **kwargs):
        calls.append(name)
        return result

    return stage


@pytest.mark.asyncio
async def test_missing_on_start_is_a_runner_noop(caplog) -> None:
    host = SimpleNamespace(on_start=AsyncMock(return_value=None))
    signal_service = SimpleNamespace(execute=AsyncMock())
    runner = _runner(
        services={
            "strategy_host": host,
            "signal_execution_service": signal_service,
        }
    )
    snapshot = object()
    caplog.set_level("INFO", logger=runner_module.logger.name)

    await runner._call_on_start(snapshot)

    host.on_start.assert_awaited_once_with(snapshot)
    assert runner.stats.on_start_called is False
    signal_service.execute.assert_not_awaited()
    assert not any(
        "Strategy on_start completed" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_called_on_start_with_no_signals_keeps_stage1_side_effects(
    caplog,
) -> None:
    host = SimpleNamespace(on_start=AsyncMock(return_value=()))
    signal_service = SimpleNamespace(execute=AsyncMock())
    runner = _runner(
        services={
            "strategy_host": host,
            "signal_execution_service": signal_service,
        }
    )
    snapshot = object()
    caplog.set_level("INFO", logger=runner_module.logger.name)

    await runner._call_on_start(snapshot)

    assert runner.stats.on_start_called is True
    signal_service.execute.assert_awaited_once()
    request = signal_service.execute.await_args.args[0]
    assert request.signals == ()
    assert request.source == "on_start"
    assert request.event_time_ms is None
    assert "Strategy on_start completed | signals=0" in caplog.messages


@pytest.mark.asyncio
async def test_on_start_exception_preserves_identity_and_stats(
    caplog,
) -> None:
    expected = RuntimeError("on_start failed")
    host = SimpleNamespace(on_start=AsyncMock(side_effect=expected))
    signal_service = SimpleNamespace(execute=AsyncMock())
    runner = _runner(
        services={
            "strategy_host": host,
            "signal_execution_service": signal_service,
        }
    )
    caplog.set_level("INFO", logger=runner_module.logger.name)

    with pytest.raises(RuntimeError) as raised:
        await runner._call_on_start(object())

    assert raised.value is expected
    assert runner.stats.on_start_called is False
    signal_service.execute.assert_not_awaited()
    assert not any(
        "Strategy on_start completed" in message
        for message in caplog.messages
    )


def _wire_startup_stages(monkeypatch, runner, calls, snapshots) -> None:
    monkeypatch.setattr(
        runner,
        "_initialize_rangebar_trust_window",
        lambda: calls.append("initialize_rangebar_trust_window"),
    )

    def set_health(phase, **kwargs):
        calls.append(f"health_{phase.value}")

    monkeypatch.setattr(runner, "_set_health", set_health)
    monkeypatch.setattr(
        runner,
        "_bootstrap_account_config_if_enabled",
        _async_stage(calls, "bootstrap_account_config"),
    )
    monkeypatch.setattr(
        runner,
        "_check_strategy_position_mode_requirements",
        _async_stage(calls, "check_position_mode"),
    )
    monkeypatch.setattr(runner, "_run_warmup", _async_stage(calls, "run_warmup"))
    monkeypatch.setattr(
        runner,
        "_warmup_range_speed_history",
        _async_stage(calls, "warmup_range_speed_history", 0),
    )
    monkeypatch.setattr(
        runner,
        "_check_startup_feature_backfills",
        _async_stage(calls, "check_startup_feature_backfills"),
    )
    monkeypatch.setattr(
        runner, "_run_recovery", _async_stage(calls, "run_recovery", snapshots)
    )
    monkeypatch.setattr(
        runner, "_run_reconciliation", _async_stage(calls, "run_reconciliation")
    )
    monkeypatch.setattr(runner, "_call_on_start", _async_stage(calls, "call_on_start"))
    monkeypatch.setattr(
        runner,
        "_evaluate_startup_catchup_once",
        _async_stage(calls, "startup_catchup"),
    )
    monkeypatch.setattr(
        runner,
        "_finish_range_speed_warmup_after_catchup",
        _async_stage(calls, "finish_range_speed_warmup"),
    )
    runner._heartbeat_service = SimpleNamespace(
        start=lambda **kwargs: calls.append("heartbeat_start")
    )
    monkeypatch.setattr(
        runner,
        "_start_range_speed_background_services",
        lambda: calls.append("start_range_speed_background_services"),
    )


@pytest.mark.asyncio
async def test_startup_phase_order_is_characterized(monkeypatch) -> None:
    calls: list[str] = []
    runner = _runner(calls=calls)
    snapshots = (object(), object())
    reconciliation_args = []
    on_start_args = []
    catchup_args = []
    _wire_startup_stages(monkeypatch, runner, calls, snapshots)

    async def reconciliation(value):
        calls.append("run_reconciliation")
        reconciliation_args.append(value)

    async def on_start(value):
        calls.append("call_on_start")
        on_start_args.append(value)

    async def catchup(value):
        calls.append("startup_catchup")
        catchup_args.append(value)

    monkeypatch.setattr(runner, "_run_reconciliation", reconciliation)
    monkeypatch.setattr(runner, "_call_on_start", on_start)
    monkeypatch.setattr(runner, "_evaluate_startup_catchup_once", catchup)
    monkeypatch.setattr(
        runner, "_start_producers", lambda: calls.append("start_producers") or []
    )
    monkeypatch.setattr(
        runner, "_start_sync_tasks", lambda: calls.append("start_sync_tasks") or []
    )

    await runner._startup()

    assert calls == [
        "initialize_rangebar_trust_window",
        "health_warming_up",
        "bootstrap_account_config",
        "check_position_mode",
        "run_warmup",
        "warmup_range_speed_history",
        "check_startup_feature_backfills",
        "health_catching_up",
        "run_recovery",
        "run_reconciliation",
        "call_on_start",
        "startup_catchup",
        "finish_range_speed_warmup",
        "heartbeat_start",
        "start_range_speed_background_services",
        "health_running",
    ]
    assert reconciliation_args == [snapshots]
    assert on_start_args == [snapshots[0]]
    assert catchup_args == [snapshots[0]]
    assert "start_producers" not in calls
    assert "start_sync_tasks" not in calls


@pytest.mark.asyncio
async def test_startup_rechecks_account_config_only_after_recovery(monkeypatch) -> None:
    calls: list[str] = []
    runner = _runner(calls=calls)
    snapshots = (object(),)
    _wire_startup_stages(monkeypatch, runner, calls, snapshots)
    runner._account_config_new_entries_blocked = True
    monkeypatch.setattr(
        runner,
        "_recheck_account_config_after_recovery",
        _async_stage(calls, "account_config_recheck"),
    )

    await runner._startup()

    assert calls.index("run_recovery") < calls.index("account_config_recheck")
    assert calls.index("account_config_recheck") < calls.index("run_reconciliation")
    assert calls.index("run_reconciliation") < calls.index("call_on_start")


def _trade() -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_time_ms=1234,
        trade_id="trade-1",
    )


@pytest.mark.asyncio
async def test_trade_processing_precedes_strategy_callback_and_execution(monkeypatch) -> None:
    calls: list[str] = []
    strategy = FakeStrategy(calls)
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
    )
    strategy.market_signals = (signal,)
    runner = _runner(strategy, calls=calls)
    runner._heartbeat_service = None
    executed = []
    monkeypatch.setattr(runner, "_set_health", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "_maybe_log_live_data_path_stats", lambda: None)
    monkeypatch.setattr(runner, "_process_trade", _async_stage(calls, "_process_trade"))

    async def execute(signals, **kwargs):
        calls.append("_execute_signals")
        executed.append((signals, kwargs))

    monkeypatch.setattr(runner, "_execute_signals", execute)
    trade = _trade()

    await runner.process_market_event(trade)

    assert calls == ["_process_trade", "strategy.on_trade", "_execute_signals"]
    assert executed == [
        ((signal,), {"source": "trade", "event_time_ms": trade.trade_time_ms})
    ]


@pytest.mark.asyncio
async def test_range_only_trade_skips_raw_strategy_callback_and_execution(monkeypatch) -> None:
    calls: list[str] = []
    strategy = FakeStrategy(calls)
    strategy.raw_trade_callbacks_enabled = False
    runner = _runner(strategy, calls=calls)
    runner._heartbeat_service = None
    monkeypatch.setattr(runner, "_set_health", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "_maybe_log_live_data_path_stats", lambda: None)
    monkeypatch.setattr(runner, "_process_trade", _async_stage(calls, "_process_trade"))
    monkeypatch.setattr(
        runner, "_execute_signals", _async_stage(calls, "_execute_signals")
    )

    await runner.process_market_event(_trade())

    assert calls == ["_process_trade"]


@pytest.mark.asyncio
async def test_injected_trade_derived_feature_pipeline_owns_runner_dispatch() -> None:
    calls: list[str] = []
    trade = _trade()

    class InjectedPipeline:
        async def process_trade(self, value):
            calls.append("pipeline.process_trade")
            assert value is trade

    runner = _runner(
        calls=calls,
        services={"trade_derived_feature_pipeline": InjectedPipeline()},
    )

    await runner._dispatch_trade_derived_features(trade)

    assert calls == ["pipeline.process_trade"]


@pytest.mark.asyncio
async def test_market_feature_dispatch_precedes_signal_execution(monkeypatch) -> None:
    calls: list[str] = []
    strategy = FakeStrategy(calls)
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
    )
    strategy.feature_signals = (signal,)
    runner = _runner(strategy, calls=calls)
    runner._heartbeat_service = None
    monkeypatch.setattr(runner, "_maybe_log_live_data_path_stats", lambda: None)
    executed = []

    async def execute(signals, **kwargs):
        calls.append("_execute_signals")
        executed.append((signals, kwargs))

    monkeypatch.setattr(runner, "_execute_signals", execute)
    event = MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_AGGREGATE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=5678,
    )

    await runner.process_market_feature(event)

    assert calls == ["strategy.on_market_feature", "_execute_signals"]
    assert executed == [
        (
            (signal,),
            {
                "source": event.type_value,
                "event_time_ms": event.event_time_ms,
                "metadata": {"feature_type": event.type_value},
            },
        )
    ]


@pytest.mark.asyncio
async def test_injected_market_feature_pipeline_preserves_runner_order(
    monkeypatch,
) -> None:
    calls: list[str] = []
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
    )

    class InjectedPipeline:
        runner = None

        async def dispatch(self, event):
            assert self.runner.stats.feature_events_seen == 1
            calls.append("pipeline.dispatch")
            return (signal,)

    pipeline = InjectedPipeline()
    runner = _runner(
        calls=calls,
        services={"market_feature_pipeline": pipeline},
    )
    pipeline.runner = runner

    class Heartbeat:
        def note_closed_bar(self, open_time_ms):
            assert open_time_ms == 4321
            calls.append("heartbeat.note_closed_bar")

    runner._heartbeat_service = Heartbeat()
    executed = []

    async def execute(signals, **kwargs):
        calls.append("_execute_signals")
        executed.append((signals, kwargs))

    monkeypatch.setattr(runner, "_execute_signals", execute)
    monkeypatch.setattr(
        runner,
        "_maybe_log_live_data_path_stats",
        lambda: calls.append("data_path_log"),
    )
    event = MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=5678,
        data={"open_time_ms": 4321},
    )

    await runner.process_market_feature(event)

    assert not hasattr(runner_module, "dispatch_market_feature_event")
    assert calls == [
        "heartbeat.note_closed_bar",
        "pipeline.dispatch",
        "_execute_signals",
        "data_path_log",
    ]
    assert executed == [
        (
            (signal,),
            {
                "source": event.type_value,
                "event_time_ms": event.event_time_ms,
                "metadata": {"feature_type": event.type_value},
            },
        )
    ]


@pytest.mark.asyncio
async def test_account_event_is_persisted_before_strategy_and_execution(monkeypatch) -> None:
    calls: list[str] = []
    strategy = FakeStrategy(calls)
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
    )
    strategy.account_signals = (signal,)
    runner = _runner(strategy, calls=calls)
    executed = []

    async def execute(signals, **kwargs):
        calls.append("_execute_signals")
        executed.append((signals, kwargs))

    monkeypatch.setattr(runner, "_execute_signals", execute)
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        event_time_ms=9012,
    )

    await runner.process_account_event(event)

    assert calls == [
        "state_store.save_account_event",
        "strategy.on_account_event",
        "_execute_signals",
    ]
    assert executed == [
        (
            (signal,),
            {"source": "account:okx", "event_time_ms": event.event_time_ms},
        )
    ]


@pytest.mark.asyncio
async def test_missing_account_event_callback_still_persists_without_execution() -> None:
    calls: list[str] = []

    class Host:
        async def on_account_event(self, event):
            calls.append("strategy_host.on_account_event")
            return None

    signal_service = SimpleNamespace(execute=AsyncMock())
    runner = _runner(
        calls=calls,
        services={
            "strategy_host": Host(),
            "signal_execution_service": signal_service,
        },
    )
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        event_time_ms=9013,
    )

    await runner.process_account_event(event)

    assert runner.stats.account_events_seen == 1
    assert runner.context.state_store.saved_account_events == [event]
    assert calls == [
        "state_store.save_account_event",
        "strategy_host.on_account_event",
    ]
    signal_service.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_called_account_event_with_no_signals_executes_empty_batch_last() -> None:
    calls: list[str] = []

    class Host:
        async def on_account_event(self, event):
            calls.append("strategy_host.on_account_event")
            return ()

    class SignalService:
        def __init__(self) -> None:
            self.requests = []

        async def execute(self, request, plan) -> None:
            calls.append("signal_execution_service.execute")
            self.requests.append(request)

    signal_service = SignalService()
    runner = _runner(
        calls=calls,
        services={
            "strategy_host": Host(),
            "signal_execution_service": signal_service,
        },
    )
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        event_time_ms=9014,
    )

    await runner.process_account_event(event)

    assert calls == [
        "state_store.save_account_event",
        "strategy_host.on_account_event",
        "signal_execution_service.execute",
    ]
    assert signal_service.requests[0].signals == ()
    assert signal_service.requests[0].source == "account:okx"
    assert signal_service.requests[0].event_time_ms == event.event_time_ms


@pytest.mark.asyncio
async def test_account_event_exception_propagates_after_persistence() -> None:
    calls: list[str] = []
    expected = RuntimeError("account callback failed")

    class Host:
        async def on_account_event(self, event):
            calls.append("strategy_host.on_account_event")
            raise expected

    signal_service = SimpleNamespace(execute=AsyncMock())
    runner = _runner(
        calls=calls,
        services={
            "strategy_host": Host(),
            "signal_execution_service": signal_service,
        },
    )
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        event_time_ms=9015,
    )

    with pytest.raises(RuntimeError) as raised:
        await runner.process_account_event(event)

    assert raised.value is expected
    assert runner.context.state_store.saved_account_events == [event]
    assert calls == [
        "state_store.save_account_event",
        "strategy_host.on_account_event",
    ]
    signal_service.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_account_snapshot_callback_updates_cache_without_logs(
    caplog,
) -> None:
    host = SimpleNamespace(on_account_snapshot=AsyncMock(return_value=False))
    runner = _runner(services={"strategy_host": host})
    snapshot = SimpleNamespace(
        balance=SimpleNamespace(
            exchange=ExchangeName.OKX,
            available=Decimal("900"),
            total=Decimal("1000"),
        )
    )
    caplog.set_level("DEBUG", logger=runner_module.logger.name)

    await runner._on_account_snapshot_synced(snapshot, "account_periodic")

    assert runner._last_snapshots == (snapshot,)
    assert runner._last_snapshot is snapshot
    assert runner._last_account_snapshot_log_state == {}
    assert runner._last_account_snapshot_log_ms == {}
    assert not any(
        "Strategy account snapshot refreshed" in message
        or "Account snapshot unchanged" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_account_snapshot_exception_keeps_cache_and_skips_logs(
    caplog,
) -> None:
    expected = RuntimeError("snapshot callback failed")
    host = SimpleNamespace(
        on_account_snapshot=AsyncMock(side_effect=expected)
    )
    runner = _runner(services={"strategy_host": host})
    snapshot = SimpleNamespace(
        balance=SimpleNamespace(
            exchange=ExchangeName.OKX,
            available=Decimal("900"),
            total=Decimal("1000"),
        )
    )
    caplog.set_level("DEBUG", logger=runner_module.logger.name)

    with pytest.raises(RuntimeError) as raised:
        await runner._on_account_snapshot_synced(
            snapshot,
            "account_periodic",
        )

    assert raised.value is expected
    assert runner._last_snapshots == (snapshot,)
    assert runner._last_snapshot is snapshot
    assert runner._last_account_snapshot_log_state == {}
    assert runner._last_account_snapshot_log_ms == {}
    assert not any(
        "Strategy account snapshot refreshed" in message
        or "Account snapshot unchanged" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_signal_execution_feedback_order_is_characterized(monkeypatch) -> None:
    calls: list[str] = []
    strategy = FakeStrategy(calls)
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
    )
    strategy.feedback_signal = signal
    intent = OrderIntent(
        intent_id="intent-1",
        strategy_id="fake",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="order-1",
        client_order_id="client-1",
        status=OrderStatus.NEW,
        side=OrderSide.SELL,
        quantity=Decimal("0.1"),
    )
    factory_calls = []
    coordinator_intents = []

    class IntentFactory:
        def create(self, value, **kwargs):
            calls.append("intent_factory.create")
            factory_calls.append((value, kwargs))
            return intent

    class Coordinator:
        async def execute(self, value):
            calls.append("coordinator.execute")
            coordinator_intents.append(value)
            return (result,)

    class SyncService:
        def __init__(self, name):
            self.name = name

        async def sync_once(self, **kwargs):
            calls.append(self.name)

    runner = _runner(
        strategy,
        calls=calls,
        services={
            "intent_factory": IntentFactory(),
            "order_coordinator": Coordinator(),
            "order_sync_service": SyncService("post_submit_order_sync"),
            "account_sync_service": SyncService("post_order_account_sync"),
        },
    )
    original_record = runner._record_order_results

    def record_results(results):
        calls.append("runtime_result_accounting")
        original_record(results)

    monkeypatch.setattr(runner, "_record_order_results", record_results)
    monkeypatch.setattr(
        runner,
        "_check_follower_close_failure",
        lambda *args: calls.append("follower_close_failure_check"),
    )

    await runner._execute_signals(
        (signal,), source="root_source", event_time_ms=3456
    )

    assert calls[:8] == [
        "intent_factory.create",
        "coordinator.execute",
        "post_submit_order_sync",
        "runtime_result_accounting",
        "state_store.save_order",
        "follower_close_failure_check",
        "post_order_account_sync",
        "strategy.on_order_results",
    ]
    assert all(value is intent for value in coordinator_intents)
    assert strategy.feedback_results[0][1][0] is result
    assert factory_calls[0][1]["source"] == "root_source"
    assert factory_calls[1][1] == {
        "source": "order_result_feedback",
        "event_time_ms": 3456,
        "metadata": {"parent_source": "root_source"},
    }
    assert len(coordinator_intents) == 6
    assert [entry[1]["source"] for entry in factory_calls] == [
        "root_source",
        "order_result_feedback",
        "order_result_feedback",
        "order_result_feedback",
        "order_result_feedback",
        "order_result_feedback",
    ]
    assert len(runner.context.alerts.emitted) == 1


@pytest.mark.asyncio
async def test_injected_strategy_host_owns_order_result_feedback(caplog) -> None:
    calls: list[str] = []
    strategy = FakeStrategy(calls)

    async def direct_feedback(**kwargs):
        raise AssertionError("Runner must not call Strategy feedback directly")

    strategy.on_order_results = direct_feedback
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
    )
    results = (
        ExchangeOrderResult(exchange=ExchangeName.OKX, ok=True),
    )
    follow_up = (
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CLOSE_LONG,
            quantity=Decimal("0.05"),
        ),
    )
    received = []

    class InjectedStrategyHost:
        async def on_order_results(self, **kwargs):
            received.append(kwargs)
            return follow_up

    runner = _runner(
        strategy,
        calls=calls,
        services={"strategy_host": InjectedStrategyHost()},
    )
    caplog.set_level("INFO", logger=runner_module.logger.name)

    returned = await runner._process_order_result_feedback(
        signal=signal,
        results=results,
        source="root_source",
        event_time_ms=4321,
    )

    assert returned is follow_up
    assert received[0]["signal"] is signal
    assert received[0]["results"] is results
    assert received[0]["source"] == "root_source"
    assert received[0]["event_time_ms"] == 4321
    assert any(
        "Strategy order results processed" in message
        and "follow_up_signals=1" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("returned", "expects_debug_log"),
    [(None, False), ((), True)],
)
async def test_order_result_feedback_distinguishes_missing_and_called_empty(
    caplog,
    returned,
    expects_debug_log,
) -> None:
    host = SimpleNamespace(on_order_results=AsyncMock(return_value=returned))
    runner = _runner(services={"strategy_host": host})
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
    )
    results = (
        ExchangeOrderResult(exchange=ExchangeName.OKX, ok=True),
    )
    caplog.set_level("DEBUG", logger=runner_module.logger.name)

    follow_up = await runner._process_order_result_feedback(
        signal=signal,
        results=results,
        source="root_source",
        event_time_ms=4322,
    )

    assert follow_up == ()
    processed_logs = [
        message
        for message in caplog.messages
        if "Strategy order results processed" in message
    ]
    if expects_debug_log:
        assert len(processed_logs) == 1
        assert "follow_up_signals=0" in processed_logs[0]
    else:
        assert processed_logs == []


@pytest.mark.asyncio
async def test_order_result_feedback_exception_propagates_without_logs(
    caplog,
) -> None:
    expected = RuntimeError("order feedback failed")
    host = SimpleNamespace(
        on_order_results=AsyncMock(side_effect=expected)
    )
    runner = _runner(services={"strategy_host": host})
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
    )
    caplog.set_level("DEBUG", logger=runner_module.logger.name)

    with pytest.raises(RuntimeError) as raised:
        await runner._process_order_result_feedback(
            signal=signal,
            results=(),
            source="root_source",
            event_time_ms=4323,
        )

    assert raised.value is expected
    assert not any(
        "Strategy order results processed" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer_error", [False, True])
async def test_run_lifecycle_and_cleanup_order_is_characterized(
    monkeypatch, consumer_error
) -> None:
    calls: list[str] = []
    runner = _runner(calls=calls)
    monkeypatch.setattr(runner, "_startup", _async_stage(calls, "startup"))
    monkeypatch.setattr(
        runner, "_start_producers", lambda: calls.append("start_producers") or []
    )
    monkeypatch.setattr(
        runner, "_start_sync_tasks", lambda: calls.append("start_sync_tasks") or []
    )

    async def consume(*, max_market_events):
        calls.append("consume_market_events")
        if consumer_error:
            raise RuntimeError("consumer failed")

    monkeypatch.setattr(runner, "_consume_market_events", consume)
    for name in (
        "_stop_market_data_modules",
        "_stop_sync_tasks",
        "_stop_producers",
        "_stop_live_persistence_writer",
    ):
        monkeypatch.setattr(runner, name, _async_stage(calls, name.removeprefix("_")))

    if consumer_error:
        with pytest.raises(RuntimeError, match="consumer failed"):
            await runner.run(max_market_events=1)
    else:
        await runner.run(max_market_events=1)

    assert calls[:5] == [
        "alerts.start",
        "startup",
        "start_producers",
        "start_sync_tasks",
        "consume_market_events",
    ]
    cleanup_names = {
        "stop_market_data_modules",
        "stop_sync_tasks",
        "stop_producers",
        "stop_live_persistence_writer",
        "alerts.stop",
    }
    assert [name for name in calls if name in cleanup_names] == [
        "stop_market_data_modules",
        "stop_sync_tasks",
        "stop_producers",
        "stop_live_persistence_writer",
        "alerts.stop",
    ]


def test_sync_task_factory_order_is_characterized() -> None:
    runner = _runner()
    stop_event = runner._stop_event
    created: list[tuple[str, object]] = []

    def task(label: str):
        def create(event):
            created.append((label, event))
            return object()

        return create

    class CapturingLifecycle:
        def start(self, factories):
            return [factory() for factory in factories]

    runner.requirements = SimpleNamespace(
        account_state=SimpleNamespace(poll_enabled=True),
        order_state=SimpleNamespace(poll_when_position_enabled=True),
    )
    runner._sync_lifecycle = CapturingLifecycle()
    runner._get_account_sync_service = lambda: SimpleNamespace(
        run_periodic=task("account")
    )
    runner._get_order_sync_service = lambda: SimpleNamespace(
        run_periodic=task("order")
    )
    runner._periodic_follower_close_check = task("follower_close")
    runner._heartbeat_service = SimpleNamespace(
        run_periodic=task("heartbeat")
    )
    runner._get_startup_feature_backfill_providers = lambda: (object(),)
    runner._periodic_feature_readiness_refresh = task("feature_readiness")

    tasks = runner._start_sync_tasks()

    assert [label for label, _ in created] == [
        "account",
        "order",
        "follower_close",
        "heartbeat",
        "feature_readiness",
    ]
    assert all(event is stop_event for _, event in created)
    assert runner._sync_tasks is tasks
