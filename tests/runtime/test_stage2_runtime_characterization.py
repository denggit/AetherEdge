from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

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

    async def on_account_event(self, event):
        self.calls.append("strategy.on_account_event")
        return self.account_signals

    async def on_order_results(self, *, signal, results, source, event_time_ms):
        self.calls.append("strategy.on_order_results")
        self.feedback_results.append((signal, results, source, event_time_ms))
        return () if self.feedback_signal is None else (self.feedback_signal,)


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
async def test_injected_strategy_host_owns_order_result_feedback() -> None:
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
        "_stop_range_speed_background_services",
        "_stop_sync_tasks",
        "_stop_producers",
        "_stop_live_persistence_writer",
        "_stop_range_repair_journal_writer",
        "_stop_range_checkpoint_writer",
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
        "stop_range_speed_background_services",
        "stop_sync_tasks",
        "stop_producers",
        "stop_live_persistence_writer",
        "stop_range_repair_journal_writer",
        "stop_range_checkpoint_writer",
        "alerts.stop",
    }
    assert [name for name in calls if name in cleanup_names] == [
        "stop_range_speed_background_services",
        "stop_sync_tasks",
        "stop_producers",
        "stop_live_persistence_writer",
        "stop_range_repair_journal_writer",
        "stop_range_checkpoint_writer",
        "alerts.stop",
    ]
