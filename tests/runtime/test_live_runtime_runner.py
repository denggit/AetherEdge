from __future__ import annotations

import asyncio
from decimal import Decimal

from src.app import (
    AppConfig,
    AppContext,
    AsyncAlertDispatcher,
    NoopAlertSink,
)
from src.platform import ExchangeName
from src.platform.data.models import MarketTrade, TradeSide
from src.planner import ExecutionPlanner
from src.runtime import (
    LiveRuntimeConfig,
    LiveRuntimeRunner,
    RuntimeMode,
    RuntimePhase,
)
from src.signals import SignalAction, TradeSignal


class FakeStrategy:
    async def on_start(self, snapshot):
        return []

    async def on_kline(self, kline):
        return []

    async def on_ticker(self, ticker):
        return []

    async def on_trade(self, trade):
        return [
            TradeSignal(
                symbol="ETH-USDT-PERP",
                action=SignalAction.OPEN_LONG,
                quantity=Decimal("0.1"),
            )
        ]

    async def on_order_book(self, order_book):
        return []

    async def on_account_event(self, event):
        return []


class FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"


class FakeStateStore:
    pass


def _app_config() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="unused",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def test_live_runtime_runner_exposes_health_without_replacing_app_runner_path():
    app_config = _app_config()
    context = AppContext(
        data=FakeData(),
        execution=FakeExecution(),
        state_store=FakeStateStore(),
        strategy=FakeStrategy(),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runtime_config = LiveRuntimeConfig(
        app=app_config,
        mode=RuntimeMode.LIVE_RUNTIME,
    )
    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=context,
        runtime_config=runtime_config,
    )

    async def scenario():
        before = await runner.health()
        running = await runner.start()
        stopped = await runner.stop()
        return before, running, stopped

    before, running, stopped = asyncio.run(scenario())

    assert before.phase is RuntimePhase.CREATED
    assert running.phase is RuntimePhase.RUNNING
    assert stopped.phase is RuntimePhase.STOPPED


def test_market_queue_full_records_drop_and_emits_alert():
    app_config = _app_config()
    app_config = AppConfig(
        symbol=app_config.symbol,
        exchanges=app_config.exchanges,
        data_exchange=app_config.data_exchange,
        strategy=app_config.strategy,
        data_streams=app_config.data_streams,
        state_db_path=app_config.state_db_path,
        market_queue_maxsize=1,
        signal_queue_maxsize=app_config.signal_queue_maxsize,
        alert_queue_maxsize=app_config.alert_queue_maxsize,
        dry_run=True,
        enable_email_alerts=False,
    )
    alerts = AsyncAlertDispatcher(NoopAlertSink())
    context = AppContext(
        data=FakeData(),
        execution=FakeExecution(),
        state_store=FakeStateStore(),
        strategy=FakeStrategy(),
        planner=ExecutionPlanner(),
        alerts=alerts,
    )
    runtime_config = LiveRuntimeConfig(
        app=app_config,
        mode=RuntimeMode.LIVE_RUNTIME,
    )
    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=context,
        runtime_config=runtime_config,
    )

    async def scenario():
        await runner._enqueue_market_event(_trade_event(1))
        await runner._enqueue_market_event(_trade_event(2))

    asyncio.run(scenario())

    assert runner.stats.market_events_dropped == 1
    assert alerts._queue.qsize() == 1


def _trade_event(ts: int) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_time_ms=ts,
        trade_id=str(ts),
    )
