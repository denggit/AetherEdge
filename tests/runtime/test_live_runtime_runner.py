from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.platform.data.models import MarketTrade, TradeSide
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode, RuntimePhase
from src.signals import SignalAction, TradeSignal


class FakeStrategy:
    async def on_start(self, snapshot):
        return []

    async def on_kline(self, kline):
        return []

    async def on_ticker(self, ticker):
        return []

    async def on_trade(self, trade):
        return [TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1"))]

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


def _app_config_for_strategy(strategy: str) -> AppConfig:
    config = _app_config()
    return AppConfig(
        symbol=config.symbol,
        exchanges=config.exchanges,
        data_exchange=config.data_exchange,
        strategy=strategy,
        data_streams=config.data_streams,
        state_db_path=config.state_db_path,
        market_queue_maxsize=config.market_queue_maxsize,
        signal_queue_maxsize=config.signal_queue_maxsize,
        alert_queue_maxsize=config.alert_queue_maxsize,
        dry_run=config.dry_run,
        enable_email_alerts=config.enable_email_alerts,
    )


def _context() -> AppContext:
    return AppContext(
        data=FakeData(),
        execution=FakeExecution(),
        state_store=FakeStateStore(),
        strategy=FakeStrategy(),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )


def _project_env(**values: str) -> ProjectEnvConfig:
    return ProjectEnvConfig(
        values=MappingProxyType(values),
        source_files=(),
        env_file=Path(".env"),
        example_file=None,
    )


class _FakeMfSupervisor:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.calls = 0
        self.error = error

    def check_and_launch(self):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return {"action": "launched", "reason": "coverage_gap"}


def test_v10b_startup_does_not_enable_mf_supervisor_by_default() -> None:
    config = _app_config_for_strategy("eth_lf_portfolio_v10b")
    supervisor = _FakeMfSupervisor()
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )

    asyncio.run(runner._check_mf_feature_backfill_at_startup())

    assert supervisor.calls == 0
    assert "mf_supervisor" not in runner._health.metadata


def test_portfolio_v1_enabled_startup_calls_mf_supervisor() -> None:
    config = _app_config_for_strategy("eth_portfolio_v1")
    supervisor = _FakeMfSupervisor()
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(
                AETHER_MF_FEATURE_BACKFILL_ENABLED="true"
            ),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )

    asyncio.run(runner._check_mf_feature_backfill_at_startup())

    assert supervisor.calls == 1
    assert runner._health.metadata["mf_supervisor"]["action"] == "launched"
    assert runner._health.metadata["mf_supervisor"]["mf_signal_ready"] is False


def test_portfolio_v1_disabled_startup_audits_disabled_reason() -> None:
    config = _app_config_for_strategy("eth_portfolio_v1")
    supervisor = _FakeMfSupervisor()
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )

    asyncio.run(runner._check_mf_feature_backfill_at_startup())

    assert supervisor.calls == 0
    assert (
        runner._health.metadata["mf_supervisor"]["reason"]
        == "mf_supervisor_disabled"
    )


def test_portfolio_v1_supervisor_failure_does_not_abort_startup() -> None:
    config = _app_config_for_strategy("eth_portfolio_v1")
    supervisor = _FakeMfSupervisor(error=RuntimeError("boom"))
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(
                AETHER_MF_FEATURE_BACKFILL_ENABLED="true"
            ),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )

    asyncio.run(runner._check_mf_feature_backfill_at_startup())

    audit = runner._health.metadata["mf_supervisor"]
    assert supervisor.calls == 1
    assert audit["reason"] == "mf_supervisor_failed"
    assert audit["coverage_ready"] is False


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
    runtime_config = LiveRuntimeConfig(app=app_config, mode=RuntimeMode.LIVE_RUNTIME)
    runner = LiveRuntimeRunner(app_config=app_config, app_context=context, runtime_config=runtime_config)

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
    runtime_config = LiveRuntimeConfig(app=app_config, mode=RuntimeMode.LIVE_RUNTIME)
    runner = LiveRuntimeRunner(app_config=app_config, app_context=context, runtime_config=runtime_config)

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
