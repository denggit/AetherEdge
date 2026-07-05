from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

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


class FakeTradeFeatureStrategy(FakeStrategy):
    def trade_feature_readiness(self):
        return {}


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


def _context(*, trade_features: bool = False) -> AppContext:
    return AppContext(
        data=FakeData(),
        execution=FakeExecution(),
        state_store=FakeStateStore(),
        strategy=(
            FakeTradeFeatureStrategy()
            if trade_features
            else FakeStrategy()
        ),
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
    def __init__(
        self,
        *,
        error: Exception | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.calls = 0
        self.error = error
        self.events = events

    def check_and_launch(self):
        self.calls += 1
        if self.events is not None:
            self.events.append("mf_supervisor")
        if self.error is not None:
            raise self.error
        return {"action": "launched", "reason": "coverage_gap"}


def _stub_non_mf_startup_work(
    runner: LiveRuntimeRunner,
    monkeypatch,
    *,
    events: list[str] | None = None,
) -> None:
    async def _noop(*_args, **_kwargs):
        return None

    async def _warmup():
        if events is not None:
            events.append("warmup")

    async def _warmup_range_speed():
        if events is not None:
            events.append("range_speed_warmup")
        return 0

    async def _recovery():
        return (object(),)

    monkeypatch.setattr(runner, "_initialize_rangebar_trust_window", lambda: None)
    monkeypatch.setattr(runner, "_bootstrap_account_config_if_enabled", _noop)
    monkeypatch.setattr(
        runner, "_check_strategy_position_mode_requirements", _noop
    )
    monkeypatch.setattr(runner, "_run_warmup", _warmup)
    monkeypatch.setattr(runner, "_warmup_range_speed_history", _warmup_range_speed)
    monkeypatch.setattr(runner, "_run_recovery", _recovery)
    monkeypatch.setattr(runner, "_run_reconciliation", _noop)
    monkeypatch.setattr(runner, "_call_on_start", _noop)
    monkeypatch.setattr(runner, "_evaluate_startup_catchup_once", _noop)
    monkeypatch.setattr(
        runner, "_finish_range_speed_warmup_after_catchup", _noop
    )
    monkeypatch.setattr(
        runner, "_start_range_speed_background_services", lambda: None
    )
    runner._heartbeat_service = SimpleNamespace(start=lambda **_kwargs: None)


def test_v10b_startup_does_not_touch_mf_supervisor(monkeypatch) -> None:
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
    _stub_non_mf_startup_work(runner, monkeypatch)

    asyncio.run(runner._startup())

    assert supervisor.calls == 0
    assert "mf_supervisor" not in runner._health.metadata
    assert runner._health.phase is RuntimePhase.RUNNING


def test_portfolio_v1_startup_calls_mf_feature_supervisor_when_enabled(
    monkeypatch,
) -> None:
    config = _app_config_for_strategy("eth_portfolio_v1")
    events: list[str] = []
    supervisor = _FakeMfSupervisor(events=events)
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(trade_features=True),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(
                AETHER_MF_FEATURE_BACKFILL_ENABLED="true"
            ),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )
    _stub_non_mf_startup_work(runner, monkeypatch, events=events)

    asyncio.run(runner._startup())

    assert supervisor.calls == 1
    assert events == ["warmup", "range_speed_warmup", "mf_supervisor"]
    assert runner._health.metadata["mf_supervisor"]["action"] == "launched"
    assert runner._health.metadata["mf_supervisor"]["mf_signal_ready"] is False
    assert runner._health.phase is RuntimePhase.RUNNING


def test_portfolio_v1_supervisor_coverage_emits_readiness_event(
    monkeypatch,
) -> None:
    class ReadySupervisor:
        def check_and_launch(self):
            return {
                "action": "none",
                "reason": "coverage_complete",
                "coverage": {
                    "mf_signal_feature_ready": True,
                    "range_footprint_ready": True,
                    "tradebar_ready": True,
                    "fixed_time_footprint_ready": True,
                    "coverage_ready": True,
                },
            }

    config = _app_config_for_strategy("eth_portfolio_v1")
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(trade_features=True),
        runtime_config=LiveRuntimeConfig(
            app=config, mode=RuntimeMode.LIVE_RUNTIME
        ),
        services={
            "project_env_config": _project_env(
                AETHER_MF_FEATURE_BACKFILL_ENABLED="true"
            ),
            "mf_feature_backfill_supervisor": ReadySupervisor(),
        },
    )
    emitted = []

    async def capture(event):
        emitted.append(event)

    monkeypatch.setattr(runner, "process_market_feature", capture)
    asyncio.run(runner._check_mf_feature_backfill_at_startup())

    assert len(emitted) == 1
    assert emitted[0].type_value == "trade_feature_readiness"
    assert emitted[0].data["mf_signal_feature_ready"] is True
    assert emitted[0].data["source"] == (
        "runtime_mf_feature_backfill_supervisor"
    )
    assert runner._health.metadata["mf_supervisor"][
        "mf_signal_ready"
    ] is True


def test_portfolio_v1_startup_marks_mf_supervisor_disabled_when_disabled(
    monkeypatch,
) -> None:
    config = _app_config_for_strategy("eth_portfolio_v1")
    supervisor = _FakeMfSupervisor()
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(trade_features=True),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )
    _stub_non_mf_startup_work(runner, monkeypatch)

    asyncio.run(runner._startup())

    assert supervisor.calls == 0
    assert (
        runner._health.metadata["mf_supervisor"]["reason"]
        == "mf_supervisor_disabled"
    )
    assert runner._health.phase is RuntimePhase.RUNNING


def test_mf_supervisor_exception_does_not_fail_startup(monkeypatch) -> None:
    config = _app_config_for_strategy("eth_portfolio_v1")
    supervisor = _FakeMfSupervisor(error=RuntimeError("boom"))
    runner = LiveRuntimeRunner(
        app_config=config,
        app_context=_context(trade_features=True),
        runtime_config=LiveRuntimeConfig(app=config, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "project_env_config": _project_env(
                AETHER_MF_FEATURE_BACKFILL_ENABLED="true"
            ),
            "mf_feature_backfill_supervisor": supervisor,
        },
    )
    _stub_non_mf_startup_work(runner, monkeypatch)

    asyncio.run(runner._startup())

    audit = runner._health.metadata["mf_supervisor"]
    assert supervisor.calls == 1
    assert audit["reason"] == "mf_supervisor_failed"
    assert audit["coverage_ready"] is False
    assert audit["mf_signal_ready"] is False
    assert runner._health.phase is RuntimePhase.RUNNING


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
