from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.app import (
    AppAlert,
    AppConfig,
    AppContext,
    AsyncAlertDispatcher,
    EmailAlertSink,
    NoopAlertSink,
)
from src.platform import ExchangeName
from src.platform.data.models import MarketTicker
from src.planner import ExecutionPlanner
from src.runtime import (
    LiveRuntimeConfig,
    LiveRuntimeRunner,
    RuntimeMode,
    RuntimePhase,
)
from src.signals import SignalAction, TradeSignal
from src.strategy import StrategyCapabilityError


_REAL_EMAIL_ALERT_SINK_SEND = EmailAlertSink.send


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


def test_non_trade_market_queue_full_records_drop_and_emits_alert():
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
        await runner.enqueue_market_event(_ticker_event(1))
        await runner.enqueue_market_event(_ticker_event(2))

    asyncio.run(scenario())

    assert runner.stats.market_events_dropped == 1
    assert alerts._queue.qsize() == 1


@pytest.mark.asyncio
async def test_capability_error_is_flushed_to_recording_sink(
    monkeypatch,
) -> None:
    app_config = _app_config()
    recorded: list[AppAlert] = []

    class RecordingSink:
        async def send(self, alert: AppAlert) -> None:
            recorded.append(alert)

    alerts = AsyncAlertDispatcher(RecordingSink())
    context = AppContext(
        data=FakeData(),
        execution=FakeExecution(),
        state_store=FakeStateStore(),
        strategy=FakeStrategy(),
        planner=ExecutionPlanner(),
        alerts=alerts,
    )
    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=app_config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
    )
    monkeypatch.setenv("AETHER_ENABLE_EMAIL_ALERT", "true")
    monkeypatch.setenv("EMAIL_SENDER", "real-looking@example.com")
    monkeypatch.setenv("EMAIL_PASSWORD", "not-a-real-password")
    monkeypatch.setenv("EMAIL_RECEIVER", "receiver@example.com")

    with pytest.raises(
        StrategyCapabilityError,
        match="strategy capability validation failed",
    ):
        await runner.run(max_market_events=0)

    assert (await runner.health()).phase is RuntimePhase.ERROR
    assert len(recorded) == 1
    assert recorded[0].subject == "AetherEdge live runtime error"
    assert recorded[0].severity == "error"
    assert "strategy capability validation failed" in recorded[0].content
    assert not isinstance(alerts._sink, EmailAlertSink)
    assert alerts._worker is not None and alerts._worker.done()
    assert alerts.sent == 1
    assert alerts.failed == 0


@pytest.mark.asyncio
async def test_capability_error_survives_alert_flush_timeout(
    monkeypatch,
) -> None:
    app_config = _app_config()
    release = asyncio.Event()

    class BlockingSink:
        async def send(self, _alert: AppAlert) -> None:
            await release.wait()

    alerts = AsyncAlertDispatcher(BlockingSink())
    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=AppContext(
            data=FakeData(),
            execution=FakeExecution(),
            state_store=FakeStateStore(),
            strategy=FakeStrategy(),
            planner=ExecutionPlanner(),
            alerts=alerts,
        ),
        runtime_config=LiveRuntimeConfig(
            app=app_config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
    )
    monkeypatch.setattr(
        "src.runtime.runner._FATAL_ALERT_FLUSH_TIMEOUT_SECONDS",
        0,
    )

    with pytest.raises(StrategyCapabilityError) as raised:
        await runner.run(max_market_events=0)

    assert "strategy capability validation failed" in str(raised.value)
    assert alerts._worker is not None and alerts._worker.done()
    assert alerts.sent == 0
    assert alerts.failed == 0


@pytest.mark.asyncio
async def test_capability_error_retries_fatal_alert_after_bounded_queue_drain() -> None:
    app_config = _app_config()
    release = asyncio.Event()
    entered = asyncio.Event()
    fatal_rejected = asyncio.Event()
    recorded: list[AppAlert] = []

    class BlockingSink:
        async def send(self, alert: AppAlert) -> None:
            if not recorded:
                entered.set()
                await release.wait()
            recorded.append(alert)

    class ObservedDispatcher(AsyncAlertDispatcher):
        def emit(self, alert: AppAlert) -> bool:
            dropped_before = self.dropped
            queued = super().emit(alert)
            if alert.severity == "error" and self.dropped > dropped_before:
                fatal_rejected.set()
            return queued

    alerts = ObservedDispatcher(BlockingSink(), maxsize=1)
    alerts.start()
    assert alerts.emit(AppAlert("existing warning", "queued")) is True
    await entered.wait()
    assert alerts.emit(AppAlert("queued warning", "fills queue")) is True
    runner = _capability_failure_runner(app_config, alerts)
    running = asyncio.create_task(runner.run(max_market_events=0))

    await fatal_rejected.wait()
    release.set()
    with pytest.raises(StrategyCapabilityError) as raised:
        await running

    assert "strategy capability validation failed" in str(raised.value)
    assert [alert.subject for alert in recorded] == [
        "existing warning",
        "queued warning",
        "AetherEdge live runtime error",
    ]
    assert alerts.dropped == 1
    assert alerts._worker is not None and alerts._worker.done()


@pytest.mark.asyncio
async def test_capability_error_logs_when_fatal_alert_retry_stays_full(
    caplog,
    monkeypatch,
) -> None:
    app_config = _app_config()
    alerts = AsyncAlertDispatcher(maxsize=1)
    assert alerts.emit(AppAlert("existing warning", "fills queue")) is True
    monkeypatch.setattr(alerts, "start", lambda: None)
    monkeypatch.setattr(
        "src.runtime.runner._FATAL_ALERT_FLUSH_TIMEOUT_SECONDS",
        0,
    )
    runner = _capability_failure_runner(app_config, alerts)

    with pytest.raises(StrategyCapabilityError) as raised:
        await runner.run(max_market_events=0)

    assert "strategy capability validation failed" in str(raised.value)
    assert alerts.dropped == 2
    assert alerts.sent == 0
    assert "Fatal runtime alert could not be queued after bounded drain" in caplog.text
    assert "subject=AetherEdge live runtime error" in caplog.text
    assert "queue_size=1" in caplog.text
    assert "maxsize=1" in caplog.text
    assert "sent=0" in caplog.text
    assert "failed=0" in caplog.text
    assert "dropped=2" in caplog.text
    assert alerts._worker is None


@pytest.mark.asyncio
async def test_capability_error_survives_email_sink_failure(monkeypatch) -> None:
    async def failed_send_email(**_kwargs):
        return False

    monkeypatch.setattr(
        EmailAlertSink,
        "send",
        _REAL_EMAIL_ALERT_SINK_SEND,
    )
    monkeypatch.setattr(
        "src.utils.email_sender.send_email",
        failed_send_email,
    )
    app_config = _app_config()
    alerts = AsyncAlertDispatcher(EmailAlertSink())
    runner = _capability_failure_runner(app_config, alerts)

    with pytest.raises(StrategyCapabilityError) as raised:
        await runner.run(max_market_events=0)

    assert "strategy capability validation failed" in str(raised.value)
    assert alerts.failed == 1
    assert alerts.sent == 0
    assert alerts._worker is not None and alerts._worker.done()


def _capability_failure_runner(
    app_config: AppConfig,
    alerts: AsyncAlertDispatcher,
) -> LiveRuntimeRunner:
    return LiveRuntimeRunner(
        app_config=app_config,
        app_context=AppContext(
            data=FakeData(),
            execution=FakeExecution(),
            state_store=FakeStateStore(),
            strategy=FakeStrategy(),
            planner=ExecutionPlanner(),
            alerts=alerts,
        ),
        runtime_config=LiveRuntimeConfig(
            app=app_config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
    )


def _ticker_event(ts: int) -> MarketTicker:
    return MarketTicker(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        time_ms=ts,
    )
