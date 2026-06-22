from __future__ import annotations

import asyncio
import sqlite3
from decimal import Decimal

import pytest

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.events import MarketFeatureEventType
from src.market_data.models import RangeBar, TimeRange
from src.market_data.storage import SqliteTradeStore
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderStatus, PositionMode
from src.platform.data.models import MarketKline, MarketTrade, TradeSide
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.order_management import OrderIntentStatus, SqliteOrderJournalStore
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode, RuntimePhase, StrategyRuntimeRequirements
from src.runtime.recovery.models import RecoveryReport
from src.runtime.tasks import ClosedBarScheduler
from src.signals import SignalAction, TradeSignal

H4 = 4 * 60 * 60_000


def _feature_requirements():
    return StrategyRuntimeRequirements.from_mapping({
        "closed_kline": {"enabled": True, "interval": "4h", "close_buffer_ms": 60000},
        "trades": {"enabled": True, "stream_enabled": True},
        "range_bars": {"enabled": True, "range_pct": "0.002", "aggregate_interval": "4h"},
    })


def _snapshot() -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT", total=Decimal("1000"), available=Decimal("1000")),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("1")),
        position_mode=PositionMode.ONE_WAY,
    )


def _app_config(*, dry_run: bool = False, data_streams=()) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.fake:Strategy",
        data_streams=tuple(data_streams),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=dry_run,
        enable_email_alerts=False,
    )


class FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, trades=()) -> None:
        self.trades = list(trades)
        self.requested_open_times = []

    async def fetch_klines(self, *, interval, limit=100, start_time_ms=None, end_time_ms=None, use_cache=True, oldest_first=False):
        self.requested_open_times.append(start_time_ms)
        open_times = [start_time_ms] if start_time_ms is not None else [H4, 2 * H4]
        return [
            MarketKline(
                exchange=ExchangeName.OKX,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                interval=interval,
                open_time_ms=open_time,
                close_time_ms=open_time + H4 - 1,
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("105"),
                volume=Decimal("10"),
                is_closed=True,
            )
            for open_time in open_times
        ]

    async def stream_trades(self):
        for trade in self.trades:
            yield trade

    async def stream_order_book(self):
        if False:
            yield None


class FakeStateStore:
    def save_snapshot(self, snapshot):
        self.snapshot = snapshot


class FeatureStrategy:
    def __init__(self, *, signal_on_aggregate: bool = False) -> None:
        self.signal_on_aggregate = signal_on_aggregate
        self.events = []
        self.on_start_called = False
        self.recovered = False

    async def on_start(self, snapshot):
        self.on_start_called = True
        self.events.append("on_start")
        return []

    async def recover(self, context):
        self.recovered = True
        return []

    async def on_trade(self, trade):
        return []

    async def on_kline(self, kline):
        return []

    async def on_ticker(self, ticker):
        return []

    async def on_order_book(self, order_book):
        return []

    async def on_account_event(self, event):
        return []

    async def on_market_feature(self, event):
        self.events.append(event.type_value)
        if self.signal_on_aggregate and event.event_type is MarketFeatureEventType.RANGE_AGGREGATE:
            return [TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.5"), created_time_ms=event.event_time_ms)]
        return []


class FakeRecoveryService:
    def __init__(self, *, ok: bool = True) -> None:
        self.called = False
        self.ok = ok

    async def recover(self, *, strategy=None):
        self.called = True
        recover = getattr(strategy, "recover", None)
        if callable(recover):
            await recover(object())
        return RecoveryReport(ok=self.ok, snapshots=(_snapshot(),), issues=() if self.ok else ("bad",))


class FakeExecutionClient:
    def __init__(self, exchange: ExchangeName, *, fail: bool = False) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.market_profile = get_market_profile("ETH-USDT-PERP")
        self.fail = fail
        self.orders = []

    async def place_order(self, request):
        if self.fail:
            raise RuntimeError(f"{self.exchange.value} failed")
        self.orders.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=f"{self.exchange.value}-1", client_order_id=request.client_order_id, status=OrderStatus.NEW, quantity=request.quantity)

    async def place_stop_market_order(self, request):
        raise AssertionError("not expected")

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        return []


class MemoryRangeBarStore:
    def __init__(self) -> None:
        self.rows = []

    def save(self, rows):
        self.rows.extend(rows)
        return len(rows)

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange):
        return [row for row in self.rows if row.symbol == symbol and str(row.range_pct) == str(Decimal(str(range_pct))) and time_range.start_time_ms <= row.end_time_ms <= time_range.end_time_ms]

    def latest_end_time_ms(self, *, symbol: str, range_pct: str):
        return max((row.end_time_ms for row in self.rows), default=None)


def _runner(strategy, *, data=None, services=None, dry_run=False, data_streams=()):
    cfg = _app_config(dry_run=dry_run, data_streams=data_streams)
    context = AppContext(
        data=data or FakeData(),
        execution=object(),
        state_store=FakeStateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runtime_config = LiveRuntimeConfig(app=cfg, mode=RuntimeMode.LIVE_RUNTIME, closed_bar_buffer_ms=60_000)
    resolved_services = dict(services or {})
    if data_streams or "range_bar_builder" in resolved_services or "range_bar_store" in resolved_services:
        resolved_services.setdefault("runtime_requirements", _feature_requirements())
    return LiveRuntimeRunner(app_config=cfg, app_context=context, runtime_config=runtime_config, services=resolved_services)


@pytest.mark.asyncio
async def test_live_runtime_calls_recovery_and_on_start_before_events():
    strategy = FeatureStrategy()
    recovery = FakeRecoveryService()
    runner = _runner(strategy, services={"recovery_service": recovery}, dry_run=True)

    stats = await runner.run(max_market_events=0)

    assert recovery.called is True
    assert strategy.recovered is True
    assert strategy.on_start_called is True
    assert strategy.events == ["on_start"]
    assert stats.on_start_called is True


@pytest.mark.asyncio
async def test_recovery_failure_marks_runtime_error_and_blocks_trading():
    strategy = FeatureStrategy()
    runner = _runner(strategy, services={"recovery_service": FakeRecoveryService(ok=False)}, dry_run=True)

    with pytest.raises(RuntimeError):
        await runner.run(max_market_events=0)

    health = await runner.health()
    assert health.phase is RuntimePhase.ERROR
    assert health.healthy is False
    assert strategy.on_start_called is False


@pytest.mark.asyncio
async def test_closed_bar_poll_uses_buffer_and_only_emits_closed_kline():
    strategy = FeatureStrategy()
    data = FakeData()
    runner1 = _runner(strategy, data=data, services={"recovery_service": None, "snapshot": _snapshot()}, dry_run=True)
    runner2 = _runner(strategy, data=data, services={"recovery_service": None, "snapshot": _snapshot()}, dry_run=True)

    early = await runner1.poll_closed_bar_once(now_ms=10 * 60 * 60_000 + 30 * 60_000)
    closed = await runner2.poll_closed_bar_once(now_ms=12 * 60 * 60_000 + 60_000)

    assert early[0].data["open_time_ms"] == H4
    assert closed[0].data["open_time_ms"] == 2 * H4
    assert all(event.data["is_closed"] for event in [*early, *closed] if event.type_value == "closed_kline")


@pytest.mark.asyncio
async def test_closed_bar_poll_backfills_rangebar_gap_before_aggregate(tmp_path):
    strategy = FeatureStrategy()
    data = FakeData()
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    range_store = MemoryRangeBarStore()

    class GapFillFeed:
        def __init__(self):
            self.calls = []

        async def fetch_trades(self, *, symbol, start_time_ms=None, end_time_ms=None, limit=1000, oldest_first=True):
            self.calls.append((start_time_ms, end_time_ms))
            return [
                MarketTrade(exchange=ExchangeName.OKX, symbol=symbol, raw_symbol="ETH-USDT-SWAP", price=Decimal("100"), quantity=Decimal("1"), side=TradeSide.BUY, trade_time_ms=2 * H4 + 1_000),
                MarketTrade(exchange=ExchangeName.OKX, symbol=symbol, raw_symbol="ETH-USDT-SWAP", price=Decimal("100.2"), quantity=Decimal("1"), side=TradeSide.SELL, trade_time_ms=2 * H4 + 2_000),
            ]

    req = StrategyRuntimeRequirements.from_mapping({
        "closed_kline": {"enabled": True, "interval": "4h", "close_buffer_ms": 60000},
        "trades": {"enabled": True, "stream_enabled": True, "warmup_enabled": True},
        "range_bars": {"enabled": True, "range_pct": "0.002", "aggregate_interval": "4h"},
    })
    feed = GapFillFeed()
    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "historical_trade_feed": feed,
            "trade_store": trade_store,
            "range_bar_store": range_store,
            "range_bar_builder": RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.1")),
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )

    events = await runner.poll_closed_bar_once(now_ms=12 * 60 * 60_000 + 60_000)

    assert any(event.event_type is MarketFeatureEventType.RANGE_AGGREGATE for event in events)
    assert feed.calls
    assert range_store.rows
    covered = trade_store.coverage_ranges(symbol="ETH-USDT-PERP", time_range=TimeRange(2 * H4, 3 * H4 - 1), source="historical_current_bucket")
    assert covered == [TimeRange(2 * H4, 3 * H4 - 1)]


@pytest.mark.asyncio
async def test_range_bar_pipeline_saves_bar_and_emits_aggregate_feature():
    strategy = FeatureStrategy()
    store = MemoryRangeBarStore()
    runner = _runner(
        strategy,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "range_bar_builder": RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.01")),
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    trade1 = MarketTrade(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", price=Decimal("100"), quantity=Decimal("1"), side=TradeSide.BUY, trade_time_ms=1_000)
    trade2 = MarketTrade(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", price=Decimal("100.2"), quantity=Decimal("1"), side=TradeSide.SELL, trade_time_ms=2_000)

    await runner.process_market_event(trade1)
    await runner.process_market_event(trade2)
    await runner.emit_range_aggregate_for_bucket(0)

    assert len(store.rows) == 1
    assert "range_bar_closed" in strategy.events
    assert "range_aggregate" in strategy.events


async def _run_smoke(tmp_path, *, binance_fail: bool):
    strategy = FeatureStrategy(signal_on_aggregate=True)
    store = MemoryRangeBarStore()
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    okx = FakeExecutionClient(ExchangeName.OKX)
    binance = FakeExecutionClient(ExchangeName.BINANCE, fail=binance_fail)
    data = FakeData(
        trades=[
            MarketTrade(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", price=Decimal("100"), quantity=Decimal("1"), side=TradeSide.BUY, trade_time_ms=1_000),
            MarketTrade(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", price=Decimal("100.2"), quantity=Decimal("1"), side=TradeSide.SELL, trade_time_ms=2_000),
        ]
    )
    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": FakeRecoveryService(),
            "execution_clients": (okx, binance),
            "order_journal": repo,
            "range_bar_builder": RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.01")),
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "closed_bar_scheduler": ClosedBarScheduler(interval_ms=H4, close_buffer_ms=60_000),
        },
        dry_run=False,
        data_streams=("trades",),
    )

    await runner.run(max_market_events=2)
    await runner.emit_range_aggregate_for_bucket(0)
    intent_id = sqlite3.connect(tmp_path / "journal.sqlite3").execute("SELECT intent_id FROM order_intents").fetchone()[0]
    return runner, repo, intent_id, okx, binance, strategy


@pytest.mark.asyncio
async def test_live_runtime_smoke_success_records_submitted_journal(tmp_path):
    runner, repo, intent_id, okx, binance, strategy = await _run_smoke(tmp_path, binance_fail=False)

    assert repo.get_intent(intent_id).status is OrderIntentStatus.SUBMITTED  # type: ignore[union-attr]
    assert len(repo.list_results(intent_id=intent_id)) == 2
    assert okx.orders[0].quantity == Decimal("5")
    assert binance.orders[0].quantity == Decimal("0.5")
    assert strategy.events[0] == "on_start"
    assert runner.stats.submitted_intents == 1
    assert (await runner.health()).healthy is True


@pytest.mark.asyncio
async def test_live_runtime_smoke_partial_failure_is_not_silent(tmp_path):
    runner, repo, intent_id, okx, binance, strategy = await _run_smoke(tmp_path, binance_fail=True)

    assert repo.get_intent(intent_id).status is OrderIntentStatus.PARTIALLY_SUBMITTED  # type: ignore[union-attr]
    assert [result.ok for result in repo.list_results(intent_id=intent_id)] == [True, False]
    assert runner.stats.partial_failures == 1
    assert (await runner.health()).healthy is False

class FakeAccountStream:
    def __init__(self, exchange: ExchangeName, events):
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.events = list(events)

    async def stream_events(self):
        for event in self.events:
            yield event


@pytest.mark.asyncio
async def test_private_account_stream_events_are_saved_and_sent_to_strategy(tmp_path):
    from src.platform.account.events import AccountEvent, AccountEventType
    from src.platform.exchanges.models import OrderSide, OrderStatus

    class AccountAwareStrategy(FeatureStrategy):
        async def on_account_event(self, event):
            self.events.append(f"account:{event.exchange.value}:{event.event_type.value}")
            return []

    class AccountStateStore(FakeStateStore):
        def __init__(self):
            self.events = []

        def save_account_event(self, event):
            self.events.append(event)

    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        event_time_ms=123,
        order_id="o1",
        order_status=OrderStatus.FILLED,
        side=OrderSide.BUY,
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
    )
    cfg = _app_config(dry_run=True, data_streams=())
    strategy = AccountAwareStrategy()
    state = AccountStateStore()
    context = AppContext(
        data=FakeData(),
        execution=object(),
        state_store=state,
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    req = StrategyRuntimeRequirements.from_mapping({"private_account_stream": {"enabled": True}})
    runner = LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=LiveRuntimeConfig(app=cfg, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "runtime_requirements": req,
            "recovery_service": FakeRecoveryService(),
            "account_event_streams": (FakeAccountStream(ExchangeName.OKX, [event]),),
        },
    )

    stats = await runner.run()

    assert stats.account_events_seen == 1
    assert state.events == [event]
    assert "account:okx:order" in strategy.events
