from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.events import MarketFeatureEventType
from src.market_data.models import MarketDataSet, RangeBar, TimeRange, WarmupRequest, WarmupResult
from src.market_data.storage import SqliteTradeStore
from src.market_data.warmup.current_rangebar import CurrentRangeBarWarmupResult
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderStatus, PositionMode
from src.platform.data.models import MarketKline, MarketTrade, TradeSide
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.order_management import OrderIntentStatus, SqliteOrderJournalStore
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode, RuntimePhase, StrategyRuntimeRequirements
from src.runtime.account_sync import RequestThrottle
from src.runtime.recovery.models import RecoveryReport
from src.runtime.requirements import ClosedKlineRequirement
from src.runtime.runner import LiveRuntimeError
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
    def __init__(self):
        self.orders = []

    def save_snapshot(self, snapshot):
        self.snapshot = snapshot

    def save_order(self, order, *, is_stop_order=False):
        self.orders.append((order, is_stop_order))

    def list_open_orders(self, *, exchange, symbol, include_stop_orders=True):
        return []

    def mark_missing_open_orders_closed(self, **kwargs):
        return 0


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

    async def fetch_order_status(self, query):
        return Order(exchange=self.exchange, symbol=query.symbol, raw_symbol=query.symbol, order_id=query.order_id, client_order_id=query.client_order_id, status=OrderStatus.FILLED, quantity=Decimal("0.5"), filled_quantity=Decimal("0.5"), raw={"avgPx": "100"})

    async def fetch_open_orders(self):
        return []

    async def fetch_stop_order_status(self, query):
        return Order(exchange=self.exchange, symbol=query.symbol, raw_symbol=query.symbol, order_id=query.stop_order_id, client_order_id=query.client_order_id, status=OrderStatus.NEW)

    async def fetch_open_stop_orders(self):
        return []

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        return []


class FakeAccountClient:
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, exchange: ExchangeName) -> None:
        self.exchange = exchange

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("1000"), available=Decimal("1000"))

    async def fetch_positions(self, symbol=None):
        return []

    async def fetch_leverage(self, *, margin_mode=None):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, leverage=Decimal("1"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


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
async def test_closed_bar_poll_does_not_backfill_historical_trades_when_trade_warmup_removed(tmp_path):
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
        "trades": {"enabled": True, "stream_enabled": True},
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

    assert [event.type_value for event in events] == ["closed_kline"]
    assert feed.calls == []
    assert range_store.rows == []
    covered = trade_store.coverage_ranges(symbol="ETH-USDT-PERP", time_range=TimeRange(2 * H4, 3 * H4 - 1), source="historical_current_bucket")
    assert covered == []


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
            "account_clients": (FakeAccountClient(ExchangeName.OKX), FakeAccountClient(ExchangeName.BINANCE)),
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

@pytest.mark.asyncio
async def test_legacy_private_account_stream_requirement_does_not_start_account_producers(tmp_path):
    cfg = _app_config(dry_run=True, data_streams=())
    strategy = FeatureStrategy()
    context = AppContext(
        data=FakeData(),
        execution=object(),
        state_store=FakeStateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    req = StrategyRuntimeRequirements.from_mapping({"trades": {"enabled": True, "stream_enabled": True}, "private_account_stream": {"enabled": True}})
    runner = LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=LiveRuntimeConfig(app=cfg, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "runtime_requirements": req,
            "recovery_service": FakeRecoveryService(),
        },
    )

    tasks = runner._start_producers()
    runner._producer_tasks = tasks
    await asyncio.sleep(0)
    await runner._stop_producers()

    assert len(tasks) == 1
    assert {item.name for item in runner._producer_monitor.snapshot()} <= {"trades"}


def test_request_sync_contexts_fail_fast_on_account_execution_exchange_mismatch():
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={
            "recovery_service": FakeRecoveryService(),
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX),),
            "account_clients": (FakeAccountClient(ExchangeName.BINANCE),),
        },
    )

    with pytest.raises(RuntimeError, match="exchange mismatch"):
        runner._get_sync_contexts()


def test_request_sync_contexts_fail_fast_on_partial_injection():
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={
            "recovery_service": FakeRecoveryService(),
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX),),
        },
    )

    with pytest.raises(RuntimeError, match="injected together"):
        runner._get_sync_contexts()


def test_request_sync_services_share_runtime_throttle():
    strategy = FeatureStrategy()
    throttle = RequestThrottle(min_interval_seconds=0)
    runner = _runner(
        strategy,
        services={
            "recovery_service": FakeRecoveryService(),
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX), FakeExecutionClient(ExchangeName.BINANCE)),
            "account_clients": (FakeAccountClient(ExchangeName.BINANCE), FakeAccountClient(ExchangeName.OKX)),
            "request_sync_throttle": throttle,
        },
    )

    account_service = runner._get_account_sync_service()
    order_service = runner._get_order_sync_service()

    assert account_service.throttle is throttle
    assert order_service.throttle is throttle
    assert [context.account.exchange for context in account_service.contexts] == [ExchangeName.OKX, ExchangeName.BINANCE]


@pytest.mark.asyncio
async def test_closed_bar_poll_emits_unavailable_range_aggregate_for_live_only_partial_bucket():
    strategy = FeatureStrategy()
    data = FakeData()
    req = StrategyRuntimeRequirements.from_mapping({
        "closed_kline": {"enabled": True, "interval": "4h", "close_buffer_ms": 60000},
        "trades": {"enabled": True, "stream_enabled": True},
        "range_bars": {"enabled": True, "range_pct": "0.002", "aggregate_interval": "4h"},
    })
    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "range_bar_store": MemoryRangeBarStore(),
            "range_bar_builder": RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.1")),
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    runner._rangebar_trust_start_bucket_ms = 3 * H4

    events = await runner.poll_closed_bar_once(now_ms=12 * 60 * 60_000 + 60_000)

    assert [event.type_value for event in events] == ["closed_kline", "range_aggregate"]
    assert events[-1].data["bar_count"] == 0
    assert events[-1].data["context_available"] is False
    assert events[-1].data["incomplete"] is True
    assert strategy.events[-2:] == ["closed_kline", "range_aggregate"]


# ────────────────────────────────────────────────────────────────────────────
# Warmup fail-fast tests
# ────────────────────────────────────────────────────────────────────────────


class FakeKlineStore:
    """In-memory store that returns no rows (used for zero-record warmup tests)."""

    def load(self, *, symbol: str, interval: str, time_range: TimeRange) -> list:
        return []


def _warmup_requirements() -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements(
        closed_kline=ClosedKlineRequirement(enabled=True, interval="4h", warmup_days=30, min_records=1),
    )


def _zero_warmup_result(request: WarmupRequest | None = None) -> WarmupResult:
    if request is None:
        request = WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, H4),
        )
    return WarmupResult(
        request=request,
        gaps_before=(),
        gaps_after=(),
        records_loaded=0,
        caught_up=True,
    )


@pytest.mark.asyncio
async def test_live_runtime_fails_when_closed_kline_warmup_loads_zero_records():
    """Live mode (dry_run=False) must raise when warmup loads 0 records."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_requirements()

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": FakeKlineStore(),
        },
        dry_run=False,
    )
    zero = _zero_warmup_result()

    with patch("src.runtime.runner.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=zero)
        with pytest.raises(LiveRuntimeError, match="zero records"):
            await runner._run_requirement_warmup()


@pytest.mark.asyncio
async def test_dry_run_allows_zero_closed_kline_warmup_with_warning():
    """Dry-run mode must NOT raise when warmup loads 0 records (warning only)."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_requirements()

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": FakeKlineStore(),
        },
        dry_run=True,
    )
    zero = _zero_warmup_result()

    with patch("src.runtime.runner.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=zero)
        # Must NOT raise
        await runner._run_requirement_warmup()

    # Verify warmup was recorded
    assert runner.stats.warmup_runs >= 1


@pytest.mark.asyncio
async def test_live_runtime_allows_warmup_with_records():
    """Live mode with records_loaded > 0 must proceed without error."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_requirements()

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": FakeKlineStore(),
        },
        dry_run=False,
    )
    result = _zero_warmup_result()
    # Simulate warmup that loaded records successfully
    result = WarmupResult(
        request=result.request,
        gaps_before=(),
        gaps_after=(),
        records_loaded=5,
        caught_up=True,
    )

    with patch("src.runtime.runner.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=result)
        # Must NOT raise
        await runner._run_requirement_warmup()

    assert runner.stats.warmup_runs >= 1


def test_closed_kline_requirement_min_records_default():
    """Default min_records is 1."""
    req = ClosedKlineRequirement()
    assert req.min_records == 1


def test_closed_kline_requirement_min_records_configurable():
    """min_records can be set via from_mapping."""
    req = StrategyRuntimeRequirements.from_mapping({
        "closed_kline": {"enabled": True, "interval": "4h", "warmup_days": 365, "min_records": 1000},
    })
    assert req.closed_kline.min_records == 1000
