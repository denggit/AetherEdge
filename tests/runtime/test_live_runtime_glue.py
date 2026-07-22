from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, TimeRange, WarmupRequest, WarmupResult
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.range_repair.store import SqliteRangeRepairJournalStore
from src.market_data.storage import SqliteKlineStore, SqliteRangeBarStore
from src.market_data.storage import SqliteTradeStore
from src.market_data.warmup.current_rangebar import CurrentRangeBarWarmupResult
from src.platform import Balance, ExchangeName, InstrumentRule, LeverageInfo, MarginMode, Order, OrderSide, OrderStatus, Position, PositionMode, PositionSide
from src.platform.config import ProjectEnvConfig
from src.platform.data.models import MarketKline, MarketTicker, MarketTrade, TradeSide
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.order_management import ExchangeOrderResult, OrderIntent, OrderIntentStatus, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.order_management.position_plan.models import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode, RuntimePhase, StrategyRuntimeRequirements
from src.runtime.market_data.range_config import RangeRuntimeConfig
from src.runtime.account_sync import RequestThrottle
from src.runtime.recovery.models import RecoveryReport
from src.runtime.recovery.service import (
    RecoveryExchangeContext,
    RuntimeRecoveryService,
)
from src.runtime.requirements import ClosedKlineRequirement
from src.runtime.runner import LiveRuntimeError
from src.reconcile.models import ReconcileReport
from src.runtime.tasks import ClosedBarScheduler
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy as V8Strategy
from strategies.eth_portfolio_v1.strategy import Strategy as PortfolioV1Strategy

H4 = 4 * 60 * 60_000


def _feature_requirements(*, strategy_id: str = "feature-test"):
    return StrategyRuntimeRequirements.from_mapping({
        "capabilities": {
            "manifest_version": 1,
            "strategy_id": strategy_id,
            "position_snapshots": False,
            "recovery_status": False,
            "market_features": True,
            "range_speed_history": False,
            "startup_preview": False,
            "pending_work": False,
        },
        "closed_kline": {"enabled": True, "interval": "4h", "close_buffer_ms": 60000},
        "trades": {"enabled": True, "stream_enabled": True},
        "range_bars": {
            "enabled": True,
            "range_pct": "0.002",
            "aggregate_interval": "4h",
            "min_bars": 5,
        },
    })


def _snapshot(
    exchange: ExchangeName = ExchangeName.OKX,
    *,
    total: Decimal | str = "1000",
    available: Decimal | str = "1000",
) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal(total), available=Decimal(available)),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("1")),
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


def _test_project_env_config(runtime_root: Path) -> ProjectEnvConfig:
    keys = (
        "AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS",
        "AETHER_STOP_POST_CHECK_ATTEMPTS",
        "AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS",
    )
    values = {key: os.environ[key] for key in keys if key in os.environ}
    values.update(
        {
            "AETHER_STATE_DB": str(runtime_root / "state.sqlite3"),
            "AETHER_ORDER_JOURNAL_DB": str(runtime_root / "order-journal.sqlite3"),
            "AETHER_POSITION_PLAN_DB": str(runtime_root / "position-plan.sqlite3"),
            "AETHER_RANGE_CHECKPOINT_DB": str(runtime_root / "range-checkpoint.sqlite3"),
            "AETHER_RANGE_REPAIR_JOURNAL_DB": str(runtime_root / "repair-journal.sqlite3"),
            "AETHER_MARKET_DATA_DB": str(runtime_root / "market-data.sqlite3"),
            "AETHER_RANGE_MICRO_REPAIR_STATUS_PATH": str(runtime_root / "micro-repair-status.json"),
            "AETHER_RANGE_MICRO_REPAIR_LOCK_PATH": str(runtime_root / "micro-repair.lock"),
            "AETHER_RANGE_BACKFILL_STATUS_PATH": str(runtime_root / "backfill-status.json"),
            "AETHER_RANGE_BACKFILL_LOCK_PATH": str(runtime_root / "backfill.lock"),
            "AETHER_RANGE_BACKFILL_RAW_ROOT": str(runtime_root / "raw"),
        }
    )
    return ProjectEnvConfig(
        values=values,
        source_files=(),
        env_file=Path(".env"),
        example_file=None,
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
    observer_id = "feature-test"
    enabled = True

    def __init__(self, *, signal_on_aggregate: bool = False) -> None:
        self.signal_on_aggregate = signal_on_aggregate
        self.events = []
        self.on_start_called = False
        self.recovered = False
        self.last_decision_audit = None
        self.account_snapshots = []

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

    async def on_account_snapshot(self, snapshot):
        self.account_snapshots.append(snapshot)

    async def on_market_feature(self, event):
        self.events.append(event.type_value)
        if self.signal_on_aggregate and event.event_type is MarketFeatureEventType.RANGE_AGGREGATE:
            return [TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.5"), created_time_ms=event.event_time_ms)]
        return []

    def market_feature_observers(self):
        return (self,)

    def decision_audit(self):
        return self.last_decision_audit

    def strategy_identity(self) -> str:
        return "feature-test"


class FakeRecoveryService:
    def __init__(self, *, ok: bool = True) -> None:
        self.called = False
        self.ok = ok

    async def recover(self, *, strategy=None):
        self.called = True
        recover = getattr(strategy, "recover", None)
        if callable(recover):
            await recover(object())
        return RecoveryReport(
            ok=self.ok,
            snapshots=(_snapshot(ExchangeName.OKX), _snapshot(ExchangeName.BINANCE)),
            issues=() if self.ok else ("bad",),
        )


class FakeExecutionClient:
    def __init__(self, exchange: ExchangeName, *, fail: bool = False, open_stop_orders=()) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.market_profile = get_market_profile("ETH-USDT-PERP")
        self.fail = fail
        self.orders = []
        self.open_stop_orders = list(open_stop_orders)

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
        return list(self.open_stop_orders)

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        return []


class FakeAccountClient:
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, exchange: ExchangeName, *, positions=()) -> None:
        self.exchange = exchange
        self.positions = list(positions)
        self.margin_mode = None
        self.leverage = Decimal("1")
        self.leverage_margin_mode = None
        self.set_margin_mode_calls: list = []
        self.set_leverage_calls: list = []

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("1000"), available=Decimal("1000"))

    async def fetch_positions(self, symbol=None):
        return list(self.positions)

    async def fetch_leverage(self, *, margin_mode=None):
        return LeverageInfo(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol=self.symbol,
            leverage=self.leverage,
            margin_mode=self.leverage_margin_mode,
        )

    async def set_margin_mode(self, margin_mode):
        self.margin_mode = margin_mode
        self.set_margin_mode_calls.append(margin_mode)
        return {}

    async def set_leverage(self, leverage, *, margin_mode=None):
        self.leverage = Decimal(str(leverage))
        self.leverage_margin_mode = margin_mode
        self.set_leverage_calls.append((self.leverage, margin_mode))
        return LeverageInfo(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol=self.symbol,
            leverage=self.leverage,
            margin_mode=margin_mode,
        )

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class MemoryRangeBarStore:
    def __init__(self) -> None:
        self.rows = []
        self.save_calls = 0

    def save(self, rows):
        self.save_calls += 1
        self.rows.extend(rows)
        return len(rows)

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange):
        return [row for row in self.rows if row.symbol == symbol and str(row.range_pct) == str(Decimal(str(range_pct))) and time_range.start_time_ms <= row.end_time_ms <= time_range.end_time_ms]

    def latest_end_time_ms(self, *, symbol: str, range_pct: str):
        return max((row.end_time_ms for row in self.rows), default=None)


class OneTradeRangeBarBuilder:
    def __init__(self, *, range_pct: Decimal = Decimal("0.002")) -> None:
        self.range_pct = range_pct
        self.next_bar_id = 1
        self.trades: list[MarketTrade] = []

    def on_trade(self, trade: MarketTrade):
        self.trades.append(trade)
        time_ms = trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms
        if time_ms is None:
            return ()
        price = trade.price
        bar = RangeBar(
            symbol=trade.symbol,
            range_pct=self.range_pct,
            bar_id=self.next_bar_id,
            start_time_ms=time_ms,
            end_time_ms=time_ms,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=trade.quantity,
            buy_notional=price * trade.quantity if trade.side is TradeSide.BUY else Decimal("0"),
            sell_notional=price * trade.quantity if trade.side is TradeSide.SELL else Decimal("0"),
            trade_count=1,
        )
        self.next_bar_id += 1
        return (bar,)


class RangeAuditStrategy(FeatureStrategy):
    def __init__(self, range_store: MemoryRangeBarStore) -> None:
        super().__init__()
        self.range_store = range_store
        self.config = {"micro_context": {"min_range_bars": 5}}

    async def on_market_feature(self, event):
        self.events.append(event.type_value)
        if event.type_value == "closed_kline":
            rows = self.range_store.load(
                symbol="ETH-USDT-PERP",
                range_pct="0.002",
                time_range=TimeRange(event.data["open_time_ms"], event.data["close_time_ms"]),
            )
            self.last_decision_audit = _decision_audit(
                reason="range_ready" if len(rows) >= 5 else "range_missing",
                actions=(),
                range_available=len(rows) >= 5,
                range_status="ok" if len(rows) >= 5 else "insufficient",
                range_bar_count=len(rows),
                range_min_required=5,
            )
        return []


def _runner(strategy, *, data=None, services=None, dry_run=False, data_streams=()):
    pytest_root = Path(os.environ["AETHER_PYTEST_STATE_ROOT"])
    runtime_root = Path(tempfile.mkdtemp(prefix="live-runtime-glue-", dir=pytest_root))
    cfg = _app_config(dry_run=dry_run, data_streams=data_streams)
    context = AppContext(
        data=data or FakeData(),
        execution=object(),
        state_store=FakeStateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runtime_config = LiveRuntimeConfig(
        app=cfg,
        mode=RuntimeMode.LIVE_RUNTIME,
        closed_bar_buffer_ms=60_000,
    )
    range_config = RangeRuntimeConfig(
        checkpoint_db_path=str(runtime_root / "range-checkpoint.sqlite3"),
        repair_journal_db=str(runtime_root / "repair-journal.sqlite3"),
        micro_repair_status_path=str(runtime_root / "micro-repair-status.json"),
        micro_repair_lock_path=str(runtime_root / "micro-repair.lock"),
        backfill_status_path=str(runtime_root / "backfill-status.json"),
        backfill_lock_path=str(runtime_root / "backfill.lock"),
        backfill_raw_root=str(runtime_root / "raw"),
        market_data_db_path=str(runtime_root / "market-data.sqlite3"),
    )
    resolved_services = dict(services or {})
    resolved_services.setdefault("project_env_config", _test_project_env_config(runtime_root))
    resolved_services.setdefault(
        "order_journal",
        SqliteOrderJournalStore(runtime_root / "order-journal.sqlite3"),
    )
    resolved_services.setdefault(
        "position_plan_store",
        SqlitePositionPlanStore(runtime_root / "position-plan.sqlite3"),
    )
    resolved_services.setdefault(
        "range_bar_store",
        SqliteRangeBarStore(runtime_root / "market-data.sqlite3"),
    )
    resolved_services.setdefault(
        "kline_store",
        SqliteKlineStore(runtime_root / "market-data.sqlite3"),
    )
    resolved_services.setdefault(
        "range_checkpoint_store",
        SqliteRangeCheckpointStore(runtime_root / "range-checkpoint.sqlite3"),
    )
    resolved_services.setdefault(
        "range_repair_journal_store",
        SqliteRangeRepairJournalStore(runtime_root / "repair-journal.sqlite3"),
    )
    if data_streams or "range_bar_builder" in resolved_services or "range_bar_store" in resolved_services:
        identity = getattr(strategy, "strategy_identity", None)
        strategy_id = identity() if callable(identity) else "feature-test"
        resolved_services.setdefault(
            "runtime_requirements",
            _feature_requirements(strategy_id=strategy_id),
        )
    return LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=runtime_config,
        range_config=range_config,
        services=resolved_services,
    )


def _trade(*, trade_time_ms: int = 1_000) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_time_ms=trade_time_ms,
    )


def _range_bar(*, bar_id: int, start_time_ms: int, end_time_ms: int, price: Decimal | str = Decimal("100")) -> RangeBar:
    price = Decimal(str(price))
    return RangeBar(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bar_id=bar_id,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1"),
        buy_notional=price,
        sell_notional=Decimal("0"),
        trade_count=1,
    )


def _v8_strategy_with_pending_initial_stop() -> V8Strategy:
    strategy = V8Strategy()
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=5,
        avg_entry=Decimal("1620.30"),
        qty=Decimal("2.55"),
        stop_price=Decimal("1686.42"),
        entry_engine="MOMENTUM_V3",
        position_id="v8-initial-stop-post-check",
        stop_confirmed=False,
    )
    return strategy


def _v8_stop_signal() -> TradeSignal:
    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_SHORT,
        quantity=Decimal("2.55"),
        trigger_price=Decimal("1686.42"),
        metadata={"target_exchanges": ["okx"], "position_id": "v8-initial-stop-post-check"},
    )


def _v8_stop_intent(signal: TradeSignal) -> OrderIntent:
    return OrderIntent(
        intent_id="intent-v8-stop-post-check",
        strategy_id="eth_lf_portfolio_v8",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )


class CountingTradeStrategy(FeatureStrategy):
    def __init__(self, *, strategy_id: str) -> None:
        super().__init__()
        self.config = type("Cfg", (), {"strategy_id": strategy_id})()
        self.trade_calls = 0

    async def on_trade(self, trade):
        self.trade_calls += 1
        return []


def _decision_audit(
    *,
    reason: str,
    actions: tuple[str, ...],
    selected_engine: str | None = None,
    selected_side: str | None = "flat",
    range_available: bool = True,
    range_status: str = "ok",
    range_bar_count: int | None = 36,
    range_min_required: int | None = 5,
    range_imbalance: str | None = "-0.08",
    range_taker_buy_ratio: str | None = "0.46",
    range_close_pos: str | None = "0.42",
    range_micro_return_pct: str | None = "-0.0012",
) -> dict[str, object]:
    return {
        "strategy_id": "test",
        "symbol": "ETH-USDT-PERP",
        "bar_open_time_ms": 2 * H4,
        "bar_close_time_ms": 3 * H4 - 1,
        "signal_count": len(actions),
        "actions": list(actions),
        "reason": reason,
        "position_in_pos": False,
        "position_side": "flat",
        "position_engine": None,
        "position_qty": "0",
        "position_stop": None,
        "pending_entry": False,
        "selected_engine": selected_engine,
        "selected_side": selected_side,
        "risk_mult": "0",
        "quality_mult": "0",
        "micro_context_available": range_available,
        "micro_aligned": False,
        "micro_contra": False,
        "micro_entry_risk_scale": "1",
        "micro_action": "skip",
        "range_available": range_available,
        "range_status": range_status,
        "range_bar_count": range_bar_count,
        "range_min_required": range_min_required,
        "range_imbalance": range_imbalance,
        "range_taker_buy_ratio": range_taker_buy_ratio,
        "range_close_pos": range_close_pos,
        "range_micro_return_pct": range_micro_return_pct,
    }


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
async def test_poll_closed_bar_logs_4h_decision_summary_no_signal(caplog):
    strategy = FeatureStrategy()
    strategy.last_decision_audit = _decision_audit(reason="flat_route", actions=())
    runner = _runner(strategy, services={"recovery_service": None, "snapshot": _snapshot()}, dry_run=True)

    caplog.set_level(logging.INFO)
    await runner.poll_closed_bar_once(now_ms=12 * 60 * 60_000 + 60_000)

    messages = "\n".join(record.getMessage() for record in caplog.records if record.levelno == logging.INFO)
    assert "4H decision completed" in messages
    assert "decision=flat_route" in messages
    assert "range_available=" in messages
    assert "range_status=" in messages
    assert "range_bar_count=" in messages
    assert "range_min_required=" in messages
    assert "close_buffer_ms=" in messages
    assert "Closed kline detected" not in messages


@pytest.mark.asyncio
async def test_poll_closed_bar_logs_4h_decision_summary_with_signal_and_range_fields(caplog):
    strategy = FeatureStrategy()
    strategy.last_decision_audit = _decision_audit(
        reason="entry_signal",
        actions=("open_long",),
        selected_engine="BULL_RECLAIM_V2",
        selected_side="long",
        range_bar_count=42,
    )
    runner = _runner(strategy, services={"recovery_service": None, "snapshot": _snapshot()}, dry_run=True)

    caplog.set_level(logging.INFO)
    await runner.poll_closed_bar_once(now_ms=12 * 60 * 60_000 + 60_000)

    messages = "\n".join(record.getMessage() for record in caplog.records if record.levelno == logging.INFO)
    assert "4H decision completed" in messages
    assert "decision=entry_signal" in messages
    assert "decision=pending_entry_exists" not in messages
    assert "actions=open_long" in messages
    assert "selected_engine=BULL_RECLAIM_V2" in messages
    assert "range_status=" in messages
    assert "range_bar_count=42" in messages
    assert "range_min_required=" in messages
    assert "Closed kline detected" not in messages


@pytest.mark.asyncio
async def test_poll_closed_bar_logs_4h_decision_summary_when_range_unavailable(caplog):
    strategy = FeatureStrategy()
    strategy.last_decision_audit = _decision_audit(
        reason="flat_route",
        actions=(),
        range_available=False,
        range_status="unavailable",
        range_bar_count=None,
        range_min_required=5,
        range_imbalance=None,
        range_taker_buy_ratio=None,
        range_close_pos=None,
        range_micro_return_pct=None,
    )
    req = StrategyRuntimeRequirements.from_mapping({
        "closed_kline": {"enabled": True, "interval": "4h", "close_buffer_ms": 60000},
        "trades": {"enabled": True, "stream_enabled": True},
        "range_bars": {"enabled": True, "range_pct": "0.002", "aggregate_interval": "4h"},
    })
    runner = _runner(
        strategy,
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

    caplog.set_level(logging.INFO)
    await runner.poll_closed_bar_once(now_ms=12 * 60 * 60_000 + 60_000)

    messages = "\n".join(record.getMessage() for record in caplog.records if record.levelno == logging.INFO)
    assert "4H decision completed" in messages
    assert "range_available=False" in messages
    assert "range_status=" in messages
    assert "range_min_required=" in messages
    assert "Closed kline detected" not in messages


@pytest.mark.asyncio
async def test_closed_bar_poll_does_not_backfill_historical_trades_when_trade_warmup_removed(tmp_path):
    strategy = FeatureStrategy()
    data = FakeData()
    trade_store = SqliteTradeStore(
        tmp_path / "market.sqlite3",
        save_raw_trades=True,
    )
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
    await runner._stop_live_persistence_writer()

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
            "runtime_requirements": StrategyRuntimeRequirements.from_mapping(
                {
                    "capabilities": {
                        "manifest_version": 1,
                        "strategy_id": "feature-test",
                        "position_snapshots": False,
                        "recovery_status": False,
                        "market_features": True,
                        "range_speed_history": False,
                        "startup_preview": False,
                        "pending_work": False,
                    },
                    "closed_kline": {"enabled": False},
                    "trades": {
                        "enabled": True,
                        "stream_enabled": True,
                    },
                    "range_bars": {
                        "enabled": True,
                        "range_pct": "0.002",
                        "aggregate_interval": "4h",
                    },
                }
            ),
            "recovery_service": FakeRecoveryService(),
            "execution_clients": (okx, binance),
            "account_clients": (FakeAccountClient(ExchangeName.OKX), FakeAccountClient(ExchangeName.BINANCE)),
            "order_journal": repo,
            "range_bar_builder": RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.01")),
            "range_bar_store": store,
            "range_bar_aggregator": RangeBarAggregator(),
            "range_checkpoint_store": SqliteRangeCheckpointStore(tmp_path / "range_checkpoint.sqlite3"),
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
    with sqlite3.connect(repo.path) as conn:
        intents = conn.execute("SELECT intent_id FROM order_intents").fetchall()
        result_exchanges = conn.execute("SELECT exchange, COUNT(*) FROM exchange_order_results WHERE intent_id = ? GROUP BY exchange", (intent_id,)).fetchall()
    assert len(intents) == 1
    assert dict(result_exchanges) == {"binance": 1, "okx": 1}
    assert okx.orders[0].quantity == Decimal("5")
    assert binance.orders[0].quantity == Decimal("0.5")
    assert strategy.events[0] == "on_start"
    assert strategy.events.count("range_aggregate") == 1
    assert runner.stats.submitted_intents == 1
    assert runner.stats.submitted_intents == len(intents)
    assert (await runner.health()).healthy is True


@pytest.mark.asyncio
async def test_live_runtime_smoke_partial_failure_is_not_silent(tmp_path):
    runner, repo, intent_id, okx, binance, strategy = await _run_smoke(tmp_path, binance_fail=True)

    assert repo.get_intent(intent_id).status is OrderIntentStatus.PARTIALLY_SUBMITTED  # type: ignore[union-attr]
    assert [result.ok for result in repo.list_results(intent_id=intent_id)] == [True, False]
    with sqlite3.connect(repo.path) as conn:
        intents = conn.execute("SELECT intent_id FROM order_intents").fetchall()
        result_exchanges = conn.execute("SELECT exchange, COUNT(*) FROM exchange_order_results WHERE intent_id = ? GROUP BY exchange", (intent_id,)).fetchall()
    assert len(intents) == 1
    assert dict(result_exchanges) == {"binance": 1, "okx": 1}
    assert strategy.events.count("range_aggregate") == 1
    assert runner.stats.partial_failures == 1
    assert runner.stats.partial_failures == len(intents)
    assert (await runner.health()).healthy is False


@pytest.mark.asyncio
async def test_range_aggregate_for_same_bucket_is_not_executed_twice(tmp_path):
    runner, repo, intent_id, okx, binance, strategy = await _run_smoke(tmp_path, binance_fail=False)

    duplicate_events = await runner.emit_range_aggregate_for_bucket(0)

    with sqlite3.connect(repo.path) as conn:
        intents = conn.execute("SELECT intent_id FROM order_intents").fetchall()
    assert duplicate_events == []
    assert len(intents) == 1
    assert len(repo.list_results(intent_id=intent_id)) == 2
    assert len(okx.orders) == 1
    assert len(binance.orders) == 1
    assert strategy.events.count("range_aggregate") == 1


@pytest.mark.asyncio
async def test_place_stop_success_but_open_stop_orders_missing_blocks_confirmed_stop():
    strategy = _v8_strategy_with_pending_initial_stop()
    runner = _runner(
        strategy,
        services={
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX),),
            "account_clients": (
                FakeAccountClient(
                    ExchangeName.OKX,
                    positions=[
                        Position(
                            exchange=ExchangeName.OKX,
                            symbol="ETH-USDT-PERP",
                            raw_symbol="ETH-USDT-SWAP",
                            side=PositionSide.SHORT,
                            quantity=Decimal("-25.5"),
                            entry_price=Decimal("1620.50"),
                        )
                    ],
                ),
            ),
        },
    )
    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._validate_order_results_before_journal(intent=_v8_stop_intent(signal), results=[result])
    await strategy.on_order_results(signal=signal, results=verified, source="test", event_time_ms=6)

    assert verified[0].ok is False
    assert verified[0].error == "stop_post_check_failed:missing_bot_owned_stop"
    assert strategy.position.confirmed_stop_price is None
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert any("stop_replace_failed_manual_required" in item for item in strategy.recovery_alerts)


@pytest.mark.asyncio
async def test_place_stop_success_and_exchange_stop_verified_confirms_stop():
    strategy = _v8_strategy_with_pending_initial_stop()
    stop_order = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        price=Decimal("1686.42"),
        quantity=Decimal("25.5"),
        raw={"reduce_only": True, "source": "aetheredge"},
    )
    runner = _runner(
        strategy,
        services={
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX, open_stop_orders=[stop_order]),),
            "account_clients": (
                FakeAccountClient(
                    ExchangeName.OKX,
                    positions=[
                        Position(
                            exchange=ExchangeName.OKX,
                            symbol="ETH-USDT-PERP",
                            raw_symbol="ETH-USDT-SWAP",
                            side=PositionSide.SHORT,
                            quantity=Decimal("-25.5"),
                            entry_price=Decimal("1620.50"),
                        )
                    ],
                ),
            ),
        },
    )
    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._validate_order_results_before_journal(intent=_v8_stop_intent(signal), results=[result])
    await strategy.on_order_results(signal=signal, results=verified, source="test", event_time_ms=6)

    assert verified[0].ok is True
    assert strategy.position.confirmed_stop_price == Decimal("1686.42")
    assert strategy.position.pending_stop_replace is False

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
async def test_account_sync_refreshes_strategy_account_snapshot():
    class BalanceAccountClient(FakeAccountClient):
        def __init__(self, exchange: ExchangeName, *, total: str, available: str) -> None:
            super().__init__(exchange)
            self.total = Decimal(total)
            self.available = Decimal(available)

        async def fetch_balance(self, asset="USDT"):
            return Balance(exchange=self.exchange, asset=asset, total=self.total, available=self.available)

    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={
            "recovery_service": FakeRecoveryService(),
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX), FakeExecutionClient(ExchangeName.BINANCE)),
            "account_clients": (
                BalanceAccountClient(ExchangeName.OKX, total="700", available="650"),
                BalanceAccountClient(ExchangeName.BINANCE, total="300", available="280"),
            ),
        },
    )

    results = await runner._get_account_sync_service().sync_once(sync_type="account_periodic")

    assert [result.success for result in results] == [True, True]
    assert [(snap.balance.exchange, snap.balance.total, snap.balance.available) for snap in strategy.account_snapshots] == [
        (ExchangeName.OKX, Decimal("700"), Decimal("650")),
        (ExchangeName.BINANCE, Decimal("300"), Decimal("280")),
    ]
    assert {snapshot.balance.exchange for snapshot in runner._last_snapshots} == {ExchangeName.OKX, ExchangeName.BINANCE}


@pytest.mark.asyncio
async def test_account_snapshot_logging_tracks_balance_by_exchange_and_sync_type(caplog, monkeypatch):
    monkeypatch.setenv("AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS", "0")
    strategy = FeatureStrategy()
    runner = _runner(strategy)
    caplog.set_level(logging.DEBUG)

    await runner._on_account_snapshot_synced(
        _snapshot(total="1000.00", available="900.00"),
        "account_periodic",
    )
    await runner._on_account_snapshot_synced(
        _snapshot(total="1000", available="900"),
        "account_periodic",
    )
    await runner._on_account_snapshot_synced(
        _snapshot(total="1000", available="901"),
        "account_periodic",
    )
    await runner._on_account_snapshot_synced(
        _snapshot(total="1001", available="901"),
        "account_periodic",
    )
    await runner._on_account_snapshot_synced(
        _snapshot(ExchangeName.BINANCE, total="1001", available="901"),
        "account_periodic",
    )
    await runner._on_account_snapshot_synced(
        _snapshot(total="1001", available="901"),
        "post_order_account",
    )

    info_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO and "Strategy account snapshot refreshed" in record.getMessage()
    ]
    debug_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG and "Account snapshot unchanged" in record.getMessage()
    ]

    assert len(info_messages) == 5
    assert sum("reason=first_snapshot" in message for message in info_messages) == 3
    assert sum("reason=balance_changed" in message for message in info_messages) == 2
    assert any(
        "available=901" in message
        and "previous_available=900" in message
        and "previous_total=1000" in message
        for message in info_messages
    )
    assert any(
        "total=1001" in message
        and "previous_available=901" in message
        and "previous_total=1000" in message
        for message in info_messages
    )
    assert len(debug_messages) == 1
    assert "available=900 total=1000" in debug_messages[0]
    assert all("reason=keepalive_unchanged" not in message for message in info_messages)
    assert len(strategy.account_snapshots) == 6
    assert {snapshot.balance.exchange for snapshot in runner._last_snapshots} == {
        ExchangeName.OKX,
        ExchangeName.BINANCE,
    }
    assert runner._last_snapshot == strategy.account_snapshots[-1]


@pytest.mark.asyncio
async def test_account_snapshot_logging_emits_unchanged_keepalive(caplog, monkeypatch):
    monkeypatch.setenv("AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS", "1")
    strategy = FeatureStrategy()
    runner = _runner(strategy)
    caplog.set_level(logging.INFO)
    snapshot = _snapshot(total="1000", available="900")

    await runner._on_account_snapshot_synced(snapshot, "account_periodic")
    key = (ExchangeName.OKX.value, "account_periodic")
    runner._last_account_snapshot_log_ms[key] = int(time.monotonic() * 1000) - 1_001
    await runner._on_account_snapshot_synced(snapshot, "account_periodic")

    info_messages = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO and "Strategy account snapshot refreshed" in record.getMessage()
    ]
    assert len(info_messages) == 2
    assert "reason=first_snapshot" in info_messages[0]
    assert "reason=keepalive_unchanged keepalive_seconds=1" in info_messages[1]
    assert len(strategy.account_snapshots) == 2


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


class PreloadedFakeKlineStore:
    """In-memory store preloaded with synthetic closed klines."""

    def __init__(self, rows: list[MarketKline] | None = None) -> None:
        self._rows = list(rows or [])

    def load(self, *, symbol: str, interval: str, time_range: TimeRange) -> list[MarketKline]:
        return [
            r for r in self._rows
            if r.symbol == symbol and r.interval == interval
            and time_range.start_time_ms <= r.open_time_ms <= time_range.end_time_ms
        ]

    def save(self, rows) -> int:
        self._rows.extend(rows)
        return len(rows)


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

    with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=zero)
        with pytest.raises(LiveRuntimeError, match="insufficient records"):
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

    with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=zero)
        # Must NOT raise
        await runner._run_requirement_warmup()

    # Verify warmup was recorded
    assert runner.stats.warmup_runs >= 1


@pytest.mark.asyncio
async def test_live_runtime_allows_warmup_with_records():
    """Live mode with available_records >= min_records must proceed without error,
    even when warmup did not load any NEW records (records_loaded=0)."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_requirements()

    # Preload the store with 5 closed klines so available_records >= min_records.
    store = PreloadedFakeKlineStore([
        MarketKline(
            exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP",
            interval="4h", open_time_ms=H4 * i, close_time_ms=H4 * (i + 1) - 1,
            open=Decimal("1000"), high=Decimal("1010"), low=Decimal("990"),
            close=Decimal("1005"), volume=Decimal("10"), is_closed=True,
        )
        for i in range(5)
    ])

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": store,
        },
        dry_run=False,
    )
    # Warmup returns newly_loaded=0 (all records already in store)
    result = WarmupResult(
        request=WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, H4 * 5),
        ),
        gaps_before=(),
        gaps_after=(),
        records_loaded=0,
        caught_up=True,
    )

    with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=result)
        # Must NOT raise: available_records=5 >= min_records=1
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


def _high_min_warmup_requirements() -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements(
        closed_kline=ClosedKlineRequirement(enabled=True, interval="4h", warmup_days=30, min_records=1000),
    )


def _few_records_warmup_result(request: WarmupRequest | None = None) -> WarmupResult:
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
        records_loaded=5,
        caught_up=True,
    )


@pytest.mark.asyncio
async def test_live_runtime_fails_when_closed_kline_warmup_below_min_records():
    """Live mode (dry_run=False) must raise when warmup loads fewer records than min_records."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _high_min_warmup_requirements()

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
    few = _few_records_warmup_result()  # records_loaded=5, min_records=1000

    with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=few)
        with pytest.raises(LiveRuntimeError, match="insufficient records"):
            await runner._run_requirement_warmup()


@pytest.mark.asyncio
async def test_dry_run_allows_closed_kline_warmup_below_min_records_with_warning():
    """Dry-run mode must NOT raise when warmup is below min_records (warning only)."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _high_min_warmup_requirements()

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
    few = _few_records_warmup_result()  # records_loaded=5, min_records=1000

    with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=few)
        # Must NOT raise
        await runner._run_requirement_warmup()

    # Verify warmup was recorded
    assert runner.stats.warmup_runs >= 1


@pytest.mark.asyncio
async def test_closed_kline_min_records_is_enforced_by_runtime():
    """Runtime actually uses min_records from requirements to block live startup."""
    strategy = FeatureStrategy()
    data = FakeData()
    # min_records=1000 but only 5 records loaded
    req = _high_min_warmup_requirements()

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
    few = _few_records_warmup_result()

    with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
        MockSvc.return_value.warmup = AsyncMock(return_value=few)
        with pytest.raises(LiveRuntimeError) as exc_info:
            await runner._run_requirement_warmup()
    error_msg = str(exc_info.value)
    assert "insufficient records" in error_msg
    assert "1000" in error_msg or "min_records" in error_msg.lower()


# ────────────────────────────────────────────────────────────────────────────
# New tests: records_loaded → available_records semantic fix (V9C-LIVE-WARMUP-010)
# ────────────────────────────────────────────────────────────────────────────


def _warmup_req(min_records: int = 1000) -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements(
        closed_kline=ClosedKlineRequirement(enabled=True, interval="4h", warmup_days=365, min_records=min_records),
    )


def _make_klines(count: int, *, step_ms: int = H4) -> list[MarketKline]:
    return [
        MarketKline(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            interval="4h",
            open_time_ms=step_ms * i,
            close_time_ms=step_ms * (i + 1) - 1,
            open=Decimal("1000"),
            high=Decimal("1010"),
            low=Decimal("990"),
            close=Decimal("1005"),
            volume=Decimal("10"),
            is_closed=True,
        )
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_live_runtime_uses_available_kline_records_not_newly_loaded_records():
    """Repository has 1000 closed klines already. Warmup returns records_loaded=0
    (no NEW records saved). The runner must use available_records (1000 >= min_records)
    to proceed without backfill or failure."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_req(min_records=1000)
    store = PreloadedFakeKlineStore(_make_klines(1000))

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": store,
        },
        dry_run=False,
    )
    # Warmup returns 0 newly loaded records — all 1000 were already in the store.
    result = WarmupResult(
        request=WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, H4 * 999),
        ),
        gaps_before=(),
        gaps_after=(),
        records_loaded=0,
        caught_up=True,
    )

    # Patch time range computation so it covers the test klines (0 .. H4*999).
    with patch("src.runtime.components.startup.closed_bar_open_time_ms", return_value=H4 * 999):
        with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
            MockSvc.return_value.warmup = AsyncMock(return_value=result)
            # Must NOT raise — available_records=1000 >= min_records=1000
            await runner._run_requirement_warmup()

    assert runner.stats.warmup_runs >= 1


@pytest.mark.asyncio
async def test_live_runtime_backfills_when_available_records_below_min():
    """Repository has 0 records. Warmup returns records_loaded=0.
    Backfill provider saves 1000 records. Runner must proceed successfully."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_req(min_records=1000)
    store = PreloadedFakeKlineStore()  # empty

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": store,
        },
        dry_run=False,
    )
    result = WarmupResult(
        request=WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, H4 * 999),
        ),
        gaps_before=(),
        gaps_after=(),
        records_loaded=0,
        caught_up=True,
    )

    from src.market_data.warmup.historical_klines import BackfillDiagnostics

    fake_diag = BackfillDiagnostics(
        symbol="ETH-USDT-PERP",
        raw_aliases=("okx:ETH-USDT-SWAP",),
        interval="4h",
        start_open_ms=0,
        end_open_ms=H4 * 999,
        start_open_utc="2024-01-01T00:00:00+00:00",
        end_open_utc="2024-02-01T00:00:00+00:00",
        records_loaded_before=0,
        records_loaded_after=1000,
        min_records=1000,
        kline_store_class="PreloadedFakeKlineStore",
        kline_store_path=":memory:",
        provider_used="MarketDataKlineProvider",
        fetched_records=1000,
        saved_records=1000,
        success=True,
    )

    with patch("src.runtime.components.startup.closed_bar_open_time_ms", return_value=H4 * 999):
        with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
            MockSvc.return_value.warmup = AsyncMock(return_value=result)
            with patch("src.market_data.warmup.kline_provider.MarketDataKlineProvider") as MockProv:
                MockProv.return_value.backfill_and_reload = AsyncMock(return_value=fake_diag)
                # Preload store after backfill to match what a real provider would do
                store._rows = _make_klines(1000)
                # Must NOT raise
                await runner._run_requirement_warmup()

    assert runner.stats.warmup_runs >= 1


@pytest.mark.asyncio
async def test_live_runtime_fails_when_available_records_below_min_after_backfill():
    """Repository has 0 records. Backfill also only provides 5 records (< min=1000).
    Runner must raise LiveRuntimeError."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_req(min_records=1000)
    store = PreloadedFakeKlineStore()  # empty

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": store,
        },
        dry_run=False,
    )
    result = WarmupResult(
        request=WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, H4 * 999),
        ),
        gaps_before=(),
        gaps_after=(),
        records_loaded=0,
        caught_up=True,
    )

    from src.market_data.warmup.historical_klines import BackfillDiagnostics

    fake_diag = BackfillDiagnostics(
        symbol="ETH-USDT-PERP",
        raw_aliases=("okx:ETH-USDT-SWAP",),
        interval="4h",
        start_open_ms=0,
        end_open_ms=H4 * 999,
        start_open_utc="2024-01-01T00:00:00+00:00",
        end_open_utc="2024-02-01T00:00:00+00:00",
        records_loaded_before=0,
        records_loaded_after=5,
        min_records=1000,
        kline_store_class="PreloadedFakeKlineStore",
        kline_store_path=":memory:",
        provider_used="MarketDataKlineProvider",
        fetched_records=5,
        saved_records=5,
        success=False,
    )

    with patch("src.runtime.components.startup.closed_bar_open_time_ms", return_value=H4 * 999):
        with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
            MockSvc.return_value.warmup = AsyncMock(return_value=result)
            with patch("src.market_data.warmup.kline_provider.MarketDataKlineProvider") as MockProv:
                MockProv.return_value.backfill_and_reload = AsyncMock(return_value=fake_diag)
                store._rows = _make_klines(5)  # backfill only gave 5
                with pytest.raises(LiveRuntimeError, match="insufficient records"):
                    await runner._run_requirement_warmup()


@pytest.mark.asyncio
async def test_live_runtime_skips_backfill_when_available_records_already_sufficient():
    """When repository already has >= min_records, backfill must NOT be invoked
    even if newly_loaded_records is 0. This is the core semantic fix."""
    strategy = FeatureStrategy()
    data = FakeData()
    req = _warmup_req(min_records=500)
    store = PreloadedFakeKlineStore(_make_klines(1000))

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": req,
            "kline_store": store,
        },
        dry_run=False,
    )
    result = WarmupResult(
        request=WarmupRequest(
            symbol="ETH-USDT-PERP",
            dataset=MarketDataSet.KLINES,
            interval="4h",
            time_range=TimeRange(0, H4 * 999),
        ),
        gaps_before=(),
        gaps_after=(),
        records_loaded=0,
        caught_up=True,
    )

    with patch("src.runtime.components.startup.closed_bar_open_time_ms", return_value=H4 * 999):
        with patch("src.runtime.components.startup.KlineWarmupService") as MockSvc:
            MockSvc.return_value.warmup = AsyncMock(return_value=result)
            with patch("src.market_data.warmup.kline_provider.MarketDataKlineProvider") as MockProv:
                await runner._run_requirement_warmup()
                # Backfill provider must NOT have been instantiated
                MockProv.assert_not_called()

    assert runner.stats.warmup_runs >= 1


# ────────────────────────────────────────────────────────────────────────────
# Reconciliation integration tests (AE-V9C-LIVE-BOOTSTRAP-012)
# ────────────────────────────────────────────────────────────────────────────


from src.order_management.reconciliation.service import LiveStateReconciliationService


@pytest.mark.asyncio
async def test_runner_has_reconciliation_service_available():
    """Reconciliation service is lazily available from runner."""
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={
            "recovery_service": FakeRecoveryService(),
            "snapshot": _snapshot(),
        },
        dry_run=True,
    )
    svc = runner._get_reconciliation_service()
    assert svc is not None
    assert isinstance(svc, LiveStateReconciliationService)


@pytest.mark.asyncio
async def test_reconciliation_stale_plan_cleaned_on_flat_exchange():
    """Runner reconciles stale plan when all exchanges are flat."""
    import tempfile
    from pathlib import Path

    strategy = FeatureStrategy()
    store = SqlitePositionPlanStore(
        str(Path(tempfile.mkdtemp()) / "plan.sqlite3")
    )
    plan = PositionPlan(
        position_id="recon-test-1",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)
    store.upsert_leg(
        LegPlan(
            position_id="recon-test-1",
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            entry_order_id="okx-order-1",
            stop_order_id="okx-stop-1",
            sync_status=LegSyncStatus.OPEN,
        )
    )

    recon_svc = LiveStateReconciliationService(
        position_plan_store=store,
        order_journal=None,
        state_store=None,
    )
    report = await recon_svc.reconcile_and_apply(
        (_snapshot(),)
    )

    assert report.stale_plans_closed >= 1
    assert len(report.fake_order_refs_found) >= 2

    p = store.get_position("recon-test-1")
    assert p is not None
    assert p.status == PositionPlanStatus.CLOSED

    for leg in store.get_legs("recon-test-1"):
        if leg.entry_order_id:
            from src.order_management.reconciliation.validation import is_fake_order_id
            assert not is_fake_order_id(leg.entry_order_id)
        if leg.stop_order_id:
            from src.order_management.reconciliation.validation import is_fake_order_id
            assert not is_fake_order_id(leg.stop_order_id)


# ── Multi-snapshot reconciliation tests (AE-V9C-LIVE-BOOTSTRAP-013) ──


@pytest.mark.asyncio
async def test_reconciliation_receives_all_snapshots():
    """Runner passes ALL exchange snapshots to reconciliation, not just one."""
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={"recovery_service": FakeRecoveryService()},
        dry_run=True,
    )
    runner._strategy_capabilities()

    # _run_recovery should return a tuple with both OKX and Binance snapshots
    snapshots = await runner._run_recovery()
    assert len(snapshots) == 2, f"Expected 2 snapshots, got {len(snapshots)}"
    assert {s.leverage.exchange for s in snapshots} == {ExchangeName.OKX, ExchangeName.BINANCE}


@pytest.mark.asyncio
async def test_reconciliation_missing_snapshot_raises():
    """Runner raises LiveRuntimeError when snapshot count doesn't match configured exchanges."""
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={"recovery_service": FakeRecoveryService()},
        dry_run=True,
    )

    # Pass only 1 snapshot when 2 are expected
    with pytest.raises(LiveRuntimeError, match="missing exchange snapshots"):
        await runner._run_reconciliation((_snapshot(ExchangeName.OKX),))


@pytest.mark.asyncio
async def test_runner_recovery_stores_last_snapshots():
    """Runner stores all snapshots in _last_snapshots for diagnostics."""
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={"recovery_service": FakeRecoveryService()},
        dry_run=True,
    )
    runner._strategy_capabilities()

    snapshots = await runner._run_recovery()
    assert runner._last_snapshots == snapshots
    assert len(runner._last_snapshots) == 2
    # Backward compat: _last_snapshot still points to first
    assert runner._last_snapshot is not None
    assert runner._last_snapshot == snapshots[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Startup catch-up P0 safety tests (AE-V9C-LIVE-STARTUP-017)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Helpers for catch-up tests ────────────────────────────────────────────────


class CatchupTestKlineStore:
    """In-memory kline store that returns controlled rows."""

    def __init__(self, rows=()) -> None:
        self._rows = list(rows)
        self.load_calls = []

    def load(self, *, symbol: str, interval: str, time_range: TimeRange):
        self.load_calls.append((symbol, interval, time_range))
        return [
            r for r in self._rows
            if r.symbol == symbol
            and r.interval == interval
            and time_range.start_time_ms <= r.open_time_ms <= time_range.end_time_ms
        ]


class CatchupTestRangeBarStore:
    """In-memory range bar store with controlled rows."""

    def __init__(self, rows=()) -> None:
        self._rows = list(rows)
        self.load_calls = []

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange):
        self.load_calls.append((symbol, range_pct, time_range))
        return self._rows

    def save(self, rows):
        self._rows.extend(rows)
        return len(rows)


class CatchupTestStateStore:
    """State store that can be toggled to have open orders."""

    def __init__(self, *, has_open: bool = False) -> None:
        self.has_open = has_open
        self.orders_saved = []

    def list_open_orders(self, *, exchange, symbol, include_stop_orders=True):
        if self.has_open:
            return [Order(
                exchange=exchange, symbol=symbol, raw_symbol=symbol,
                order_id="open-1", client_order_id="client-open-1",
                status=OrderStatus.NEW, side="buy",
                quantity=Decimal("1"), filled_quantity=Decimal("0"),
                raw={"ordId": "open-1"},
            )]
        return []

    def save_order(self, order, *, is_stop_order=False):
        self.orders_saved.append((order, is_stop_order))

    def save_snapshot(self, snapshot):
        pass


class CatchupTestFrozenPosition:
    """A minimal position stand-in so hasattr(pos, 'quantity') works."""

    def __init__(self, quantity: Decimal) -> None:
        self.quantity = quantity


class CatchupTestStrategy:
    """Strategy that returns controlled signals.

    When *signal_action* is an OPEN_* action, :meth:`on_market_feature` sets
    ``self.pending_entry`` to a truthy sentinel, mimicking real V9C behaviour.
    """

    observer_id = "catchup-test"
    enabled = True

    def __init__(self, *, signal_action: str | None = None, in_pos: bool = False,
                 pending_entry: object = None, config: dict | None = None,
                 signal_metadata: dict | None = None) -> None:
        self.signal_action = signal_action
        self.position = type("Position", (), {"in_pos": in_pos})()
        self.pending_entry = pending_entry
        self.config = config or {}
        self.signal_metadata = dict(signal_metadata or {})
        self.events_received = []
        self.buffer = type("Buffer", (), {"evaluated_bars": set()})()
        self.bar_ready_events: list = []

    async def on_market_feature(self, event):
        self.events_received.append(event)
        if self.signal_action is None:
            return []
        action = SignalAction(self.signal_action)
        # Simulate V9C: an OPEN signal sets pending_entry.
        if action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT}:
            self.pending_entry = True  # truthy sentinel
        return [TradeSignal(
            symbol="ETH-USDT-PERP",
            action=action,
            quantity=Decimal("1"),
            reason="test_catchup",
            metadata={**{"test": True}, **self.signal_metadata},
        )]

    async def on_start(self, snapshot):
        return []

    def market_feature_observers(self):
        return (self,)

    def strategy_identity(self) -> str:
        return "test-catchup"

    def has_pending_strategy_work(self) -> bool:
        return self.pending_entry is not None

    def capture_startup_preview_state(self) -> object:
        return (
            self.pending_entry,
            set(self.buffer.evaluated_bars),
            len(self.bar_ready_events),
        )

    def restore_startup_preview_state(self, state: object) -> None:
        pending_entry, evaluated_bars, events_len = state
        self.pending_entry = pending_entry
        self.buffer.evaluated_bars = set(evaluated_bars)
        del self.bar_ready_events[events_len:]


class CatchupTestData:
    """Data feed that returns controlled ticker price and klines."""

    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, *, ticker_price: Decimal | None = None,
                 klines=()) -> None:
        self._ticker_price = ticker_price
        self._klines = list(klines)

    async def fetch_ticker(self):
        if self._ticker_price is None:
            raise RuntimeError("ticker unavailable")
        from src.platform.data.models import MarketTicker
        return MarketTicker(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            price=self._ticker_price,
            time_ms=int(time.time() * 1000),
        )

    async def fetch_klines(self, *, interval, limit=100, start_time_ms=None,
                           end_time_ms=None, use_cache=True, oldest_first=False):
        return self._klines

    async def stream_trades(self):
        if False:
            yield None

    async def stream_order_book(self):
        if False:
            yield None


class FakeRangeBarAggregatorForTest:
    """Aggregator that returns controlled aggregates."""

    def __init__(self, *, aggregates=()) -> None:
        self._aggregates = list(aggregates)

    def aggregate(self, rows, *, bucket_ms: int):
        return self._aggregates


# ── P0-1: runner doesn't crash on missing method ──────────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_does_not_raise_missing_method():
    """Direct call to _evaluate_startup_catchup_once must not raise
    AttributeError from missing _has_unresolved_follower_close or
    any other undefined method."""
    strategy = CatchupTestStrategy()
    data = CatchupTestData(ticker_price=Decimal("100"))
    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore(),
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    # Should not raise AttributeError
    try:
        await runner._evaluate_startup_catchup_once(_snapshot())
    except AttributeError as e:
        pytest.fail(f"_evaluate_startup_catchup_once raised AttributeError: {e}")
    # Even if skipped, the method must exist and be callable
    assert runner._has_unresolved_follower_close() is False


# ── P0-4: range aggregate unavailable → skip, no placeholder ──────────────────


@pytest.mark.asyncio
async def test_startup_catchup_skips_when_aggregate_event_missing():
    """Range bar store has 0 rows → skip, strategy must NOT receive
    range_aggregate_unavailable placeholder."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000  # 12:02:00 → within 300s window
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    data = CatchupTestData(ticker_price=Decimal("105"))

    # Empty range bar store → no aggregate
    range_store = CatchupTestRangeBarStore()
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[])

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    # Override time to simulate fresh window
    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Strategy must NOT have received any events (no placeholder fed)
    assert len(strategy.events_received) == 0, (
        f"Strategy received {len(strategy.events_received)} events; "
        "should be 0 because range aggregate was unavailable"
    )


# ── raw rows present but aggregate bar_count < min_range_bars → skip ──────────


@pytest.mark.asyncio
async def test_startup_catchup_requires_aggregate_bar_count():
    """Range bar store has rows but aggregate.bar_count < min_range_bars → skip."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(
        signal_action="open_long",
        config={"micro_context": {"min_range_bars": 5}},
    )
    data = CatchupTestData(ticker_price=Decimal("105"))

    # Range bar store has rows, but aggregate has bar_count=2 < min_range_bars=5
    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
        close=Decimal("100.5"), volume=Decimal("1"),
        buy_notional=Decimal("50"), sell_notional=Decimal("50"),
        trade_count=10,
    )])
    from src.market_data.models import RangeBarAggregate
    weak_aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=2,  # < min_range_bars (5)
        first_open=Decimal("100"), last_close=Decimal("100.5"),
        high=Decimal("101"), low=Decimal("99"),
        buy_notional_sum=Decimal("50"), sell_notional_sum=Decimal("50"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("100"),
    )
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[weak_aggregate])

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Strategy must NOT have received events
    assert len(strategy.events_received) == 0


# ── P0-2: price guard uses current ticker price, not kline.close ──────────────


@pytest.mark.asyncio
async def test_startup_catchup_uses_current_market_price_for_price_guard():
    """kline.close=100 but current_price=101 → adverse for LONG → discard signal."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("95"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("100"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    # Current ticker price = 101, 1% above kline.close
    data = CatchupTestData(ticker_price=Decimal("101"))

    from src.market_data.models import RangeBarAggregate
    aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=10, first_open=Decimal("100"), last_close=Decimal("100.5"),
        high=Decimal("101"), low=Decimal("99"),
        buy_notional_sum=Decimal("500"), sell_notional_sum=Decimal("500"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("1000"),
    )

    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
        close=Decimal("100.5"), volume=Decimal("1"),
        buy_notional=Decimal("500"), sell_notional=Decimal("500"),
        trade_count=100,
    )])
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[aggregate])

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Signal should be discarded by price guard (current=101, theoretical_open ~ 100,
    # LONG: upper bound = 100 * 1.0015 = 100.15, fail)
    # Strategy receives events (for preview) but nothing should execute
    assert len(strategy.events_received) > 0, "Strategy should receive preview events"


# ── P0-3: side from real signal action, not kline colour ─────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_price_guard_uses_signal_side_not_kline_direction():
    """Kline is red (close < open) but strategy returns OPEN_SHORT.
    Price guard must use SHORT rules, not guess LONG from kline."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    # Kline is red: close < open
    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("110"), high=Decimal("115"), low=Decimal("95"),
        close=Decimal("100"), volume=Decimal("10"), is_closed=True,
    )

    # Strategy returns OPEN_SHORT (not guessed from kline colour)
    strategy = CatchupTestStrategy(signal_action="open_short")
    # Current price = 99.70, theoretical_open = 100
    # SHORT: lower bound = 100 * (1 - 0.0015) = 99.85, fail
    data = CatchupTestData(ticker_price=Decimal("99.70"))

    from src.market_data.models import RangeBarAggregate
    aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=10, first_open=Decimal("100"), last_close=Decimal("100"),
        high=Decimal("110"), low=Decimal("95"),
        buy_notional_sum=Decimal("500"), sell_notional_sum=Decimal("500"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("1000"),
    )

    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("95"),
        close=Decimal("100"), volume=Decimal("1"),
        buy_notional=Decimal("500"), sell_notional=Decimal("500"),
        trade_count=100,
    )])
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[aggregate])

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    # Also set current_4h_open kline for theoretical_open fetch
    current_kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=current_4h_open, close_time_ms=current_4h_open + h4_ms - 1,
        open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
        close=Decimal("100"), volume=Decimal("0"), is_closed=False,
    )
    data._klines = [current_kline]

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Strategy receives events (for preview); signal discarded by price guard
    assert len(strategy.events_received) > 0, "Strategy should receive preview events"


# ── exchange snapshot has active position → skip ──────────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_skips_when_snapshot_has_active_position():
    """Exchange snapshot with position.quantity != 0 → skip catchup."""
    strategy = CatchupTestStrategy(signal_action="open_long")
    data = CatchupTestData(ticker_price=Decimal("100"))

    # Snapshot with a position
    pos_snapshot = PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT",
                        total=Decimal("1000"), available=Decimal("1000")),
        positions=[CatchupTestFrozenPosition(Decimal("1"))],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
                              raw_symbol="ETH-USDT-SWAP", leverage=Decimal("1")),
        position_mode=PositionMode.ONE_WAY,
    )

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": pos_snapshot,
            "kline_store": CatchupTestKlineStore(),
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    # Should skip immediately due to active position; no AttributeError
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(pos_snapshot)

    assert len(strategy.events_received) == 0


# ── strategy.position.in_pos → skip ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_skips_when_strategy_position_active():
    """Strategy.position.in_pos=True → skip catchup."""
    strategy = CatchupTestStrategy(signal_action="open_long", in_pos=True)
    data = CatchupTestData(ticker_price=Decimal("100"))

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore(),
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    assert len(strategy.events_received) == 0


# ── state_store has open order → skip ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_skips_when_state_store_has_open_order():
    """StateStore has open orders → skip catchup."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    data = CatchupTestData(ticker_price=Decimal("105"))

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    # AppContext is frozen, so mock _has_open_orders to return True
    with patch.object(runner, "_has_open_orders", return_value=True):
        with patch("time.time", return_value=now_ms / 1000):
            await runner._evaluate_startup_catchup_once(_snapshot())

    assert len(strategy.events_received) == 0


# ── unresolved follower close → skip ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_skips_when_position_plan_requires_follower_close():
    """PositionPlan with MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED → skip."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    data = CatchupTestData(ticker_price=Decimal("105"))

    # Position plan store with unresolved follower close
    import tempfile
    plan_store = SqlitePositionPlanStore(
        str(Path(tempfile.mkdtemp()) / "plan_catchup.sqlite3")
    )
    plan = PositionPlan(
        position_id="catchup-test-1",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    plan_store.upsert_position(plan)

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "position_plan_store": plan_store,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    assert len(strategy.events_received) == 0


# ── valid catch-up: all guards pass → signal executes ─────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_executes_open_signal_when_all_guards_pass():
    """Fresh window, valid range aggregate, no positions, strategy returns
    OPEN_LONG, current price within guard → signal executes with
    source='startup_catchup' and metadata.startup_catchup=True."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000  # 12:02:00 → within 300s
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    data = CatchupTestData(ticker_price=Decimal("105"))

    from src.market_data.models import RangeBarAggregate
    aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=10, first_open=Decimal("100"), last_close=Decimal("105"),
        high=Decimal("110"), low=Decimal("90"),
        buy_notional_sum=Decimal("500"), sell_notional_sum=Decimal("500"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("1000"),
    )

    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("1"),
        buy_notional=Decimal("500"), sell_notional=Decimal("500"),
        trade_count=100,
    )])
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[aggregate])

    executed_signals = []

    class CaptureExecutionRunner:
        """Wrapper to capture _execute_signals calls."""
        pass

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    original_execute = runner._execute_signals

    async def capture_execute(signals, *, source, event_time_ms, metadata=None,
                              feedback_depth=0):
        executed_signals.extend(signals)
        # In dry run, the real _execute_signals just logs; we track what
        # would have been executed.
        for signal in signals:
            runner.stats.signals_seen += 1
            runner.stats.dry_run_actions += 1

    runner._execute_signals = capture_execute

    # Current 4H kline for theoretical_open fetch
    current_kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=current_4h_open, close_time_ms=current_4h_open + h4_ms - 1,
        open=Decimal("105"), high=Decimal("105"), low=Decimal("105"),
        close=Decimal("105"), volume=Decimal("0"), is_closed=False,
    )
    data._klines = [current_kline]

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Strategy should have received preview events
    assert len(strategy.events_received) >= 2, (
        f"Expected >= 2 preview events (closed_kline + range_aggregate), "
        f"got {len(strategy.events_received)}"
    )

    # Since we captured _execute_signals, verify the signal was passed
    assert len(executed_signals) >= 1, (
        f"Expected >= 1 executed signal, got {len(executed_signals)}"
    )
    signal = executed_signals[0]
    assert signal.metadata.get("startup_catchup") is True
    assert signal.action == SignalAction.OPEN_LONG

    # Valid catch-up must preserve pending_entry (set by strategy preview).
    assert strategy.pending_entry is not None, (
        "pending_entry should be preserved after valid catch-up execution"
    )


# ── current price unavailable → skip ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_startup_catchup_skips_when_current_price_unavailable():
    """When fetch_ticker raises, catchup must skip with
    reason=current_price_unavailable."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    # ticker_price=None → fetch_ticker raises
    data = CatchupTestData(ticker_price=None)

    from src.market_data.models import RangeBarAggregate
    aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=10, first_open=Decimal("100"), last_close=Decimal("105"),
        high=Decimal("110"), low=Decimal("90"),
        buy_notional_sum=Decimal("500"), sell_notional_sum=Decimal("500"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("1000"),
    )

    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("1"),
        buy_notional=Decimal("500"), sell_notional=Decimal("500"),
        trade_count=100,
    )])
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[aggregate])

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Strategy must NOT have received events — skipped before preview
    assert len(strategy.events_received) == 0


# ── _has_unresolved_follower_close is callable and correct ────────────────────


def test_has_unresolved_follower_close_method_exists_and_works():
    """Verify the method exists on runner and returns correct value."""
    strategy = CatchupTestStrategy()
    runner = _runner(
        strategy,
        services={"recovery_service": None, "snapshot": _snapshot()},
        dry_run=True,
    )
    # Method exists
    assert callable(runner._has_unresolved_follower_close)
    # With no position plan store → returns False
    assert runner._has_unresolved_follower_close() is False


# ═══════════════════════════════════════════════════════════════════════════════
# Startup catch-up preview state capture / restore (AE-V9C-LIVE-STARTUP-018)
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_startup_catchup_price_guard_failure_restores_pending_entry():
    """Price guard failure must restore strategy.pending_entry to preview‑before
    state and NOT execute any signal."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("95"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("100"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(signal_action="open_long")
    # Current price = 101 → adverse for LONG (1% above close)
    data = CatchupTestData(ticker_price=Decimal("101"))

    from src.market_data.models import RangeBarAggregate
    aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=10, first_open=Decimal("100"), last_close=Decimal("100.5"),
        high=Decimal("101"), low=Decimal("99"),
        buy_notional_sum=Decimal("500"), sell_notional_sum=Decimal("500"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("1000"),
    )

    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
        close=Decimal("100.5"), volume=Decimal("1"),
        buy_notional=Decimal("500"), sell_notional=Decimal("500"),
        trade_count=100,
    )])
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[aggregate])

    executed_signals = []

    async def capture_execute(signals, *, source, event_time_ms, metadata=None,
                              feedback_depth=0):
        executed_signals.extend(signals)

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False
    runner._execute_signals = capture_execute

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Price guard failed → pending_entry must be restored to None.
    assert strategy.pending_entry is None, (
        "pending_entry should be restored after price_guard_failed"
    )
    # No signal executed.
    assert executed_signals == []


@pytest.mark.asyncio
async def test_startup_catchup_order_journal_duplicate_restores_pending_entry(tmp_path):
    """OrderJournal duplicate position_id must skip execution and restore
    strategy.pending_entry."""
    h4_ms = 4 * 60 * 60_000
    now_ms = 12 * 60 * 60_000 + 120_000
    current_4h_open = 12 * 60 * 60_000
    candidate_open = current_4h_open - h4_ms
    candidate_close = current_4h_open - 1

    kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=candidate_open, close_time_ms=candidate_close,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("10"), is_closed=True,
    )

    strategy = CatchupTestStrategy(
        signal_action="open_long",
        signal_metadata={"position_id": "v9c-duplicate-position"},
    )
    data = CatchupTestData(ticker_price=Decimal("105"))

    from src.market_data.models import RangeBarAggregate
    aggregate = RangeBarAggregate(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bucket_start_ms=candidate_open, bucket_end_ms=candidate_open + h4_ms - 1,
        bar_count=10, first_open=Decimal("100"), last_close=Decimal("105"),
        high=Decimal("110"), low=Decimal("90"),
        buy_notional_sum=Decimal("500"), sell_notional_sum=Decimal("500"),
        delta_notional_sum=Decimal("0"), notional_sum=Decimal("1000"),
    )

    range_store = CatchupTestRangeBarStore([RangeBar(
        symbol="ETH-USDT-PERP", range_pct=Decimal("0.002"),
        bar_id=1, start_time_ms=candidate_open, end_time_ms=candidate_open + 1000,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
        close=Decimal("105"), volume=Decimal("1"),
        buy_notional=Decimal("500"), sell_notional=Decimal("500"),
        trade_count=100,
    )])
    aggregator = FakeRangeBarAggregatorForTest(aggregates=[aggregate])

    # Pre-populate OrderJournal with a matching intent.
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    dup_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("1"),
        metadata={"position_id": "v9c-duplicate-position"},
        created_time_ms=100,
    )
    dup_intent = OrderIntent(
        intent_id="dup-intent-1",
        strategy_id="test",
        signal=dup_signal,
        target_exchanges=(ExchangeName.OKX,),
        status=OrderIntentStatus.SUBMITTED,
    )
    assert journal.claim_intent(dup_intent) is True

    executed_signals = []

    async def capture_execute(signals, *, source, event_time_ms, metadata=None,
                              feedback_depth=0):
        executed_signals.extend(signals)

    runner = _runner(
        strategy,
        data=data,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": CatchupTestKlineStore([kline]),
            "range_bar_store": range_store,
            "range_bar_aggregator": aggregator,
            "order_journal": journal,
            "runtime_requirements": _feature_requirements(),
        },
        dry_run=True,
    )
    runner._startup_catchup_evaluated = False
    runner._execute_signals = capture_execute

    # Current 4H kline for theoretical_open fetch
    current_kline = MarketKline(
        exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP", interval="4h",
        open_time_ms=current_4h_open, close_time_ms=current_4h_open + h4_ms - 1,
        open=Decimal("105"), high=Decimal("105"), low=Decimal("105"),
        close=Decimal("105"), volume=Decimal("0"), is_closed=False,
    )
    data._klines = [current_kline]

    with patch("time.time", return_value=now_ms / 1000):
        await runner._evaluate_startup_catchup_once(_snapshot())

    # Duplicate journal hit → pending_entry must be restored.
    assert strategy.pending_entry is None, (
        "pending_entry should be restored after order_journal_duplicate"
    )
    # No signal executed.
    assert executed_signals == []


def test_runner_has_no_startup_catchup_dedupe_todo():
    """runner.py must NOT contain the old startup catch-up dedupe TODO."""
    text = Path("src/runtime/runner.py").read_text(encoding="utf-8")
    assert "TODO: also check OrderJournal" not in text
    assert "TODO: also check OrderJournal / PositionPlan / StateStore for dedup" not in text


@pytest.mark.asyncio
async def test_market_queue_backlog_logs_warning_without_alert_or_drop(caplog):
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    runner._market_queue = asyncio.Queue(maxsize=50000)

    for i in range(501):
        runner._market_queue.put_nowait(_trade(trade_time_ms=i))

    caplog.set_level(logging.WARNING)
    await runner._enqueue_market_event(_trade(trade_time_ms=10_000))

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Market queue backlog high" in messages
    assert runner.stats.market_events_dropped == 0
    alerts = list(runner.context.alerts._queue._queue)  # noqa: SLF001
    assert all(alert.subject != "AetherEdge market queue full" for alert in alerts)


@pytest.mark.asyncio
async def test_market_queue_full_drops_only_when_maxsize_reached():
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    runner._market_queue = asyncio.Queue(maxsize=2)
    runner._market_queue.put_nowait(_trade(trade_time_ms=1))
    runner._market_queue.put_nowait(_trade(trade_time_ms=2))

    await runner._enqueue_market_event(_trade(trade_time_ms=3))

    assert runner.stats.market_events_dropped == 1
    alert = runner.context.alerts._queue.get_nowait()  # noqa: SLF001
    assert alert.subject == "AetherEdge market queue full"
    assert "dropped_total=1" in alert.content
    assert "pid=" in alert.content
    assert "runtime_id=" in alert.content


@pytest.mark.asyncio
async def test_market_queue_full_logs_full_without_backlog_warning(caplog):
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    runner._market_queue = asyncio.Queue(maxsize=2)
    runner._market_queue.put_nowait(_trade(trade_time_ms=1))
    runner._market_queue.put_nowait(_trade(trade_time_ms=2))

    caplog.set_level(logging.WARNING)
    await runner._enqueue_market_event(_trade(trade_time_ms=3))

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Market queue full; dropped oldest event" in messages
    assert "Market queue backlog high" not in messages
    assert runner.stats.market_events_dropped == 1


@pytest.mark.asyncio
async def test_market_queue_full_trade_marks_range_context_degraded():
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    runner._market_queue = asyncio.Queue(maxsize=2)
    runner._market_queue.put_nowait(_trade(trade_time_ms=2 * H4 + 1))
    runner._market_queue.put_nowait(_trade(trade_time_ms=2 * H4 + 2))

    await runner._enqueue_market_event(_trade(trade_time_ms=2 * H4 + 3))

    assert runner._range_context_degraded_buckets[2 * H4] == "market_queue_dropped_trade"


@pytest.mark.asyncio
async def test_dispatcher_dropped_trade_degrades_range_and_invalidates_journal():
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    invalidations = []
    runner._range_repair_journal = type(
        "Journal",
        (),
        {
            "invalidate": lambda self, **values: invalidations.append(values),
        },
    )()
    trade = _trade(trade_time_ms=2 * H4 + 3)

    await runner._handle_market_data_trade_drop(trade)

    assert runner.stats.market_events_dropped == 1
    assert runner._range_context_degraded_buckets[2 * H4] == "market_queue_dropped_trade"
    assert invalidations == [
        {
            "bucket_start_ms": 2 * H4,
            "status": "journal_invalid_dropped_trade",
            "reason": "market_queue_dropped_trade",
            "dropped_trades": 1,
        }
    ]


@pytest.mark.asyncio
async def test_mid_bucket_restart_uses_store_range_aggregate_when_enough_rows():
    range_store = MemoryRangeBarStore()
    open_time_ms = 2 * H4
    for i in range(5):
        range_store.save([
            _range_bar(
                bar_id=i + 1,
                start_time_ms=open_time_ms + i * 1_000,
                end_time_ms=open_time_ms + i * 1_000,
            )
        ])
    strategy = RangeAuditStrategy(range_store)
    runner = _runner(
        strategy,
        data=FakeData(),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "range_bar_store": range_store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    runner._rangebar_trust_start_bucket_ms = open_time_ms + H4

    events = await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)

    assert [event.type_value for event in events] == ["closed_kline", "range_aggregate"]
    assert events[-1].data["bar_count"] == 5
    assert events[-1].data.get("reason") != "live_trade_collection_started_mid_bucket"
    assert strategy.last_decision_audit["range_available"] is True
    assert strategy.last_decision_audit["range_status"] == "ok"


@pytest.mark.asyncio
async def test_mid_bucket_restart_without_store_rows_emits_unavailable():
    range_store = MemoryRangeBarStore()
    open_time_ms = 2 * H4
    strategy = RangeAuditStrategy(range_store)
    runner = _runner(
        strategy,
        data=FakeData(),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "range_bar_store": range_store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    runner._rangebar_trust_start_bucket_ms = open_time_ms + H4

    events = await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)

    assert [event.type_value for event in events] == ["closed_kline", "range_aggregate"]
    assert events[-1].data["context_available"] is False
    assert events[-1].data["incomplete"] is True
    assert events[-1].data["reason"] == "live_trade_collection_started_mid_bucket"


@pytest.mark.asyncio
async def test_mid_bucket_restart_with_insufficient_store_rows_does_not_emit_partial_range_aggregate(monkeypatch):
    range_store = MemoryRangeBarStore()
    open_time_ms = 2 * H4
    for i in range(4):
        range_store.save([
            _range_bar(
                bar_id=i + 1,
                start_time_ms=open_time_ms + i * 1_000,
                end_time_ms=open_time_ms + i * 1_000,
            )
        ])
    strategy = RangeAuditStrategy(range_store)
    runner = _runner(
        strategy,
        data=FakeData(),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "range_bar_store": range_store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    runner._rangebar_trust_start_bucket_ms = open_time_ms + H4
    processed_features = []
    original_process_market_feature = runner.process_market_feature

    async def capture_process_market_feature(event):
        processed_features.append(event)
        await original_process_market_feature(event)

    monkeypatch.setattr(runner, "process_market_feature", capture_process_market_feature)

    events = await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)

    assert [event.type_value for event in events] == ["closed_kline", "range_aggregate"]
    assert events[-1].data["context_available"] is False
    assert events[-1].data["incomplete"] is True
    assert events[-1].data["reason"] == "live_trade_collection_started_mid_bucket"
    partial_aggregates = [
        event
        for event in processed_features
        if event.type_value == "range_aggregate"
        and event.data.get("context_available", True) is True
        and event.data.get("bar_count") == 4
    ]
    assert partial_aggregates == []


@pytest.mark.asyncio
async def test_non_mid_bucket_emits_range_aggregate_even_when_store_rows_exist():
    range_store = MemoryRangeBarStore()
    open_time_ms = 2 * H4
    for i in range(5):
        range_store.save([
            _range_bar(
                bar_id=i + 1,
                start_time_ms=open_time_ms + i * 1_000,
                end_time_ms=open_time_ms + i * 1_000,
            )
        ])
    strategy = RangeAuditStrategy(range_store)
    runner = _runner(
        strategy,
        data=FakeData(),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "range_bar_store": range_store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    runner._rangebar_trust_start_bucket_ms = None

    events = await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)

    assert [event.type_value for event in events] == ["closed_kline", "range_aggregate"]
    assert events[-1].data["bar_count"] == 5
    assert strategy.last_decision_audit["range_available"] is True
    assert strategy.last_decision_audit["range_status"] == "ok"


@pytest.mark.asyncio
async def test_emit_range_aggregate_for_bucket_still_processes_feature():
    range_store = MemoryRangeBarStore()
    for i in range(5):
        range_store.save([
            _range_bar(
                bar_id=i + 1,
                start_time_ms=i * 1_000,
                end_time_ms=i * 1_000,
            )
        ])
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "range_bar_store": range_store,
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )

    events = await runner.emit_range_aggregate_for_bucket(0)

    assert [event.type_value for event in events] == ["range_aggregate"]
    assert events[0].data["bar_count"] == 5
    assert "range_aggregate" in strategy.events
    assert runner.stats.range_aggregates_created == 1


@pytest.mark.asyncio
async def test_runtime_start_logs_market_queue_settings(caplog, monkeypatch):
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)

    async def fake_startup():
        return None

    async def fake_consume_market_events(*, max_market_events):
        return None

    monkeypatch.setattr(runner, "_startup", fake_startup)
    monkeypatch.setattr(runner, "_start_producers", lambda: [])
    monkeypatch.setattr(runner, "_start_sync_tasks", lambda: [])
    monkeypatch.setattr(runner, "_consume_market_events", fake_consume_market_events)

    caplog.set_level(logging.INFO)
    await runner.run(max_market_events=0)

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Market queue settings" in messages
    assert "maxsize=" in messages
    assert "backlog_warn_threshold=" in messages
    assert "drain_batch_size=" in messages
    assert "full_alert_cooldown_seconds=300" in messages


@pytest.mark.asyncio
async def test_degraded_range_bucket_still_allows_closed_kline_decision():
    strategy = FeatureStrategy()
    runner = _runner(
        strategy,
        data=FakeData(),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "range_bar_store": MemoryRangeBarStore(),
            "range_bar_builder": RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.1")),
            "range_bar_aggregator": RangeBarAggregator(),
        },
        dry_run=True,
    )
    runner._range_context_degraded_buckets[2 * H4] = "market_queue_dropped_trade"

    events = await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)

    assert [event.type_value for event in events] == ["closed_kline", "range_aggregate"]
    assert events[1].data["context_available"] is False
    assert events[1].data["incomplete"] is True
    assert events[1].data["reason"] == "market_queue_dropped_trade"
    assert not any(
        event.type_value == "range_aggregate" and event.data.get("context_available", True) is True
        for event in events
    )
    assert "closed_kline" in strategy.events


@pytest.mark.asyncio
async def test_explicit_range_only_trade_skips_on_trade_callback(monkeypatch):
    strategy = CountingTradeStrategy(strategy_id="eth_lf_portfolio_v9c_reclaim_priority")
    strategy.raw_trade_callbacks_enabled = False
    runner = _runner(strategy, dry_run=True)
    processed_trades = []

    async def fake_process_trade(event):
        processed_trades.append(event)

    monkeypatch.setattr(runner, "_process_trade", fake_process_trade)

    await runner.process_market_event(_trade())

    assert len(processed_trades) == 1
    assert strategy.trade_calls == 0


@pytest.mark.asyncio
async def test_non_v9c_trade_event_still_calls_on_trade_callback(monkeypatch):
    strategy = CountingTradeStrategy(strategy_id="other_strategy")
    runner = _runner(strategy, dry_run=True)
    processed_trades = []

    async def fake_process_trade(event):
        processed_trades.append(event)

    monkeypatch.setattr(runner, "_process_trade", fake_process_trade)

    await runner.process_market_event(_trade())

    assert len(processed_trades) == 1
    assert strategy.trade_calls == 1


@pytest.mark.asyncio
async def test_trade_health_update_is_throttled(monkeypatch):
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    health_calls = []

    def fake_set_health(*args, **kwargs):
        health_calls.append((args, kwargs))

    monkeypatch.setattr(runner, "_set_health", fake_set_health)

    await runner.process_market_event(_trade(trade_time_ms=1))
    await runner.process_market_event(_trade(trade_time_ms=2))
    await runner.process_market_event(_trade(trade_time_ms=3))

    trade_health_calls = len(health_calls)
    assert trade_health_calls == 1

    await runner.process_market_event(
        MarketTicker(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal("100"),
            time_ms=4,
        )
    )

    assert len(health_calls) == trade_health_calls + 1


@pytest.mark.asyncio
async def test_market_consumer_drains_batch(monkeypatch):
    strategy = FeatureStrategy()
    runner = _runner(strategy, dry_run=True)
    runner._market_queue_drain_batch_size = 10
    for i in range(3):
        runner._market_queue.put_nowait(_trade(trade_time_ms=i))
    processed = []
    producer_health_checks = 0

    async def fake_process_market_event(event):
        processed.append(event)
        runner.stats.market_events_seen += 1

    def fake_raise_on_unhealthy_producer():
        nonlocal producer_health_checks
        producer_health_checks += 1

    monkeypatch.setattr(runner, "process_market_event", fake_process_market_event)
    monkeypatch.setattr(runner, "_raise_on_unhealthy_producer", fake_raise_on_unhealthy_producer)

    await runner._consume_market_events(max_market_events=3)

    assert len(processed) == 3
    assert producer_health_checks == 1


@pytest.mark.asyncio
async def test_market_failure_gate_runs_before_closed_bar_poll(monkeypatch):
    runner = _runner(FeatureStrategy(), dry_run=True)
    polled = False

    def fail_market_data() -> None:
        raise RuntimeError("market data already failed")

    async def poll() -> list:
        nonlocal polled
        polled = True
        return []

    monkeypatch.setattr(runner, "_raise_on_unhealthy_market_data", fail_market_data)
    monkeypatch.setattr(runner, "poll_closed_bar_once", poll)

    with pytest.raises(RuntimeError, match="market data already failed"):
        await runner._consume_market_events(max_market_events=1)
    assert polled is False


@pytest.mark.asyncio
async def test_direct_closed_bar_poll_rejects_failed_market_runtime(monkeypatch):
    runner = _runner(FeatureStrategy(), dry_run=True)
    feature_calls = []

    class FailedRuntime:
        def raise_if_failed(self) -> None:
            raise RuntimeError("feature pipeline unhealthy")

    runner._market_data_runtime = FailedRuntime()

    async def capture(event) -> None:
        feature_calls.append(event)

    monkeypatch.setattr(runner, "process_market_feature", capture)
    with pytest.raises(RuntimeError, match="feature pipeline unhealthy"):
        await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)
    assert feature_calls == []


@pytest.mark.asyncio
async def test_incomplete_trade_window_suppresses_entire_closed_bar_decision():
    from src.runtime.market_data.integrity import TradeDataIntegrityTracker

    tracker = TradeDataIntegrityTracker()
    tracker.mark_dropped(2 * H4 + 1_000, "trade_dispatcher_drop")
    runner = _runner(
        FeatureStrategy(),
        data=FakeData(),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "runtime_requirements": _feature_requirements(),
            "trade_data_integrity_tracker": tracker,
        },
        dry_run=True,
    )

    events = await runner.poll_closed_bar_once(now_ms=3 * H4 + 60_000)

    assert events == []
    assert runner.stats.closed_klines_seen == 0
    # Skipped windows are permanently recorded.
    assert runner._closed_bar_scheduler.is_skipped(2 * H4)
    assert runner._market_queue.empty()


# ═══════════════════════════════════════════════════════════════════════════════
# Stop order post-check via coordinator post_result_validator + retry
# (AE-V9C-LIVE-STOP-POSTCHECK-002)
#
# Design: post_result_validator runs inside MultiExchangeOrderCoordinator,
# NOT duplicated in LiveRuntimeRunner._execute_signals().  The validator
# retries up to 3 times (0.5 s delay) to tolerate brief exchange latency
# before marking a stop result ok=False.
# ═══════════════════════════════════════════════════════════════════════════════


from src.runtime.requirements import AccountStateRequirement, OrderStateRequirement

_NO_POST_SUBMIT_SYNC_REQ = StrategyRuntimeRequirements(
    account_state=AccountStateRequirement(post_order_sync_enabled=False),
    order_state=OrderStateRequirement(post_submit_sync_enabled=False),
)


# ── Helpers ─────────────────────────────────────────────────────────────────


class CountingFakeExecutionClient(FakeExecutionClient):
    """FakeExecutionClient that tracks fetch_open_stop_orders call count
    and supports per-call return sequences for retry tests."""

    def __init__(self, exchange, *, open_stop_orders_sequence=(), fail=False):
        super().__init__(exchange, fail=fail)
        self.open_stop_orders_sequence = list(open_stop_orders_sequence)
        self.fetch_open_stop_orders_calls = 0

    async def fetch_open_stop_orders(self):
        idx = self.fetch_open_stop_orders_calls
        self.fetch_open_stop_orders_calls += 1
        if idx < len(self.open_stop_orders_sequence):
            return list(self.open_stop_orders_sequence[idx])
        return list(self.open_stop_orders)


def _valid_bot_stop() -> Order:
    """Return a bot-owned valid protective stop for a SHORT position."""
    return Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        price=Decimal("1686.42"),
        quantity=Decimal("25.5"),
        raw={"reduce_only": True, "source": "aetheredge"},
    )


class CountingFakeAccountClient(FakeAccountClient):
    """FakeAccountClient that tracks fetch_positions call count
    and supports per-call return sequences for retry tests."""

    def __init__(self, exchange, *, positions_sequence=()):
        super().__init__(exchange)
        self.positions_sequence = list(positions_sequence)
        self.fetch_positions_calls = 0

    async def fetch_positions(self, symbol=None):
        idx = self.fetch_positions_calls
        self.fetch_positions_calls += 1
        if idx < len(self.positions_sequence):
            return list(self.positions_sequence[idx])
        return list(self.positions)


def _short_position() -> Position:
    return Position(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        side=PositionSide.SHORT,
        quantity=Decimal("-25.5"),
        entry_price=Decimal("1620.50"),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Test 1: post-check failure via coordinator (not runner) → stop NOT confirmed
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_execute_stop_signal_post_check_failure_blocks_confirmed_stop():
    """Coordinator post_result_validator marks stop ok=False when exchange
    has no valid bot-owned stop.  Runner does NOT call the post-check
    a second time — the coordinator's verified result flows through
    _record_order_results / _process_order_result_feedback."""
    strategy = _v8_strategy_with_pending_initial_stop()

    runner = _runner(
        strategy,
        services={
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX),),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
            "runtime_requirements": _NO_POST_SUBMIT_SYNC_REQ,
        },
    )

    signal = _v8_stop_signal()

    # ── Mock coordinator whose execute() applies the post-check internally,
    #     mirroring the real MultiExchangeOrderCoordinator behaviour. ─────
    async def mock_execute(intent):
        raw = [
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id="okx-stop-1",
                client_order_id="AEOKSS0123456789ABCDEF",
                status=OrderStatus.NEW,
            )
        ]
        verified = await runner._validate_order_results_before_journal(
            intent=intent, results=raw
        )
        return list(verified)

    coordinator = AsyncMock()
    coordinator.execute = mock_execute

    with patch.object(runner, "_get_order_coordinator", return_value=coordinator):
        await runner._execute_signals([signal], source="test", event_time_ms=6)

    # ── Post-check must have failed → strategy must NOT confirm stop ────
    assert strategy.position.confirmed_stop_price is None, (
        "confirmed_stop_price must be None after post-check failure"
    )
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert any(
        "stop_replace_failed_manual_required" in item
        for item in strategy.recovery_alerts
    ), f"Expected stop_replace_failed alert; got {strategy.recovery_alerts}"

    # ── Runner stats must reflect failure ─────────────────────────────────
    assert runner.stats.failed_intents == 1, (
        f"Expected 1 failed intent; got {runner.stats.failed_intents}"
    )
    assert runner.stats.submitted_intents == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Test 2: post-check success via coordinator → stop confirmed
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_execute_stop_signal_post_check_success_confirms_stop():
    """Coordinator post_result_validator verifies stop → strategy confirms."""
    strategy = _v8_strategy_with_pending_initial_stop()

    runner = _runner(
        strategy,
        services={
            "execution_clients": (
                FakeExecutionClient(ExchangeName.OKX, open_stop_orders=[_valid_bot_stop()]),
            ),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
            "runtime_requirements": _NO_POST_SUBMIT_SYNC_REQ,
        },
    )

    signal = _v8_stop_signal()

    async def mock_execute(intent):
        raw = [
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id="okx-stop-1",
                client_order_id="AEOKSS0123456789ABCDEF",
                status=OrderStatus.NEW,
            )
        ]
        verified = await runner._validate_order_results_before_journal(
            intent=intent, results=raw
        )
        return list(verified)

    coordinator = AsyncMock()
    coordinator.execute = mock_execute

    with patch.object(runner, "_get_order_coordinator", return_value=coordinator):
        await runner._execute_signals([signal], source="test", event_time_ms=6)

    # ── Post-check must pass → strategy must confirm stop ─────────────────
    assert strategy.position.confirmed_stop_price == Decimal("1686.42"), (
        f"Expected confirmed_stop_price=1686.42; "
        f"got {strategy.position.confirmed_stop_price}"
    )
    assert strategy.position.pending_stop_replace is False
    assert runner.stats.submitted_intents == 1
    assert runner.stats.failed_intents == 0


@pytest.mark.asyncio
async def test_portfolio_v1_startup_recovers_existing_positions_and_stops(
    tmp_path,
    monkeypatch,
) -> None:
    canonical = Decimal("1738.2542231936259150")
    effective = Decimal("1738.25")
    now_minute = (int(time.time() * 1000) // 60_000) * 60_000
    plan_store = SqlitePositionPlanStore(tmp_path / "restart-plans.sqlite3")
    lf_position_id = "portfolio-v1-restart-lf"
    mf_position_id = f"mf-low-sweep-time48-{now_minute}"
    plan_store.upsert_position(
        PositionPlan(
            position_id=lf_position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=canonical,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.6"),
            master_filled_qty_base=Decimal("0.6"),
            metadata={
                "sleeve_id": "lf",
                "average_entry_price": "2000",
            },
        )
    )
    plan_store.upsert_position(
        PositionPlan(
            position_id=mf_position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="MF_LOW_SWEEP_TIME48",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.4"),
            master_filled_qty_base=Decimal("0.4"),
            metadata={
                "sleeve_id": "mf",
                "position_id": mf_position_id,
                "engine": "MF_LOW_SWEEP_TIME48",
                "signal_time_ms": now_minute - 49 * 60_000,
                "entry_tradebar_open_time_ms": now_minute - 48 * 60_000,
                "entry_execution_time_ms": now_minute - 48 * 60_000,
                "time48_holding_minutes": 48,
                "fixed_time_exit_holding_minutes": 48,
                "exit_variant": "time48",
                "quantity_scope": "mf_sleeve_quantity",
                "protective_stop_required": False,
                "average_entry_price": "2000",
                "target_exchanges": ["okx", "binance"],
                "exchange_quantities_base": {
                    "okx": "0.4",
                    "binance": "0.4",
                },
            },
        )
    )
    for position_id, quantity, sleeve, stop_required in (
        (lf_position_id, Decimal("0.6"), "lf", True),
        (mf_position_id, Decimal("0.4"), "mf", False),
    ):
        for exchange, role in (
            (ExchangeName.OKX, LegRole.MASTER),
            (ExchangeName.BINANCE, LegRole.FOLLOWER),
        ):
            plan_store.upsert_leg(
                LegPlan(
                    position_id=position_id,
                    exchange=exchange,
                    role=role,
                    target_qty_base=quantity,
                    filled_qty_base=quantity,
                    stop_price=effective if stop_required else None,
                    stop_order_id=(
                        f"{position_id}-{exchange.value}-stop"
                        if stop_required
                        else None
                    ),
                    sync_status=LegSyncStatus.OPEN,
                )
            )

    class RestartAccount(FakeAccountClient):
        def __init__(self, exchange, position):
            super().__init__(exchange, positions=[position])
            self.set_calls = []

        async def fetch_leverage(self, *, margin_mode=None):
            return LeverageInfo(
                exchange=self.exchange,
                symbol=self.symbol,
                raw_symbol=(
                    "ETH-USDT-SWAP"
                    if self.exchange is ExchangeName.OKX
                    else "ETHUSDT"
                ),
                leverage=Decimal("15"),
                margin_mode=MarginMode.ISOLATED,
            )

        async def fetch_position_mode(self):
            return PositionMode.HEDGE

        async def set_margin_mode(self, margin_mode):
            self.set_calls.append(("set_margin_mode", margin_mode))
            raise AssertionError("matching restart must not change margin mode")

        async def set_leverage(self, leverage, *, margin_mode=None):
            self.set_calls.append(("set_leverage", leverage))
            raise AssertionError("matching restart must not change leverage")

    class RestartExecution(FakeExecutionClient):
        def __init__(self, exchange, stop):
            super().__init__(exchange, open_stop_orders=[stop])
            self.write_calls = []

        async def fetch_instrument_rule(self):
            return InstrumentRule(
                exchange=self.exchange,
                symbol=self.symbol,
                raw_symbol=(
                    "ETH-USDT-SWAP"
                    if self.exchange is ExchangeName.OKX
                    else "ETHUSDT"
                ),
                price_tick=Decimal("0.01"),
            )

        async def place_order(self, request):
            self.write_calls.append(("place_order", request))
            raise AssertionError("restart must not open a duplicate position")

        async def place_stop_market_order(self, request):
            self.write_calls.append(("place_stop_market_order", request))
            raise AssertionError("restart must not place a duplicate stop")

        async def cancel_stop_order(self, request):
            self.write_calls.append(("cancel_stop_order", request))
            raise AssertionError("restart must not cancel a valid stop")

        async def cancel_all_stop_orders(self):
            self.write_calls.append(("cancel_all_stop_orders", None))
            raise AssertionError("restart must not cancel valid stops")

    def position(exchange):
        return Position(
            exchange=exchange,
            symbol="ETH-USDT-PERP",
            raw_symbol=(
                "ETH-USDT-SWAP"
                if exchange is ExchangeName.OKX
                else "ETHUSDT"
            ),
            side=PositionSide.LONG,
            quantity=(
                Decimal("10")
                if exchange is ExchangeName.OKX
                else Decimal("1")
            ),
            entry_price=Decimal("2000"),
        )

    def stop(exchange):
        return Order(
            exchange=exchange,
            symbol="ETH-USDT-PERP",
            raw_symbol=(
                "ETH-USDT-SWAP"
                if exchange is ExchangeName.OKX
                else "ETHUSDT"
            ),
            order_id=f"{lf_position_id}-{exchange.value}-stop",
            client_order_id=None,
            status=OrderStatus.NEW,
            side=OrderSide.SELL,
            price=effective,
            quantity=(
                Decimal("6")
                if exchange is ExchangeName.OKX
                else None
            ),
            raw={
                "position_id": lf_position_id,
                (
                    "posSide"
                    if exchange is ExchangeName.OKX
                    else "positionSide"
                ): "long",
                "reduceOnly": "true",
                **(
                    {"closePosition": "true"}
                    if exchange is ExchangeName.BINANCE
                    else {}
                ),
            },
        )

    accounts = tuple(
        RestartAccount(exchange, position(exchange))
        for exchange in (ExchangeName.OKX, ExchangeName.BINANCE)
    )
    executions = tuple(
        RestartExecution(exchange, stop(exchange))
        for exchange in (ExchangeName.OKX, ExchangeName.BINANCE)
    )
    strategy = PortfolioV1Strategy()
    runner = _runner(
        strategy,
        services={
            "account_clients": accounts,
            "execution_clients": executions,
            "position_plan_store": plan_store,
            "project_env_config": ProjectEnvConfig(
                values={
                    "AETHER_LIVE_TRADING": "true",
                    "AETHER_DRY_RUN": "false",
                    "MARGIN_MODE": "isolated",
                    "OKX_LEVERAGE": "15",
                    "BINANCE_LEVERAGE": "15",
                },
                source_files=(),
                env_file=Path(".env"),
                example_file=None,
            ),
        },
    )

    class CleanReconciler:
        def __init__(self, exchange):
            self.exchange = exchange

        async def check(self):
            return ReconcileReport(
                exchange=self.exchange,
                symbol="ETH-USDT-PERP",
                checked_at_ms=now_minute,
            )

    runner._recovery_service = RuntimeRecoveryService(
        exchange_contexts=tuple(
            RecoveryExchangeContext(
                account=account,
                execution=execution,
                state_store=runner.context.state_store,
                reconciler=CleanReconciler(account.exchange),
                leverage_margin_mode=MarginMode.ISOLATED,
            )
            for account, execution in zip(accounts, executions, strict=True)
        ),
        position_plan_store=plan_store,
    )

    monkeypatch.setattr(runner, "_initialize_rangebar_trust_window", lambda: None)
    monkeypatch.setattr(runner, "_run_warmup", AsyncMock())
    monkeypatch.setattr(
        runner,
        "_warmup_range_speed_history",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(runner, "_check_startup_feature_backfills", AsyncMock())
    monkeypatch.setattr(runner, "_run_reconciliation", AsyncMock())
    monkeypatch.setattr(runner, "_evaluate_startup_catchup_once", AsyncMock())
    monkeypatch.setattr(
        runner,
        "_finish_range_speed_warmup_after_catchup",
        AsyncMock(),
    )
    monkeypatch.setattr(
        runner,
        "_start_range_speed_background_services",
        lambda: None,
    )
    monkeypatch.setattr(runner._heartbeat_service, "start", lambda **kwargs: None)

    await runner._startup()

    assert runner._health.phase is RuntimePhase.RUNNING
    assert strategy.recovery_blocking_manual_required is False
    assert strategy.position.in_pos is True
    assert strategy.position.position_id == lf_position_id
    assert strategy.position.confirmed_stop_price == effective
    assert strategy.mf_sleeve.active is True
    assert strategy.mf_sleeve.position_id == mf_position_id
    assert strategy.position.legs["okx"].stop_order_id == (
        f"{lf_position_id}-okx-stop"
    )
    assert strategy.position.legs["binance"].stop_order_id == (
        f"{lf_position_id}-binance-stop"
    )
    assert runner.stats.order_intents_created == 0
    assert runner.stats.signals_seen == 0
    assert all(account.set_calls == [] for account in accounts)
    assert all(execution.write_calls == [] for execution in executions)


# ═══════════════════════════════════════════════════════════════════════════════
# Test 3: OPEN signal NOT affected by stop post-check
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_open_signal_does_not_run_stop_post_check_before_followup_stop():
    """OPEN_SHORT entry signal must NOT be affected by stop post-check.
    The post-check only applies to PLACE_STOP_LOSS_* signals.
    Open result should remain ok=True even when no open_stop_orders exist."""
    strategy = FeatureStrategy()

    open_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_SHORT,
        quantity=Decimal("0.5"),
        metadata={"target_exchanges": ["okx"]},
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (FakeExecutionClient(ExchangeName.OKX),),
            "account_clients": (FakeAccountClient(ExchangeName.OKX),),
            "runtime_requirements": _NO_POST_SUBMIT_SYNC_REQ,
        },
    )

    # ── Mock coordinator: returns ok=True result for open order ───────────
    coordinator = AsyncMock()
    coordinator.execute = AsyncMock(
        return_value=[
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id="okx-open-1",
                client_order_id="client-open-1",
                status=OrderStatus.FILLED,
                filled_quantity=Decimal("5"),
                avg_fill_price=Decimal("100.00"),
            )
        ]
    )

    with patch.object(runner, "_get_order_coordinator", return_value=coordinator):
        await runner._execute_signals([open_signal], source="test", event_time_ms=7)

    # ── Non-stop signal must pass through unchanged ───────────────────────
    assert runner.stats.submitted_intents == 1
    assert runner.stats.failed_intents == 0
    # OPEN_SHORT should not require open_stop_orders to exist
    # The post-check simply returns results unchanged for non-stop signals


# ═══════════════════════════════════════════════════════════════════════════════
# Test 4: retry succeeds when open_stop_order becomes visible on 3rd attempt
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_stop_post_check_retries_until_open_stop_order_visible(monkeypatch):
    """Post-check retries up to 3 times.  When the stop appears on the 3rd
    fetch_open_stop_orders() call, the result remains ok=True."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "3")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0.01")

    strategy = _v8_strategy_with_pending_initial_stop()

    # open_stop_orders_sequence: empty, empty, valid
    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[], [], [_valid_bot_stop()]],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    # ── Post-check must succeed on 3rd attempt ────────────────────────────
    assert verified[0].ok is True
    assert exec_client.fetch_open_stop_orders_calls == 3, (
        f"Expected 3 fetch_open_stop_orders calls; "
        f"got {exec_client.fetch_open_stop_orders_calls}"
    )
    assert verified[0].raw.get("stop_post_check_attempts") == 3, (
        f"Expected stop_post_check_attempts=3; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )

    # ── Strategy feedback: stop must be confirmed ─────────────────────────
    await strategy.on_order_results(
        signal=signal, results=verified, source="test", event_time_ms=6
    )
    assert strategy.position.confirmed_stop_price == Decimal("1686.42")
    assert strategy.position.pending_stop_replace is False


@pytest.mark.asyncio
async def test_stop_post_check_accepts_exchange_tick_normalized_price(
    monkeypatch,
):
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "1")
    canonical = Decimal("1738.2542231936259150")
    strategy = V8Strategy()
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=5,
        avg_entry=Decimal("1800"),
        qty=Decimal("2.55"),
        stop_price=canonical,
        entry_engine="MOMENTUM_V3",
        position_id="tick-normalized-post-check",
        stop_confirmed=False,
    )
    stop = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="okx-normalized-stop",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        price=Decimal("1738.25"),
        quantity=Decimal("25.5"),
        raw={"reduceOnly": "true", "source": "aetheredge"},
    )

    class RuleAwareExecution(CountingFakeExecutionClient):
        async def fetch_instrument_rule(self):
            return InstrumentRule(
                exchange=ExchangeName.OKX,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                price_tick=Decimal("0.01"),
            )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (
                RuleAwareExecution(
                    ExchangeName.OKX,
                    open_stop_orders_sequence=[[stop]],
                ),
            ),
            "account_clients": (
                FakeAccountClient(
                    ExchangeName.OKX,
                    positions=[_short_position()],
                ),
            ),
        },
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_SHORT,
        quantity=Decimal("2.55"),
        trigger_price=canonical,
        metadata={
            "target_exchanges": ["okx"],
            "position_id": "tick-normalized-post-check",
        },
    )

    verified = await runner._verify_stop_order_results(
        signal=signal,
        results=(
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id=stop.order_id,
                client_order_id=stop.client_order_id,
                status=OrderStatus.NEW,
            ),
        ),
    )

    assert verified[0].ok is True
    assert verified[0].raw["canonical_stop_price"] == (
        "1738.254223193625915"
    )
    assert verified[0].raw["effective_expected_stop_price"] == "1738.25"
    assert verified[0].raw["actual_exchange_stop_price"] == "1738.25"
    assert verified[0].raw["price_tick"] == "0.01"
    assert verified[0].raw["price_difference"] == "0"
    assert verified[0].raw["confirmed_stop_price"] == "1738.25"



# ═══════════════════════════════════════════════════════════════════════════════
# Test 5: retry exhausted → post-check fails
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_stop_post_check_fails_after_retry_exhausted(monkeypatch):
    """Post-check retries 3 times; all return empty open_stop_orders →
    result ok=False, strategy enters manual_required."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "3")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0.01")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[], [], []],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    # ── All attempts exhausted → result must be ok=False ──────────────────
    assert verified[0].ok is False
    assert "stop_post_check_failed" in (verified[0].error or "")
    assert exec_client.fetch_open_stop_orders_calls == 3, (
        f"Expected 3 fetch_open_stop_orders calls; "
        f"got {exec_client.fetch_open_stop_orders_calls}"
    )
    assert verified[0].raw.get("stop_post_check_attempts") == 3, (
        f"Expected stop_post_check_attempts=3; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )

    # ── Strategy feedback: stop must NOT be confirmed ─────────────────────
    await strategy.on_order_results(
        signal=signal, results=verified, source="test", event_time_ms=6
    )
    assert strategy.position.confirmed_stop_price is None
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True


# ═══════════════════════════════════════════════════════════════════════════════
# Test 6: post-check runs exactly once (not duplicated by runner)
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_stop_post_check_runs_once_via_coordinator(monkeypatch):
    """Verify runner._execute_signals() does NOT call the post-check a second
    time.  The coordinator (mocked but simulating internal post_result_validator)
    is the sole caller.  fetch_open_stop_orders must be called exactly the
    retry count — not doubled."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "3")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0.01")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[], [], [_valid_bot_stop()]],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
            "runtime_requirements": _NO_POST_SUBMIT_SYNC_REQ,
        },
    )

    signal = _v8_stop_signal()

    # ── Mock coordinator whose execute() internally calls the post-check ──
    async def mock_execute(intent):
        raw = [
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id="okx-stop-1",
                client_order_id="AEOKSS0123456789ABCDEF",
                status=OrderStatus.NEW,
            )
        ]
        verified = await runner._validate_order_results_before_journal(
            intent=intent, results=raw
        )
        return list(verified)

    coordinator = AsyncMock()
    coordinator.execute = mock_execute

    with patch.object(runner, "_get_order_coordinator", return_value=coordinator):
        await runner._execute_signals([signal], source="test", event_time_ms=6)

    # ── fetch_open_stop_orders called exactly 3 times (retry), NOT 6 ──────
    assert exec_client.fetch_open_stop_orders_calls == 3, (
        f"Expected 3 fetch_open_stop_orders calls (retry only, no duplicate); "
        f"got {exec_client.fetch_open_stop_orders_calls}"
    )

    # ── Strategy received verified result (succeeded on 3rd attempt) ─────
    assert strategy.position.confirmed_stop_price == Decimal("1686.42")
    assert runner.stats.submitted_intents == 1
    assert runner.stats.failed_intents == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Stop post-check env parsing & exchange position validation tests
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_stop_post_check_attempts_env_invalid_falls_back_to_default(monkeypatch):
    """AETHER_STOP_POST_CHECK_ATTEMPTS=abc → falls back to default 3."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "abc")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[], [], []],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    # ── Falls back to 3 attempts; all return [] → fail ─────────────────
    assert exec_client.fetch_open_stop_orders_calls == 3, (
        f"Expected 3 fetch_open_stop_orders calls (default fallback); "
        f"got {exec_client.fetch_open_stop_orders_calls}"
    )
    assert verified[0].ok is False
    assert verified[0].raw.get("stop_post_check_attempts") == 3, (
        f"Expected stop_post_check_attempts=3; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )


@pytest.mark.asyncio
async def test_stop_post_check_attempts_env_clamped_to_one(monkeypatch):
    """AETHER_STOP_POST_CHECK_ATTEMPTS=0 → clamped to 1."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "0")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[]],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    # ── Clamped to 1 attempt; returns [] → fail ─────────────────────────
    assert exec_client.fetch_open_stop_orders_calls == 1, (
        f"Expected 1 fetch_open_stop_orders call (clamped to 1); "
        f"got {exec_client.fetch_open_stop_orders_calls}"
    )
    assert verified[0].ok is False
    assert verified[0].raw.get("stop_post_check_attempts") == 1, (
        f"Expected stop_post_check_attempts=1; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )


@pytest.mark.asyncio
async def test_stop_post_check_delay_env_invalid_does_not_crash(monkeypatch):
    """AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS=abc → falls back to
    0.5, does not raise."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "1")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "abc")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[]],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[_short_position()]),
            ),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    # ── Must not raise ──────────────────────────────────────────────────
    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    assert verified[0].ok is False
    assert verified[0].raw.get("stop_post_check_attempts") == 1, (
        f"Expected stop_post_check_attempts=1; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )


@pytest.mark.asyncio
async def test_stop_post_check_missing_exchange_position_does_not_confirm_stop(
    monkeypatch,
):
    """When fetch_positions() returns no active position, stop post-check
    must NOT confirm the stop — even if open_stop_orders has a valid bot-owned
    stop order."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "1")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = FakeExecutionClient(
        ExchangeName.OKX, open_stop_orders=[_valid_bot_stop()]
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (
                FakeAccountClient(ExchangeName.OKX, positions=[]),
            ),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    # ── Must fail — no exchange position ────────────────────────────────
    assert verified[0].ok is False
    assert verified[0].error == "stop_post_check_failed:missing_exchange_position", (
        f"Expected 'stop_post_check_failed:missing_exchange_position'; "
        f"got {verified[0].error}"
    )
    assert verified[0].raw.get("invalid_reason") == "missing_exchange_position", (
        f"Expected invalid_reason='missing_exchange_position'; "
        f"got {verified[0].raw.get('invalid_reason')}"
    )
    assert verified[0].raw.get("stop_post_check_attempts") == 1, (
        f"Expected stop_post_check_attempts=1; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )

    # ── Strategy feedback: stop must NOT be confirmed ────────────────────
    await strategy.on_order_results(
        signal=signal, results=verified, source="test", event_time_ms=6
    )
    assert strategy.position.confirmed_stop_price is None, (
        "confirmed_stop_price must be None when no exchange position exists"
    )
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True


@pytest.mark.asyncio
async def test_stop_post_check_retries_until_exchange_position_visible(monkeypatch):
    """Post-check retries up to 3 times.  Position appears on 3rd fetch_positions()
    call; stop appears on all 3 fetch_open_stop_orders() calls.  Must succeed."""
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_ATTEMPTS", "3")
    monkeypatch.setenv("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "0")

    strategy = _v8_strategy_with_pending_initial_stop()

    exec_client = CountingFakeExecutionClient(
        ExchangeName.OKX,
        open_stop_orders_sequence=[[_valid_bot_stop()], [_valid_bot_stop()], [_valid_bot_stop()]],
    )
    acct_client = CountingFakeAccountClient(
        ExchangeName.OKX,
        positions_sequence=[[], [], [_short_position()]],
    )

    runner = _runner(
        strategy,
        services={
            "execution_clients": (exec_client,),
            "account_clients": (acct_client,),
        },
    )

    signal = _v8_stop_signal()
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        client_order_id="AEOKSS0123456789ABCDEF",
        status=OrderStatus.NEW,
    )

    verified = await runner._verify_stop_order_results(
        signal=signal, results=[result]
    )

    # ── Post-check must succeed on 3rd attempt ──────────────────────────
    assert verified[0].ok is True, (
        f"Expected ok=True; got ok=False error={verified[0].error}"
    )
    assert acct_client.fetch_positions_calls == 3, (
        f"Expected 3 fetch_positions calls; "
        f"got {acct_client.fetch_positions_calls}"
    )
    assert exec_client.fetch_open_stop_orders_calls == 3, (
        f"Expected 3 fetch_open_stop_orders calls; "
        f"got {exec_client.fetch_open_stop_orders_calls}"
    )
    assert verified[0].raw.get("stop_post_check_attempts") == 3, (
        f"Expected stop_post_check_attempts=3; "
        f"got {verified[0].raw.get('stop_post_check_attempts')}"
    )

    # ── Strategy feedback: stop must be confirmed ───────────────────────
    await strategy.on_order_results(
        signal=signal, results=verified, source="test", event_time_ms=6
    )
    assert strategy.position.confirmed_stop_price == Decimal("1686.42"), (
        f"Expected confirmed_stop_price=1686.42; "
        f"got {strategy.position.confirmed_stop_price}"
    )
