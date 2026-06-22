from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.order_management import SqliteOrderJournalStore
from src.platform import Balance, ExchangeName, LeverageInfo, MarginMode, PositionMode
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import Order, OrderQuery, OrderSide, OrderStatus, OrderType, StopOrderQuery
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime.requirements import StrategyRuntimeRequirements
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy

H4 = 4 * 60 * 60_000


@dataclass(frozen=True)
class _StaticEngine:
    name: str
    priority: int
    side: Side

    def evaluate(self, context: BarReadyContext):
        if self.side is Side.FLAT:
            return None
        return EngineSignal(side=self.side, engine=self.name, priority=self.priority, reason="runtime_integration")


class _FakeFeatureBuilder:
    def build_latest(self, klines, *, target_close_time_ms):
        from strategies.eth_lf_portfolio_v8.features.live_features import V8EngineFeatureRows

        return V8EngineFeatureRows(
            momentum={"atr": "10", "long_exit_channel": False, "short_exit_channel": False},
            bear={"atr": "10", "short_exit_channel": False},
            bull={"atr": "10", "long_exit_channel": False},
        )


class _FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    async def fetch_klines(self, *args, **kwargs):
        return []

    async def stream_trades(self):
        if False:
            yield None

    async def stream_order_book(self):
        if False:
            yield None


class _FakeStateStore:
    def __init__(self) -> None:
        self.account_events = []

    def save_account_event(self, event):
        self.account_events.append(event)


class _FakeExecutionClient:
    def __init__(self, exchange: ExchangeName) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.market_profile = get_market_profile("ETH-USDT-PERP")
        self.orders = []
        self.stop_orders = []
        self.cancel_stop_calls = 0

    async def place_order(self, request):
        self.orders.append(request)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-order-{len(self.orders)}",
            client_order_id=request.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            price=Decimal("2000"),
            raw={"avgPx": "2000", "fee": "-0.01", "feeCcy": "USDT"},
        )

    async def fetch_order_status(self, query: OrderQuery):
        request = self.orders[-1]
        return Order(
            exchange=self.exchange,
            symbol=query.symbol,
            raw_symbol=query.symbol,
            order_id=query.order_id,
            client_order_id=query.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            price=Decimal("2000"),
            raw={"avgPx": "2000", "fee": "-0.01", "feeCcy": "USDT"},
        )

    async def place_stop_market_order(self, request):
        self.stop_orders.append(request)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-stop-{len(self.stop_orders)}",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
            side=request.side,
            order_type=OrderType.MARKET,
            quantity=request.quantity,
            filled_quantity=Decimal("0"),
            price=request.trigger_price,
            raw={"avgPx": str(request.trigger_price)},
        )

    async def fetch_stop_order_status(self, query: StopOrderQuery):
        request = self.stop_orders[-1]
        return Order(
            exchange=self.exchange,
            symbol=query.symbol,
            raw_symbol=query.symbol,
            order_id=query.stop_order_id,
            client_order_id=query.client_order_id,
            status=OrderStatus.NEW,
            side=request.side,
            order_type=OrderType.MARKET,
            quantity=request.quantity,
            filled_quantity=Decimal("0"),
            price=request.trigger_price,
            raw={"avgPx": str(request.trigger_price)},
        )

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        self.cancel_stop_calls += 1
        return [
            Order(
                exchange=self.exchange,
                symbol=self.symbol,
                raw_symbol=self.symbol,
                order_id=f"{self.exchange.value}-cancel-stop-{self.cancel_stop_calls}",
                client_order_id=None,
                status=OrderStatus.CANCELED,
            )
        ]


@pytest.mark.asyncio
async def test_v8_live_runtime_routes_entry_and_leg_specific_stops(tmp_path) -> None:
    strategy = Strategy()
    from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter

    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="MOMENTUM_V3", priority=150, side=Side.LONG),))
    strategy.feature_builder = _FakeFeatureBuilder()
    await strategy.on_start(_snapshot())

    okx = _FakeExecutionClient(ExchangeName.OKX)
    binance = _FakeExecutionClient(ExchangeName.BINANCE)
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    state = _FakeStateStore()
    cfg = _app_config(dry_run=False)
    runner = LiveRuntimeRunner(
        app_config=cfg,
        app_context=AppContext(
            data=_FakeData(),
            execution=object(),
            state_store=state,
            strategy=strategy,
            planner=ExecutionPlanner(),
            alerts=AsyncAlertDispatcher(NoopAlertSink()),
        ),
        runtime_config=LiveRuntimeConfig(app=cfg, mode=RuntimeMode.LIVE_RUNTIME, warmup_enabled=False),
        services={
            "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
            "recovery_service": None,
            "snapshot": _snapshot(),
            "execution_clients": (okx, binance),
            "order_journal": journal,
        },
    )

    close_time_ms = 1_700_000_000_000
    await runner.process_market_feature(_closed_kline(close_time_ms))
    await runner.process_market_feature(_range_aggregate(close_time_ms))

    assert runner.stats.signals_seen == 1
    assert runner.stats.submitted_intents == 1
    assert len(okx.orders) == 1
    assert len(binance.orders) == 1
    pending_qty = strategy.pending_entry.quantity  # type: ignore[union-attr]

    await runner.process_account_event(
        AccountEvent(
            exchange=ExchangeName.OKX,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            event_time_ms=close_time_ms + 1,
            order_status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            price=Decimal("2000"),
            filled_quantity=pending_qty,
        )
    )

    assert len(okx.stop_orders) == 1
    assert len(binance.stop_orders) == 0
    assert okx.stop_orders[0].trigger_price == Decimal("1978.0")
    assert runner.stats.account_events_seen == 1
    assert state.account_events[0].exchange is ExchangeName.OKX

    await runner.process_account_event(
        AccountEvent(
            exchange=ExchangeName.BINANCE,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            event_time_ms=close_time_ms + 2,
            order_status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            price=Decimal("2001"),
            filled_quantity=pending_qty,
        )
    )

    assert len(binance.stop_orders) == 1
    assert binance.stop_orders[0].trigger_price == Decimal("1978.0")
    import sqlite3

    result_count = sqlite3.connect(tmp_path / "journal.sqlite3").execute("SELECT COUNT(*) FROM exchange_order_results").fetchone()[0]
    assert result_count >= 6  # entry on two exchanges, cancel+stop on master, cancel+stop on follower
    assert (await runner.health()).healthy is True


def _app_config(*, dry_run: bool) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.eth_lf_portfolio_v8:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=dry_run,
        enable_email_alerts=False,
    )


def _snapshot() -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT", total=Decimal("1000"), available=Decimal("1000")),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("10"), margin_mode=MarginMode.ISOLATED),
        position_mode=PositionMode.ONE_WAY,
    )


def _closed_kline(close_time_ms: int) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=close_time_ms,
        data={
            "open_time_ms": close_time_ms - H4,
            "close_time_ms": close_time_ms,
            "open": "100",
            "high": "110",
            "low": "95",
            "close": "108",
            "volume": "1000",
            "is_closed": True,
        },
    )


def _range_aggregate(close_time_ms: int) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_AGGREGATE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=close_time_ms,
        data={
            "range_pct": "0.002",
            "bucket_start_ms": close_time_ms - H4,
            "bucket_end_ms": close_time_ms,
            "bar_count": 8,
            "first_open": "100",
            "last_close": "108",
            "high": "110",
            "low": "95",
            "buy_notional_sum": "60000",
            "sell_notional_sum": "40000",
            "delta_notional_sum": "20000",
            "notional_sum": "100000",
            "micro_return_pct": "0.08",
            "imbalance": "0.1",
            "taker_buy_ratio": "0.6",
            "close_pos": "0.8",
        },
    )
