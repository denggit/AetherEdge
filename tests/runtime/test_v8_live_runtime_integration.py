from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal

import pytest

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.order_management import (
    MasterFollowerExecutionPolicy,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    RetryPolicy,
    SqliteOrderJournalStore,
)
from src.order_management.models import ExchangeOrderResult
from src.signals import SignalAction, TradeSignal
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
        self.orders = []

    def save_account_event(self, event):
        self.account_events.append(event)

    def save_order(self, order, *, is_stop_order=False):
        self.orders.append((order, is_stop_order))

    def list_open_orders(self, *, exchange, symbol, include_stop_orders=True):
        return []

    def mark_missing_open_orders_closed(self, **kwargs):
        return 0


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

    async def fetch_open_orders(self):
        return []

    async def fetch_open_stop_orders(self):
        return []


class _FakeAccountClient:
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, exchange: ExchangeName) -> None:
        self.exchange = exchange

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("1000"), available=Decimal("1000"))

    async def fetch_positions(self, symbol=None):
        return []

    async def fetch_leverage(self, *, margin_mode=None):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, leverage=Decimal("10"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


@pytest.mark.asyncio
async def test_v8_live_runtime_routes_entry_and_leg_specific_stops(tmp_path) -> None:
    strategy = Strategy()
    from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter

    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="MOMENTUM_V3", priority=150, side=Side.LONG),))
    strategy.feature_builder = _FakeFeatureBuilder()
    await strategy.on_start(_snapshot())
    strategy.exchange_equity["binance"] = Decimal("100")

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
            "account_clients": (_FakeAccountClient(ExchangeName.OKX), _FakeAccountClient(ExchangeName.BINANCE)),
            "order_journal": journal,
        },
    )

    close_time_ms = 1_700_000_000_000
    await runner.process_market_feature(_closed_kline(close_time_ms))
    await runner.process_market_feature(_range_aggregate(close_time_ms))

    assert runner.stats.signals_seen == 5
    assert runner.stats.submitted_intents == 5
    assert len(okx.orders) == 1
    assert len(binance.orders) == 1
    assert len(okx.stop_orders) == 1
    assert len(binance.stop_orders) == 1
    assert okx.stop_orders[0].trigger_price == Decimal("1978.0")
    assert binance.stop_orders[0].trigger_price == Decimal("1978.0")
    assert runner.stats.account_events_seen == 0
    assert state.account_events == []
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


@pytest.mark.asyncio
async def test_order_sync_remains_active_when_master_closed_follower_unresolved(tmp_path) -> None:
    """position plan with MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED status keeps order sync active."""
    from src.order_management import (
        LegPlan,
        LegRole,
        LegSyncStatus,
        PositionPlan,
        PositionPlanStatus,
        SqlitePositionPlanStore,
    )

    plan_store = SqlitePositionPlanStore(tmp_path / "plan.sqlite3")
    position_id = "p-unresolved-1"
    plan_store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="eth_lf_portfolio_v8",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0.1"),
            sync_status=LegSyncStatus.CLOSED,
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0"),
            sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
        )
    )

    active = plan_store.list_active_positions()
    assert len(active) == 1
    assert active[0].position_id == position_id
    assert active[0].status == PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED


@pytest.mark.asyncio
async def test_runtime_alerts_when_follower_close_fails_after_master_close(tmp_path) -> None:
    """Alert emitted when follower_close_after_master_close fails after all retry attempts."""
    from src.app.alerts import AppAlert, AsyncAlertDispatcher

    captured: list[AppAlert] = []

    class _CaptureSink:
        async def send(self, alert: AppAlert) -> None:
            captured.append(alert)

    sink = _CaptureSink()
    alerts = AsyncAlertDispatcher(sink)

    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")

    class _AlwaysFailsClient:
        def __init__(self, exchange: ExchangeName) -> None:
            self.exchange = exchange
            self.symbol = "ETH-USDT-PERP"
            self.attempts = 0

        @property
        def market_profile(self):
            from src.platform import get_market_profile
            return get_market_profile("ETH-USDT-PERP")

        async def place_order(self, request):
            self.attempts += 1
            raise RuntimeError("simulated exchange error")

        async def place_stop_market_order(self, request):
            raise NotImplementedError

        async def cancel_all_orders(self):
            return []

        async def cancel_all_stop_orders(self):
            return []

    binance = _AlwaysFailsClient(ExchangeName.BINANCE)
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
        follower_close_retry=RetryPolicy(max_attempts=3, retry_delay_seconds=0),
    )
    coordinator = MultiExchangeOrderCoordinator(clients=[binance], repository=repo, master_follower_policy=policy)

    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={
            "execution_purpose": "follower_close_after_master_close",
            "target_exchanges": ["binance"],
            "position_id": "p-alert-1",
            "strategy_id": "eth_lf_portfolio_v8",
        },
    )
    intent = OrderIntent(intent_id="fc-alert", strategy_id="v8", signal=close_signal, target_exchanges=(ExchangeName.BINANCE,))

    alerts.start()
    results = await coordinator.execute(intent)

    assert binance.attempts == 3
    assert not results[0].ok

    # Verify alert can be emitted for this failure scenario
    alerts.emit(
        AppAlert(
            subject="AetherEdge follower close failed after master close",
            severity="error",
            content=(
                "strategy_id=eth_lf_portfolio_v8\n"
                "position_id=p-alert-1\n"
                "master_exchange=okx\n"
                "follower_exchange=binance\n"
                "symbol=ETH-USDT-PERP\n"
                "side=close_long\n"
                "quantity=0.1\n"
                "order_id=N/A\n"
                "client_order_id=N/A\n"
                "attempts=3\n"
                "error=simulated exchange error\n"
                "timestamp=0\n"
            ),
        )
    )

    await asyncio.sleep(0.2)
    await alerts.stop()

    assert len(captured) >= 1
    alert = captured[0]
    assert alert.subject == "AetherEdge follower close failed after master close"
    assert alert.severity == "error"
    assert "follower close failed after master close" in alert.subject.lower()
