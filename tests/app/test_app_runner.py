import asyncio
from decimal import Decimal

from src.app import AppConfig, AppContext, AppRunner, AsyncAlertDispatcher, NoopAlertSink
from src.platform import ExchangeName, Order, OrderSide, OrderStatus, OrderType
from src.platform.data.models import MarketTrade, TradeSide
from src.planner import ExecutionPlanner
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


class ErrorStrategy(FakeStrategy):
    async def on_trade(self, trade):
        raise RuntimeError("boom")


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self):
        self.orders = []

    async def place_order(self, request):
        self.orders.append(request)
        return Order(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol="ETH-USDT-SWAP",
            order_id="1",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
        )

    async def place_stop_market_order(self, request):
        raise AssertionError("not expected")

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        return []


class FakeAlertSink:
    def __init__(self):
        self.alerts = []

    async def send(self, alert):
        self.alerts.append(alert)


class FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"


class FakeStateStore:
    pass


def _config(*, dry_run):
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="unused",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=5,
        alert_queue_maxsize=3,
        dry_run=dry_run,
        enable_email_alerts=False,
    )


def _trade():
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        side=TradeSide.BUY,
    )


def test_app_runner_processes_market_event_into_execution_order():
    execution = FakeExecution()
    context = AppContext(
        data=FakeData(),
        execution=execution,
        state_store=FakeStateStore(),
        strategy=FakeStrategy(),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runner = AppRunner(config=_config(dry_run=False), context=context)

    asyncio.run(runner.process_market_event(_trade()))

    assert runner.stats.market_events_seen == 1
    assert runner.stats.signals_seen == 1
    assert len(execution.orders) == 1
    assert execution.orders[0].side is OrderSide.BUY
    assert execution.orders[0].order_type is OrderType.MARKET


def test_app_runner_dry_run_does_not_call_execution():
    execution = FakeExecution()
    context = AppContext(
        data=FakeData(),
        execution=execution,
        state_store=FakeStateStore(),
        strategy=FakeStrategy(),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runner = AppRunner(config=_config(dry_run=True), context=context)

    asyncio.run(runner.process_market_event(_trade()))

    assert execution.orders == []
    assert runner.stats.dry_run_actions == 1


def test_app_runner_sends_strategy_errors_to_alert_queue_without_blocking():
    sink = FakeAlertSink()
    alerts = AsyncAlertDispatcher(sink, maxsize=3)
    context = AppContext(
        data=FakeData(),
        execution=FakeExecution(),
        state_store=FakeStateStore(),
        strategy=ErrorStrategy(),
        planner=ExecutionPlanner(),
        alerts=alerts,
    )
    runner = AppRunner(config=_config(dry_run=True), context=context)

    async def scenario():
        alerts.start()
        await runner.process_market_event(_trade())
        await asyncio.sleep(0)
        await alerts.stop()

    asyncio.run(scenario())

    assert runner.stats.errors == 1
    assert len(sink.alerts) == 1
    assert "boom" in sink.alerts[0].content
