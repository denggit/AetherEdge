import asyncio
from decimal import Decimal

import pytest

from src.platform import ExchangeConfig, ExchangeName, Order, OrderSide, OrderStatus, Position, PositionSide, TriggerPriceType
from src.platform.execution import LiveTradingBlocked, create_execution_client


class FakeExecutionClient:
    def __init__(self, exchange):
        self.exchange = exchange
        self.stop_requests = []

    async def place_order(self, request):
        raise AssertionError("not used")

    async def place_stop_market_order(self, request):
        self.stop_requests.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id="sl", client_order_id=request.client_order_id, status=OrderStatus.NEW)

    async def cancel_order(self, request):
        raise AssertionError("not used")

    async def amend_order(self, request):
        raise AssertionError("not used")

    async def fetch_order_status(self, query):
        raise AssertionError("not used")

    async def fetch_open_orders(self, symbol):
        return []


def test_okx_place_stop_loss_for_long_position_uses_quantity_reduce_only():
    fake = FakeExecutionClient(ExchangeName.OKX)
    execution = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=fake, validate_orders=False)
    position = Position(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", side=PositionSide.LONG, quantity=Decimal("2"))

    order = asyncio.run(
        execution.place_stop_loss_for_position(
            position,
            trigger_price=Decimal("2800"),
            client_order_id="sl-long",
            trigger_price_type=TriggerPriceType.MARK,
        )
    )

    request = fake.stop_requests[0]
    assert order.status is OrderStatus.NEW
    assert request.side is OrderSide.SELL
    assert request.quantity == Decimal("2")
    assert request.reduce_only is True
    assert request.close_position is False
    assert request.position_side is PositionSide.LONG


def test_binance_place_stop_loss_for_short_position_uses_close_position():
    fake = FakeExecutionClient(ExchangeName.BINANCE)
    execution = create_execution_client("binance", config=ExchangeConfig(sandbox=True), exchange_client=fake, validate_orders=False)
    position = Position(exchange=ExchangeName.BINANCE, symbol="ETH-USDT-PERP", raw_symbol="ETHUSDT", side=PositionSide.SHORT, quantity=Decimal("0.5"))

    asyncio.run(execution.place_stop_loss_for_position(position, trigger_price=Decimal("3200")))

    request = fake.stop_requests[0]
    assert request.side is OrderSide.BUY
    assert request.quantity is None
    assert request.reduce_only is False
    assert request.close_position is True
    assert request.position_side is PositionSide.SHORT


def test_live_safety_gate_blocks_stop_order_writes():
    fake = FakeExecutionClient(ExchangeName.OKX)
    execution = create_execution_client(
        "okx",
        config=ExchangeConfig(sandbox=False, live_trading_enabled=False),
        exchange_client=fake,
        validate_orders=False,
    )
    position = Position(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", side=PositionSide.LONG, quantity=Decimal("1"))

    with pytest.raises(LiveTradingBlocked):
        asyncio.run(execution.place_stop_loss_for_position(position, trigger_price=Decimal("2800")))
