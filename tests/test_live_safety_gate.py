import asyncio
from decimal import Decimal

import pytest

from src.platform import ExchangeConfig, ExchangeName, Order, OrderQuery, OrderRequest, OrderSide, OrderStatus, OrderType
from src.platform.execution import LiveTradingBlocked, create_execution_client


class FakeExecutionClient:
    exchange = ExchangeName.OKX

    async def place_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id="1", client_order_id=None, status=OrderStatus.NEW)

    async def place_stop_market_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id="sl", client_order_id=request.client_order_id, status=OrderStatus.NEW)

    async def cancel_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id=request.order_id, client_order_id=None, status=OrderStatus.CANCELED)

    async def amend_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id=request.order_id, client_order_id=None, status=OrderStatus.NEW)

    async def fetch_order_status(self, query):
        return Order(exchange=self.exchange, symbol=query.symbol, raw_symbol="raw", order_id=query.order_id, client_order_id=query.client_order_id, status=OrderStatus.NEW)

    async def fetch_open_orders(self, symbol):
        return []


def test_live_safety_gate_blocks_writes_when_not_sandbox_and_not_enabled():
    execution = create_execution_client(
        "okx",
        config=ExchangeConfig(sandbox=False, live_trading_enabled=False),
        exchange_client=FakeExecutionClient(),
        validate_orders=False,
    )

    with pytest.raises(LiveTradingBlocked):
        asyncio.run(
            execution.place_order(
                OrderRequest(
                    symbol="ETH-USDT-PERP",
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=Decimal("0.01"),
                )
            )
        )


def test_live_safety_gate_allows_reads_when_writes_are_blocked():
    execution = create_execution_client(
        "okx",
        config=ExchangeConfig(sandbox=False, live_trading_enabled=False),
        exchange_client=FakeExecutionClient(),
        validate_orders=False,
    )

    order = asyncio.run(execution.fetch_order_status(OrderQuery(symbol="ETH-USDT-PERP", order_id="1")))
    open_orders = asyncio.run(execution.fetch_open_orders())

    assert order.status is OrderStatus.NEW
    assert open_orders == []


def test_live_safety_gate_allows_writes_in_sandbox():
    execution = create_execution_client(
        "okx",
        config=ExchangeConfig(sandbox=True, live_trading_enabled=False),
        exchange_client=FakeExecutionClient(),
        validate_orders=False,
    )

    order = asyncio.run(
        execution.place_order(
            OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.01"),
            )
        )
    )

    assert order.status is OrderStatus.NEW
