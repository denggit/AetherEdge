from __future__ import annotations

import asyncio

import pytest

from src.platform import CancelOrderRequest, CancelStopOrderRequest, ExchangeConfig, ExchangeName, Order, OrderStatus
from src.platform.execution import LiveTradingBlocked, create_execution_client


class _FakeClient:
    exchange = ExchangeName.OKX

    async def cancel_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=request.order_id, client_order_id=request.client_order_id, status=OrderStatus.CANCELED)

    async def cancel_all_orders(self, symbol):
        return []

    async def cancel_stop_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=request.stop_order_id, client_order_id=request.client_order_id, status=OrderStatus.CANCELED)

    async def cancel_all_stop_orders(self, symbol):
        return []


def _execution():
    return create_execution_client("okx", config=ExchangeConfig(sandbox=False, live_trading_enabled=False), exchange_client=_FakeClient(), validate_orders=False)


def test_live_safety_gate_blocks_cancel_order_when_live_disabled() -> None:
    with pytest.raises(LiveTradingBlocked):
        asyncio.run(_execution().cancel_order(CancelOrderRequest(symbol="ETH-USDT-PERP", order_id="1")))


def test_live_safety_gate_blocks_cancel_all_orders_when_live_disabled() -> None:
    with pytest.raises(LiveTradingBlocked):
        asyncio.run(_execution().cancel_all_orders())


def test_live_safety_gate_blocks_cancel_stop_order_when_live_disabled() -> None:
    with pytest.raises(LiveTradingBlocked):
        asyncio.run(_execution().cancel_stop_order(CancelStopOrderRequest(symbol="ETH-USDT-PERP", stop_order_id="s1")))


def test_live_safety_gate_blocks_cancel_all_stop_orders_when_live_disabled() -> None:
    with pytest.raises(LiveTradingBlocked):
        asyncio.run(_execution().cancel_all_stop_orders())
