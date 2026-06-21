from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import StopOrderSyncService
from src.platform import ExchangeName, Order, OrderSide, OrderStatus, StopMarketOrderRequest


class FakeExecutionClient:
    def __init__(self, exchange: ExchangeName, *, fail: bool = False) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.fail = fail
        self.cancelled = 0
        self.placed = []

    async def cancel_all_stop_orders(self):
        self.cancelled += 1
        return []

    async def place_stop_market_order(self, request):
        if self.fail:
            raise RuntimeError("boom")
        self.placed.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=f"{self.exchange.value}-stop", client_order_id=request.client_order_id, status=OrderStatus.NEW, side=request.side, quantity=request.quantity)


@pytest.mark.asyncio
async def test_stop_order_sync_replaces_stops_on_each_exchange():
    okx = FakeExecutionClient(ExchangeName.OKX)
    binance = FakeExecutionClient(ExchangeName.BINANCE)
    request = StopMarketOrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.SELL, trigger_price=Decimal("2900"), quantity=Decimal("0.1"), client_order_id="stop-1")
    service = StopOrderSyncService([okx, binance])

    results = await service.replace_all(request)

    assert [result.ok for result in results] == [True, True]
    assert okx.cancelled == 1
    assert binance.cancelled == 1
    assert okx.placed[0].trigger_price == Decimal("2900")


@pytest.mark.asyncio
async def test_stop_order_sync_returns_failure_per_exchange():
    okx = FakeExecutionClient(ExchangeName.OKX)
    binance = FakeExecutionClient(ExchangeName.BINANCE, fail=True)
    request = StopMarketOrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.SELL, trigger_price=Decimal("2900"), quantity=Decimal("0.1"), client_order_id="stop-1")
    service = StopOrderSyncService([okx, binance])

    results = await service.replace_all(request)

    assert [result.ok for result in results] == [True, False]
    assert results[1].error == "boom"
