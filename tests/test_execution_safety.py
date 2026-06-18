import asyncio
from decimal import Decimal

import pytest

from src.platform import AmendOrderRequest, ExchangeConfig, ExchangeName, Order, OrderRequest, OrderSide, OrderStatus, OrderType
from src.platform.execution import ExecutionRiskLimits, MultiExchangeExecutionClient, RiskCheckError, create_execution_client
from src.platform.exchanges.models import InstrumentRule


class FakeExchangeClient:
    def __init__(self, exchange=ExchangeName.OKX):
        self.exchange = exchange
        self.placed = []
        self.amended = []

    async def fetch_instrument_rule(self, symbol):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=symbol,
            raw_symbol="ETH-USDT-SWAP" if self.exchange == ExchangeName.OKX else "ETHUSDT",
            price_tick=Decimal("0.1"),
            quantity_step=Decimal("0.01"),
            min_quantity=Decimal("0.01"),
            min_notional=Decimal("5"),
        )

    async def place_order(self, request):
        self.placed.append(request)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol="raw",
            order_id="1",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
            price=request.price,
            quantity=request.quantity,
        )

    async def cancel_order(self, request):
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id=request.order_id, status=OrderStatus.CANCELED)

    async def amend_order(self, request):
        self.amended.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id=request.order_id, client_order_id=request.client_order_id, status=OrderStatus.NEW)


def test_execution_normalizes_quantity_price_before_place_order():
    fake = FakeExchangeClient()
    execution = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=fake)

    order = asyncio.run(
        execution.place_order(
            OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.019"),
                price=Decimal("3000.19"),
            )
        )
    )

    assert fake.placed[0].quantity == Decimal("0.01")
    assert fake.placed[0].price == Decimal("3000.1")
    assert order.quantity == Decimal("0.01")


def test_execution_risk_gate_rejects_too_small_quantity():
    execution = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=FakeExchangeClient())

    with pytest.raises(RiskCheckError):
        asyncio.run(
            execution.place_order(
                OrderRequest(
                    symbol="ETH-USDT-PERP",
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.009"),
                    price=Decimal("3000"),
                )
            )
        )


def test_execution_amend_uses_native_adapter_method():
    fake = FakeExchangeClient()
    execution = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=fake)

    asyncio.run(execution.amend_order(AmendOrderRequest(symbol="ETH-USDT-PERP", order_id="1", new_quantity=Decimal("0.029"))))

    assert fake.amended[0].new_quantity == Decimal("0.02")


def test_multi_exchange_execution_collects_partial_failures():
    okx = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=FakeExchangeClient(ExchangeName.OKX))

    class BrokenClient(FakeExchangeClient):
        async def place_order(self, request):
            raise RuntimeError("boom")

    binance = create_execution_client("binance", config=ExchangeConfig(sandbox=True), exchange_client=BrokenClient(ExchangeName.BINANCE))
    multi = MultiExchangeExecutionClient([okx, binance])

    results = asyncio.run(
        multi.place_order_all(
            OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.02"),
                price=Decimal("3000"),
            )
        )
    )

    assert [result.exchange for result in results] == [ExchangeName.OKX, ExchangeName.BINANCE]
    assert results[0].ok is True
    assert results[1].ok is False
