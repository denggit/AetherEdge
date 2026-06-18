import asyncio
from decimal import Decimal

from src.platform.account import create_account_client
from src.platform.execution import create_execution_client
from src.platform.exchanges.models import (
    Balance,
    CancelOrderRequest,
    ExchangeConfig,
    ExchangeName,
    Order,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionSide,
)


class FakeExchangeClient:
    exchange = ExchangeName.OKX

    def __init__(self):
        self.placed = []
        self.canceled = []

    async def place_order(self, request):
        self.placed.append(request)
        return Order(
            exchange=ExchangeName.OKX,
            symbol=request.symbol,
            raw_symbol="ETH-USDT-SWAP",
            order_id="new-1",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
        )

    async def cancel_order(self, request):
        self.canceled.append(request)
        return Order(
            exchange=ExchangeName.OKX,
            symbol=request.symbol,
            raw_symbol="ETH-USDT-SWAP",
            order_id=request.order_id,
            client_order_id=request.client_order_id,
            status=OrderStatus.CANCELED,
        )

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=ExchangeName.OKX, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return [
            Position(
                exchange=ExchangeName.OKX,
                symbol=symbol or "ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                side=PositionSide.BOTH,
                quantity=Decimal("0.1"),
            )
        ]


def test_execution_facade_places_cancels_and_replaces_orders():
    exchange_client = FakeExchangeClient()
    execution = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=exchange_client)

    order = asyncio.run(
        execution.replace_order(
            CancelOrderRequest(symbol="ETH-USDT-PERP", order_id="old-1"),
            OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.1"),
                price=Decimal("3000"),
            ),
        )
    )

    assert len(exchange_client.canceled) == 1
    assert len(exchange_client.placed) == 1
    assert order.status is OrderStatus.NEW


def test_account_facade_reads_balance_and_positions_only():
    account = create_account_client("okx", exchange_client=FakeExchangeClient())

    balance = asyncio.run(account.fetch_balance("USDT"))
    positions = asyncio.run(account.fetch_positions("ETH-USDT-PERP"))

    assert balance.available == Decimal("90")
    assert positions[0].quantity == Decimal("0.1")
