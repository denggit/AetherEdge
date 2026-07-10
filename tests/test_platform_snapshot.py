import asyncio
from decimal import Decimal

from src.platform import (
    Balance,
    ExchangeName,
    InstrumentRule,
    LeverageInfo,
    MarginMode,
    Order,
    OrderStatus,
    Position,
    PositionMode,
    PositionSide,
    fetch_platform_snapshot,
)


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return [Position(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", side=PositionSide.BOTH, quantity=Decimal("0"))]

    async def fetch_leverage(self, *, margin_mode=MarginMode.CROSS):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3"), margin_mode=margin_mode)

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_open_orders(self):
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", order_id="1", client_order_id=None, status=OrderStatus.NEW)]

    async def fetch_open_stop_orders(self):
        return []

    async def fetch_instrument_rule(self):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol="ETH-USDT-SWAP",
            price_tick=Decimal("0.01"),
        )


def test_fetch_platform_snapshot_collects_read_only_state():
    snapshot = asyncio.run(fetch_platform_snapshot(account=FakeAccount(), execution=FakeExecution()))

    assert snapshot.symbol == "ETH-USDT-PERP"
    assert snapshot.balance.available == Decimal("90")
    assert len(snapshot.positions) == 1
    assert len(snapshot.open_orders) == 1
    assert snapshot.open_stop_orders == []
    assert snapshot.leverage.leverage == Decimal("3")
    assert snapshot.position_mode is PositionMode.ONE_WAY
    assert snapshot.instrument_rule is not None
    assert snapshot.instrument_rule.price_tick == Decimal("0.01")
