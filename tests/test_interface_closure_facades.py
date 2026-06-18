import asyncio
from decimal import Decimal

import pytest

from src.platform import (
    CancelStopOrderRequest,
    ExchangeConfig,
    ExchangeName,
    MarginMode,
    Order,
    OrderStatus,
    PositionMode,
    StopOrderQuery,
    create_account_client,
    create_execution_client,
)


class FakeExecutionClient:
    exchange = ExchangeName.OKX

    def __init__(self):
        self.calls = []

    async def cancel_all_orders(self, symbol):
        self.calls.append(("cancel_all_orders", symbol))
        return [Order(exchange=self.exchange, symbol=symbol, raw_symbol="raw", order_id="1", client_order_id=None, status=OrderStatus.CANCELED)]

    async def cancel_stop_order(self, request):
        self.calls.append(("cancel_stop_order", request))
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol="raw", order_id=request.stop_order_id, client_order_id=request.client_order_id, status=OrderStatus.CANCELED)

    async def cancel_all_stop_orders(self, symbol):
        self.calls.append(("cancel_all_stop_orders", symbol))
        return [Order(exchange=self.exchange, symbol=symbol, raw_symbol="raw", order_id="sl", client_order_id=None, status=OrderStatus.CANCELED)]

    async def fetch_stop_order_status(self, query):
        self.calls.append(("fetch_stop_order_status", query))
        return Order(exchange=self.exchange, symbol=query.symbol, raw_symbol="raw", order_id=query.stop_order_id, client_order_id=query.client_order_id, status=OrderStatus.NEW)

    async def fetch_open_stop_orders(self, symbol):
        self.calls.append(("fetch_open_stop_orders", symbol))
        return [Order(exchange=self.exchange, symbol=symbol, raw_symbol="raw", order_id="sl", client_order_id=None, status=OrderStatus.NEW)]


class FakeAccountClient:
    exchange = ExchangeName.BINANCE

    def __init__(self):
        self.calls = []

    async def fetch_balance(self, asset="USDT"):
        raise AssertionError("not used")

    async def fetch_positions(self, symbol=None):
        raise AssertionError("not used")

    async def fetch_leverage(self, symbol, *, margin_mode=MarginMode.CROSS):
        self.calls.append(("fetch_leverage", symbol, margin_mode))
        from src.platform import LeverageInfo

        return LeverageInfo(exchange=self.exchange, symbol=symbol, raw_symbol="ETHUSDT", leverage=Decimal("3"), margin_mode=margin_mode)

    async def set_leverage(self, request):
        self.calls.append(("set_leverage", request))
        from src.platform import LeverageInfo

        return LeverageInfo(exchange=self.exchange, symbol=request.symbol, raw_symbol="ETHUSDT", leverage=request.leverage, margin_mode=request.margin_mode)

    async def set_margin_mode(self, symbol, margin_mode):
        self.calls.append(("set_margin_mode", symbol, margin_mode))
        return {"symbol": symbol, "margin_mode": margin_mode.value}

    async def fetch_position_mode(self):
        self.calls.append(("fetch_position_mode",))
        return PositionMode.HEDGE

    async def set_position_mode(self, mode):
        self.calls.append(("set_position_mode", mode))
        return mode


def test_execution_facade_exposes_stop_management_and_cancel_all_only_through_bound_symbol():
    fake = FakeExecutionClient()
    execution = create_execution_client("okx", exchange_client=fake, validate_orders=False, config=ExchangeConfig(sandbox=True))

    asyncio.run(execution.cancel_all_orders())
    asyncio.run(execution.fetch_stop_order_status(StopOrderQuery(symbol="ETH-USDT-PERP", stop_order_id="sl")))
    asyncio.run(execution.fetch_open_stop_orders())
    asyncio.run(execution.cancel_stop_order(CancelStopOrderRequest(symbol="ETH-USDT-PERP", stop_order_id="sl")))
    asyncio.run(execution.cancel_all_stop_orders())

    assert fake.calls[0] == ("cancel_all_orders", "ETH-USDT-PERP")
    assert fake.calls[1][0] == "fetch_stop_order_status"
    assert fake.calls[2] == ("fetch_open_stop_orders", "ETH-USDT-PERP")
    assert fake.calls[3][0] == "cancel_stop_order"
    assert fake.calls[4] == ("cancel_all_stop_orders", "ETH-USDT-PERP")

    with pytest.raises(ValueError):
        asyncio.run(execution.fetch_stop_order_status(StopOrderQuery(symbol="SOL-USDT-PERP", stop_order_id="sl")))


def test_account_facade_exposes_leverage_margin_and_position_mode_interfaces():
    fake = FakeAccountClient()
    account = create_account_client("binance", exchange_client=fake)

    leverage = asyncio.run(account.fetch_leverage())
    updated = asyncio.run(account.set_leverage(Decimal("5"), margin_mode=MarginMode.ISOLATED))
    margin = asyncio.run(account.set_margin_mode(MarginMode.ISOLATED))
    mode = asyncio.run(account.fetch_position_mode())
    new_mode = asyncio.run(account.set_position_mode(PositionMode.ONE_WAY))

    assert leverage.leverage == Decimal("3")
    assert updated.leverage == Decimal("5")
    assert margin["margin_mode"] == "isolated"
    assert mode is PositionMode.HEDGE
    assert new_mode is PositionMode.ONE_WAY
    assert fake.calls[0] == ("fetch_leverage", "ETH-USDT-PERP", MarginMode.CROSS)
    assert fake.calls[1][0] == "set_leverage"
    assert fake.calls[2] == ("set_margin_mode", "ETH-USDT-PERP", MarginMode.ISOLATED)
