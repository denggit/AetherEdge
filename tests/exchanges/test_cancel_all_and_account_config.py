import asyncio
from decimal import Decimal

from src.platform import (
    ExchangeConfig,
    ExchangeName,
    MarginMode,
    PositionMode,
    create_exchange_client,
)


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        return self.responses.pop(0)


def test_okx_cancel_all_orders_fetches_then_cancels_each_order():
    http = FakeHttpClient(
        [
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "ordId": "1", "state": "live"}, {"instId": "ETH-USDT-SWAP", "ordId": "2", "state": "live"}]},
            {"code": "0", "data": [{"ordId": "1", "sCode": "0"}]},
            {"code": "0", "data": [{"ordId": "2", "sCode": "0"}]},
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    canceled = asyncio.run(client.cancel_all_orders("ETH-USDT-PERP"))

    assert len(canceled) == 2
    assert http.calls[0]["url"].endswith("/api/v5/trade/orders-pending")
    assert http.calls[1]["url"].endswith("/api/v5/trade/cancel-order")
    assert http.calls[2]["url"].endswith("/api/v5/trade/cancel-order")


def test_binance_cancel_all_orders_uses_all_open_orders_endpoint():
    http = FakeHttpClient([{"code": 200, "msg": "success"}])
    client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    canceled = asyncio.run(client.cancel_all_orders("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/fapi/v1/allOpenOrders")
    assert http.calls[0]["method"] == "DELETE"
    assert http.calls[0]["params"]["symbol"] == "ETHUSDT"
    assert canceled[0].status.value == "canceled"


def test_okx_leverage_and_position_mode_interfaces():
    http = FakeHttpClient(
        [
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "lever": "3", "posSide": "net"}]},
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "lever": "5"}]},
            {"code": "0", "data": [{"posMode": "net_mode"}]},
            {"code": "0", "data": [{"posMode": "long_short_mode"}]},
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    leverage = asyncio.run(client.fetch_leverage("ETH-USDT-PERP"))
    updated = asyncio.run(client.set_leverage(type("Req", (), {"symbol": "ETH-USDT-PERP", "leverage": Decimal("5"), "margin_mode": MarginMode.CROSS, "position_side": None})()))
    mode = asyncio.run(client.fetch_position_mode())
    new_mode = asyncio.run(client.set_position_mode(PositionMode.HEDGE))

    assert http.calls[0]["url"].endswith("/api/v5/account/leverage-info")
    assert http.calls[1]["url"].endswith("/api/v5/account/set-leverage")
    assert http.calls[1]["json_body"]["lever"] == "5"
    assert http.calls[2]["url"].endswith("/api/v5/account/config")
    assert http.calls[3]["url"].endswith("/api/v5/account/set-position-mode")
    assert leverage.leverage == Decimal("3")
    assert updated.leverage == Decimal("5")
    assert mode is PositionMode.ONE_WAY
    assert new_mode is PositionMode.HEDGE


def test_binance_leverage_margin_and_position_mode_interfaces():
    http = FakeHttpClient(
        [
            [{"symbol": "ETHUSDT", "positionSide": "BOTH", "positionAmt": "0", "entryPrice": "0", "unRealizedProfit": "0", "leverage": "3"}],
            {"symbol": "ETHUSDT", "leverage": 5},
            {"code": 200, "msg": "success"},
            {"dualSidePosition": True},
            {"code": 200, "msg": "success"},
        ]
    )
    client = create_exchange_client(ExchangeName.BINANCE, ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    leverage = asyncio.run(client.fetch_leverage("ETH-USDT-PERP"))
    updated = asyncio.run(client.set_leverage(type("Req", (), {"symbol": "ETH-USDT-PERP", "leverage": Decimal("5"), "margin_mode": MarginMode.CROSS, "position_side": None})()))
    margin = asyncio.run(client.set_margin_mode("ETH-USDT-PERP", MarginMode.ISOLATED))
    mode = asyncio.run(client.fetch_position_mode())
    new_mode = asyncio.run(client.set_position_mode(PositionMode.ONE_WAY))

    assert http.calls[0]["url"].endswith("/fapi/v3/positionRisk")
    assert http.calls[1]["url"].endswith("/fapi/v1/leverage")
    assert http.calls[2]["url"].endswith("/fapi/v1/marginType")
    assert http.calls[2]["params"]["marginType"] == "ISOLATED"
    assert http.calls[3]["url"].endswith("/fapi/v1/positionSide/dual")
    assert http.calls[4]["url"].endswith("/fapi/v1/positionSide/dual")
    assert leverage.leverage == Decimal("3")
    assert updated.leverage == Decimal("5")
    assert margin["code"] == 200
    assert mode is PositionMode.HEDGE
    assert new_mode is PositionMode.ONE_WAY
