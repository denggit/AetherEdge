import asyncio
from decimal import Decimal

from src.platform import ExchangeConfig, ExchangeName, OrderQuery, OrderStatus, create_exchange_client


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        return self.responses.pop(0)


def test_okx_fetch_order_status_and_open_orders():
    http = FakeHttpClient(
        [
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "ordId": "1", "clOrdId": "c1", "state": "live", "side": "buy", "ordType": "limit", "px": "3000", "sz": "0.1", "accFillSz": "0"}]},
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "ordId": "2", "state": "partially_filled", "side": "sell", "ordType": "limit", "px": "3100", "sz": "0.2", "accFillSz": "0.1"}]},
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    order = asyncio.run(client.fetch_order_status(OrderQuery(symbol="ETH-USDT-PERP", order_id="1")))
    open_orders = asyncio.run(client.fetch_open_orders("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/api/v5/trade/order")
    assert http.calls[0]["params"] == {"instId": "ETH-USDT-SWAP", "ordId": "1"}
    assert http.calls[1]["url"].endswith("/api/v5/trade/orders-pending")
    assert order.status is OrderStatus.NEW
    assert open_orders[0].status is OrderStatus.PARTIALLY_FILLED
    assert open_orders[0].filled_quantity == Decimal("0.1")


def test_binance_fetch_order_status_and_open_orders():
    http = FakeHttpClient(
        [
            {"symbol": "ETHUSDT", "orderId": 1, "clientOrderId": "c1", "status": "NEW", "side": "BUY", "type": "LIMIT", "price": "3000", "origQty": "0.1", "executedQty": "0"},
            [{"symbol": "ETHUSDT", "orderId": 2, "clientOrderId": "c2", "status": "PARTIALLY_FILLED", "side": "SELL", "type": "LIMIT", "price": "3100", "origQty": "0.2", "executedQty": "0.1"}],
        ]
    )
    client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    order = asyncio.run(client.fetch_order_status(OrderQuery(symbol="ETH-USDT-PERP", order_id="1")))
    open_orders = asyncio.run(client.fetch_open_orders("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/fapi/v1/order")
    assert http.calls[0]["params"]["symbol"] == "ETHUSDT"
    assert http.calls[0]["params"]["orderId"] == "1"
    assert http.calls[1]["url"].endswith("/fapi/v1/openOrders")
    assert order.status is OrderStatus.NEW
    assert open_orders[0].status is OrderStatus.PARTIALLY_FILLED
