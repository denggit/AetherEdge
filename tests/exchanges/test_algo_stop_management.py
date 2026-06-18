import asyncio
from decimal import Decimal

from src.platform import (
    CancelStopOrderRequest,
    ExchangeConfig,
    OrderStatus,
    StopOrderQuery,
    create_exchange_client,
)


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        return self.responses.pop(0)


def test_okx_stop_order_query_open_and_cancel_interfaces():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    {
                        "instId": "ETH-USDT-SWAP",
                        "algoId": "a1",
                        "algoClOrdId": "sl1",
                        "state": "live",
                        "side": "sell",
                        "slTriggerPx": "2800",
                        "sz": "0.2",
                    }
                ],
            },
            {
                "code": "0",
                "data": [
                    {
                        "instId": "ETH-USDT-SWAP",
                        "algoId": "a2",
                        "state": "live",
                        "side": "buy",
                        "slTriggerPx": "3200",
                        "sz": "0.1",
                    }
                ],
            },
            {"code": "0", "data": [{"algoId": "a1", "algoClOrdId": "sl1", "sCode": "0"}]},
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    order = asyncio.run(client.fetch_stop_order_status(StopOrderQuery(symbol="ETH-USDT-PERP", stop_order_id="a1")))
    open_orders = asyncio.run(client.fetch_open_stop_orders("ETH-USDT-PERP"))
    canceled = asyncio.run(client.cancel_stop_order(CancelStopOrderRequest(symbol="ETH-USDT-PERP", stop_order_id="a1", client_order_id="sl1")))

    assert http.calls[0]["url"].endswith("/api/v5/trade/order-algo")
    assert http.calls[0]["params"] == {"instId": "ETH-USDT-SWAP", "ordType": "conditional", "algoId": "a1"}
    assert http.calls[1]["url"].endswith("/api/v5/trade/orders-algo-pending")
    assert http.calls[1]["params"]["ordType"] == "conditional"
    assert http.calls[2]["url"].endswith("/api/v5/trade/cancel-algos")
    assert http.calls[2]["json_body"] == [{"instId": "ETH-USDT-SWAP", "algoId": "a1", "algoClOrdId": "sl1"}]
    assert order.status is OrderStatus.NEW
    assert open_orders[0].price == Decimal("3200")
    assert canceled.status is OrderStatus.CANCELED


def test_okx_cancel_all_stop_orders_fetches_then_cancels_each_algo_order():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    {"instId": "ETH-USDT-SWAP", "algoId": "a1", "state": "live"},
                    {"instId": "ETH-USDT-SWAP", "algoId": "a2", "state": "live"},
                ],
            },
            {"code": "0", "data": [{"algoId": "a1", "sCode": "0"}]},
            {"code": "0", "data": [{"algoId": "a2", "sCode": "0"}]},
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    canceled = asyncio.run(client.cancel_all_stop_orders("ETH-USDT-PERP"))

    assert len(canceled) == 2
    assert http.calls[0]["url"].endswith("/api/v5/trade/orders-algo-pending")
    assert http.calls[1]["url"].endswith("/api/v5/trade/cancel-algos")
    assert http.calls[2]["url"].endswith("/api/v5/trade/cancel-algos")


def test_binance_stop_order_query_open_and_cancel_interfaces():
    http = FakeHttpClient(
        [
            {"algoId": 1, "clientAlgoId": "sl1", "algoStatus": "NEW", "side": "SELL", "triggerPrice": "2800", "quantity": "0.2"},
            [{"algoId": 2, "clientAlgoId": "sl2", "algoStatus": "NEW", "side": "BUY", "triggerPrice": "3200", "quantity": "0.1"}],
            {"algoId": 1, "clientAlgoId": "sl1", "algoStatus": "CANCELED", "side": "SELL", "triggerPrice": "2800", "quantity": "0.2"},
            {"code": 200, "msg": "success"},
        ]
    )
    client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    order = asyncio.run(client.fetch_stop_order_status(StopOrderQuery(symbol="ETH-USDT-PERP", stop_order_id="1")))
    open_orders = asyncio.run(client.fetch_open_stop_orders("ETH-USDT-PERP"))
    canceled = asyncio.run(client.cancel_stop_order(CancelStopOrderRequest(symbol="ETH-USDT-PERP", stop_order_id="1")))
    all_canceled = asyncio.run(client.cancel_all_stop_orders("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/fapi/v1/algoOrder")
    assert http.calls[0]["method"] == "GET"
    assert http.calls[1]["url"].endswith("/fapi/v1/openAlgoOrders")
    assert http.calls[2]["url"].endswith("/fapi/v1/algoOrder")
    assert http.calls[2]["method"] == "DELETE"
    assert http.calls[3]["url"].endswith("/fapi/v1/algoOpenOrders")
    assert order.status is OrderStatus.NEW
    assert open_orders[0].order_id == "2"
    assert canceled.status is OrderStatus.CANCELED
    assert all_canceled[0].status is OrderStatus.CANCELED
