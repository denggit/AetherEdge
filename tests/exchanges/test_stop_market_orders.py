import asyncio
from decimal import Decimal

from src.platform import ExchangeConfig, OrderSide, OrderStatus, StopMarketOrderRequest, TriggerPriceType, create_exchange_client


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        return self.responses.pop(0)


def test_okx_places_reduce_only_stop_market_algo_order():
    http = FakeHttpClient([{"code": "0", "data": [{"algoId": "a1", "algoClOrdId": "sl1", "sCode": "0"}]}])
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    order = asyncio.run(
        client.place_stop_market_order(
            StopMarketOrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.SELL,
                quantity=Decimal("0.2"),
                trigger_price=Decimal("2800"),
                client_order_id="sl1",
                reduce_only=True,
                trigger_price_type=TriggerPriceType.MARK,
            )
        )
    )

    call = http.calls[0]
    assert call["url"].endswith("/api/v5/trade/order-algo")
    assert call["json_body"]["instId"] == "ETH-USDT-SWAP"
    assert call["json_body"]["ordType"] == "conditional"
    assert call["json_body"]["slTriggerPx"] == "2800"
    assert call["json_body"]["slOrdPx"] == "-1"
    assert call["json_body"]["slTriggerPxType"] == "mark"
    assert call["json_body"]["reduceOnly"] == "true"
    assert order.status is OrderStatus.NEW
    assert order.order_id == "a1"


def test_binance_places_stop_market_algo_order_with_quantity():
    http = FakeHttpClient(
        [
            {
                "algoId": 123,
                "clientAlgoId": "sl1",
                "algoStatus": "NEW",
                "side": "SELL",
                "type": "STOP_MARKET",
                "triggerPrice": "2800",
                "quantity": "0.2",
            }
        ]
    )
    client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    order = asyncio.run(
        client.place_stop_market_order(
            StopMarketOrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.SELL,
                quantity=Decimal("0.2"),
                trigger_price=Decimal("2800"),
                client_order_id="sl1",
                reduce_only=True,
            )
        )
    )

    call = http.calls[0]
    assert call["url"].endswith("/fapi/v1/algoOrder")
    assert call["params"]["algoType"] == "CONDITIONAL"
    assert call["params"]["symbol"] == "ETHUSDT"
    assert call["params"]["type"] == "STOP_MARKET"
    assert call["params"]["triggerPrice"] == "2800"
    assert call["params"]["workingType"] == "CONTRACT_PRICE"
    assert call["params"]["quantity"] == "0.2"
    assert call["params"]["reduceOnly"] == "true"
    assert order.order_id == "123"


def test_binance_close_position_stop_omits_quantity_and_reduce_only():
    http = FakeHttpClient([{"algoId": 456, "algoStatus": "NEW", "side": "BUY", "triggerPrice": "3200"}])
    client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    asyncio.run(
        client.place_stop_market_order(
            StopMarketOrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                trigger_price=Decimal("3200"),
                close_position=True,
            )
        )
    )

    params = http.calls[0]["params"]
    assert params["closePosition"] == "true"
    assert "quantity" not in params
    assert "reduceOnly" not in params
