import asyncio
from decimal import Decimal

from src.platform.exchanges import (
    CancelOrderRequest,
    ExchangeConfig,
    ExchangeName,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    create_exchange_client,
)


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "json_body": json_body,
                "headers": headers or {},
                "timeout_seconds": timeout_seconds,
            }
        )
        return self.responses.pop(0)


def test_binance_public_klines_are_mapped_to_unified_model():
    http = FakeHttpClient(
        [
            [[1710000000000, "3000", "3010", "2990", "3005", "12", 1710000059999, "36000"]],
        ]
    )
    client = create_exchange_client("binance", ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="1m", limit=1))

    assert http.calls[0]["url"].endswith("/fapi/v1/klines")
    assert http.calls[0]["params"] == {"symbol": "ETHUSDT", "interval": "1m", "limit": 1}
    assert rows[0].exchange is ExchangeName.BINANCE
    assert rows[0].symbol == "ETH-USDT-PERP"
    assert rows[0].raw_symbol == "ETHUSDT"
    assert rows[0].close == Decimal("3005")


def test_binance_place_and_cancel_order_use_same_business_request_model():
    http = FakeHttpClient(
        [
            {
                "orderId": 123,
                "clientOrderId": "client-1",
                "status": "NEW",
                "side": "BUY",
                "type": "MARKET",
                "origQty": "0.01",
                "executedQty": "0",
            },
            {
                "orderId": 123,
                "clientOrderId": "client-1",
                "status": "CANCELED",
                "side": "BUY",
                "type": "MARKET",
                "origQty": "0.01",
                "executedQty": "0",
            },
        ]
    )
    cfg = ExchangeConfig(api_key="key", api_secret="secret", sandbox=True)
    client = create_exchange_client(ExchangeName.BINANCE, cfg, http_client=http)

    order = asyncio.run(
        client.place_order(
            OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.01"),
                client_order_id="client-1",
            )
        )
    )
    canceled = asyncio.run(client.cancel_order(CancelOrderRequest(symbol="ETH-USDT-PERP", order_id="123")))

    assert http.calls[0]["url"].endswith("/fapi/v1/order")
    assert http.calls[0]["method"] == "POST"
    assert http.calls[0]["params"]["symbol"] == "ETHUSDT"
    assert http.calls[0]["params"]["side"] == "BUY"
    assert http.calls[0]["params"]["type"] == "MARKET"
    assert http.calls[0]["params"]["quantity"] == "0.01"
    assert http.calls[0]["params"]["newClientOrderId"] == "client-1"
    assert "timestamp" in http.calls[0]["params"]
    assert "signature" in http.calls[0]["params"]
    assert http.calls[0]["headers"]["X-MBX-APIKEY"] == "key"
    assert order.status is OrderStatus.NEW
    assert canceled.status is OrderStatus.CANCELED


def test_binance_account_queries_use_usdm_v3_endpoints():
    http = FakeHttpClient(
        [
            [{"asset": "USDT", "balance": "100", "availableBalance": "90"}],
            [
                {
                    "symbol": "ETHUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "0.1",
                    "entryPrice": "3000",
                    "unRealizedProfit": "1.5",
                    "leverage": "3",
                }
            ],
        ]
    )
    cfg = ExchangeConfig(api_key="key", api_secret="secret")
    client = create_exchange_client(ExchangeName.BINANCE, cfg, http_client=http)

    balance = asyncio.run(client.fetch_balance("USDT"))
    positions = asyncio.run(client.fetch_positions("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/fapi/v3/balance")
    assert http.calls[1]["url"].endswith("/fapi/v3/positionRisk")
    assert balance.available == Decimal("90")
    assert positions[0].quantity == Decimal("0.1")
