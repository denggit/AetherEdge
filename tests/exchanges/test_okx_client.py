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


def test_okx_public_klines_preserve_exchange_order_by_default():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    ["1710000060000", "3010", "3020", "3000", "3015", "13", "130", "39000", "1"],
                    ["1710000000000", "3000", "3010", "2990", "3005", "12", "120", "36000", "1"],
                ],
            }
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="1m", limit=2))

    assert http.calls[0]["url"].endswith("/api/v5/market/candles")
    assert http.calls[0]["params"] == {"instId": "ETH-USDT-SWAP", "bar": "1m", "limit": 2}
    assert rows[0].exchange is ExchangeName.OKX
    assert rows[0].symbol == "ETH-USDT-PERP"
    assert rows[0].raw_symbol == "ETH-USDT-SWAP"
    assert [row.open_time_ms for row in rows] == [1710000060000, 1710000000000]
    assert rows[0].close == Decimal("3015")



def test_okx_public_klines_maps_normalized_4h_to_exchange_4H():
    http = FakeHttpClient([{"code": "0", "data": []}])
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="4h", limit=10))

    assert rows == []
    assert http.calls[0]["params"]["bar"] == "4H"


def test_okx_public_klines_can_normalize_oldest_first_when_requested():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    ["1710000060000", "3010", "3020", "3000", "3015", "13", "130", "39000", "1"],
                    ["1710000000000", "3000", "3010", "2990", "3005", "12", "120", "36000", "1"],
                ],
            }
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="1m", limit=2, oldest_first=True))

    assert [row.open_time_ms for row in rows] == [1710000000000, 1710000060000]



def test_okx_fetch_historical_trades_filters_time_range():
    http = FakeHttpClient([
        {
            "code": "0",
            "data": [
                {"tradeId": "3", "px": "100.3", "sz": "1", "side": "buy", "ts": "3000"},
                {"tradeId": "2", "px": "100.2", "sz": "2", "side": "sell", "ts": "2000"},
                {"tradeId": "1", "px": "100.1", "sz": "3", "side": "buy", "ts": "1000"},
            ],
        }
    ])
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_trades("ETH-USDT-PERP", start_time_ms=1500, end_time_ms=3000, limit=100, oldest_first=True))

    assert http.calls[0]["url"].endswith("/api/v5/market/history-trades")
    assert http.calls[0]["params"]["instId"] == "ETH-USDT-SWAP"
    assert [row.trade_id for row in rows] == ["2", "3"]
    assert rows[0].price == Decimal("100.2")
    assert rows[0].side is OrderSide.SELL


def test_okx_place_and_cancel_order_use_same_business_request_model():
    http = FakeHttpClient(
        [
            {"code": "0", "data": [{"ordId": "okx-1", "clOrdId": "client-1", "sCode": "0"}]},
            {"code": "0", "data": [{"ordId": "okx-1", "clOrdId": "client-1", "sCode": "0"}]},
        ]
    )
    cfg = ExchangeConfig(api_key="key", api_secret="secret", passphrase="pass", sandbox=True)
    client = create_exchange_client("okx", cfg, http_client=http)

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
    canceled = asyncio.run(client.cancel_order(CancelOrderRequest(symbol="ETH-USDT-PERP", order_id="okx-1")))

    assert http.calls[0]["url"].endswith("/api/v5/trade/order")
    assert http.calls[0]["json_body"] == {
        "instId": "ETH-USDT-SWAP",
        "tdMode": "cross",
        "side": "buy",
        "ordType": "market",
        "sz": "0.01",
        "clOrdId": "client-1",
    }
    assert http.calls[0]["headers"]["OK-ACCESS-KEY"] == "key"
    assert http.calls[0]["headers"]["x-simulated-trading"] == "1"
    assert order.status is OrderStatus.NEW
    assert canceled.status is OrderStatus.CANCELED


def test_okx_private_requests_include_content_type_header():
    http = FakeHttpClient([{"code": "0", "data": [{"details": [{"ccy": "USDT", "cashBal": "1", "availBal": "1"}]}]}])
    cfg = ExchangeConfig(api_key="k", api_secret="s", passphrase="p")
    client = create_exchange_client("okx", cfg, http_client=http)

    import asyncio

    asyncio.run(client.fetch_balance("USDT"))

    headers = http.calls[0]["headers"]
    assert headers["Content-Type"] == "application/json"
    assert "OK-ACCESS-KEY" in headers
