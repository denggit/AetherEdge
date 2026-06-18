import asyncio
from decimal import Decimal

import pytest

from src.platform import AmendOrderRequest, ExchangeConfig, ExchangeName, create_exchange_client
from src.platform.exchanges.errors import ExchangeConfigError


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
            }
        )
        return self.responses.pop(0)


def test_okx_fetches_instrument_rule_and_maps_precision_fields():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    {
                        "instId": "ETH-USDT-SWAP",
                        "tickSz": "0.01",
                        "lotSz": "0.01",
                        "minSz": "0.01",
                        "maxLmtSz": "1000",
                        "ctVal": "0.01",
                    }
                ],
            }
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(), http_client=http)

    rule = asyncio.run(client.fetch_instrument_rule("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/api/v5/public/instruments")
    assert http.calls[0]["params"] == {"instType": "SWAP", "instId": "ETH-USDT-SWAP"}
    assert rule.price_tick == Decimal("0.01")
    assert rule.quantity_step == Decimal("0.01")
    assert rule.min_quantity == Decimal("0.01")


def test_binance_fetches_exchange_info_and_maps_filters():
    http = FakeHttpClient(
        [
            {
                "symbols": [
                    {
                        "symbol": "ETHUSDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }
                ]
            }
        ]
    )
    client = create_exchange_client("binance", ExchangeConfig(), http_client=http)

    rule = asyncio.run(client.fetch_instrument_rule("ETH-USDT-PERP"))

    assert http.calls[0]["url"].endswith("/fapi/v1/exchangeInfo")
    assert rule.price_tick == Decimal("0.01")
    assert rule.quantity_step == Decimal("0.001")
    assert rule.min_quantity == Decimal("0.001")
    assert rule.min_notional == Decimal("5")


def test_okx_native_amend_order_maps_to_amend_endpoint():
    http = FakeHttpClient([{"code": "0", "data": [{"ordId": "1", "clOrdId": "c1", "sCode": "0"}]}])
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    order = asyncio.run(
        client.amend_order(
            AmendOrderRequest(
                symbol="ETH-USDT-PERP",
                order_id="1",
                new_quantity=Decimal("0.02"),
                new_price=Decimal("3000.1"),
            )
        )
    )

    assert http.calls[0]["url"].endswith("/api/v5/trade/amend-order")
    assert http.calls[0]["json_body"]["newSz"] == "0.02"
    assert http.calls[0]["json_body"]["newPx"] == "3000.1"
    assert order.order_id == "1"


def test_binance_native_modify_order_requires_quantity_and_price():
    client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s"), http_client=FakeHttpClient([]))

    with pytest.raises(ExchangeConfigError):
        asyncio.run(client.amend_order(AmendOrderRequest(symbol="ETH-USDT-PERP", order_id="1", new_price=Decimal("3000"))))
