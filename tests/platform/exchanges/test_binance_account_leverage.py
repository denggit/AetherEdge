from __future__ import annotations

import asyncio
from decimal import Decimal

from src.platform.exchanges import ExchangeConfig, ExchangeName, MarginMode, create_exchange_client
from src.platform.exchanges.errors import ExchangeApiError


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def test_binance_fetch_leverage_parses_position_risk_leverage_and_margin_type() -> None:
    http = FakeHttpClient(
        [
            [
                {
                    "symbol": "ETHUSDT",
                    "positionSide": "BOTH",
                    "positionAmt": "0",
                    "entryPrice": "0",
                    "unRealizedProfit": "0",
                    "leverage": "15",
                    "marginType": "isolated",
                }
            ]
        ]
    )
    client = create_exchange_client(ExchangeName.BINANCE, ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    leverage = asyncio.run(client.fetch_leverage("ETH-USDT-PERP", margin_mode=MarginMode.ISOLATED))

    assert http.calls[0]["url"].endswith("/fapi/v3/positionRisk")
    assert http.calls[0]["params"]["symbol"] == "ETHUSDT"
    assert leverage.leverage == Decimal("15")
    assert leverage.margin_mode is MarginMode.ISOLATED


def test_binance_set_margin_mode_already_isolated_error_is_ok() -> None:
    http = FakeHttpClient(
        [
            ExchangeApiError(
                "Binance error",
                status_code=400,
                payload={"code": -4046, "msg": "No need to change margin type."},
            )
        ]
    )
    client = create_exchange_client(ExchangeName.BINANCE, ExchangeConfig(api_key="k", api_secret="s"), http_client=http)

    result = asyncio.run(client.set_margin_mode("ETH-USDT-PERP", MarginMode.ISOLATED))

    assert http.calls[0]["url"].endswith("/fapi/v1/marginType")
    assert http.calls[0]["params"]["symbol"] == "ETHUSDT"
    assert http.calls[0]["params"]["marginType"] == "ISOLATED"
    assert result["code"] == -4046
    assert result["marginType"] == "ISOLATED"
