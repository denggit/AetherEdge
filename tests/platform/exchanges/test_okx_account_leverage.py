from __future__ import annotations

import asyncio
from decimal import Decimal

from src.platform.exchanges import ExchangeConfig, LeverageRequest, MarginMode, create_exchange_client


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        return self.responses.pop(0)


def test_okx_set_and_fetch_leverage_for_isolated_swap_symbol() -> None:
    http = FakeHttpClient(
        [
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "lever": "15", "mgnMode": "isolated", "posSide": "net"}]},
            {"code": "0", "data": [{"instId": "ETH-USDT-SWAP", "lever": "15", "mgnMode": "isolated"}]},
        ]
    )
    client = create_exchange_client("okx", ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), http_client=http)

    before = asyncio.run(client.fetch_leverage("ETH-USDT-PERP", margin_mode=MarginMode.ISOLATED))
    updated = asyncio.run(
        client.set_leverage(
            LeverageRequest(symbol="ETH-USDT-PERP", leverage=Decimal("15"), margin_mode=MarginMode.ISOLATED)
        )
    )

    assert http.calls[0]["url"].endswith("/api/v5/account/leverage-info")
    assert http.calls[0]["params"] == {"instId": "ETH-USDT-SWAP", "mgnMode": "isolated"}
    assert http.calls[1]["url"].endswith("/api/v5/account/set-leverage")
    assert http.calls[1]["json_body"] == {"instId": "ETH-USDT-SWAP", "lever": "15", "mgnMode": "isolated"}
    assert before.leverage == Decimal("15")
    assert before.margin_mode is MarginMode.ISOLATED
    assert updated.leverage == Decimal("15")
    assert updated.margin_mode is MarginMode.ISOLATED
