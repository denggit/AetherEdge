from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.platform.account.events import AccountEventType
from src.platform.account.websocket.binance import BinanceAccountEventStream
from src.platform.account.websocket.okx import OkxAccountEventStream
from src.platform.exchanges.models import ExchangeConfig, OrderStatus
from src.platform.exchanges.errors import PrivateCredentialValidationError


class _Connector:
    async def connect(self, url):  # pragma: no cover - not used
        raise NotImplementedError


class _BinanceClient:
    async def create_user_stream_listen_key(self):  # pragma: no cover - not used
        return "listen"


class _CountingConnector:
    def __init__(self) -> None:
        self.connect_calls = 0

    async def connect(self, _url):
        self.connect_calls += 1
        raise AssertionError("private WebSocket connected with invalid credentials")


def test_okx_order_event_uses_avgpx_or_fillpx_for_market_fill() -> None:
    stream = OkxAccountEventStream(symbol="ETH-USDT-PERP", config=ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), connector=_Connector())

    events = stream._map_message(
        '{"arg":{"channel":"orders"},"data":[{"instId":"ETH-USDT-SWAP","state":"filled","side":"buy","px":"0","avgPx":"2001.5","fillPx":"2001","accFillSz":"1","uTime":"1"}]}'
    )

    assert len(events) == 1
    assert events[0].event_type is AccountEventType.ORDER
    assert events[0].order_status is OrderStatus.FILLED
    assert events[0].price == Decimal("2001.5")


def test_binance_order_trade_update_uses_average_or_last_fill_price_for_market_fill() -> None:
    stream = BinanceAccountEventStream(symbol="ETH-USDT-PERP", config=ExchangeConfig(), exchange_client=_BinanceClient(), connector=_Connector())

    events = stream._map_message(
        '{"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"ETHUSDT","X":"FILLED","S":"BUY","p":"0","ap":"2002.5","L":"2002","z":"0.5","q":"0.5"}}'
    )

    assert len(events) == 1
    assert events[0].order_status is OrderStatus.FILLED
    assert events[0].price == Decimal("2002.5")


def test_filled_event_with_missing_or_zero_price_is_not_silently_accepted() -> None:
    okx = OkxAccountEventStream(symbol="ETH-USDT-PERP", config=ExchangeConfig(api_key="k", api_secret="s", passphrase="p"), connector=_Connector())
    binance = BinanceAccountEventStream(symbol="ETH-USDT-PERP", config=ExchangeConfig(), exchange_client=_BinanceClient(), connector=_Connector())

    okx_event = okx._map_message(
        '{"arg":{"channel":"orders"},"data":[{"instId":"ETH-USDT-SWAP","state":"filled","side":"buy","px":"0","avgPx":"0","fillPx":"","accFillSz":"1","uTime":"1"}]}'
    )[0]
    binance_event = binance._map_message(
        '{"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"ETHUSDT","X":"FILLED","S":"BUY","p":"0","ap":"0","L":"0","z":"0.5","q":"0.5"}}'
    )[0]

    assert okx_event.order_status is OrderStatus.FILLED
    assert okx_event.price is None
    assert binance_event.order_status is OrderStatus.FILLED
    assert binance_event.price is None


def test_okx_private_websocket_rejects_placeholder_before_login_payload():
    connector = _CountingConnector()
    stream = OkxAccountEventStream(
        symbol="ETH-USDT-PERP",
        config=ExchangeConfig(
            api_key="canary_okx_key",
            api_secret="canary_okx_secret",
            passphrase="${OKX_PASSPHRASE}",
        ),
        connector=connector,
    )

    async def consume_once() -> None:
        async for _event in stream.stream_events():
            break

    with pytest.raises(PrivateCredentialValidationError) as exc_info:
        asyncio.run(consume_once())

    text = str(exc_info.value)
    assert "placeholder_fields=passphrase" in text
    assert "canary_okx_key" not in text
    assert "canary_okx_secret" not in text
    assert connector.connect_calls == 0
