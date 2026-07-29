from __future__ import annotations

import json
from decimal import Decimal

import pytest
from websockets.exceptions import ConnectionClosedError

from src.platform.data.websocket.okx import OkxTradeWebSocketFeed
from src.platform.exchanges.models import ExchangeName


class FakeWebSocketConnection:
    def __init__(self, items):
        self.items = list(items)
        self.sent: list[str] = []
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.items:
            raise StopAsyncIteration
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


class SequencedConnector:
    def __init__(self, connections):
        self.connections = list(connections)
        self.urls: list[str] = []

    async def connect(self, url: str):
        self.urls.append(url)
        return self.connections.pop(0)


async def _first_trade(feed: OkxTradeWebSocketFeed):
    async for trade in feed.stream_trades():
        return trade
    raise AssertionError("no trade yielded")


def _trade_message(*, price: str = "3000.1", ts: str = "1710000000000") -> str:
    return json.dumps(
        {
            "arg": {"channel": "trades", "instId": "ETH-USDT-SWAP"},
            "data": [
                {
                    "instId": "ETH-USDT-SWAP",
                    "tradeId": "1",
                    "px": price,
                    "sz": "0.2",
                    "side": "buy",
                    "ts": ts,
                }
            ],
        }
    )


@pytest.mark.asyncio
async def test_okx_trade_websocket_reconnects_after_connection_closed():
    connector = SequencedConnector(
        [
            FakeWebSocketConnection([ConnectionClosedError(None, None)]),
            FakeWebSocketConnection([_trade_message()]),
        ]
    )
    feed = OkxTradeWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=0,
        max_reconnects=1,
    )

    trade = await _first_trade(feed)

    assert len(connector.urls) == 2
    assert trade.exchange is ExchangeName.OKX
    assert trade.symbol == "ETH-USDT-PERP"
    assert trade.raw_symbol == "ETH-USDT-SWAP"
    assert trade.price == Decimal("3000.1")


@pytest.mark.asyncio
async def test_okx_trade_websocket_raises_when_reconnect_disabled():
    connector = SequencedConnector([FakeWebSocketConnection([ConnectionClosedError(None, None)])])
    feed = OkxTradeWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect=False,
        reconnect_delay_seconds=0,
    )

    with pytest.raises(ConnectionClosedError):
        async for _ in feed.stream_trades():
            pass

    assert len(connector.urls) == 1


@pytest.mark.asyncio
async def test_okx_trade_websocket_raises_after_max_reconnects():
    connector = SequencedConnector(
        [
            FakeWebSocketConnection([ConnectionClosedError(None, None)]),
            FakeWebSocketConnection([ConnectionClosedError(None, None)]),
        ]
    )
    feed = OkxTradeWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=0,
        max_reconnects=1,
    )

    with pytest.raises(ConnectionClosedError):
        async for _ in feed.stream_trades():
            pass

    assert len(connector.urls) == 2
