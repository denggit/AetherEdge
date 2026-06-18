import asyncio
import json
from decimal import Decimal

from src.platform.data import create_market_data_feed
from src.platform.exchanges import ExchangeConfig, ExchangeName
from tests.data_feed.test_rest_feed import FakeExchangeClient


class FakeWebSocketConnection:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []
        self.closed = False

    def __aiter__(self):
        self._iter = iter(self.messages)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


class FakeWebSocketConnector:
    def __init__(self, messages):
        self.messages = messages
        self.urls = []
        self.connection = None

    async def connect(self, url: str):
        self.urls.append(url)
        self.connection = FakeWebSocketConnection(self.messages)
        return self.connection


async def _first_order_book(feed):
    async for order_book in feed.stream_order_book():
        return order_book
    raise AssertionError("no order book yielded")


def test_okx_orderbook_websocket_stream_maps_books5():
    connector = FakeWebSocketConnector(
        [
            json.dumps({"event": "subscribe", "arg": {"channel": "books5", "instId": "ETH-USDT-SWAP"}}),
            json.dumps(
                {
                    "arg": {"channel": "books5", "instId": "ETH-USDT-SWAP"},
                    "data": [
                        {
                            "instId": "ETH-USDT-SWAP",
                            "bids": [["3000", "1", "0", "1"]],
                            "asks": [["3001", "2", "0", "1"]],
                            "ts": "1710000000000",
                        }
                    ],
                }
            ),
        ]
    )
    feed = create_market_data_feed(
        "okx",
        symbol="ETH-USDT-PERP",
        config=ExchangeConfig(),
        exchange_client=FakeExchangeClient(),
        websocket_connector=connector,
    )

    order_book = asyncio.run(_first_order_book(feed))

    assert connector.urls == ["wss://ws.okx.com:8443/ws/v5/public"]
    assert json.loads(connector.connection.sent[0]) == {
        "op": "subscribe",
        "args": [{"channel": "books5", "instId": "ETH-USDT-SWAP"}],
    }
    assert order_book.exchange is ExchangeName.OKX
    assert order_book.bids[0].price == Decimal("3000")
    assert order_book.asks[0].quantity == Decimal("2")


def test_binance_orderbook_websocket_stream_maps_depth5():
    connector = FakeWebSocketConnector(
        [
            json.dumps(
                {
                    "e": "depthUpdate",
                    "E": 1710000000000,
                    "s": "ETHUSDT",
                    "b": [["3000", "1"]],
                    "a": [["3001", "2"]],
                }
            )
        ]
    )
    feed = create_market_data_feed(
        "binance",
        symbol="ETH-USDT-PERP",
        config=ExchangeConfig(),
        exchange_client=FakeExchangeClient(),
        websocket_connector=connector,
    )

    order_book = asyncio.run(_first_order_book(feed))

    assert connector.urls == ["wss://fstream.binance.com/ws/ethusdt@depth5@100ms"]
    assert order_book.exchange is ExchangeName.BINANCE
    assert order_book.raw_symbol == "ETHUSDT"
    assert order_book.bids[0].price == Decimal("3000")
    assert order_book.asks[0].quantity == Decimal("2")
