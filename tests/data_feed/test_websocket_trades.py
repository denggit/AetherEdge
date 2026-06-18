import asyncio
import json
from decimal import Decimal

from src.platform.data import TradeSide, create_market_data_feed
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


async def _first_trade(feed):
    async for trade in feed.stream_trades():
        return trade
    raise AssertionError("no trade yielded")


def test_okx_websocket_trade_stream_maps_tick_messages():
    connector = FakeWebSocketConnector(
        [
            json.dumps({"event": "subscribe", "arg": {"channel": "trades", "instId": "ETH-USDT-SWAP"}}),
            json.dumps(
                {
                    "arg": {"channel": "trades", "instId": "ETH-USDT-SWAP"},
                    "data": [
                        {
                            "instId": "ETH-USDT-SWAP",
                            "tradeId": "1",
                            "px": "3000.1",
                            "sz": "0.2",
                            "side": "buy",
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
        config=ExchangeConfig(sandbox=False),
        exchange_client=FakeExchangeClient(),
        websocket_connector=connector,
    )

    trade = asyncio.run(_first_trade(feed))

    assert connector.urls == ["wss://ws.okx.com:8443/ws/v5/public"]
    assert connector.connection is not None
    assert json.loads(connector.connection.sent[0]) == {
        "op": "subscribe",
        "args": [{"channel": "trades", "instId": "ETH-USDT-SWAP"}],
    }
    assert trade.exchange is ExchangeName.OKX
    assert trade.symbol == "ETH-USDT-PERP"
    assert trade.raw_symbol == "ETH-USDT-SWAP"
    assert trade.price == Decimal("3000.1")
    assert trade.quantity == Decimal("0.2")
    assert trade.side is TradeSide.BUY
    assert trade.trade_id == "1"


def test_binance_websocket_trade_stream_maps_tick_messages():
    connector = FakeWebSocketConnector(
        [
            json.dumps(
                {
                    "e": "aggTrade",
                    "E": 1710000000001,
                    "s": "ETHUSDT",
                    "a": 100,
                    "p": "3000.2",
                    "q": "0.3",
                    "T": 1710000000000,
                    "m": True,
                }
            )
        ]
    )
    feed = create_market_data_feed(
        "binance",
        symbol="ETH-USDT-PERP",
        config=ExchangeConfig(sandbox=False),
        exchange_client=FakeExchangeClient(),
        websocket_connector=connector,
    )

    trade = asyncio.run(_first_trade(feed))

    assert connector.urls == ["wss://fstream.binance.com/ws/ethusdt@aggTrade"]
    assert trade.exchange is ExchangeName.BINANCE
    assert trade.symbol == "ETH-USDT-PERP"
    assert trade.raw_symbol == "ETHUSDT"
    assert trade.price == Decimal("3000.2")
    assert trade.quantity == Decimal("0.3")
    assert trade.side is TradeSide.SELL
    assert trade.trade_id == "100"


def test_market_data_feed_can_disable_trade_stream_for_rest_only_mode():
    feed = create_market_data_feed(
        "okx",
        symbol="ETH-USDT-PERP",
        exchange_client=FakeExchangeClient(),
        enable_trade_stream=False,
    )

    async def consume():
        async for _ in feed.stream_trades():
            pass

    try:
        asyncio.run(consume())
    except NotImplementedError as exc:
        assert "No trade stream" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("REST-only feed should not stream trades")


def test_binance_trade_stream_reconnects_after_disconnect():
    class SequencedConnector:
        def __init__(self):
            self.urls = []
            self.messages = [[], [json.dumps({"e": "aggTrade", "E": 2, "s": "ETHUSDT", "a": 2, "p": "3001", "q": "0.1", "T": 2, "m": False})]]

        async def connect(self, url: str):
            self.urls.append(url)
            return FakeWebSocketConnection(self.messages.pop(0))

    connector = SequencedConnector()
    feed = create_market_data_feed(
        "binance",
        symbol="ETH-USDT-PERP",
        config=ExchangeConfig(),
        exchange_client=FakeExchangeClient(),
        websocket_connector=connector,
        reconnect_delay_seconds=0,
        max_reconnects=1,
    )

    trade = asyncio.run(_first_trade(feed))

    assert len(connector.urls) == 2
    assert trade.price == Decimal("3001")
