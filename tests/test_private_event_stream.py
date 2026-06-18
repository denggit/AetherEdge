import asyncio
import json
from collections.abc import AsyncIterator

from src.platform import (
    AccountEventType,
    ExchangeConfig,
    ExchangeName,
    OrderSide,
    OrderStatus,
    PositionSide,
    create_account_event_stream,
    create_exchange_client,
)


class FakeConnection:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []
        self.closed = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iter()

    async def _iter(self):
        for message in self.messages:
            yield message

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


class FakeConnector:
    def __init__(self, connection):
        self.connection = connection
        self.urls = []

    async def connect(self, url: str):
        self.urls.append(url)
        return self.connection


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append({"method": method, "url": url, "params": params, "json_body": json_body, "headers": headers or {}})
        return self.responses.pop(0)


def test_okx_private_event_stream_logs_in_subscribes_and_maps_order_events():
    connection = FakeConnection(
        [
            json.dumps({"event": "login", "code": "0"}),
            json.dumps(
                {
                    "arg": {"channel": "orders"},
                    "data": [
                        {
                            "instId": "ETH-USDT-SWAP",
                            "ordId": "1",
                            "clOrdId": "c1",
                            "state": "filled",
                            "side": "buy",
                            "posSide": "long",
                            "px": "3000",
                            "sz": "0.1",
                            "accFillSz": "0.1",
                            "uTime": "1710000000000",
                        }
                    ],
                }
            ),
        ]
    )
    connector = FakeConnector(connection)
    stream = create_account_event_stream(
        "okx",
        config=ExchangeConfig(api_key="k", api_secret="s", passphrase="p", sandbox=True),
        connector=connector,
        reconnect=False,
    )

    events = asyncio.run(_collect(stream.stream_events()))

    assert connector.urls[0].startswith("wss://wspap.okx.com")
    assert json.loads(connection.sent[0])["op"] == "login"
    subscribe = json.loads(connection.sent[1])
    assert subscribe["op"] == "subscribe"
    assert {arg["channel"] for arg in subscribe["args"]} == {"orders", "account", "positions"}
    assert events[0].event_type is AccountEventType.SYSTEM
    assert events[1].event_type is AccountEventType.ORDER
    assert events[1].symbol == "ETH-USDT-PERP"
    assert events[1].order_status is OrderStatus.FILLED
    assert events[1].side is OrderSide.BUY
    assert events[1].position_side is PositionSide.LONG


def test_binance_private_event_stream_creates_listen_key_and_maps_order_and_account_events():
    http = FakeHttpClient([{"listenKey": "listen-1"}])
    exchange_client = create_exchange_client("binance", ExchangeConfig(api_key="k", api_secret="s", sandbox=True), http_client=http)
    connection = FakeConnection(
        [
            json.dumps(
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 1710000000000,
                    "o": {
                        "s": "ETHUSDT",
                        "i": 123,
                        "c": "c1",
                        "X": "PARTIALLY_FILLED",
                        "S": "SELL",
                        "ps": "BOTH",
                        "p": "3100",
                        "q": "0.2",
                        "z": "0.1",
                    },
                }
            ),
            json.dumps(
                {
                    "e": "ACCOUNT_UPDATE",
                    "E": 1710000001000,
                    "a": {
                        "B": [{"a": "USDT", "wb": "100", "cw": "90"}],
                        "P": [{"s": "ETHUSDT", "ps": "BOTH", "pa": "0.1", "ep": "3000"}],
                    },
                }
            ),
        ]
    )
    connector = FakeConnector(connection)
    stream = create_account_event_stream(
        "binance",
        config=ExchangeConfig(api_key="k", api_secret="s", sandbox=True),
        exchange_client=exchange_client,
        connector=connector,
        reconnect=False,
    )

    events = asyncio.run(_collect(stream.stream_events()))

    assert http.calls[0]["url"].endswith("/fapi/v1/listenKey")
    assert http.calls[0]["method"] == "POST"
    assert http.calls[0]["headers"]["X-MBX-APIKEY"] == "k"
    assert connector.urls[0].endswith("/listen-1")
    assert events[0].event_type is AccountEventType.ORDER
    assert events[0].order_status is OrderStatus.PARTIALLY_FILLED
    assert events[0].side is OrderSide.SELL
    assert events[1].event_type is AccountEventType.BALANCE
    assert events[1].asset == "USDT"
    assert events[2].event_type is AccountEventType.POSITION
    assert events[2].symbol == "ETH-USDT-PERP"


async def _collect(iterator):
    return [event async for event in iterator]
