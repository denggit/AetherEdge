from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from src.platform.data.websocket.okx_open_interest import (
    OkxOpenInterestWebSocketFeed,
)
from src.platform.data.websocket import okx_public
from src.platform.data.websocket.okx_public import (
    OkxPublicWebSocketProtocolError,
    OkxSubscriptionError,
)


class _Connection:
    def __init__(self, messages) -> None:
        self.messages = iter(messages)
        self.sent = []
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.messages)
        except StopIteration:
            raise StopAsyncIteration

    async def send(self, message):
        self.sent.append(message)

    async def close(self):
        self.closed = True


class _Connector:
    def __init__(self, connection) -> None:
        self.connections = (
            connection if isinstance(connection, list) else [connection]
        )
        self.calls = 0

    async def connect(self, _url):
        connection = self.connections[self.calls]
        self.calls += 1
        return connection


def _payload(*, oi_ccy="12.5", oi_usd="25000") -> str:
    row = {
        "instType": "SWAP",
        "instId": "ETH-USDT-SWAP",
        "oi": "100.123456789",
        "ts": "1710000000000",
    }
    if oi_ccy is not None:
        row["oiCcy"] = oi_ccy
    if oi_usd is not None:
        row["oiUsd"] = oi_usd
    return json.dumps(
        {
            "arg": {
                "channel": "open-interest",
                "instId": "ETH-USDT-SWAP",
            },
            "data": [row],
        }
    )


def test_open_interest_maps_distinct_units_and_optional_values() -> None:
    feed = OkxOpenInterestWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=_Connector(_Connection([])),
    )
    event = feed._map_message(_payload())[0]
    missing = feed._map_message(
        _payload(oi_ccy=None, oi_usd=None)
    )[0]

    assert event.open_interest_contracts == Decimal("100.123456789")
    assert event.open_interest_base == Decimal("12.5")
    assert event.open_interest_usd == Decimal("25000")
    assert event.instrument_type == "SWAP"
    assert event.event_time_ms == 1710000000000
    assert missing.open_interest_base is None
    assert missing.open_interest_usd is None


def test_open_interest_stream_subscribes_to_public_channel() -> None:
    connection = _Connection(
        [
            json.dumps(
                {
                    "event": "subscribe",
                    "arg": {
                        "channel": "open-interest",
                        "instId": "ETH-USDT-SWAP",
                    },
                }
            ),
            _payload(),
        ]
    )
    feed = OkxOpenInterestWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=_Connector(connection),
        reconnect=False,
    )

    async def first():
        async for event in feed.stream_open_interest():
            return event
        raise AssertionError("no OI event")

    event = asyncio.run(first())
    assert event.open_interest_contracts == Decimal("100.123456789")
    assert json.loads(connection.sent[0]) == {
        "op": "subscribe",
        "args": [
            {
                "channel": "open-interest",
                "instId": "ETH-USDT-SWAP",
            }
        ],
    }
    assert connection.closed is True


def test_protocol_error_reconnects_and_recovers_valid_open_interest() -> None:
    first = _Connection(["{malformed-json"])
    second = _Connection([_payload()])
    connector = _Connector([first, second])
    feed = OkxOpenInterestWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=0,
        max_reconnects=1,
    )

    async def first_event():
        async for event in feed.stream_open_interest():
            await feed.close()
            return event
        raise AssertionError("no recovered OI event")

    event = asyncio.run(first_event())

    assert event.open_interest_contracts == Decimal("100.123456789")
    assert connector.calls == 2
    assert first.closed is True


def test_subscription_error_remains_terminal_without_reconnect() -> None:
    connection = _Connection(
        [
            json.dumps(
                {
                    "event": "error",
                    "code": "60012",
                    "msg": "bad channel",
                }
            )
        ]
    )
    connector = _Connector(connection)
    feed = OkxOpenInterestWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=0,
    )

    async def consume() -> None:
        async for _event in feed.stream_open_interest():
            pass

    with pytest.raises(
        OkxSubscriptionError,
        match="code=60012.*msg=bad channel",
    ):
        asyncio.run(consume())

    assert connector.calls == 1
    assert connection.closed is True


def test_protocol_errors_stop_after_max_reconnects() -> None:
    connections = [
        _Connection(["not-json"]),
        _Connection(["still-not-json"]),
    ]
    connector = _Connector(connections)
    feed = OkxOpenInterestWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=0,
        max_reconnects=1,
    )

    async def consume() -> None:
        async for _event in feed.stream_open_interest():
            pass

    with pytest.raises(
        OkxPublicWebSocketProtocolError,
        match="invalid OKX public websocket JSON",
    ):
        asyncio.run(consume())

    assert connector.calls == 2
    assert all(connection.closed for connection in connections)


def test_cancellation_during_reconnect_backoff_does_not_reconnect(
    monkeypatch,
) -> None:
    connection = _Connection(["not-json"])
    connector = _Connector(connection)
    feed = OkxOpenInterestWebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=30,
    )
    backoff_started = asyncio.Event()

    async def blocking_sleep(_delay: float) -> None:
        backoff_started.set()
        await asyncio.Future()

    monkeypatch.setattr(okx_public.asyncio, "sleep", blocking_sleep)

    async def consume() -> None:
        async for _event in feed.stream_open_interest():
            pass

    async def scenario() -> None:
        task = asyncio.create_task(consume())
        await backoff_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert connector.calls == 1
    assert connection.closed is True
