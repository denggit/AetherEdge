from __future__ import annotations

import asyncio
import json
from decimal import Decimal

from src.platform.data.websocket.okx_open_interest import (
    OkxOpenInterestWebSocketFeed,
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
        self.connection = connection

    async def connect(self, _url):
        return self.connection


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
