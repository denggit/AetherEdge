from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from src.platform.data.order_book import OrderBookSequenceGap
from src.platform.data.websocket.okx_order_book_l2 import (
    OkxOrderBookL2WebSocketFeed,
)
from src.platform.data.websocket.okx_public import OkxSubscriptionError


class _Connection:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.sent: list[str] = []
        self.closed = False

    def __aiter__(self):
        self._iterator = iter(self.messages)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            raise StopAsyncIteration

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


class _Connector:
    def __init__(self, connections: list[_Connection]) -> None:
        self.connections = connections
        self.calls = 0

    async def connect(self, _url: str) -> _Connection:
        connection = self.connections[self.calls]
        self.calls += 1
        return connection


def _message(
    *,
    action: str,
    prev_seq_id: int,
    seq_id: int,
    bids: list[list[str]],
    asks: list[list[str]],
    ts: int,
    checksum: int = 0,
) -> str:
    return json.dumps(
        {
            "arg": {
                "channel": "books",
                "instId": "ETH-USDT-SWAP",
            },
            "action": action,
            "data": [
                {
                    "bids": bids,
                    "asks": asks,
                    "prevSeqId": prev_seq_id,
                    "seqId": seq_id,
                    "ts": str(ts),
                    "checksum": checksum,
                }
            ],
        }
    )


def test_snapshot_and_update_publish_complete_l2_book() -> None:
    feed = OkxOrderBookL2WebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=_Connector([]),
    )
    snapshot = feed._map_message(
        _message(
            action="snapshot",
            prev_seq_id=-1,
            seq_id=10,
            bids=[["100", "2", "0", "3"]],
            asks=[["101", "4", "0", "5"]],
            ts=1000,
        )
    )[0]
    update = feed._map_message(
        _message(
            action="update",
            prev_seq_id=10,
            seq_id=11,
            bids=[["100", "0", "0", "0"], ["99", "7", "0", "8"]],
            asks=[["100.5", "6", "0", "9"]],
            ts=1001,
        )
    )[0]

    assert snapshot.sequence_id == 10
    assert snapshot.bids[0].order_count == 3
    assert [item.price for item in update.bids] == [Decimal("99")]
    assert [item.price for item in update.asks] == [
        Decimal("100.5"),
        Decimal("101"),
    ]
    assert update.previous_sequence_id == 10
    assert update.raw == {
        "channel": "books",
        "action": "update",
        "checksum": 0,
    }


def test_empty_update_is_heartbeat_without_duplicate_event() -> None:
    feed = OkxOrderBookL2WebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=_Connector([]),
    )
    feed._map_message(
        _message(
            action="snapshot",
            prev_seq_id=-1,
            seq_id=10,
            bids=[],
            asks=[],
            ts=1000,
        )
    )
    events = feed._map_message(
        _message(
            action="update",
            prev_seq_id=10,
            seq_id=10,
            bids=[],
            asks=[],
            ts=1001,
        )
    )
    assert events == []
    assert feed.heartbeat_count == 1
    assert feed.ready is True


def test_sequence_reset_is_allowed_but_gap_discards_state() -> None:
    feed = OkxOrderBookL2WebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=_Connector([]),
    )
    feed._map_message(
        _message(
            action="snapshot",
            prev_seq_id=-1,
            seq_id=100,
            bids=[],
            asks=[],
            ts=1000,
        )
    )
    event = feed._map_message(
        _message(
            action="update",
            prev_seq_id=100,
            seq_id=5,
            bids=[],
            asks=[["101", "1", "0", "1"]],
            ts=1001,
        )
    )[0]
    assert event.sequence_id == 5

    with pytest.raises(OrderBookSequenceGap):
        feed._map_message(
            _message(
                action="update",
                prev_seq_id=99,
                seq_id=100,
                bids=[],
                asks=[],
                ts=1002,
            )
        )
    assert feed.ready is False
    assert feed.last_sequence_id is None
    assert feed.sequence_gap_count == 1


def test_stream_subscribes_to_books_and_rebuilds_after_gap() -> None:
    first = _Connection(
        [
            _message(
                action="snapshot",
                prev_seq_id=-1,
                seq_id=10,
                bids=[["100", "1", "0", "1"]],
                asks=[],
                ts=1000,
            ),
            _message(
                action="update",
                prev_seq_id=99,
                seq_id=100,
                bids=[],
                asks=[],
                ts=1001,
            ),
        ]
    )
    second = _Connection(
        [
            _message(
                action="snapshot",
                prev_seq_id=-1,
                seq_id=20,
                bids=[["200", "1", "0", "1"]],
                asks=[],
                ts=2000,
            )
        ]
    )
    connector = _Connector([first, second])
    feed = OkxOrderBookL2WebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=connector,
        reconnect_delay_seconds=0,
        max_reconnects=1,
    )

    async def collect() -> list[int]:
        output = []
        async for event in feed.stream_order_book_l2():
            output.append(event.sequence_id)
            if len(output) == 2:
                await feed.close()
                break
        return output

    assert asyncio.run(collect()) == [10, 20]
    assert json.loads(first.sent[0]) == {
        "op": "subscribe",
        "args": [{"channel": "books", "instId": "ETH-USDT-SWAP"}],
    }
    assert connector.calls == 2
    assert feed.resync_count == 1


def test_subscription_error_is_not_silently_reconnected() -> None:
    feed = OkxOrderBookL2WebSocketFeed(
        symbol="ETH-USDT-PERP",
        connector=_Connector([]),
    )
    with pytest.raises(
        OkxSubscriptionError,
        match="code=60012.*msg=bad channel",
    ):
        feed._map_message(
            json.dumps(
                {
                    "event": "error",
                    "code": "60012",
                    "msg": "bad channel",
                }
            )
        )
