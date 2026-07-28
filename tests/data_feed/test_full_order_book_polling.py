from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.platform.data.models import MarketFullOrderBook
from src.platform.data.polling import FullOrderBookPollingStream
from src.platform.exchanges.models import ExchangeName


def _snapshot(ts: int) -> MarketFullOrderBook:
    return MarketFullOrderBook(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        bids=(),
        asks=(),
        event_time_ms=ts,
        requested_depth=5000,
    )


class _Fetcher:
    def __init__(self, snapshots) -> None:
        self.snapshots = iter(snapshots)
        self.calls = 0

    async def fetch_full_order_book(self, *, symbol: str, depth: int):
        self.calls += 1
        return next(self.snapshots)


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps = []

    def __call__(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


def test_full_order_book_polling_is_immediate_periodic_and_deduplicated() -> None:
    fetcher = _Fetcher([_snapshot(1), _snapshot(1), _snapshot(2)])
    clock = _Clock()
    stream = FullOrderBookPollingStream(
        fetcher=fetcher,
        symbol="ETH-USDT-PERP",
        clock=clock,
        sleep=clock.sleep,
    )

    async def collect():
        events = []
        async for event in stream.stream_full_order_book():
            events.append(event)
            if len(events) == 2:
                await stream.stop()
        return events

    events = asyncio.run(collect())
    assert [event.event_time_ms for event in events] == [1, 2]
    assert fetcher.calls == 3
    assert stream.duplicate_snapshot_count == 1
    assert clock.sleeps == [3.0, 3.0]


def test_full_order_book_polling_validates_configuration() -> None:
    fetcher = _Fetcher([])
    with pytest.raises(ValueError):
        FullOrderBookPollingStream(
            fetcher=fetcher,
            symbol="ETH-USDT-PERP",
            depth=0,
        )
    with pytest.raises(ValueError):
        FullOrderBookPollingStream(
            fetcher=fetcher,
            symbol="ETH-USDT-PERP",
            poll_interval_seconds=0.5,
        )
