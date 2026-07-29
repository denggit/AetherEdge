from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable
from typing import TypeAlias

from src.platform.data.models import (
    MarketEvent,
    MarketEventType,
    MarketFullOrderBook,
    MarketOpenInterest,
    MarketOrderBookL2,
)
from src.platform.exchanges.models import ExchangeName


LatestStateMarketEvent: TypeAlias = (
    MarketOrderBookL2 | MarketFullOrderBook | MarketOpenInterest
)
LatestStateKey: TypeAlias = tuple[
    MarketEventType,
    ExchangeName,
    str,
]

LATEST_STATE_EVENT_TYPES = frozenset(
    {
        MarketEventType.ORDER_BOOK_L2,
        MarketEventType.FULL_ORDER_BOOK,
        MarketEventType.OPEN_INTEREST,
    }
)


def is_latest_state_market_event(
    event: MarketEvent,
) -> bool:
    return (
        isinstance(
            event,
            (MarketOrderBookL2, MarketFullOrderBook, MarketOpenInterest),
        )
        and event.event_type in LATEST_STATE_EVENT_TYPES
    )


class LatestStateMarketEventMailbox:
    """Keep one pending latest-state event per type/exchange/symbol key."""

    def __init__(
        self,
        *,
        max_pending_keys: int = 1024,
        notify: Callable[[], None] | None = None,
    ) -> None:
        if type(max_pending_keys) is not int or max_pending_keys <= 0:
            raise ValueError("max_pending_keys must be a positive integer")
        self._events: dict[LatestStateKey, LatestStateMarketEvent] = {}
        self._pending_keys: asyncio.Queue[LatestStateKey] = asyncio.Queue(
            maxsize=max_pending_keys
        )
        self._coalesced_by_type: Counter[MarketEventType] = Counter()
        self._notify = notify

    def publish(self, event: LatestStateMarketEvent) -> bool:
        if not is_latest_state_market_event(event):
            raise TypeError(
                "latest-state mailbox only accepts L2, full-book, and "
                "open-interest events"
            )
        key = self._key(event)
        replaced = key in self._events
        if replaced:
            self._events[key] = event
            self._coalesced_by_type[event.event_type] += 1
        else:
            self._pending_keys.put_nowait(key)
            self._events[key] = event
        if self._notify is not None:
            self._notify()
        return replaced

    async def get(self) -> LatestStateMarketEvent:
        key = await self._pending_keys.get()
        return self._events.pop(key)

    def get_nowait(self) -> LatestStateMarketEvent:
        key = self._pending_keys.get_nowait()
        return self._events.pop(key)

    def empty(self) -> bool:
        return self._pending_keys.empty()

    def qsize(self) -> int:
        return self._pending_keys.qsize()

    @property
    def coalesced_count(self) -> int:
        return sum(self._coalesced_by_type.values())

    def coalesced_count_for(
        self,
        event_type: MarketEventType,
    ) -> int:
        return self._coalesced_by_type[event_type]

    @staticmethod
    def _key(event: LatestStateMarketEvent) -> LatestStateKey:
        return (
            event.event_type,
            event.exchange,
            event.raw_symbol,
        )


__all__ = [
    "LATEST_STATE_EVENT_TYPES",
    "LatestStateMarketEvent",
    "LatestStateMarketEventMailbox",
    "is_latest_state_market_event",
]
