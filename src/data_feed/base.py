from __future__ import annotations

from typing import Any, Mapping, Protocol

from src.data_feed.market_events import MarketCandleEvent, MarketTradeEvent
from src.exchanges.models import ExchangeName

MarketDataEvent = MarketTradeEvent | MarketCandleEvent


class MarketDataFeed(Protocol):
    @property
    def exchange(self) -> ExchangeName:
        ...

    @property
    def canonical_symbol(self) -> str:
        ...

    @property
    def raw_symbol(self) -> str:
        ...

    def stream_names(self) -> tuple[str, ...]:
        ...

    def map_message(self, payload: Mapping[str, Any]) -> MarketDataEvent | None:
        ...
