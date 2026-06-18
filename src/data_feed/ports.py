from __future__ import annotations

from typing import AsyncIterator, Protocol

from src.exchanges.models import ExchangeName
from src.data_feed.models import MarketEvent, MarketKline, MarketTicker, MarketTrade


class MarketDataFeed(Protocol):
    """Unified market data feed used by strategy/runtime code.

    The feed depends on exchange public market-data methods, not private trading
    APIs. Trading, balance and position calls must stay in execution/exchanges.
    """

    @property
    def exchange(self) -> ExchangeName:
        ...

    @property
    def symbol(self) -> str:
        ...

    async def fetch_klines(
        self,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketKline]:
        ...

    async def fetch_ticker(self) -> MarketTicker:
        ...

    def stream_trades(self) -> AsyncIterator[MarketTrade]:
        ...

    def stream_events(self) -> AsyncIterator[MarketEvent]:
        ...
