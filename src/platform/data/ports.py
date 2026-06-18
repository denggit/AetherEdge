from __future__ import annotations

from typing import AsyncIterator, Protocol

from src.platform.exchanges.models import ExchangeName
from src.platform.markets import MarketProfile
from src.platform.data.models import MarketEvent, MarketKline, MarketOrderBook, MarketTicker, MarketTrade


class MarketDataFeed(Protocol):
    """Unified market data interface used by strategy/runtime code."""

    @property
    def exchange(self) -> ExchangeName:
        ...

    @property
    def symbol(self) -> str:
        ...

    @property
    def market_profile(self) -> MarketProfile:
        ...

    async def fetch_klines(
        self,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        use_cache: bool = True,
        oldest_first: bool = False,
    ) -> list[MarketKline]:
        ...

    async def fetch_ticker(self) -> MarketTicker:
        ...

    def stream_trades(self) -> AsyncIterator[MarketTrade]:
        ...

    def stream_order_book(self) -> AsyncIterator[MarketOrderBook]:
        ...

    def stream_events(self) -> AsyncIterator[MarketEvent]:
        ...
