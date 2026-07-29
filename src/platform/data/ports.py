from __future__ import annotations

from typing import AsyncIterator, Protocol

from src.platform.exchanges.names import ExchangeName
from src.platform.markets import MarketProfile
from src.platform.data.models import MarketEvent, MarketKline, MarketOrderBook, MarketTicker, MarketTrade


class KlineFetcher(Protocol):
    async def fetch_klines(
        self,
        symbol: str,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        oldest_first: bool = False,
    ) -> list[MarketKline]:
        ...


class TickerFetcher(Protocol):
    async def fetch_ticker(self, symbol: str) -> MarketTicker:
        ...


class HistoricalTradeFetcher(Protocol):
    async def fetch_trades(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
        oldest_first: bool = True,
        max_pages: int | None = None,
    ) -> list[MarketTrade]:
        ...


class TradeIdAnchoredHistoryFetcher(Protocol):
    async def fetch_trades_between_ids(
        self,
        symbol: str,
        *,
        newer_trade_id: str,
        older_trade_id: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
        max_pages: int = 20,
        oldest_first: bool = True,
        partial_on_pagination: bool = False,
    ) -> list[MarketTrade]:
        ...


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

    async def fetch_trades(
        self,
        *,
        symbol: str | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
        oldest_first: bool = True,
        max_pages: int | None = None,
    ) -> list[MarketTrade]:
        ...

    async def fetch_trades_between_ids(
        self,
        *,
        symbol: str | None = None,
        newer_trade_id: str,
        older_trade_id: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
        max_pages: int = 20,
        oldest_first: bool = True,
        partial_on_pagination: bool = False,
    ) -> list[MarketTrade]:
        ...

    def stream_trades(self) -> AsyncIterator[MarketTrade]:
        ...

    def stream_order_book(self) -> AsyncIterator[MarketOrderBook]:
        ...

    def stream_events(self) -> AsyncIterator[MarketEvent]:
        ...
