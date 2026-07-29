from __future__ import annotations

from typing import AsyncIterator

from src.platform.data.models import (
    MarketEvent,
    MarketKline,
    MarketOrderBook,
    MarketTicker,
    MarketTrade,
)
from src.platform.data.ports import (
    HistoricalTradeFetcher,
    KlineFetcher,
    TickerFetcher,
    TradeIdAnchoredHistoryFetcher,
)
from src.platform.data.storage import MarketDataStore
from src.platform.data.websocket.ports import OrderBookStream, TradeStream
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import MarketProfile


class RestMarketDataFeed:
    """REST-backed market data feed with optional WS streams and local cache."""

    def __init__(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        market_profile: MarketProfile,
        kline_fetcher: KlineFetcher,
        ticker_fetcher: TickerFetcher,
        historical_trade_fetcher: HistoricalTradeFetcher | None = None,
        anchored_trade_fetcher: TradeIdAnchoredHistoryFetcher | None = None,
        trade_stream: TradeStream | None = None,
        order_book_stream: OrderBookStream | None = None,
        store: MarketDataStore | None = None,
    ) -> None:
        self._exchange = exchange
        self._symbol = symbol
        self._market_profile = market_profile
        self._kline_fetcher = kline_fetcher
        self._ticker_fetcher = ticker_fetcher
        self._historical_trade_fetcher = historical_trade_fetcher
        self._anchored_trade_fetcher = anchored_trade_fetcher
        self._trade_stream = trade_stream
        self._order_book_stream = order_book_stream
        self._store = store
        self.last_historical_trade_pages = 0

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def market_profile(self) -> MarketProfile:
        return self._market_profile

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
        if self._store is not None and use_cache:
            cached = self._store.load_klines(
                exchange=self.exchange,
                symbol=self._symbol,
                interval=interval,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            if len(cached) >= limit:
                return cached[-limit:]

        rows = await self._kline_fetcher.fetch_klines(
            self._symbol,
            interval=interval,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            oldest_first=oldest_first,
        )
        if self._store is not None:
            self._store.save_klines(rows)
        return rows

    async def fetch_ticker(self) -> MarketTicker:
        return await self._ticker_fetcher.fetch_ticker(self._symbol)

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
        if symbol is not None and symbol != self._symbol:
            raise ValueError(f"data feed is bound to {self._symbol}, got {symbol}")
        fetcher = self._historical_trade_fetcher
        if fetcher is None:
            raise NotImplementedError(
                "No historical trade fetcher configured for this feed"
            )
        rows = await fetcher.fetch_trades(
            self._symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=limit,
            oldest_first=oldest_first,
            max_pages=max_pages,
        )
        self.last_historical_trade_pages = max(
            1,
            int(fetcher.last_historical_trade_pages or 1),
        )
        return rows

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
        if symbol is not None and symbol != self._symbol:
            raise ValueError(
                f"data feed is bound to {self._symbol}, got {symbol}"
            )
        fetcher = self._anchored_trade_fetcher
        if fetcher is None:
            raise NotImplementedError(
                "No trade-ID anchored history fetcher configured for this feed"
            )
        rows = await fetcher.fetch_trades_between_ids(
            self._symbol,
            newer_trade_id=str(newer_trade_id),
            older_trade_id=str(older_trade_id),
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=int(limit),
            max_pages=int(max_pages),
            oldest_first=bool(oldest_first),
            partial_on_pagination=partial_on_pagination,
        )
        self.last_historical_trade_pages = max(
            1,
            int(fetcher.last_historical_trade_pages or 1),
        )
        return rows

    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        if self._trade_stream is None:
            raise NotImplementedError("No trade stream configured for this feed")
        async for trade in self._trade_stream.stream_trades():
            if self._store is not None:
                self._store.save_trade(trade)
            yield trade

    async def stream_order_book(self) -> AsyncIterator[MarketOrderBook]:
        if self._order_book_stream is None:
            raise NotImplementedError("No order book stream configured for this feed")
        async for order_book in self._order_book_stream.stream_order_book():
            if self._store is not None:
                self._store.save_order_book(order_book)
            yield order_book

    async def stream_events(self) -> AsyncIterator[MarketEvent]:
        async for trade in self.stream_trades():
            yield trade
