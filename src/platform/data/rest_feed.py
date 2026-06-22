from __future__ import annotations

from typing import AsyncIterator

from src.platform.data.models import (
    MarketEvent,
    MarketKline,
    MarketOrderBook,
    MarketTicker,
    MarketTrade,
    market_kline_from_exchange,
    market_ticker_from_exchange,
    market_trade_from_exchange,
)
from src.platform.data.storage import MarketDataStore
from src.platform.data.websocket.ports import OrderBookStream, TradeStream
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.ports import ExchangeMarketDataClient
from src.platform.markets import MarketProfile


class RestMarketDataFeed:
    """REST-backed market data feed with optional WS streams and local cache."""

    def __init__(
        self,
        *,
        exchange_client: ExchangeMarketDataClient,
        symbol: str,
        market_profile: MarketProfile,
        trade_stream: TradeStream | None = None,
        order_book_stream: OrderBookStream | None = None,
        store: MarketDataStore | None = None,
    ) -> None:
        self._exchange_client = exchange_client
        self._symbol = symbol
        self._market_profile = market_profile
        self._trade_stream = trade_stream
        self._order_book_stream = order_book_stream
        self._store = store

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange_client.exchange

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

        rows = await self._exchange_client.fetch_klines(
            self._symbol,
            interval=interval,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            oldest_first=oldest_first,
        )
        klines = [market_kline_from_exchange(row) for row in rows]
        if self._store is not None:
            self._store.save_klines(klines)
        return klines

    async def fetch_ticker(self) -> MarketTicker:
        ticker = await self._exchange_client.fetch_ticker(self._symbol)
        return market_ticker_from_exchange(ticker)

    async def fetch_trades(
        self,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
        oldest_first: bool = True,
    ) -> list[MarketTrade]:
        fetch = getattr(self._exchange_client, "fetch_trades", None)
        if not callable(fetch):
            raise NotImplementedError(f"Historical trades are not supported for {self.exchange.value}")
        rows = await fetch(
            self._symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=limit,
            oldest_first=oldest_first,
        )
        return [market_trade_from_exchange(row) for row in rows]

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
