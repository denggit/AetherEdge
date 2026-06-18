from __future__ import annotations

from typing import AsyncIterator

from src.data_feed.models import (
    MarketEvent,
    MarketKline,
    MarketTicker,
    MarketTrade,
    market_kline_from_exchange,
    market_ticker_from_exchange,
)
from src.data_feed.websocket.ports import TradeStream
from src.exchanges.models import ExchangeName
from src.exchanges.ports import ExchangeClient


class RestMarketDataFeed:
    """REST-backed market data feed with optional WebSocket trade stream.

    It only uses public market-data methods from ExchangeClient for REST data.
    Tick streaming is delegated to a small exchange-specific TradeStream adapter.
    Private trading/account APIs remain outside data_feed.
    """

    def __init__(
        self,
        *,
        exchange_client: ExchangeClient,
        symbol: str,
        trade_stream: TradeStream | None = None,
    ) -> None:
        self._exchange_client = exchange_client
        self._symbol = symbol
        self._trade_stream = trade_stream

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange_client.exchange

    @property
    def symbol(self) -> str:
        return self._symbol

    async def fetch_klines(
        self,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketKline]:
        rows = await self._exchange_client.fetch_klines(
            self._symbol,
            interval=interval,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        return [market_kline_from_exchange(row) for row in rows]

    async def fetch_ticker(self) -> MarketTicker:
        ticker = await self._exchange_client.fetch_ticker(self._symbol)
        return market_ticker_from_exchange(ticker)

    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        if self._trade_stream is None:
            raise NotImplementedError("No trade stream configured for this feed")
        async for trade in self._trade_stream.stream_trades():
            yield trade

    async def stream_events(self) -> AsyncIterator[MarketEvent]:
        async for trade in self.stream_trades():
            yield trade
