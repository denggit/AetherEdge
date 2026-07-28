from __future__ import annotations

from typing import AsyncIterator, Protocol

from src.platform.data.models import (
    MarketOpenInterest,
    MarketOrderBook,
    MarketOrderBookL2,
    MarketTrade,
)


class WebSocketConnection(Protocol):
    def __aiter__(self) -> AsyncIterator[str | bytes]:
        ...

    async def send(self, message: str) -> None:
        ...

    async def close(self) -> None:
        ...


class WebSocketConnector(Protocol):
    async def connect(self, url: str) -> WebSocketConnection:
        ...


class TradeStream(Protocol):
    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        ...


class OrderBookStream(Protocol):
    async def stream_order_book(self) -> AsyncIterator[MarketOrderBook]:
        ...


class OrderBookL2Stream(Protocol):
    async def stream_order_book_l2(self) -> AsyncIterator[MarketOrderBookL2]:
        ...


class OpenInterestStream(Protocol):
    async def stream_open_interest(self) -> AsyncIterator[MarketOpenInterest]:
        ...
