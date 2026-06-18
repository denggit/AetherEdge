from __future__ import annotations

from typing import AsyncIterator, Protocol

from src.data_feed.models import MarketTrade


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
