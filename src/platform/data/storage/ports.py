from __future__ import annotations

from typing import Protocol

from src.platform.data.models import MarketKline, MarketOrderBook, MarketTrade
from src.platform.exchanges.models import ExchangeName


class MarketDataStore(Protocol):
    def save_klines(self, rows: list[MarketKline]) -> None:
        ...

    def load_klines(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketKline]:
        ...

    def save_trade(self, trade: MarketTrade) -> None:
        ...

    def load_trades(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketTrade]:
        ...

    def save_order_book(self, order_book: MarketOrderBook) -> None:
        ...

    def load_order_books(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketOrderBook]:
        ...
