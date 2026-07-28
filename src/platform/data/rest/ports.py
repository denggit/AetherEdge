from __future__ import annotations

from typing import Protocol

from src.platform.data.models import MarketFullOrderBook


class FullOrderBookSnapshotFetcher(Protocol):
    async def fetch_full_order_book(
        self,
        *,
        symbol: str,
        depth: int = 5000,
    ) -> MarketFullOrderBook:
        ...


__all__ = ["FullOrderBookSnapshotFetcher"]
