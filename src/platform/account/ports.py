from __future__ import annotations

from typing import Protocol

from src.platform.exchanges.models import Balance, ExchangeName, Position
from src.platform.markets import MarketProfile


class AccountClient(Protocol):
    """Single account query interface used by runtime code."""

    @property
    def exchange(self) -> ExchangeName:
        ...

    @property
    def symbol(self) -> str:
        ...

    @property
    def market_profile(self) -> MarketProfile:
        ...

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        ...

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        ...
