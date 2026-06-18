from __future__ import annotations

from typing import Protocol

from src.platform.exchanges.models import Balance, ExchangeName, Position


class AccountClient(Protocol):
    """Single account query interface used by runtime code."""

    @property
    def exchange(self) -> ExchangeName:
        ...

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        ...

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        ...
