from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping, Protocol

from src.platform.exchanges.models import (
    Balance,
    ExchangeName,
    LeverageInfo,
    MarginMode,
    Position,
    PositionMode,
)
from src.platform.markets import MarketProfile


class AccountClient(Protocol):
    """Single account/config query interface used by runtime code."""

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

    async def fetch_leverage(self, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        ...

    async def set_leverage(self, leverage: Decimal, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        ...

    async def set_margin_mode(self, margin_mode: MarginMode) -> Mapping[str, Any]:
        ...

    async def fetch_position_mode(self) -> PositionMode:
        ...

    async def set_position_mode(self, mode: PositionMode) -> PositionMode:
        ...
