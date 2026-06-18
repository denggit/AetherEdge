from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.platform.exchanges.models import (
    Balance,
    ExchangeName,
    LeverageInfo,
    LeverageRequest,
    MarginMode,
    Position,
    PositionMode,
)
from src.platform.exchanges.ports import ExchangeAccountClient
from src.platform.markets import MarketProfile


class ExchangeAccountService:
    """Account facade bound to one exchange + one canonical market symbol."""

    def __init__(self, exchange_client: ExchangeAccountClient, *, symbol: str, market_profile: MarketProfile) -> None:
        self._exchange_client = exchange_client
        self._symbol = symbol
        self._market_profile = market_profile

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange_client.exchange

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def market_profile(self) -> MarketProfile:
        return self._market_profile

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        return await self._exchange_client.fetch_balance(asset)

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        resolved_symbol = symbol or self._symbol
        self._ensure_bound_symbol(resolved_symbol)
        return await self._exchange_client.fetch_positions(resolved_symbol)

    async def fetch_leverage(self, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        return await self._exchange_client.fetch_leverage(self._symbol, margin_mode=margin_mode)

    async def set_leverage(self, leverage: Decimal, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        return await self._exchange_client.set_leverage(
            LeverageRequest(symbol=self._symbol, leverage=leverage, margin_mode=margin_mode)
        )

    async def set_margin_mode(self, margin_mode: MarginMode) -> Mapping[str, Any]:
        return await self._exchange_client.set_margin_mode(self._symbol, margin_mode)

    async def fetch_position_mode(self) -> PositionMode:
        return await self._exchange_client.fetch_position_mode()

    async def set_position_mode(self, mode: PositionMode) -> PositionMode:
        return await self._exchange_client.set_position_mode(mode)

    def _ensure_bound_symbol(self, symbol: str) -> None:
        if symbol != self._symbol:
            raise ValueError(f"account client is bound to {self._symbol}, got {symbol}")
