from __future__ import annotations

from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import Balance, ExchangeName, Position
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
        if resolved_symbol != self._symbol:
            raise ValueError(f"account client is bound to {self._symbol}, got {resolved_symbol}")
        return await self._exchange_client.fetch_positions(resolved_symbol)
