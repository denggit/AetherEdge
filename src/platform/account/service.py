from __future__ import annotations

from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import Balance, ExchangeName, Position
from src.platform.exchanges.ports import ExchangeAccountClient


class ExchangeAccountService:
    """Account facade: balance and position reads only."""

    def __init__(self, exchange_client: ExchangeAccountClient) -> None:
        self._exchange_client = exchange_client

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange_client.exchange

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        return await self._exchange_client.fetch_balance(asset)

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        return await self._exchange_client.fetch_positions(symbol)
