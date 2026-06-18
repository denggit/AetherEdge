from __future__ import annotations

from typing import Protocol

from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot


class StrategyPort(Protocol):
    """Future strategy interface.

    A strategy should observe normalized platform data and return signals in a
    later module. It must not import exchange adapters directly.
    """

    async def on_start(self, snapshot: PlatformSnapshot) -> None:
        ...

    async def on_kline(self, kline: MarketKline) -> None:
        ...

    async def on_ticker(self, ticker: MarketTicker) -> None:
        ...

    async def on_trade(self, trade: MarketTrade) -> None:
        ...

    async def on_order_book(self, order_book: MarketOrderBook) -> None:
        ...

    async def on_account_event(self, event: AccountEvent) -> None:
        ...
