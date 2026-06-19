from __future__ import annotations

from typing import Protocol, Sequence

from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.signals import TradeSignal


class StrategyPort(Protocol):
    """Future strategy interface.

    A strategy observes normalized platform data and returns standardized
    signals. It must not import exchange adapters directly.
    """

    async def on_start(self, snapshot: PlatformSnapshot) -> Sequence[TradeSignal]:
        ...

    async def on_kline(self, kline: MarketKline) -> Sequence[TradeSignal]:
        ...

    async def on_ticker(self, ticker: MarketTicker) -> Sequence[TradeSignal]:
        ...

    async def on_trade(self, trade: MarketTrade) -> Sequence[TradeSignal]:
        ...

    async def on_order_book(self, order_book: MarketOrderBook) -> Sequence[TradeSignal]:
        ...

    async def on_account_event(self, event: AccountEvent) -> Sequence[TradeSignal]:
        ...
