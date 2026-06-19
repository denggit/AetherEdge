from __future__ import annotations

from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.signals import TradeSignal


class Strategy:
    """Empty plugin used to verify the app runner wiring.

    It intentionally emits no signal and contains no trading logic.
    """

    async def on_start(self, snapshot: PlatformSnapshot) -> list[TradeSignal]:
        return []

    async def on_kline(self, kline: MarketKline) -> list[TradeSignal]:
        return []

    async def on_ticker(self, ticker: MarketTicker) -> list[TradeSignal]:
        return []

    async def on_trade(self, trade: MarketTrade) -> list[TradeSignal]:
        return []

    async def on_order_book(self, order_book: MarketOrderBook) -> list[TradeSignal]:
        return []

    async def on_account_event(self, event: AccountEvent) -> list[TradeSignal]:
        return []
