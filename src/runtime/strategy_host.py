from __future__ import annotations

from collections.abc import Sequence

from src.order_management.models import ExchangeOrderResult
from src.platform.account.events import AccountEvent
from src.platform.data.models import (
    MarketEvent,
    MarketEventType,
    MarketKline,
    MarketOrderBook,
    MarketTicker,
    MarketTrade,
)
from src.platform.snapshot import PlatformSnapshot
from src.signals import TradeSignal


class StrategyHost:
    """Compatibility boundary for the runtime's basic Strategy callbacks."""

    def __init__(self, strategy: object) -> None:
        self._strategy = strategy

    async def on_start(
        self, snapshot: PlatformSnapshot
    ) -> Sequence[TradeSignal] | None:
        handler = getattr(self._strategy, "on_start", None)
        if not callable(handler):
            return None
        signals = await handler(snapshot)
        return () if signals is None else signals

    async def on_market_event(
        self, event: MarketEvent
    ) -> Sequence[TradeSignal]:
        if isinstance(event, MarketKline) or event.event_type is MarketEventType.KLINE:
            handler = getattr(self._strategy, "on_kline", None)
        elif isinstance(event, MarketTicker) or event.event_type is MarketEventType.TICKER:
            handler = getattr(self._strategy, "on_ticker", None)
        elif isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE:
            handler = getattr(self._strategy, "on_trade", None)
        elif isinstance(event, MarketOrderBook) or event.event_type is MarketEventType.ORDER_BOOK:
            handler = getattr(self._strategy, "on_order_book", None)
        else:
            handler = None
        if not callable(handler):
            return ()
        return await handler(event) or ()

    async def on_account_event(
        self, event: AccountEvent
    ) -> Sequence[TradeSignal] | None:
        handler = getattr(self._strategy, "on_account_event", None)
        if not callable(handler):
            return None
        signals = await handler(event)
        return () if signals is None else signals

    async def on_account_snapshot(self, snapshot: PlatformSnapshot) -> bool:
        handler = getattr(self._strategy, "on_account_snapshot", None)
        if not callable(handler):
            return False
        await handler(snapshot)
        return True

    async def on_order_results(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        source: str,
        event_time_ms: int | None,
    ) -> Sequence[TradeSignal] | None:
        handler = getattr(self._strategy, "on_order_results", None)
        if not callable(handler):
            return None
        signals = await handler(
            signal=signal,
            results=results,
            source=source,
            event_time_ms=event_time_ms,
        )
        return () if signals is None else signals


__all__ = ["StrategyHost"]
