from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

from src.market_data.events import MarketFeatureEvent
from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.reconcile.models import ReconcileReport
from src.signals import TradeSignal


@dataclass(frozen=True)
class StrategyRecoveryContext:
    """Strategy-facing recovery input.

    Runtime owns the generic recovery orchestration. Concrete strategies can use
    this context to rebuild their own internal state without importing runtime
    or exchange adapters.
    """

    snapshots: tuple[PlatformSnapshot, ...]
    reconcile_reports: tuple[ReconcileReport, ...]
    order_intent_ids: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


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


class RecoverableStrategyPort(Protocol):
    """Optional strategy extension used by runtime recovery."""

    async def recover(self, context: StrategyRecoveryContext) -> Sequence[TradeSignal]:
        ...


class MarketFeatureStrategyPort(Protocol):
    """Optional strategy extension for reusable market feature events."""

    async def on_market_feature(self, event: MarketFeatureEvent) -> Sequence[TradeSignal]:
        ...
