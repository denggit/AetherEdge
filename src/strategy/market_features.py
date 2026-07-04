from __future__ import annotations

from typing import Awaitable, Protocol, Sequence, runtime_checkable

from src.market_data.events import MarketFeatureEvent
from src.signals import TradeSignal


MarketFeatureObserverResult = (
    Sequence[TradeSignal]
    | Awaitable[Sequence[TradeSignal] | None]
    | None
)


@runtime_checkable
class MarketFeatureObserver(Protocol):
    """Strategy-side port for normalized market feature events."""

    observer_id: str
    enabled: bool

    def on_market_feature(
        self,
        event: MarketFeatureEvent,
    ) -> MarketFeatureObserverResult:
        ...


@runtime_checkable
class MarketFeatureObserverProvider(Protocol):
    """Optional strategy extension exposing ordered feature observers."""

    def market_feature_observers(self) -> Sequence[MarketFeatureObserver]:
        ...


__all__ = [
    "MarketFeatureObserver",
    "MarketFeatureObserverProvider",
    "MarketFeatureObserverResult",
]
