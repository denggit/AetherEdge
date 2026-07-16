from __future__ import annotations

import inspect
from collections.abc import Sequence

from src.market_data.events import MarketFeatureEvent
from src.signals import TradeSignal
from src.strategy.market_features import (
    MarketFeatureObserver,
    MarketFeatureObserverProvider,
)


def resolve_market_feature_observers(
    strategy: object,
) -> tuple[MarketFeatureObserver, ...]:
    """Resolve observers through the public provider capability."""

    if not isinstance(strategy, MarketFeatureObserverProvider):
        return ()
    provided = strategy.market_feature_observers()
    if (
        not isinstance(provided, Sequence)
        or isinstance(provided, (str, bytes, bytearray))
    ):
        raise TypeError(
            "market_feature_observers() must return a sequence of observers"
        )
    observers: list[MarketFeatureObserver] = []
    observer_ids: set[str] = set()
    for observer in provided:
        if not isinstance(observer, MarketFeatureObserver):
            raise TypeError(
                "market feature observer must define observer_id, enabled, "
                "and callable on_market_feature"
            )
        observer_id = observer.observer_id
        if not isinstance(observer_id, str) or not observer_id.strip():
            raise ValueError("market feature observer_id must be non-empty")
        if observer_id in observer_ids:
            raise ValueError(
                f"duplicate market feature observer_id: {observer_id}"
            )
        observer_ids.add(observer_id)
        if observer.enabled is not False:
            observers.append(observer)
    return tuple(observers)


async def dispatch_market_feature_event(
    strategy: object,
    event: MarketFeatureEvent,
) -> tuple[TradeSignal, ...]:
    """Dispatch one normalized feature event to each resolved observer."""

    return await _dispatch_to_strategy_observers(strategy, event)


async def _dispatch_to_strategy_observers(
    strategy: object,
    event: MarketFeatureEvent,
) -> tuple[TradeSignal, ...]:
    """Dispatch through the dynamically resolved observer boundary."""

    signals: list[TradeSignal] = []
    for observer in resolve_market_feature_observers(strategy):
        result = observer.on_market_feature(event)
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            continue
        if (
            not isinstance(result, Sequence)
            or isinstance(result, (str, bytes, bytearray))
        ):
            raise TypeError(
                "on_market_feature() must return a sequence of signals or None"
            )
        signals.extend(result)
    return tuple(signals)


class MarketFeaturePipeline:
    """Resolve and dispatch normalized market features to Strategy observers."""

    def __init__(self, strategy: object) -> None:
        self._strategy = strategy

    def resolve_observers(self) -> tuple[MarketFeatureObserver, ...]:
        return resolve_market_feature_observers(self._strategy)

    async def dispatch(
        self,
        event: MarketFeatureEvent,
    ) -> tuple[TradeSignal, ...]:
        return await _dispatch_to_strategy_observers(self._strategy, event)


__all__ = [
    "MarketFeaturePipeline",
    "dispatch_market_feature_event",
    "resolve_market_feature_observers",
]
