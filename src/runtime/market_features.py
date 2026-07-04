from __future__ import annotations

import inspect
from collections.abc import Sequence

from src.market_data.events import MarketFeatureEvent
from src.signals import TradeSignal
from src.strategy.market_features import MarketFeatureObserver


def resolve_market_feature_observers(
    strategy: object,
) -> tuple[MarketFeatureObserver, ...]:
    """Resolve provider observers first, then the legacy strategy handler."""

    provider = getattr(strategy, "market_feature_observers", None)
    if callable(provider):
        provided = provider()
        if (
            not isinstance(provided, Sequence)
            or isinstance(provided, (str, bytes, bytearray))
        ):
            raise TypeError(
                "market_feature_observers() must return a sequence of observers"
            )
        return tuple(
            observer
            for observer in provided
            if getattr(observer, "enabled", True) is not False
        )

    legacy_handler = getattr(strategy, "on_market_feature", None)
    if callable(legacy_handler) and getattr(strategy, "enabled", True) is not False:
        return (strategy,)  # type: ignore[return-value]
    return ()


async def dispatch_market_feature_event(
    strategy: object,
    event: MarketFeatureEvent,
) -> tuple[TradeSignal, ...]:
    """Dispatch one normalized feature event to each resolved observer."""

    signals: list[TradeSignal] = []
    for observer in resolve_market_feature_observers(strategy):
        handler = getattr(observer, "on_market_feature", None)
        if not callable(handler):
            raise TypeError(
                "market feature observer must define callable on_market_feature"
            )

        result = handler(event)
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


__all__ = [
    "dispatch_market_feature_event",
    "resolve_market_feature_observers",
]
