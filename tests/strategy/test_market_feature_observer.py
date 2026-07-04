from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_features import dispatch_market_feature_event
from src.signals import SignalAction, TradeSignal
from src.strategy import MarketFeatureObserver


def _event() -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="1m",
        event_time_ms=100,
    )


def _signal(reason: str) -> TradeSignal:
    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
        reason=reason,
    )


@dataclass
class _SyncObserver:
    observer_id: str = "trend_context"
    enabled: bool = True
    calls: list[MarketFeatureEvent] = field(default_factory=list)

    def on_market_feature(
        self,
        event: MarketFeatureEvent,
    ) -> list[TradeSignal]:
        self.calls.append(event)
        return [_signal("sync")]


@dataclass
class _AsyncObserver:
    observer_id: str = "volatility_context"
    enabled: bool = True

    async def on_market_feature(
        self,
        event: MarketFeatureEvent,
    ) -> tuple[TradeSignal, ...]:
        return (_signal("async"),)


def test_structural_observer_satisfies_runtime_checkable_protocol() -> None:
    observer = _SyncObserver(observer_id="arbitrary_observer")

    assert isinstance(observer, MarketFeatureObserver)
    assert observer.observer_id == "arbitrary_observer"


@pytest.mark.asyncio
async def test_sync_and_async_observers_can_return_signals() -> None:
    class Provider:
        def market_feature_observers(self):
            return (_SyncObserver(), _AsyncObserver())

    signals = await dispatch_market_feature_event(Provider(), _event())

    assert tuple(signal.reason for signal in signals) == ("sync", "async")


@pytest.mark.asyncio
async def test_disabled_observer_is_not_called() -> None:
    observer = _SyncObserver(enabled=False)

    class Provider:
        def market_feature_observers(self):
            return (observer,)

    assert await dispatch_market_feature_event(Provider(), _event()) == ()
    assert observer.calls == []
