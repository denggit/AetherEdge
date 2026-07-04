from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_features import (
    dispatch_market_feature_event,
    resolve_market_feature_observers,
)
from src.runtime import runner as runner_module
from src.signals import SignalAction, TradeSignal


def _event(event_time_ms: int = 100) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="1m",
        event_time_ms=event_time_ms,
    )


def _signal(reason: str, *, metadata=None) -> TradeSignal:
    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
        reason=reason,
        metadata={} if metadata is None else metadata,
    )


@dataclass
class _Observer:
    observer_id: str
    result: object = None
    enabled: bool = True
    calls: list[str] = field(default_factory=list)

    def on_market_feature(self, event: MarketFeatureEvent):
        self.calls.append(self.observer_id)
        return self.result


def test_provider_resolution_is_ordered_filters_disabled_and_has_priority() -> None:
    first = _Observer("first")
    disabled = _Observer("disabled", enabled=False)
    second = _Observer("second")

    class Strategy:
        def market_feature_observers(self):
            return (first, disabled, second)

        def on_market_feature(self, event):
            raise AssertionError("legacy handler must not be resolved")

    assert resolve_market_feature_observers(Strategy()) == (first, second)


@pytest.mark.asyncio
async def test_provider_dispatch_preserves_order_and_does_not_deduplicate() -> None:
    calls: list[str] = []

    class First:
        observer_id = "first"
        enabled = True

        def on_market_feature(self, event):
            calls.append("first")
            return [_signal("first")]

    class Second:
        observer_id = "second"
        enabled = True

        async def on_market_feature(self, event):
            calls.append("second")
            return (_signal("second"),)

    first = First()
    second = Second()

    class Strategy:
        def market_feature_observers(self):
            return (first, second, first)

        def on_market_feature(self, event):
            raise AssertionError("provider must take priority")

    signals = await dispatch_market_feature_event(Strategy(), _event())

    assert calls == ["first", "second", "first"]
    assert tuple(signal.reason for signal in signals) == (
        "first",
        "second",
        "first",
    )


@pytest.mark.asyncio
async def test_legacy_strategy_handler_remains_supported() -> None:
    class LegacyStrategy:
        async def on_market_feature(self, event):
            return (_signal("legacy"),)

    signals = await dispatch_market_feature_event(LegacyStrategy(), _event())

    assert tuple(signal.reason for signal in signals) == ("legacy",)


@pytest.mark.asyncio
async def test_none_result_and_missing_boundary_return_empty() -> None:
    observer = _Observer("none", result=None)

    class Provider:
        def market_feature_observers(self):
            return (observer,)

    assert await dispatch_market_feature_event(Provider(), _event()) == ()
    assert await dispatch_market_feature_event(object(), _event()) == ()


@pytest.mark.asyncio
async def test_non_sequence_observer_result_raises_type_error() -> None:
    observer = _Observer("invalid", result=123)

    class Provider:
        def market_feature_observers(self):
            return (observer,)

    with pytest.raises(TypeError, match="sequence of signals"):
        await dispatch_market_feature_event(Provider(), _event())


@pytest.mark.asyncio
async def test_observer_without_callable_handler_raises_type_error() -> None:
    observer = SimpleNamespace(observer_id="missing", enabled=True)

    class Provider:
        def market_feature_observers(self):
            return (observer,)

    with pytest.raises(TypeError, match="callable on_market_feature"):
        await dispatch_market_feature_event(Provider(), _event())


@pytest.mark.asyncio
async def test_observer_exception_is_not_swallowed() -> None:
    class BrokenObserver:
        observer_id = "broken"
        enabled = True

        def on_market_feature(self, event):
            raise RuntimeError("observer failed")

    class Provider:
        def market_feature_observers(self):
            return (BrokenObserver(),)

    with pytest.raises(RuntimeError, match="observer failed"):
        await dispatch_market_feature_event(Provider(), _event())


@pytest.mark.asyncio
async def test_dispatcher_preserves_signal_and_metadata_identity() -> None:
    metadata = {"scope": ["one", "two"]}
    signal = _signal("identity", metadata=metadata)
    observer = _Observer("identity", result=[signal])

    class Provider:
        def market_feature_observers(self):
            return (observer,)

    dispatched = await dispatch_market_feature_event(Provider(), _event())

    assert dispatched[0] is signal
    assert dispatched[0].metadata is metadata


@pytest.mark.asyncio
async def test_runner_process_market_feature_uses_dispatcher(monkeypatch) -> None:
    strategy = object()
    signal = _signal("runner")
    dispatched: list[tuple[object, MarketFeatureEvent]] = []
    executed: list[tuple[tuple[TradeSignal, ...], dict]] = []

    async def fake_dispatch(target, event):
        dispatched.append((target, event))
        return (signal,)

    async def fake_execute(signals, **kwargs):
        executed.append((tuple(signals), kwargs))

    monkeypatch.setattr(
        runner_module,
        "dispatch_market_feature_event",
        fake_dispatch,
    )
    runner = object.__new__(runner_module.LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=strategy)
    runner.stats = SimpleNamespace(feature_events_seen=0)
    runner._heartbeat_service = None
    runner._execute_signals = fake_execute
    event = _event()

    await runner.process_market_feature(event)

    assert dispatched == [(strategy, event)]
    assert runner.stats.feature_events_seen == 1
    assert executed == [
        (
            (signal,),
            {
                "source": "closed_kline",
                "event_time_ms": 100,
                "metadata": {"feature_type": "closed_kline"},
            },
        )
    ]


@pytest.mark.asyncio
async def test_startup_preview_uses_same_dispatcher(monkeypatch) -> None:
    strategy = object()
    events = (_event(100), _event(200))
    calls: list[MarketFeatureEvent] = []

    async def fake_dispatch(target, event):
        assert target is strategy
        calls.append(event)
        return (_signal(str(event.event_time_ms)),)

    monkeypatch.setattr(
        runner_module,
        "dispatch_market_feature_event",
        fake_dispatch,
    )
    runner = object.__new__(runner_module.LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=strategy)

    signals = await runner._preview_strategy_market_features(events)

    assert calls == list(events)
    assert tuple(signal.reason for signal in signals) == ("100", "200")
