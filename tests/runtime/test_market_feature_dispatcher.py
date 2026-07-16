from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_features import (
    MarketFeaturePipeline,
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
async def test_pipeline_holds_only_strategy_and_resolves_observers_each_time() -> None:
    calls: list[str] = []
    seen_events: list[MarketFeatureEvent] = []
    provider_calls = 0

    class First:
        observer_id = "first"
        enabled = True

        def on_market_feature(self, event):
            calls.append("first")
            seen_events.append(event)
            return (_signal("first"),)

    class Disabled:
        observer_id = "disabled"
        enabled = False

        def on_market_feature(self, event):
            raise AssertionError("disabled observer must not be dispatched")

    class AsyncSecond:
        observer_id = "async_second"
        enabled = None

        async def on_market_feature(self, event):
            calls.append("async_second")
            seen_events.append(event)
            return [_signal("async_second")]

    class NoneResult:
        observer_id = "none"
        enabled = True

        def on_market_feature(self, event):
            calls.append("none")
            seen_events.append(event)
            return None

    class Dynamic:
        observer_id = "dynamic"
        enabled = True

        def on_market_feature(self, event):
            calls.append("dynamic")
            seen_events.append(event)
            return (_signal("dynamic"),)

    class Strategy:
        def market_feature_observers(self):
            nonlocal provider_calls
            provider_calls += 1
            if provider_calls == 1:
                return (First(), Disabled(), AsyncSecond(), NoneResult())
            return (Dynamic(),)

        def on_market_feature(self, event):
            raise AssertionError("provider must take priority over legacy callback")

    strategy = Strategy()
    pipeline = MarketFeaturePipeline(strategy)
    event = _event()

    first_signals = await pipeline.dispatch(event)
    second_signals = await pipeline.dispatch(event)

    assert vars(pipeline) == {"_strategy": strategy}
    assert provider_calls == 2
    assert calls == ["first", "async_second", "none", "dynamic"]
    assert all(seen is event for seen in seen_events)
    assert tuple(signal.reason for signal in first_signals) == (
        "first",
        "async_second",
    )
    assert tuple(signal.reason for signal in second_signals) == ("dynamic",)


@pytest.mark.asyncio
async def test_pipeline_rejects_non_sequence_provider_result() -> None:
    class Strategy:
        def market_feature_observers(self):
            return object()

    with pytest.raises(TypeError, match="sequence of observers"):
        await MarketFeaturePipeline(Strategy()).dispatch(_event())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [123, "not-signals", b"not-signals", bytearray(b"not-signals")],
)
async def test_pipeline_rejects_invalid_observer_results(result) -> None:
    observer = _Observer("invalid", result=result)

    class Strategy:
        def market_feature_observers(self):
            return (observer,)

    with pytest.raises(TypeError, match="sequence of signals"):
        await MarketFeaturePipeline(Strategy()).dispatch(_event())


@pytest.mark.asyncio
async def test_pipeline_rejects_missing_handler_and_propagates_callback_error() -> None:
    missing = SimpleNamespace(observer_id="missing", enabled=True)

    class MissingStrategy:
        def market_feature_observers(self):
            return (missing,)

    with pytest.raises(TypeError, match="callable on_market_feature"):
        await MarketFeaturePipeline(MissingStrategy()).dispatch(_event())

    class BrokenStrategy:
        observer_id = "broken"
        enabled = True

        def market_feature_observers(self):
            return (self,)

        def on_market_feature(self, event):
            raise RuntimeError("observer failed")

    with pytest.raises(RuntimeError, match="observer failed"):
        await MarketFeaturePipeline(BrokenStrategy()).dispatch(_event())


@pytest.mark.asyncio
async def test_pipeline_does_not_invoke_runtime_side_effect_boundaries() -> None:
    signal = _signal("pipeline-only")

    class Strategy:
        observer_id = "pipeline-only"
        enabled = True

        def market_feature_observers(self):
            return (self,)

        def on_market_feature(self, event):
            return (signal,)

        def _execute_signals(self, *args, **kwargs):
            raise AssertionError("Pipeline must not execute signals")

        def coordinator(self, *args, **kwargs):
            raise AssertionError("Pipeline must not coordinate orders")

        def sync(self, *args, **kwargs):
            raise AssertionError("Pipeline must not sync state")

        def persist(self, *args, **kwargs):
            raise AssertionError("Pipeline must not persist state")

        def alerts(self, *args, **kwargs):
            raise AssertionError("Pipeline must not emit alerts")

    dispatched = await MarketFeaturePipeline(Strategy()).dispatch(_event())

    assert dispatched == (signal,)
    assert dispatched[0] is signal


@pytest.mark.asyncio
async def test_provider_dispatch_rejects_duplicate_observer_ids() -> None:
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

    with pytest.raises(ValueError, match="duplicate market feature observer_id"):
        await dispatch_market_feature_event(Strategy(), _event())

    assert calls == []


@pytest.mark.asyncio
async def test_strategy_without_observer_provider_is_not_dispatched() -> None:
    class StrategyWithoutProvider:
        async def on_market_feature(self, event):
            raise AssertionError("handler must remain plugin-private")

    signals = await dispatch_market_feature_event(StrategyWithoutProvider(), _event())

    assert signals == ()


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
async def test_runner_process_market_feature_uses_pipeline() -> None:
    strategy = object()
    signal = _signal("runner")
    dispatched: list[MarketFeatureEvent] = []
    executed: list[tuple[tuple[TradeSignal, ...], dict]] = []

    class FakePipeline:
        async def dispatch(self, event):
            dispatched.append(event)
            return (signal,)

    async def fake_execute(signals, **kwargs):
        executed.append((tuple(signals), kwargs))

    runner = object.__new__(runner_module.LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=strategy)
    runner.stats = SimpleNamespace(feature_events_seen=0)
    runner._heartbeat_service = None
    runner._market_feature_pipeline = FakePipeline()
    runner._execute_signals = fake_execute
    runner._maybe_log_live_data_path_stats = lambda: None
    event = _event()

    await runner.process_market_feature(event)

    assert dispatched == [event]
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
async def test_startup_preview_uses_same_pipeline() -> None:
    events = (_event(100), _event(200))
    calls: list[MarketFeatureEvent] = []

    class FakePipeline:
        async def dispatch(self, event):
            calls.append(event)
            return (_signal(str(event.event_time_ms)),)

    runner = object.__new__(runner_module.LiveRuntimeRunner)
    runner._market_feature_pipeline = FakePipeline()

    signals = await runner._preview_strategy_market_features(events)

    assert calls == list(events)
    assert tuple(signal.reason for signal in signals) == ("100", "200")
