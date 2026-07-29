from __future__ import annotations

from src.runtime import runner as runner_module
from src.runtime.components.market_events import MarketEventsComponent
from src.runtime.runner import LiveRuntimeRunner


def test_runner_has_no_dynamic_component_method_scanner() -> None:
    assert not hasattr(runner_module, "_compatibility_component_methods")
    assert not hasattr(runner_module, "_COMPATIBILITY_COMPONENT_METHODS")


def test_instance_compatibility_patch_does_not_mutate_component_class() -> None:
    first = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    second = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    first_component = MarketEventsComponent(first)
    second_component = MarketEventsComponent(second)
    object.__setattr__(
        first,
        "_runtime_components",
        {MarketEventsComponent: first_component},
    )
    object.__setattr__(
        second,
        "_runtime_components",
        {MarketEventsComponent: second_component},
    )
    original = MarketEventsComponent._process_market_event

    replacement = object()
    first._process_market_event = replacement

    assert first.__dict__["_process_market_event"] is replacement
    assert MarketEventsComponent._process_market_event is original
    assert second._process_market_event.__self__ is second
