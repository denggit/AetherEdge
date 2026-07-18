from __future__ import annotations

import pytest

from src.runtime import runner as runner_module
from src.runtime.components.market_events import MarketEventsComponent
from src.runtime.runner import LiveRuntimeRunner


def test_component_method_conflicts_are_rejected(monkeypatch) -> None:
    class First:
        def collide(self):
            return None

    class Second:
        def collide(self):
            return None

    monkeypatch.setattr(runner_module, "COMPONENT_TYPES", (First, Second))

    with pytest.raises(RuntimeError, match="component method conflict"):
        runner_module._compatibility_component_methods()


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
    original = MarketEventsComponent._process_trade

    replacement = object()
    first._process_trade = replacement

    assert first.__dict__["_process_trade"] is replacement
    assert MarketEventsComponent._process_trade is original
    assert second._process_trade.__self__ is second_component
