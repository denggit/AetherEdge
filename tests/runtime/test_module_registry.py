from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from src.runtime.module import (
    CapabilityId,
    ModuleHealth,
    ModuleHost,
    ModuleState,
)
from src.runtime.registry import (
    CapabilityResolutionError,
    DependencyResolver,
    ModuleDefinition,
    ModuleRegistry,
)
from src.runtime.market_data.runtime import MarketDataRuntime


TRADES = CapabilityId("market.trades")
BOOKS = CapabilityId("market.order_book")
RANGE = CapabilityId("feature.range_bars")
FOOTPRINT = CapabilityId("feature.trade_footprint")


@dataclass
class FakeModule:
    module_id: str
    provides: frozenset[CapabilityId]
    requires: frozenset[CapabilityId]
    calls: list[str]
    state: ModuleState = ModuleState.CREATED

    async def prepare(self) -> None:
        self.calls.append(f"prepare:{self.module_id}")
        self.state = ModuleState.PREPARED

    async def start(self) -> None:
        self.calls.append(f"start:{self.module_id}")
        self.state = ModuleState.RUNNING

    async def stop(self) -> None:
        self.calls.append(f"stop:{self.module_id}")
        self.state = ModuleState.STOPPED

    def health(self) -> ModuleHealth:
        return ModuleHealth(module_id=self.module_id, state=self.state)


@dataclass
class FactoryProbe:
    calls: list[str] = field(default_factory=list)
    instances: list[FakeModule] = field(default_factory=list)

    def factory(
        self,
        module_id: str,
        provides: frozenset[CapabilityId],
        requires: frozenset[CapabilityId] = frozenset(),
    ):
        def create() -> FakeModule:
            self.calls.append(module_id)
            module = FakeModule(
                module_id=module_id,
                provides=provides,
                requires=requires,
                calls=[],
            )
            self.instances.append(module)
            return module

        return create


def _registry(probe: FactoryProbe) -> ModuleRegistry:
    registry = ModuleRegistry()
    registry.register(
        ModuleDefinition(
            module_id="trade-stream",
            provides=frozenset({TRADES}),
            requires=frozenset(),
            factory=probe.factory("trade-stream", frozenset({TRADES})),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="order-book-stream",
            provides=frozenset({BOOKS}),
            requires=frozenset(),
            factory=probe.factory("order-book-stream", frozenset({BOOKS})),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="range-bars",
            provides=frozenset({RANGE}),
            requires=frozenset({TRADES}),
            factory=probe.factory(
                "range-bars", frozenset({RANGE}), frozenset({TRADES})
            ),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="trade-footprint",
            provides=frozenset({FOOTPRINT}),
            requires=frozenset({TRADES}),
            factory=probe.factory(
                "trade-footprint",
                frozenset({FOOTPRINT}),
                frozenset({TRADES}),
            ),
        )
    )
    return registry


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ((), ()),
        ((TRADES,), ("trade-stream",)),
        ((BOOKS,), ("order-book-stream",)),
        ((RANGE,), ("trade-stream", "range-bars")),
    ],
)
def test_dependency_matrix_is_minimal_and_does_not_instantiate(
    requested,
    expected,
) -> None:
    probe = FactoryProbe()
    registry = _registry(probe)

    plan = DependencyResolver(registry).resolve(requested)

    assert plan.module_ids == expected
    assert probe.calls == []


def test_multiple_features_share_one_trade_source_instance() -> None:
    probe = FactoryProbe()
    registry = _registry(probe)
    plan = DependencyResolver(registry).resolve((RANGE, FOOTPRINT))

    modules = registry.instantiate(plan)

    assert plan.module_ids == (
        "trade-stream",
        "range-bars",
        "trade-footprint",
    )
    assert plan.shared_capabilities == frozenset({TRADES})
    assert probe.calls.count("trade-stream") == 1
    assert len(modules) == 3


def test_disabled_modules_are_never_constructed() -> None:
    probe = FactoryProbe()
    registry = _registry(probe)

    modules = registry.instantiate(DependencyResolver(registry).resolve(()))

    assert modules == ()
    assert probe.calls == []


@pytest.mark.asyncio
async def test_module_host_starts_in_plan_order_and_stops_in_reverse() -> None:
    calls: list[str] = []
    source = FakeModule(
        "trade-stream", frozenset({TRADES}), frozenset(), calls
    )
    feature = FakeModule(
        "range-bars", frozenset({RANGE}), frozenset({TRADES}), calls
    )
    host = ModuleHost((source, feature))

    await host.start()
    await host.stop()

    assert calls == [
        "prepare:trade-stream",
        "prepare:range-bars",
        "start:trade-stream",
        "start:range-bars",
        "stop:range-bars",
        "stop:trade-stream",
    ]


@pytest.mark.asyncio
async def test_market_source_shutdown_priority_drains_before_feature_stop() -> None:
    calls: list[str] = []
    source = FakeModule(
        "trade-stream", frozenset({TRADES}), frozenset(), calls
    )
    source.shutdown_priority = 100
    feature = FakeModule(
        "range-bars", frozenset({RANGE}), frozenset({TRADES}), calls
    )
    host = ModuleHost((source, feature))

    await host.start()
    await host.stop()

    assert calls[-2:] == ["stop:trade-stream", "stop:range-bars"]


@pytest.mark.asyncio
async def test_prepare_failure_rolls_back_partially_prepared_module() -> None:
    calls: list[str] = []

    class PartialFailure(FakeModule):
        async def prepare(self) -> None:
            self.calls.append(f"prepare:{self.module_id}")
            raise RuntimeError("prepare failed after allocating")

    first = FakeModule("first", frozenset({TRADES}), frozenset(), calls)
    failing = PartialFailure(
        "failing",
        frozenset({RANGE}),
        frozenset({TRADES}),
        calls,
    )
    host = ModuleHost((first, failing))

    with pytest.raises(RuntimeError, match="module startup failed"):
        await host.prepare()

    assert calls == [
        "prepare:first",
        "prepare:failing",
        "stop:failing",
        "stop:first",
    ]


def test_duplicate_capability_provider_is_rejected() -> None:
    probe = FactoryProbe()
    registry = _registry(probe)

    with pytest.raises(CapabilityResolutionError, match="duplicate"):
        registry.register(
            ModuleDefinition(
                module_id="second-trade-stream",
                provides=frozenset({TRADES}),
                requires=frozenset(),
                factory=probe.factory(
                    "second-trade-stream", frozenset({TRADES})
                ),
            )
        )


@pytest.mark.asyncio
async def test_market_runtime_starts_consumers_then_processor_then_sources() -> None:
    calls: list[str] = []
    probe = FactoryProbe()
    registry = _registry(probe)
    for module_id, definition in list(registry._definitions.items()):
        registry._definitions[module_id] = ModuleDefinition(
            module_id=definition.module_id,
            provides=definition.provides,
            requires=definition.requires,
            factory=lambda value=definition: FakeModule(
                value.module_id, value.provides, value.requires, calls
            ),
        )

    class Processor:
        def set_trade_modules(self, modules):
            self.modules = modules

        async def start(self):
            calls.append("start:processor")

        def stop_accepting_controls(self):
            calls.append("stop-controls:processor")

        def stop_accepting(self):
            calls.append("stop-accepting:processor")

        async def stop(self):
            calls.append("stop:processor")

        def raise_if_failed(self):
            return None

    runtime = MarketDataRuntime(registry=registry, event_processor=Processor())
    await runtime.start((RANGE, BOOKS))
    assert calls.index("start:range-bars") < calls.index("start:processor")
    assert calls.index("start:processor") < calls.index("start:trade-stream")
    assert calls.index("start:trade-stream") < calls.index("start:order-book-stream")

    await runtime.stop()
    assert calls.index("stop-controls:processor") < calls.index("stop:trade-stream")
    assert calls.index("stop:trade-stream") < calls.index("stop-accepting:processor")
    assert calls.index("stop-accepting:processor") < calls.index("stop:processor")
    assert calls.index("stop:processor") < calls.index("stop:range-bars")


@pytest.mark.asyncio
async def test_consumer_start_failure_never_starts_processor_or_trade_source() -> None:
    calls: list[str] = []

    class FailingConsumer(FakeModule):
        async def start(self) -> None:
            self.calls.append(f"start:{self.module_id}")
            raise RuntimeError("consumer start failed")

    registry = ModuleRegistry()
    registry.register(
        ModuleDefinition(
            module_id="trade-stream",
            provides=frozenset({TRADES}),
            requires=frozenset(),
            factory=lambda: FakeModule(
                "trade-stream", frozenset({TRADES}), frozenset(), calls
            ),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="range-bars",
            provides=frozenset({RANGE}),
            requires=frozenset({TRADES}),
            factory=lambda: FailingConsumer(
                "range-bars",
                frozenset({RANGE}),
                frozenset({TRADES}),
                calls,
            ),
        )
    )

    class Processor:
        def set_trade_modules(self, modules):
            return None

        async def start(self):
            calls.append("start:processor")

        def stop_accepting(self):
            return None

        async def stop(self):
            calls.append("stop:processor")

        def raise_if_failed(self):
            return None

    runtime = MarketDataRuntime(registry=registry, event_processor=Processor())

    with pytest.raises(RuntimeError, match="consumer start failed"):
        await runtime.start((RANGE,))

    assert "start:processor" not in calls
    assert "start:trade-stream" not in calls
