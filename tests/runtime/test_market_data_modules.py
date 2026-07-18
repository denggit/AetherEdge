from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_data import (
    BackpressurePolicy,
    BoundedEventDispatcher,
    BoundedOrderedEventDispatcher,
    FixedTimeTradeBarModule,
    FixedTimeTradeBarModuleConfig,
    RangeFootprintModule,
    RangeFootprintModuleConfig,
    TradeFootprintModule,
    TradeFootprintModuleConfig,
    TradeStreamModule,
    MarketDataRuntime,
    build_market_data_registry,
)
from src.runtime.capabilities import (
    FEATURE_FIXED_TIME_TRADE_BARS,
    FEATURE_TRADE_FOOTPRINT,
    MARKET_ORDER_BOOK,
    MARKET_TRADES,
    FEATURE_RANGE_BARS,
)
from src.runtime.module import ModuleHealth
from src.runtime.market_data import features as feature_modules
from src.runtime.module import ModuleState


def _trade(trade_id: str) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
    )


class FakeTradeStream:
    def __init__(self, events) -> None:
        self.events = tuple(events)
        self.subscriptions = 0
        self.closed = 0
        self._hold = asyncio.Event()

    async def stream_trades(self):
        self.subscriptions += 1
        try:
            for event in self.events:
                yield event
            await self._hold.wait()
        finally:
            self.closed += 1


class FakeBuilder:
    def __init__(self, output) -> None:
        self.output = output
        self.calls = 0

    def on_trade(self, trade):
        self.calls += 1
        return (self.output,)


@pytest.mark.asyncio
async def test_one_trade_stream_fans_out_to_multiple_consumers() -> None:
    first_seen = asyncio.Event()
    second_seen = asyncio.Event()
    first = []
    second = []

    async def first_handler(event):
        first.append(event)
        first_seen.set()

    async def second_handler(event):
        second.append(event)
        second_seen.set()

    dispatcher = BoundedEventDispatcher[MarketTrade]()
    dispatcher.subscribe(
        subscriber_id="first",
        handler=first_handler,
        maxsize=4,
    )
    dispatcher.subscribe(
        subscriber_id="second",
        handler=second_handler,
        maxsize=4,
    )
    trade = _trade("one")
    stream = FakeTradeStream((trade,))
    module = TradeStreamModule(stream=stream, dispatcher=dispatcher)

    await module.prepare()
    await module.start()
    await asyncio.wait_for(
        asyncio.gather(first_seen.wait(), second_seen.wait()),
        timeout=1,
    )

    assert stream.subscriptions == 1
    assert first == [trade]
    assert second == [trade]
    assert dispatcher.task_count == 2
    assert module.health().background_tasks == 1

    await module.stop()

    assert stream.closed == 1
    assert dispatcher.task_count == 0
    assert module.health().state is ModuleState.STOPPED


@pytest.mark.asyncio
async def test_drop_oldest_backpressure_never_blocks_publisher() -> None:
    seen = []
    received = asyncio.Event()

    async def handler(event):
        seen.append(event)
        received.set()

    dispatcher = BoundedEventDispatcher[MarketTrade]()
    dispatcher.subscribe(
        subscriber_id="slow",
        handler=handler,
        maxsize=1,
        policy=BackpressurePolicy.DROP_OLDEST,
    )
    first = _trade("first")
    second = _trade("second")

    assert dispatcher.publish(first).dropped == 0
    assert dispatcher.publish(second).dropped == 1
    assert dispatcher.health()[0].dropped == 1

    await dispatcher.start()
    await asyncio.wait_for(received.wait(), timeout=1)
    await dispatcher.stop()

    assert seen == [second]


@pytest.mark.asyncio
async def test_drop_newest_reports_delivery_failure() -> None:
    dispatcher = BoundedEventDispatcher[MarketTrade]()
    dispatcher.subscribe(
        subscriber_id="slow",
        handler=lambda event: None,
        maxsize=1,
        policy=BackpressurePolicy.DROP_NEWEST,
    )

    first = dispatcher.publish(_trade("first"))
    second = dispatcher.publish(_trade("second"))

    assert first.delivered == 1 and first.dropped == 0
    assert second.delivered == 0 and second.dropped == 1
    assert dispatcher.task_count == 0


@pytest.mark.asyncio
async def test_unstarted_dispatcher_has_no_tasks_or_periodic_work() -> None:
    dispatcher = BoundedEventDispatcher[MarketTrade]()
    dispatcher.subscribe(
        subscriber_id="disabled",
        handler=lambda event: None,
        maxsize=1,
    )

    assert dispatcher.task_count == 0
    await dispatcher.stop()
    assert dispatcher.task_count == 0


@pytest.mark.asyncio
async def test_ordered_dispatcher_preserves_feature_order_off_receive_path() -> None:
    calls = []
    completed = asyncio.Event()

    async def first(event):
        calls.append("first")

    async def second(event):
        calls.append("second")
        completed.set()

    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=2)
    dispatcher.subscribe(subscriber_id="first", handler=first)
    dispatcher.subscribe(subscriber_id="second", handler=second)
    await dispatcher.start()

    result = dispatcher.publish(_trade("ordered"))
    await asyncio.wait_for(completed.wait(), timeout=1)
    await dispatcher.stop()

    assert result.delivered == 2
    assert calls == ["first", "second"]
    assert dispatcher.task_count == 0


@pytest.mark.asyncio
async def test_independent_trade_features_preserve_legacy_emit_order(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        feature_modules,
        "range_footprint_feature",
        lambda value, *, exchange: ("range", value),
    )
    monkeypatch.setattr(
        feature_modules,
        "fixed_time_trade_bar_feature",
        lambda value, **kwargs: ("fixed", value),
    )
    monkeypatch.setattr(
        feature_modules,
        "trade_footprint_feature",
        lambda value, *, exchange: ("footprint", value),
    )
    emitted = []
    completed = asyncio.Event()

    async def publish(event):
        emitted.append(event)
        if len(emitted) == 3:
            completed.set()

    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=8)
    range_builder = FakeBuilder("range-output")
    fixed_builder = FakeBuilder("fixed-output")
    footprint_builder = FakeBuilder("footprint-output")
    RangeFootprintModule(
        config=RangeFootprintModuleConfig(),
        dispatcher=dispatcher,
        publish=publish,
        builder=range_builder,
    )
    FixedTimeTradeBarModule(
        config=FixedTimeTradeBarModuleConfig(),
        dispatcher=dispatcher,
        publish=publish,
        builder=fixed_builder,
    )
    TradeFootprintModule(
        config=TradeFootprintModuleConfig(),
        dispatcher=dispatcher,
        publish=publish,
        builder=footprint_builder,
    )
    source = TradeStreamModule(
        stream=FakeTradeStream((_trade("one"),)),
        dispatcher=dispatcher,
    )

    await source.prepare()
    await source.start()
    await asyncio.wait_for(completed.wait(), timeout=1)
    await source.stop()

    assert emitted == [
        ("range", "range-output"),
        ("fixed", "fixed-output"),
        ("footprint", "footprint-output"),
    ]
    assert range_builder.calls == fixed_builder.calls == footprint_builder.calls == 1


def test_unrequested_feature_module_has_no_subscription_or_builder() -> None:
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=8)
    builder = FakeBuilder("fixed")

    FixedTimeTradeBarModule(
        config=FixedTimeTradeBarModuleConfig(),
        dispatcher=dispatcher,
        publish=lambda event: None,
        builder=builder,
    )

    assert dispatcher.subscriber_ids == ("fixed-time-trade-bars",)
    assert builder.calls == 0
    assert dispatcher.task_count == 0


class FakeOrderBookStream:
    def __init__(self) -> None:
        self.subscriptions = 0
        self.closed = 0
        self._hold = asyncio.Event()

    async def stream_order_book(self):
        self.subscriptions += 1
        try:
            await self._hold.wait()
            if False:  # pragma: no cover - keep this an async generator
                yield None
        finally:
            self.closed += 1


@pytest.mark.asyncio
async def test_market_data_runtime_empty_plan_constructs_nothing() -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory():
        created["trades"] += 1
        return FakeTradeStream(())

    def book_factory():
        created["books"] += 1
        return FakeOrderBookStream()

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=trade_factory,
            create_order_book_stream=book_factory,
            publish_feature=lambda event: None,
        )
    )

    plan = await runtime.start(())

    assert plan.module_ids == ()
    assert created == {"trades": 0, "books": 0}
    assert runtime.state().health == ()
    await runtime.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requested", "expected_modules", "expected_created"),
    [
        (
            {MARKET_TRADES},
            ("trade-stream",),
            {"trades": 1, "books": 0},
        ),
        (
            {MARKET_ORDER_BOOK},
            ("order-book-stream",),
            {"trades": 0, "books": 1},
        ),
    ],
)
async def test_market_data_runtime_starts_only_requested_source(
    requested,
    expected_modules,
    expected_created,
) -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory():
        created["trades"] += 1
        return FakeTradeStream(())

    def book_factory():
        created["books"] += 1
        return FakeOrderBookStream()

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=trade_factory,
            create_order_book_stream=book_factory,
            publish_feature=lambda event: None,
        )
    )

    plan = await runtime.start(requested)

    assert plan.module_ids == expected_modules
    assert created == expected_created
    assert runtime.state().started_module_ids == expected_modules
    await runtime.stop()


@pytest.mark.asyncio
async def test_two_features_share_one_lazy_trade_source() -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory():
        created["trades"] += 1
        return FakeTradeStream(())

    def book_factory():
        created["books"] += 1
        return FakeOrderBookStream()

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=trade_factory,
            create_order_book_stream=book_factory,
            publish_feature=lambda event: None,
        )
    )

    plan = await runtime.start(
        {FEATURE_FIXED_TIME_TRADE_BARS, FEATURE_TRADE_FOOTPRINT}
    )

    assert plan.module_ids == (
        "trade-stream",
        "fixed-time-trade-bars",
        "trade-footprint",
    )
    assert plan.shared_capabilities == frozenset({MARKET_TRADES})
    assert created == {"trades": 1, "books": 0}
    await runtime.stop()


@pytest.mark.asyncio
async def test_range_only_plan_starts_trade_and_range_only() -> None:
    created = {"trades": 0, "books": 0, "range": 0}

    def trade_factory():
        created["trades"] += 1
        return FakeTradeStream(())

    def book_factory():
        created["books"] += 1
        return FakeOrderBookStream()

    class FakeRangeModule:
        module_id = "range-bars"
        provides = frozenset({FEATURE_RANGE_BARS})
        requires = frozenset({MARKET_TRADES})

        def __init__(self) -> None:
            created["range"] += 1
            self.state = ModuleState.CREATED

        async def prepare(self):
            self.state = ModuleState.PREPARED

        async def start(self):
            self.state = ModuleState.RUNNING

        async def stop(self):
            self.state = ModuleState.STOPPED

        def health(self):
            return ModuleHealth(
                module_id=self.module_id,
                state=self.state,
            )

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=trade_factory,
            create_order_book_stream=book_factory,
            publish_feature=lambda event: None,
            create_range_module=lambda dispatcher: FakeRangeModule(),
        )
    )

    plan = await runtime.start({FEATURE_RANGE_BARS})

    assert plan.module_ids == ("trade-stream", "range-bars")
    assert created == {"trades": 1, "books": 0, "range": 1}
    await runtime.stop()
