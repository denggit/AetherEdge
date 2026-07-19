from __future__ import annotations

import asyncio
from contextlib import suppress
from decimal import Decimal

import pytest

from src.platform.data.models import (
    MarketOrderBook,
    MarketTrade,
    TradeSide,
)
from src.platform.exchanges.models import ExchangeName
from src.runtime.capabilities import MARKET_ORDER_BOOK
from src.runtime.market_data.catalog import build_market_data_registry
from src.runtime.market_data.dispatcher import (
    BackpressurePolicy,
    BoundedEventDispatcher,
    BoundedOrderedEventDispatcher,
    DispatcherDrainTimeout,
)
from src.runtime.market_data.features import (
    FixedTimeTradeBarModule,
    FixedTimeTradeBarModuleConfig,
    RangeFootprintModule,
    RangeFootprintModuleConfig,
    TradeFootprintModule,
    TradeFootprintModuleConfig,
)
from src.runtime.market_data.integrity import (
    OrderBookDataIntegrityTracker,
    TradeDataIntegrityTracker,
)
from src.runtime.market_data.runtime import (
    MarketDataRuntime,
    MarketDataRuntimeError,
)
from src.runtime.market_data.sources import TradeStreamModule


def _trade(
    trade_id: str,
    time_ms: int,
    *,
    price: str = "100",
) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
        trade_time_ms=time_ms,
        event_time_ms=time_ms,
    )


def _book(time_ms: int) -> MarketOrderBook:
    return MarketOrderBook(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        bids=(),
        asks=(),
        event_time_ms=time_ms,
    )


@pytest.mark.asyncio
async def test_barrier_waits_for_trade_published_after_barrier_started() -> None:
    release = asyncio.Event()
    seen: list[str] = []
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=8,
        event_time_ms=lambda trade: trade.trade_time_ms,
    )

    async def consume(trade: MarketTrade) -> None:
        await release.wait()
        seen.append(trade.trade_id or "")

    dispatcher.subscribe(subscriber_id="consumer", handler=consume)
    await dispatcher.start()
    dispatcher.publish(_trade("initial", 100))
    barrier = asyncio.create_task(
        dispatcher.drain_through(100, timeout_seconds=1)
    )
    loop = asyncio.get_running_loop()
    loop.call_soon(dispatcher.publish, _trade("late", 99))
    loop.call_soon(release.set)

    result = await asyncio.wait_for(barrier, timeout=1)
    await dispatcher.stop()

    assert result.completed
    assert seen == ["initial", "late"]


@pytest.mark.asyncio
async def test_future_trade_does_not_block_current_cutoff_barrier() -> None:
    future_started = asyncio.Event()
    release_future = asyncio.Event()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=8,
        event_time_ms=lambda trade: trade.trade_time_ms,
    )

    async def consume(trade: MarketTrade) -> None:
        if trade.trade_id == "future":
            future_started.set()
            await release_future.wait()

    dispatcher.subscribe(subscriber_id="consumer", handler=consume)
    await dispatcher.start()
    dispatcher.publish(_trade("cutoff", 100))
    dispatcher.publish(_trade("future", 101))
    barrier = asyncio.create_task(
        dispatcher.drain_through(100, timeout_seconds=1)
    )

    await asyncio.wait_for(future_started.wait(), timeout=1)
    result = await asyncio.wait_for(barrier, timeout=1)
    assert result.completed
    release_future.set()
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_failed_dispatcher_never_returns_success_for_empty_barrier() -> None:
    failed = asyncio.Event()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=4)

    async def broken(_trade: MarketTrade) -> None:
        failed.set()
        raise RuntimeError("ordered failure")

    dispatcher.subscribe(subscriber_id="broken", handler=broken)
    await dispatcher.start()
    dispatcher.publish(_trade("one", 100))
    await asyncio.wait_for(failed.wait(), timeout=1)
    with pytest.raises(RuntimeError, match="ordered failure"):
        await dispatcher.drain_through(100, timeout_seconds=1)
    with suppress(RuntimeError):
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_dispatcher_failure_while_barrier_waits_is_propagated() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=4,
        event_time_ms=lambda trade: trade.trade_time_ms,
    )

    async def broken(_trade: MarketTrade) -> None:
        started.set()
        await release.wait()
        raise RuntimeError("failure during barrier")

    dispatcher.subscribe(subscriber_id="broken", handler=broken)
    await dispatcher.start()
    dispatcher.publish(_trade("one", 100))
    await asyncio.wait_for(started.wait(), timeout=1)
    barrier = asyncio.create_task(
        dispatcher.drain_through(100, timeout_seconds=1)
    )
    release.set()
    with pytest.raises(RuntimeError, match="failure during barrier"):
        await barrier
    with suppress(RuntimeError):
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_trade_feature_windows_suppress_drop_then_recover_cleanly() -> None:
    integrity = TradeDataIntegrityTracker()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=8)
    fixed_events = []
    footprint_events = []

    async def publish_fixed(event) -> None:
        fixed_events.append(event)

    async def publish_footprint(event) -> None:
        footprint_events.append(event)

    fixed = FixedTimeTradeBarModule(
        config=FixedTimeTradeBarModuleConfig(
            contract_value="1",
            large_trade_threshold_notional="1",
        ),
        dispatcher=dispatcher,
        publish=publish_fixed,
        integrity=integrity,
    )
    footprint = TradeFootprintModule(
        config=TradeFootprintModuleConfig(
            contract_value="1",
            price_bucket_size="1",
        ),
        dispatcher=dispatcher,
        publish=publish_footprint,
        integrity=integrity,
    )

    dirty = (_trade("d1", 60_001), _trade("d2", 60_002, price="101"))
    for trade in dirty:
        await fixed.process_trade(trade)
        await footprint.process_trade(trade)
    integrity.mark_dropped(60_003, "drop_oldest")
    boundary = _trade("clean-start", 120_001, price="102")
    await fixed.process_trade(boundary)
    await footprint.process_trade(boundary)

    assert fixed_events == []
    assert footprint_events == []
    assert fixed.features_suppressed == 1
    assert footprint.features_suppressed == 1

    clean = _trade("clean-two", 120_002, price="103")
    next_boundary = _trade("next", 180_001, price="104")
    for trade in (clean, next_boundary):
        await fixed.process_trade(trade)
        await footprint.process_trade(trade)

    assert len(fixed_events) == 1
    assert len(footprint_events) == 1
    assert fixed.last_invalid_reason is None
    assert footprint.last_invalid_reason is None


@pytest.mark.asyncio
async def test_range_footprint_discards_dirty_active_aggregation() -> None:
    integrity = TradeDataIntegrityTracker()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=8)
    events = []

    async def publish(event) -> None:
        events.append(event)

    module = RangeFootprintModule(
        config=RangeFootprintModuleConfig(
            contract_value="1",
            range_pct="0.002",
            price_step="1",
        ),
        dispatcher=dispatcher,
        publish=publish,
        integrity=integrity,
    )

    await module.process_trade(_trade("active", 60_001, price="100"))
    integrity.mark_dropped(60_002, "drop_newest")
    await module.process_trade(_trade("restart", 60_003, price="101"))
    assert events == []
    await module.process_trade(_trade("clean-close", 60_004, price="102"))

    assert len(events) == 1
    assert module.features_suppressed == 1
    assert events[0].data["range_start_ms"] == 60_003


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "invalid_time"),
    [
        (BackpressurePolicy.DROP_OLDEST, 101),
        (BackpressurePolicy.DROP_NEWEST, 102),
    ],
)
async def test_trade_source_overflow_marks_integrity_for_both_policies(
    policy: BackpressurePolicy,
    invalid_time: int,
) -> None:
    release = asyncio.Event()
    first_started = asyncio.Event()
    integrity = TradeDataIntegrityTracker()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=1,
        policy=policy,
    )

    async def slow(_trade: MarketTrade) -> None:
        first_started.set()
        await release.wait()

    dispatcher.subscribe(subscriber_id="slow", handler=slow)

    class BurstStream:
        async def stream_trades(self):
            yield _trade("first", 100)
            await first_started.wait()
            yield _trade("second", 101)
            yield _trade("third", 102)
            await asyncio.Event().wait()

    module = TradeStreamModule(
        stream=BurstStream(),
        dispatcher=dispatcher,
        integrity=integrity,
    )
    await module.prepare()
    await module.start()
    while integrity.dropped_count == 0:
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await asyncio.get_running_loop().run_in_executor(None, lambda: None)

    assert not integrity.is_complete(invalid_time, invalid_time)
    release.set()
    await module.stop()


@pytest.mark.asyncio
async def test_unordered_subscriber_failure_and_drain_timeout_are_explicit() -> None:
    failed = asyncio.Event()
    dispatcher = BoundedEventDispatcher[MarketOrderBook](
        drain_timeout_seconds=0.01
    )

    async def broken(_event: MarketOrderBook) -> None:
        failed.set()
        raise RuntimeError("book consumer failed")

    dispatcher.subscribe(subscriber_id="broken", handler=broken, maxsize=2)
    await dispatcher.start()
    dispatcher.publish(_book(1))
    await asyncio.wait_for(failed.wait(), timeout=1)
    with pytest.raises(RuntimeError, match="book consumer failed"):
        dispatcher.raise_if_failed()
    rejected = dispatcher.publish(_book(2))
    assert rejected.dropped == 1
    await dispatcher.stop()

    blocked = BoundedEventDispatcher[MarketOrderBook](
        drain_timeout_seconds=0.01
    )
    blocker = asyncio.Event()
    blocked.subscribe(
        subscriber_id="blocked",
        handler=lambda _event: blocker.wait(),
        maxsize=2,
    )
    await blocked.start()
    blocked.publish(_book(3))
    with pytest.raises(DispatcherDrainTimeout):
        await blocked.stop()


class _IdleTradeStream:
    async def stream_trades(self):
        await asyncio.Event().wait()
        if False:
            yield None


@pytest.mark.asyncio
async def test_order_book_consumer_failure_reaches_market_runtime() -> None:
    hold = asyncio.Event()

    class OneBookStream:
        async def stream_order_book(self):
            yield _book(1)
            await hold.wait()

    async def broken(_event: MarketOrderBook) -> None:
        raise RuntimeError("injected order book consumer failure")

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=_IdleTradeStream,
            create_order_book_stream=OneBookStream,
            publish_feature=lambda _event: None,
            consume_order_book=broken,
        )
    )
    await runtime.start({MARKET_ORDER_BOOK})

    with pytest.raises(
        MarketDataRuntimeError,
        match="injected order book consumer failure",
    ):
        await asyncio.wait_for(runtime.wait_failed(), timeout=1)
    await runtime.stop()


@pytest.mark.asyncio
async def test_order_book_overflow_marks_resync_and_fails_runtime() -> None:
    hold_consumer = asyncio.Event()
    integrity = OrderBookDataIntegrityTracker()
    dispatcher = BoundedEventDispatcher[MarketOrderBook]()

    async def slow(_event: MarketOrderBook) -> None:
        await hold_consumer.wait()

    dispatcher.subscribe(subscriber_id="slow", handler=slow, maxsize=1)

    class BurstBookStream:
        async def stream_order_book(self):
            for time_ms in (1, 2, 3):
                yield _book(time_ms)
            await asyncio.Event().wait()

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=_IdleTradeStream,
            create_order_book_stream=BurstBookStream,
            publish_feature=lambda _event: None,
            order_book_dispatcher=dispatcher,
            order_book_integrity=integrity,
        )
    )
    await runtime.start({MARKET_ORDER_BOOK})

    with pytest.raises(MarketDataRuntimeError, match="resync required"):
        await asyncio.wait_for(runtime.wait_failed(), timeout=1)
    assert integrity.snapshot().resync_required
    hold_consumer.set()
    await runtime.stop()


@pytest.mark.asyncio
async def test_empty_market_runtime_creates_no_supervisor_or_pending_task() -> None:
    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=_IdleTradeStream,
            create_order_book_stream=lambda: None,
            publish_feature=lambda _event: None,
        )
    )
    current = asyncio.current_task()
    before = {task for task in asyncio.all_tasks() if task is not current}
    await runtime.start(())
    after = {task for task in asyncio.all_tasks() if task is not current}

    assert runtime.supervisor_task is None
    assert after == before
    await runtime.stop()
    assert {
        task for task in asyncio.all_tasks() if task is not current
    } == before
