from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.platform.data.models import MarketOrderBook, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.runtime.capabilities import FEATURE_RANGE_BARS, MARKET_ORDER_BOOK, MARKET_TRADES
from src.runtime.market_data.catalog import build_market_data_registry
from src.runtime.market_data.dispatcher import (
    BackpressurePolicy,
    BoundedEventDispatcher,
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
from src.runtime.market_data.runtime import MarketDataRuntime, MarketDataRuntimeError
from src.runtime.market_data.processor import MarketEventProcessor
from src.runtime.market_data.pipeline_plan import ResolvedMarketPipelinePlan
from src.runtime.module import ModuleHealth, ModuleState


def _trade(trade_id: str, time_ms: int, *, price: str = "100") -> MarketTrade:
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
async def test_trade_feature_windows_suppress_drop_then_recover_cleanly() -> None:
    integrity = TradeDataIntegrityTracker()
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
        publish=publish_fixed,
        integrity=integrity,
    )
    footprint = TradeFootprintModule(
        config=TradeFootprintModuleConfig(
            contract_value="1",
            price_bucket_size="1",
        ),
        publish=publish_footprint,
        integrity=integrity,
    )

    for trade in (_trade("d1", 60_001), _trade("d2", 60_002, price="101")):
        await fixed.process_trade(trade)
        await footprint.process_trade(trade)
    integrity.mark_dropped(60_003, "drop_oldest")
    boundary = _trade("clean-start", 120_001, price="102")
    await fixed.process_trade(boundary)
    await footprint.process_trade(boundary)

    assert fixed_events == footprint_events == []
    assert fixed.features_suppressed == footprint.features_suppressed == 1

    for trade in (
        _trade("clean-two", 120_002, price="103"),
        _trade("next", 180_001, price="104"),
    ):
        await fixed.process_trade(trade)
        await footprint.process_trade(trade)

    assert len(fixed_events) == len(footprint_events) == 1
    assert fixed.last_invalid_reason is footprint.last_invalid_reason is None


@pytest.mark.asyncio
async def test_range_footprint_discards_dirty_active_aggregation() -> None:
    integrity = TradeDataIntegrityTracker()
    events = []

    async def publish(event) -> None:
        events.append(event)

    module = RangeFootprintModule(
        config=RangeFootprintModuleConfig(
            contract_value="1",
            range_pct="0.002",
            price_step="1",
        ),
        publish=publish,
        integrity=integrity,
    )

    await module.process_trade(_trade("active", 60_001))
    integrity.mark_dropped(60_002, "drop_newest")
    await module.process_trade(_trade("restart", 60_003, price="101"))
    assert events == []
    await module.process_trade(_trade("clean-close", 60_004, price="102"))

    assert len(events) == 1
    assert module.features_suppressed == 1
    assert events[0].data["range_start_ms"] == 60_003


@pytest.mark.asyncio
async def test_order_book_dispatcher_backpressure_and_failure_are_explicit() -> None:
    dispatcher = BoundedEventDispatcher[MarketOrderBook]()
    dispatcher.subscribe(
        subscriber_id="slow",
        handler=lambda _event: None,
        maxsize=1,
        policy=BackpressurePolicy.DROP_NEWEST,
    )
    assert dispatcher.publish(_book(1)).dropped == 0
    rejected = dispatcher.publish(_book(2))
    assert rejected.delivered == 0
    assert rejected.dropped_events == (_book(2),)

    failed = asyncio.Event()
    broken = BoundedEventDispatcher[MarketOrderBook]()

    async def fail(_event: MarketOrderBook) -> None:
        failed.set()
        raise RuntimeError("book consumer failed")

    broken.subscribe(subscriber_id="broken", handler=fail, maxsize=2)
    await broken.start()
    broken.publish(_book(3))
    await failed.wait()
    with pytest.raises(RuntimeError, match="book consumer failed"):
        broken.raise_if_failed()
    await broken.stop()


@pytest.mark.asyncio
async def test_order_book_dispatcher_drain_timeout_is_explicit() -> None:
    blocker = asyncio.Event()
    dispatcher = BoundedEventDispatcher[MarketOrderBook](
        drain_timeout_seconds=0.01
    )
    dispatcher.subscribe(
        subscriber_id="blocked",
        handler=lambda _event: blocker.wait(),
        maxsize=2,
    )
    await dispatcher.start()
    dispatcher.publish(_book(1))
    with pytest.raises(DispatcherDrainTimeout):
        await dispatcher.stop()


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
    dispatcher.subscribe(
        subscriber_id="slow",
        handler=lambda _event: hold_consumer.wait(),
        maxsize=1,
    )

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
async def test_empty_market_runtime_creates_no_background_task() -> None:
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

    assert runtime.supervisor_task is None
    assert {task for task in asyncio.all_tasks() if task is not current} == before
    await runtime.stop()


@pytest.mark.asyncio
async def test_trade_disconnect_fails_runtime_and_degrades_integrity() -> None:
    integrity = TradeDataIntegrityTracker()

    class RangeProbe:
        module_id = "range-bars"
        provides = frozenset({FEATURE_RANGE_BARS})
        requires = frozenset({MARKET_TRADES})

        def __init__(self) -> None:
            self.degraded = []

        async def process_trade(self, _trade) -> None:
            return None

        async def prepare(self) -> None:
            return None

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def health(self):
            return ModuleHealth(self.module_id, ModuleState.RUNNING, True)

        def mark_trade_incomplete(self, event_ms, reason, revision) -> None:
            self.degraded.append((event_ms, reason, revision))

    class DisconnectingTradeStream:
        async def stream_trades(self):
            yield _trade("last-seen", 123)
            raise OSError("transient websocket disconnect")

    probe = RangeProbe()
    processor = MarketEventProcessor(trade_modules=[probe], integrity=integrity)
    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=DisconnectingTradeStream,
            create_order_book_stream=lambda: None,
            publish_feature=lambda _event: None,
            create_range_module=lambda: probe,
            trade_integrity=integrity,
            trade_processor=processor,
        ),
        event_processor=processor,
        pipeline_plan=ResolvedMarketPipelinePlan(
            True, False, False, ("range-bars",)
        ),
    )
    await runtime.start({FEATURE_RANGE_BARS})

    with pytest.raises(MarketDataRuntimeError, match="websocket disconnect"):
        await asyncio.wait_for(runtime.wait_failed(), timeout=1)
    assert "trade_source_disconnected" in (integrity.invalid_reason(123, 123) or "")
    assert probe.degraded[-1][1] == "trade_source_disconnected"
    await runtime.stop()


@pytest.mark.asyncio
async def test_active_trade_source_shutdown_does_not_mark_disconnect() -> None:
    integrity = TradeDataIntegrityTracker()
    processor = MarketEventProcessor(integrity=integrity)
    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=_IdleTradeStream,
            create_order_book_stream=lambda: None,
            publish_feature=lambda _event: None,
            trade_integrity=integrity,
            trade_processor=processor,
        ),
        event_processor=processor,
    )
    await runtime.start({MARKET_TRADES})
    await runtime.stop()
    assert integrity.dropped_count == 0
