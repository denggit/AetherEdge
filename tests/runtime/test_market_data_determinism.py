from __future__ import annotations

import asyncio
from contextlib import suppress
from decimal import Decimal

import pytest

from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_data.dispatcher import (
    BackpressurePolicy,
    BoundedOrderedEventDispatcher,
)
from src.runtime.market_data.catalog import build_market_data_registry
from src.runtime.market_data.runtime import (
    MarketDataRuntime,
    MarketDataRuntimeError,
)
from src.runtime.capabilities import MARKET_TRADES
from src.runtime.capabilities import FEATURE_FIXED_TIME_TRADE_BARS
from src.runtime.market_data import features as feature_module


def _trade(trade_id: str, event_time_ms: int) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
        trade_time_ms=event_time_ms,
        event_time_ms=event_time_ms,
    )


@pytest.mark.asyncio
async def test_complete_trade_pipeline_is_strictly_ordered_across_trades() -> None:
    calls: list[tuple[str, str]] = []
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=8,
        event_time_ms=lambda trade: trade.trade_time_ms,
    )

    def handler(name: str):
        async def consume(trade: MarketTrade) -> None:
            calls.append((trade.trade_id or "", name))

        return consume

    # Registration order is intentionally scrambled. Production ordering is
    # explicit and therefore cannot depend on module resolution or task timing.
    dispatcher.subscribe(subscriber_id="raw", handler=handler("raw"), order=500)
    dispatcher.subscribe(subscriber_id="range", handler=handler("range"), order=400)
    dispatcher.subscribe(subscriber_id="fixed", handler=handler("fixed"), order=200)
    dispatcher.subscribe(
        subscriber_id="range-footprint",
        handler=handler("range-footprint"),
        order=100,
    )
    dispatcher.subscribe(
        subscriber_id="trade-footprint",
        handler=handler("trade-footprint"),
        order=300,
    )
    await dispatcher.start()

    dispatcher.publish(_trade("one", 100))
    dispatcher.publish(_trade("two", 101))
    barrier = await dispatcher.drain_through(101, timeout_seconds=1)
    await dispatcher.stop()

    expected_per_trade = [
        "range-footprint",
        "fixed",
        "trade-footprint",
        "range",
        "raw",
    ]
    assert barrier.completed
    assert calls == [
        (trade_id, consumer)
        for trade_id in ("one", "two")
        for consumer in expected_per_trade
    ]


@pytest.mark.asyncio
async def test_closed_barrier_waits_for_full_raw_callback_completion() -> None:
    raw_started = asyncio.Event()
    release_raw = asyncio.Event()
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=4,
        event_time_ms=lambda trade: trade.trade_time_ms,
    )

    async def raw(_trade: MarketTrade) -> None:
        raw_started.set()
        await release_raw.wait()

    dispatcher.subscribe(subscriber_id="raw", handler=raw, order=500)
    await dispatcher.start()
    dispatcher.publish(_trade("one", 100))
    await asyncio.wait_for(raw_started.wait(), timeout=1)

    incomplete = await dispatcher.drain_through(100, timeout_seconds=0)
    assert not incomplete.completed
    assert incomplete.pending == 1

    release_raw.set()
    complete = await dispatcher.drain_through(100, timeout_seconds=1)
    await dispatcher.stop()
    assert complete.completed
    assert complete.pending == 0


@pytest.mark.parametrize(
    ("policy", "expected_dropped_id", "expected_delivered"),
    [
        (BackpressurePolicy.DROP_OLDEST, "one", 1),
        (BackpressurePolicy.DROP_NEWEST, "two", 0),
    ],
)
def test_ordered_overflow_reports_exact_event_and_count(
    policy: BackpressurePolicy,
    expected_dropped_id: str,
    expected_delivered: int,
) -> None:
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=1,
        policy=policy,
    )
    dispatcher.subscribe(subscriber_id="consumer", handler=lambda _event: None)

    first = dispatcher.publish(_trade("one", 100))
    second = dispatcher.publish(_trade("two", 101))

    assert first.dropped == 0
    assert second.dropped == 1
    assert second.delivered == expected_delivered
    assert second.dropped_events[0].trade_id == expected_dropped_id
    assert dispatcher.dropped_count == 1


def test_repeated_drop_oldest_overflow_accumulates_exactly() -> None:
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=1)
    results = [dispatcher.publish(_trade(str(index), index)) for index in range(5)]

    assert [result.dropped for result in results] == [0, 1, 1, 1, 1]
    assert [
        result.dropped_events[0].trade_id
        for result in results[1:]
    ] == ["0", "1", "2", "3"]
    assert dispatcher.dropped_count == 4


@pytest.mark.asyncio
async def test_shutdown_rejects_new_events_after_draining_received_backlog() -> None:
    seen: list[str] = []
    dispatcher = BoundedOrderedEventDispatcher[MarketTrade](maxsize=4)

    async def consume(trade: MarketTrade) -> None:
        seen.append(trade.trade_id or "")

    dispatcher.subscribe(subscriber_id="consumer", handler=consume)
    await dispatcher.start()
    dispatcher.publish(_trade("accepted", 100))
    await dispatcher.stop()

    rejected = dispatcher.publish(_trade("rejected", 101))
    assert seen == ["accepted"]
    assert rejected.delivered == 0
    assert rejected.dropped == 1
    assert rejected.dropped_events[0].trade_id == "rejected"


class _IdleBookStream:
    async def stream_order_book(self):
        await asyncio.Event().wait()
        if False:
            yield None


@pytest.mark.asyncio
async def test_trade_source_failure_is_supervised_by_market_data_runtime() -> None:
    class FailingTradeStream:
        async def stream_trades(self):
            raise RuntimeError("trade stream failed")
            if False:
                yield None

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=FailingTradeStream,
            create_order_book_stream=_IdleBookStream,
            publish_feature=lambda _event: None,
        )
    )
    await runtime.start({MARKET_TRADES})

    with pytest.raises(MarketDataRuntimeError, match="trade stream failed"):
        await asyncio.wait_for(runtime.wait_failed(), timeout=1)

    with suppress(RuntimeError):
        await runtime.stop()


@pytest.mark.asyncio
async def test_raw_subscriber_failure_is_supervised_by_market_data_runtime() -> None:
    hold = asyncio.Event()

    class OneTradeStream:
        async def stream_trades(self):
            yield _trade("one", 100)
            await hold.wait()

    async def broken_raw(_trade: MarketTrade) -> None:
        raise RuntimeError("raw callback failed")

    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=OneTradeStream,
            create_order_book_stream=_IdleBookStream,
            publish_feature=lambda _event: None,
            consume_trade=broken_raw,
        )
    )
    await runtime.start({MARKET_TRADES})

    with pytest.raises(MarketDataRuntimeError, match="raw callback failed"):
        await asyncio.wait_for(runtime.wait_failed(), timeout=1)

    with suppress(RuntimeError):
        await runtime.stop()


@pytest.mark.asyncio
async def test_feature_failure_is_supervised_and_visible_in_health(
    monkeypatch,
) -> None:
    hold = asyncio.Event()

    class OneTradeStream:
        async def stream_trades(self):
            yield _trade("one", 100)
            await hold.wait()

    class BrokenBuilder:
        def __init__(self, **_kwargs) -> None:
            pass

        def on_trade(self, _trade):
            raise RuntimeError("feature handler failed")

    monkeypatch.setattr(
        feature_module,
        "FixedTimeTradeBarBuilder",
        BrokenBuilder,
    )
    runtime = MarketDataRuntime(
        registry=build_market_data_registry(
            create_trade_stream=OneTradeStream,
            create_order_book_stream=_IdleBookStream,
            publish_feature=lambda _event: None,
        )
    )
    await runtime.start({FEATURE_FIXED_TIME_TRADE_BARS})

    with pytest.raises(MarketDataRuntimeError, match="feature handler failed"):
        await asyncio.wait_for(runtime.wait_failed(), timeout=1)

    health = runtime.state().health
    assert any(
        item.module_id == "fixed-time-trade-bars" and not item.healthy
        for item in health
    )
    with suppress(RuntimeError):
        await runtime.stop()
