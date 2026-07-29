from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.app import (
    AppConfig,
    AppContext,
    AsyncAlertDispatcher,
    NoopAlertSink,
)
from src.planner import ExecutionPlanner
from src.platform.data.models import (
    MarketFullOrderBook,
    MarketKline,
    MarketOpenInterest,
    MarketOrderBook,
    MarketOrderBookL2,
)
from src.platform.exchanges.models import ExchangeName
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime.components.latest_state_mailbox import (
    LatestStateMarketEventMailbox,
)


class _Data:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"


class _Strategy:
    def __init__(self) -> None:
        self.klines: list[MarketKline] = []
        self.order_books: list[MarketOrderBook] = []
        self.order_books_l2: list[MarketOrderBookL2] = []
        self.full_order_books: list[MarketFullOrderBook] = []
        self.open_interest: list[MarketOpenInterest] = []

    async def on_kline(self, event: MarketKline):
        self.klines.append(event)
        return []

    async def on_order_book(self, event: MarketOrderBook):
        self.order_books.append(event)
        return []

    async def on_order_book_l2(self, event: MarketOrderBookL2):
        self.order_books_l2.append(event)
        return []

    async def on_full_order_book(self, event: MarketFullOrderBook):
        self.full_order_books.append(event)
        return []

    async def on_open_interest(self, event: MarketOpenInterest):
        self.open_interest.append(event)
        return []


def _runner(
    strategy: _Strategy | None = None,
    *,
    market_queue_maxsize: int = 10,
) -> tuple[LiveRuntimeRunner, _Strategy]:
    strategy = strategy or _Strategy()
    app_config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="latest-state-test",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=market_queue_maxsize,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    context = AppContext(
        data=_Data(),
        execution=object(),
        state_store=object(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=app_config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
    )
    return runner, strategy


def _l2(
    sequence_id: int,
    *,
    raw_symbol: str = "ETH-USDT-SWAP",
) -> MarketOrderBookL2:
    return MarketOrderBookL2(
        exchange=ExchangeName.OKX,
        symbol=raw_symbol.replace("-SWAP", "-PERP"),
        raw_symbol=raw_symbol,
        bids=(),
        asks=(),
        event_time_ms=sequence_id,
        sequence_id=sequence_id,
        previous_sequence_id=sequence_id - 1,
    )


def _full(event_time_ms: int) -> MarketFullOrderBook:
    return MarketFullOrderBook(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        bids=(),
        asks=(),
        event_time_ms=event_time_ms,
        requested_depth=5000,
    )


def _oi(event_time_ms: int) -> MarketOpenInterest:
    return MarketOpenInterest(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        instrument_type="SWAP",
        open_interest_contracts=Decimal(event_time_ms),
        open_interest_base=None,
        open_interest_usd=None,
        event_time_ms=event_time_ms,
    )


def _kline(open_time_ms: int) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="1m",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=Decimal("1"),
    )


def _legacy_book(event_time_ms: int) -> MarketOrderBook:
    return MarketOrderBook(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        bids=(),
        asks=(),
        event_time_ms=event_time_ms,
    )


def test_same_key_keeps_only_latest_event_and_counts_replacements() -> None:
    mailbox = LatestStateMarketEventMailbox()

    replaced = [mailbox.publish(_l2(index)) for index in range(1, 101)]

    assert replaced == [False] + [True] * 99
    assert mailbox.qsize() == 1
    assert mailbox.coalesced_count == 99
    assert mailbox.get_nowait().sequence_id == 100
    assert mailbox.empty()


def test_event_types_and_symbols_have_independent_pending_keys() -> None:
    mailbox = LatestStateMarketEventMailbox()

    mailbox.publish(_l2(1))
    mailbox.publish(_full(2))
    mailbox.publish(_oi(3))
    mailbox.publish(_l2(4, raw_symbol="BTC-USDT-SWAP"))

    assert mailbox.qsize() == 4
    assert {
        type(mailbox.get_nowait())
        for _ in range(4)
    } == {
        MarketOrderBookL2,
        MarketFullOrderBook,
        MarketOpenInterest,
    }


def test_pending_key_queue_is_bounded() -> None:
    mailbox = LatestStateMarketEventMailbox(max_pending_keys=1)
    mailbox.publish(_l2(1))

    with pytest.raises(asyncio.QueueFull):
        mailbox.publish(_l2(2, raw_symbol="BTC-USDT-SWAP"))

    assert mailbox.qsize() == 1
    assert mailbox.get_nowait().raw_symbol == "ETH-USDT-SWAP"


@pytest.mark.asyncio
async def test_runtime_coalesces_all_three_latest_state_types_before_strategy() -> None:
    runner, strategy = _runner(market_queue_maxsize=1)

    for index in range(1, 101):
        await runner.enqueue_market_event(_l2(index))
        await runner.enqueue_market_event(_full(index))
        await runner.enqueue_market_event(_oi(index))

    assert runner._market_queue.empty()
    assert runner._latest_state_mailbox.qsize() == 3
    assert runner.stats.latest_state_events_coalesced == 297
    assert runner.stats.order_book_l2_coalesced == 99
    assert runner.stats.full_order_book_coalesced == 99
    assert runner.stats.open_interest_coalesced == 99
    assert runner.stats.market_events_dropped == 0

    await runner._consume_market_events(max_market_events=3)

    assert [event.sequence_id for event in strategy.order_books_l2] == [100]
    assert [
        event.event_time_ms for event in strategy.full_order_books
    ] == [100]
    assert [
        event.event_time_ms for event in strategy.open_interest
    ] == [100]


@pytest.mark.asyncio
async def test_normal_market_events_keep_fifo_and_legacy_books_queue_path() -> None:
    runner, strategy = _runner()
    for index in (1, 2, 3):
        await runner.enqueue_market_event(_kline(index))
    await runner.enqueue_market_event(_legacy_book(4))

    assert runner._market_queue.qsize() == 4
    assert runner._latest_state_mailbox.empty()

    await runner._consume_market_events(max_market_events=4)

    assert [event.open_time_ms for event in strategy.klines] == [1, 2, 3]
    assert [event.event_time_ms for event in strategy.order_books] == [4]


@pytest.mark.asyncio
async def test_continuous_normal_and_latest_state_events_do_not_starve() -> None:
    runner, _strategy = _runner()
    runner._market_queue_drain_batch_size = 1
    processed: list[str] = []
    normal_count = 0
    latest_count = 0

    async def process(event) -> None:
        nonlocal normal_count, latest_count
        runner.stats.market_events_seen += 1
        if isinstance(event, MarketKline):
            normal_count += 1
            processed.append("normal")
            if normal_count < 5:
                await runner.enqueue_market_event(_kline(normal_count + 1))
        else:
            latest_count += 1
            processed.append("latest")
            if latest_count < 5:
                await runner.enqueue_market_event(_l2(latest_count + 1))

    runner.process_market_event = process
    await runner.enqueue_market_event(_kline(1))
    await runner.enqueue_market_event(_l2(1))

    await runner._consume_market_events(max_market_events=10)

    assert normal_count == 5
    assert latest_count == 5
    assert processed[:2] == ["normal", "latest"]


@pytest.mark.asyncio
async def test_waiting_consumer_stops_without_pending_wait_tasks() -> None:
    runner, _strategy = _runner()
    consumer = asyncio.create_task(
        runner._consume_market_events(max_market_events=None)
    )
    await asyncio.sleep(0)

    runner._stop_event.set()
    runner._market_event_available.set()
    await asyncio.wait_for(consumer, timeout=1)

    assert consumer.done()
    assert not consumer.cancelled()
