from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import RangeBar
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_data import (
    BoundedOrderedEventDispatcher,
    RangeBarModule,
    RangeBarModuleConfig,
)
from src.runtime.module import ModuleState


class FakeBuilder:
    def __init__(self, bars=()) -> None:
        self.bars = tuple(bars)
        self.calls = 0
        self.discards = 0

    def on_trade(self, trade):
        self.calls += 1
        return self.bars

    def snapshot_state(self):
        return {"active": None}

    def discard_active_bar(self):
        self.discards += 1


class FakeBarStore:
    def __init__(self, rows=()) -> None:
        self.rows = list(rows)
        self.loads = 0

    def load(self, **kwargs):
        self.loads += 1
        return list(self.rows)


class FakeCheckpointStore:
    pass


class FakeCheckpointWriter:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.submitted = []

    def start(self):
        self.started += 1

    def stop(self, *, flush):
        self.stopped += 1

    def submit(self, checkpoint):
        self.submitted.append(checkpoint)
        return True


class FakePersistence:
    def __init__(self) -> None:
        self.bars = []
        self.aggregates = []

    def persist_range_bar(self, bar, **kwargs):
        self.bars.append(bar)
        return True

    def persist_completed_range_aggregate(self, aggregate, **kwargs):
        self.aggregates.append(aggregate)
        return True


def _bar() -> RangeBar:
    return RangeBar(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bar_id=1,
        start_time_ms=1,
        end_time_ms=2,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=Decimal("1"),
        buy_notional=Decimal("100"),
        sell_notional=Decimal("0"),
        trade_count=1,
    )


def _trade() -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("101"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id="trade-1",
        trade_time_ms=2,
    )


def _module(*, builder, persistence, emitted, publish=None):
    return RangeBarModule(
        config=RangeBarModuleConfig(
            symbol="ETH-USDT-PERP",
            exchange=ExchangeName.OKX,
            range_pct=Decimal("0.002"),
            contract_value=Decimal("0.01"),
            bucket_interval_ms=100,
            aggregate_interval="100ms",
            checkpoint_interval_ms=10_000,
        ),
        dispatcher=BoundedOrderedEventDispatcher(maxsize=4),
        publish=publish or (lambda event: _append(emitted, event)),
        persistence=persistence,
        builder=builder,
        bar_store=FakeBarStore(),
        checkpoint_store=FakeCheckpointStore(),
        checkpoint_writer=FakeCheckpointWriter(),
        clock_ms=lambda: 1,
    )


async def _append(target, value):
    target.append(value)


@pytest.mark.asyncio
async def test_range_module_owns_builder_persistence_and_shutdown() -> None:
    emitted = []
    persistence = FakePersistence()
    builder = FakeBuilder((_bar(),))
    module = _module(
        builder=builder,
        persistence=persistence,
        emitted=emitted,
    )

    await module.prepare()
    await module.start()
    await module.process_trade(_trade())

    assert builder.calls == 1
    assert persistence.bars == [_bar()]
    assert emitted[0].type_value == "range_bar_closed"
    assert module.health().state is ModuleState.RUNNING
    assert module.health().metadata[0] == ("bars_closed", "1")

    events = await module.emit_aggregate_for_bucket(0)
    assert len(events) == 1
    assert persistence.aggregates[0].bar_count == 1

    await module.stop()
    assert module.health().state is ModuleState.STOPPED
    assert module.checkpoint_writer.stopped == 1


def test_range_module_has_no_resources_before_instantiation() -> None:
    dispatcher = BoundedOrderedEventDispatcher(maxsize=4)

    assert dispatcher.subscriber_ids == ()
    assert dispatcher.task_count == 0


@pytest.mark.asyncio
async def test_range_aggregate_publish_failure_is_retryable_and_commits_once() -> None:
    emitted = []
    attempts = 0

    async def publish(event):
        nonlocal attempts
        if event.type_value == "range_bar_closed":
            return
        attempts += 1
        if attempts == 1:
            raise RuntimeError("first publish failed")
        emitted.append(event)

    module = _module(
        builder=FakeBuilder((_bar(),)),
        persistence=FakePersistence(),
        emitted=emitted,
        publish=publish,
    )
    await module.prepare()
    await module.process_trade(_trade())

    with pytest.raises(RuntimeError, match="first publish failed"):
        await module.emit_aggregate_for_bucket(0)

    retried = await module.emit_aggregate_for_bucket(0)
    duplicate = await module.emit_aggregate_for_bucket(0)

    assert attempts == 2
    assert len(retried) == 1
    assert duplicate == []
    assert emitted == retried
