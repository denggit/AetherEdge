from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import RangeBar, RangeCoverageStatus
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_data import (
    RangeBarModule,
    RangeBarModuleConfig,
)
from src.runtime.market_data.integrity import TradeDataIntegrityTracker
from src.runtime.market_data.range_integrity import RangeBucketIntegrityStatus
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
    def save_bucket_integrity(self, _state) -> None:
        return None


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
        publish=publish or (lambda event: _append(emitted, event)),
        persistence=persistence,
        builder=builder,
        bar_store=FakeBarStore(),
        checkpoint_store=FakeCheckpointStore(),
        checkpoint_writer=FakeCheckpointWriter(),
        clock_ms=lambda: 1,
    )


def test_integrity_persistence_failure_marks_range_module_unhealthy(tmp_path) -> None:
    class FailingIntegrityStore(SqliteRangeCheckpointStore):
        def save_bucket_integrity(self, state) -> None:
            raise OSError("integrity disk failure")

    module = RangeBarModule(
        config=RangeBarModuleConfig(
            symbol="ETH-USDT-PERP",
            exchange=ExchangeName.OKX,
            range_pct=Decimal("0.002"),
            contract_value=Decimal("0.01"),
            bucket_interval_ms=100,
            aggregate_interval="100ms",
        ),
        builder=FakeBuilder(),
        bar_store=FakeBarStore(),
        checkpoint_store=FailingIntegrityStore(tmp_path / "checkpoint.sqlite3"),
        checkpoint_writer=FakeCheckpointWriter(),
        clock_ms=lambda: 1,
    )

    with pytest.raises(OSError, match="integrity disk failure"):
        module.mark_degraded(bucket_start_ms=0, reason="processor_drop")

    health = module.health()
    assert health.state is ModuleState.ERROR
    assert health.healthy is False
    assert "integrity disk failure" in (health.detail or "")


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


def test_range_degraded_state_survives_restart_and_repair_is_revision_bound(
    tmp_path,
) -> None:
    db_path = tmp_path / "range.sqlite3"
    config = RangeBarModuleConfig(
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        range_pct=Decimal("0.002"),
        contract_value=Decimal("0.01"),
        bucket_interval_ms=100,
        aggregate_interval="100ms",
        checkpoint_db_path=str(db_path),
    )

    first = RangeBarModule(
        config=config,
        builder=FakeBuilder(),
        bar_store=FakeBarStore((_bar(),)),
        checkpoint_store=SqliteRangeCheckpointStore(db_path),
        checkpoint_writer=FakeCheckpointWriter(),
        integrity=TradeDataIntegrityTracker(),
        clock_ms=lambda: 50,
    )
    first.initialize_recovery()
    first.mark_degraded(bucket_start_ms=0, reason="source_drop")
    assert first.bucket_integrity(0).status is RangeBucketIntegrityStatus.DEGRADED
    assert first.coverage(0).coverage_status != RangeCoverageStatus.COMPLETE.value
    assert first.rows_for_bucket(0) == []

    restarted = RangeBarModule(
        config=config,
        builder=FakeBuilder(),
        bar_store=FakeBarStore((_bar(),)),
        checkpoint_store=SqliteRangeCheckpointStore(db_path),
        checkpoint_writer=FakeCheckpointWriter(),
        integrity=TradeDataIntegrityTracker(),
        clock_ms=lambda: 50,
    )
    restarted.initialize_recovery()
    assert restarted.degraded_reason(0) == "source_drop"
    assert restarted.rows_for_bucket(0) == []
    assert not restarted.mark_repaired(0, through_revision=1)

    token = restarted.begin_repair(0)
    assert token == 1
    assert restarted.bucket_integrity(0).status is RangeBucketIntegrityStatus.REPAIRING
    assert restarted.coverage(0).coverage_status != RangeCoverageStatus.COMPLETE.value
    assert restarted.rows_for_bucket(0) == []
    assert restarted.mark_repaired(0, through_revision=token)
    assert restarted.bucket_integrity(0).status is RangeBucketIntegrityStatus.REPAIRED
    assert restarted.coverage(0).coverage_status == RangeCoverageStatus.COMPLETE.value
    assert restarted.rows_for_bucket(0) == [_bar()]

    restarted.mark_degraded(bucket_start_ms=0, reason="new_drop")
    assert restarted.degraded_reason(0) == "new_drop"
    assert restarted.coverage(0).coverage_status != RangeCoverageStatus.COMPLETE.value


def test_repairing_and_durable_trade_window_remain_incomplete_after_restart(
    tmp_path,
) -> None:
    db_path = tmp_path / "range.sqlite3"
    config = RangeBarModuleConfig(
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        range_pct=Decimal("0.002"),
        contract_value=Decimal("0.01"),
        bucket_interval_ms=100,
        aggregate_interval="100ms",
        checkpoint_db_path=str(db_path),
    )
    first_tracker = TradeDataIntegrityTracker()
    first = RangeBarModule(
        config=config,
        builder=FakeBuilder(),
        bar_store=FakeBarStore((_bar(),)),
        checkpoint_store=SqliteRangeCheckpointStore(db_path),
        checkpoint_writer=FakeCheckpointWriter(),
        integrity=first_tracker,
        clock_ms=lambda: 50,
    )
    first.initialize_recovery()
    first.mark_degraded(bucket_start_ms=0, reason="disconnect")
    first.begin_repair(0)

    restarted_tracker = TradeDataIntegrityTracker()
    restarted = RangeBarModule(
        config=config,
        builder=FakeBuilder(),
        bar_store=FakeBarStore((_bar(),)),
        checkpoint_store=SqliteRangeCheckpointStore(db_path),
        checkpoint_writer=FakeCheckpointWriter(),
        integrity=restarted_tracker,
        clock_ms=lambda: 50,
    )
    restarted.initialize_recovery()
    state = restarted.bucket_integrity(0)
    assert state.status is RangeBucketIntegrityStatus.REPAIRING
    assert not state.complete
    assert restarted.rows_for_bucket(0) == []
    assert restarted.aggregates_for_bucket(0) == []
    assert restarted_tracker.invalid_reason(0, 99) is not None
    assert restarted_tracker.dropped_count == 0
    assert restarted_tracker.issues_since(0) == ()


@pytest.mark.asyncio
async def test_range_module_appends_repair_journal_without_raw_trade_callback() -> None:
    class Journal:
        def __init__(self) -> None:
            self.trades = []

        def append(self, trade) -> None:
            self.trades.append(trade)

    journal = Journal()
    module = _module(
        builder=FakeBuilder(), persistence=FakePersistence(), emitted=[]
    )
    module._repair_journal = journal
    trade = _trade()
    await module.process_trade(trade)
    assert journal.trades == [trade]


def test_repairing_clean_revision_still_fails_closed() -> None:
    tracker = TradeDataIntegrityTracker()
    module = _module(
        builder=FakeBuilder(), persistence=FakePersistence(), emitted=[]
    )
    module.configure_integrity(tracker)
    assert module.begin_repair(0) == 0
    assert tracker.invalid_reason(0, 99) is not None
    assert module.coverage(0).coverage_status != RangeCoverageStatus.COMPLETE.value
