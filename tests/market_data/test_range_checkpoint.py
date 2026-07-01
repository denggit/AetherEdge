from __future__ import annotations

import threading
import time
from decimal import Decimal

from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_QUEUED,
    MIN_VALID_COMPLETED_AGGREGATE_MS,
    RangeBuilderCheckpoint,
    RangeCheckpointWriter,
    RangeMicroRepairJob,
    SqliteRangeCheckpointStore,
)


def _checkpoint(*, updated_at_ms: int, sequence: int = 1) -> RangeBuilderCheckpoint:
    return RangeBuilderCheckpoint(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=10_000,
        bucket_end_ms=19_999,
        last_trade_id=str(sequence),
        last_trade_ts_ms=updated_at_ms,
        last_ws_recv_ts_ms=updated_at_ms,
        range_bar_count=sequence,
        aggregate={"rf_bar_count": sequence},
        builder_state={
            "version": 1,
            "range_pct": "0.002",
            "contract_value": "0.01",
            "active": None,
            "day_seq": {},
        },
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        missing_gap_ms=0,
        checkpoint_updated_at_ms=updated_at_ms,
    )


def _aggregate(*, bucket_start_ms: int, count: int) -> RangeBarAggregate:
    return RangeBarAggregate(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bucket_start_ms=bucket_start_ms,
        bucket_end_ms=bucket_start_ms + 9_999,
        bar_count=count,
        first_open=Decimal("100"),
        last_close=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("99"),
        buy_notional_sum=Decimal("60"),
        sell_notional_sum=Decimal("40"),
        delta_notional_sum=Decimal("20"),
        notional_sum=Decimal("100"),
    )


def test_checkpoint_can_be_saved_and_recovered_for_current_bucket(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "range.sqlite3")
    store.save_checkpoint(_checkpoint(updated_at_ms=20_000))

    result = store.recover_current_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.0020",
        bucket_start_ms=10_000,
        now_ms=20_500,
    )

    assert result.coverage_status == "RECOVERED_DEGRADED_MINOR"
    assert result.recovered_from_checkpoint is True
    assert result.checkpoint is not None
    assert result.checkpoint.builder_state["version"] == 1
    assert result.checkpoint.range_bar_count == 1


def test_checkpoint_too_old_is_recovered_incomplete(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "range.sqlite3")
    store.save_checkpoint(_checkpoint(updated_at_ms=20_000))

    result = store.recover_current_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=10_000,
        now_ms=90_001,
    )

    assert result.coverage_status == "RECOVERED_INCOMPLETE"
    assert result.checkpoint is not None
    assert result.checkpoint_age_ms == 70_001


def test_missing_checkpoint_is_cold_start_partial(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "range.sqlite3")

    result = store.recover_current_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=10_000,
        now_ms=15_000,
    )

    assert result.coverage_status == "COLD_START_PARTIAL"
    assert result.checkpoint is None
    assert result.missing_gap_ms == 5_000


class _BlockingStore:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.saved: list[RangeBuilderCheckpoint] = []

    def save_checkpoint(self, checkpoint: RangeBuilderCheckpoint) -> None:
        self.entered.set()
        self.release.wait(timeout=2)
        self.saved.append(checkpoint)


def test_bounded_writer_submit_does_not_wait_for_database_and_keeps_latest() -> None:
    store = _BlockingStore()
    writer = RangeCheckpointWriter(store, max_pending=1)
    writer.start()
    writer.submit(_checkpoint(updated_at_ms=20_000, sequence=1))
    assert store.entered.wait(timeout=1)

    started = time.perf_counter()
    for sequence in range(2, 102):
        assert writer.submit(
            _checkpoint(updated_at_ms=20_000 + sequence, sequence=sequence)
        )
    elapsed = time.perf_counter() - started

    assert elapsed < 0.1
    assert writer.pending_count == 1
    assert writer.replaced >= 99
    store.release.set()
    writer.stop(flush=True)
    assert store.saved[-1].range_bar_count == 101


def test_completed_history_reads_only_complete_in_time_order_and_excludes_current(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "range.sqlite3")
    for bucket_start, count, coverage in (
        (MIN_VALID_COMPLETED_AGGREGATE_MS + 30_000, 3, "COMPLETE"),
        (MIN_VALID_COMPLETED_AGGREGATE_MS + 10_000, 1, "COMPLETE"),
        (MIN_VALID_COMPLETED_AGGREGATE_MS + 20_000, 99, "RECOVERED_DEGRADED_MINOR"),
        (MIN_VALID_COMPLETED_AGGREGATE_MS + 40_000, 4, "COMPLETE"),
    ):
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(bucket_start_ms=bucket_start, count=count),
            coverage_status=coverage,
            completed_at_ms=bucket_start + 10_000,
        )

    rows = store.load_complete_history(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        before_bucket_end_ms=MIN_VALID_COMPLETED_AGGREGATE_MS + 49_999,
        limit=1080,
    )

    assert [row.bucket_start_ms for row in rows] == [
        MIN_VALID_COMPLETED_AGGREGATE_MS + 10_000,
        MIN_VALID_COMPLETED_AGGREGATE_MS + 30_000,
    ]
    assert [row.rf_bar_count for row in rows] == [1, 3]


def test_completed_aggregate_rejects_suspicious_timestamp(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "range.sqlite3")

    saved = store.save_completed_aggregate(
        exchange="okx",
        aggregate=_aggregate(bucket_start_ms=0, count=1),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=1,
    )

    assert saved is False
    assert store.history_counts(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
    ) == (0, 0)


def test_micro_repair_worker_checkpoint_and_status_are_persisted(
    tmp_path,
) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "range.sqlite3")
    start = MIN_VALID_COMPLETED_AGGREGATE_MS + 100_000
    aggregate = _aggregate(bucket_start_ms=start, count=3)
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=aggregate.bucket_start_ms,
        bucket_end_ms=aggregate.bucket_end_ms,
        checkpoint_last_trade_id="10",
        checkpoint_last_trade_ts_ms=start + 100,
        builder_state={"version": 1},
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=87_580,
        status=MICRO_REPAIR_QUEUED,
        created_at_ms=start,
        updated_at_ms=start,
    )
    store.enqueue_micro_repair(job)

    saved = store.load_micro_repair_job(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
    )
    assert saved is not None
    assert saved.checkpoint_last_trade_id == "10"

    assert store.mark_micro_repair_status(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
        status=MICRO_REPAIR_FAILED,
        updated_at_ms=aggregate.bucket_end_ms + 2,
        last_error="REST unavailable",
    )
    failed = store.load_micro_repair_job(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
    )
    assert failed is not None
    assert failed.status == MICRO_REPAIR_FAILED
    assert failed.last_error == "REST unavailable"
