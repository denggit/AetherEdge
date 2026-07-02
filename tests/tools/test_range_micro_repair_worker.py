from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.market_data.backfill.status_store import RangeBackfillStatusStore
from src.market_data.derived import RangeBarBuilder
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_SUCCESS,
    RangeBuilderCheckpoint,
    RangeMicroRepairJob,
    SqliteRangeCheckpointStore,
)
from src.market_data.range_repair import (
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    JOURNAL_INVALID_QUEUE_OVERFLOW,
    JOURNAL_INVALID_WRITER_ERROR,
    RangeRepairJournalState,
    RangeRepairTrade,
    SqliteRangeRepairJournalStore,
)
from src.platform.data.models import (
    MarketDataSource,
    MarketTrade,
    TradeSide,
)
from src.platform.exchanges.models import ExchangeName
from tools import range_micro_repair_worker as worker

BUCKET_START = 1_780_000_000_000
BUCKET_END = BUCKET_START + 9_999
CHECKPOINT_TS = BUCKET_START + 100
FIRST_LIVE_TS = CHECKPOINT_TS + 88


def _seed(checkpoint_db) -> SqliteRangeCheckpointStore:
    store = SqliteRangeCheckpointStore(checkpoint_db)
    builder = RangeBarBuilder(range_pct="0.001", contract_value="0.01")
    builder.on_trade(
        MarketTrade(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal("100"),
            quantity=Decimal("1"),
            side=TradeSide.BUY,
            trade_id="cp",
            trade_time_ms=CHECKPOINT_TS,
        )
    )
    store.save_checkpoint(
        RangeBuilderCheckpoint(
            exchange="okx",
            symbol="ETH-USDT-PERP",
            range_pct="0.001",
            bucket_start_ms=BUCKET_START,
            bucket_end_ms=BUCKET_END,
            last_trade_id="cp",
            last_trade_ts_ms=CHECKPOINT_TS,
            last_ws_recv_ts_ms=CHECKPOINT_TS,
            range_bar_count=0,
            aggregate={},
            builder_state=builder.snapshot_state(),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            missing_gap_ms=500,
            checkpoint_updated_at_ms=CHECKPOINT_TS,
        )
    )
    store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol="ETH-USDT-PERP",
            range_pct=Decimal("0.001"),
            bucket_start_ms=BUCKET_START,
            bucket_end_ms=BUCKET_END,
            bar_count=1,
            first_open=Decimal("100"),
            last_close=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            buy_notional_sum=Decimal("1"),
            sell_notional_sum=Decimal("0"),
            delta_notional_sum=Decimal("1"),
            notional_sum=Decimal("1"),
        ),
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=500,
        completed_at_ms=BUCKET_END,
    )
    return store


def _args(tmp_path) -> list[str]:
    return [
        "--exchange",
        "okx",
        "--symbol",
        "ETH-USDT-PERP",
        "--range-pct",
        "0.001",
        "--bucket-start-ms",
        str(BUCKET_START),
        "--bucket-end-ms",
        str(BUCKET_END),
        "--coverage-status",
        RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        "--missing-gap-ms",
        "500",
        "--checkpoint-db",
        str(tmp_path / "checkpoint.sqlite3"),
        "--market-db",
        str(tmp_path / "market.sqlite3"),
        "--journal-db",
        str(tmp_path / "journal.sqlite3"),
        "--status-path",
        str(tmp_path / "status.json"),
        "--lock-path",
        str(tmp_path / "repair.lock"),
        "--max-seconds",
        "1",
        "--wait-poll-seconds",
        "0.01",
    ]


def _seed_journal(tmp_path, *, invalid_status: str | None = None):
    journal = SqliteRangeRepairJournalStore(tmp_path / "journal.sqlite3")
    journal.open_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        checkpoint_last_trade_ts_ms=CHECKPOINT_TS,
        checkpoint_last_trade_id="cp",
        updated_at_ms=CHECKPOINT_TS,
    )
    journal.record_first_live_trade(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        trade_time_ms=FIRST_LIVE_TS,
        trade_id="j1",
        recorded_at_ms=FIRST_LIVE_TS,
    )
    journal.append_trades(
        [
            RangeRepairTrade(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                range_pct="0.001",
                bucket_start_ms=BUCKET_START,
                trade_time_ms=FIRST_LIVE_TS,
                event_time_ms=FIRST_LIVE_TS,
                trade_id="j1",
                raw_symbol="ETH-USDT-SWAP",
                side="buy",
                price="100.4",
                quantity="1",
                source="websocket",
                created_at_ms=FIRST_LIVE_TS,
            )
        ]
    )
    if invalid_status is not None:
        journal.invalidate(
            exchange="okx",
            symbol="ETH-USDT-PERP",
            range_pct="0.001",
            bucket_start_ms=BUCKET_START,
            status=invalid_status,
            last_error="test invalid journal",
            dropped_trades=(
                0
                if invalid_status == JOURNAL_INVALID_WRITER_ERROR
                else 1
            ),
            writer_failures=(
                1
                if invalid_status == JOURNAL_INVALID_WRITER_ERROR
                else 0
            ),
        )
    journal.finalize(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        finalized_at_ms=BUCKET_END + 1,
    )
    return journal


def _finalized_journal_state() -> RangeRepairJournalState:
    return RangeRepairJournalState(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        checkpoint_last_trade_ts_ms=CHECKPOINT_TS,
        checkpoint_last_trade_id="cp",
        first_live_trade_ts_ms=FIRST_LIVE_TS,
        first_live_trade_id="j1",
        first_live_trade_recorded_at_ms=FIRST_LIVE_TS,
        last_journal_trade_ts_ms=FIRST_LIVE_TS + 10,
        journal_trade_count=7,
        dropped_trades=0,
        writer_failures=0,
        finalized=True,
        finalized_at_ms=BUCKET_END + 1,
        status="journal_finalized",
        last_error=None,
        updated_at_ms=BUCKET_END + 2,
    )


def test_journal_job_helper_preserves_checkpoint_update_fields() -> None:
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        checkpoint_last_trade_id="cp",
        checkpoint_last_trade_ts_ms=CHECKPOINT_TS,
        builder_state={"version": 1},
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=500,
    )

    assert worker._journal_job_fields(
        job,
        _finalized_journal_state(),
        repair_gap_start_ms=CHECKPOINT_TS + 1,
        repair_gap_end_ms=FIRST_LIVE_TS - 1,
        updated_at_ms=BUCKET_END + 3,
    ) == {
        "first_live_trade_ts_ms": FIRST_LIVE_TS,
        "first_live_trade_id": "j1",
        "repair_gap_start_ms": CHECKPOINT_TS + 1,
        "repair_gap_end_ms": FIRST_LIVE_TS - 1,
        "journal_start_ms": FIRST_LIVE_TS,
        "journal_end_ms": BUCKET_END,
        "journal_status": "journal_finalized",
        "updated_at_ms": BUCKET_END + 3,
    }


def test_worker_status_helpers_preserve_complete_payload(
    monkeypatch,
) -> None:
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        checkpoint_last_trade_id="cp",
        checkpoint_last_trade_ts_ms=CHECKPOINT_TS,
        builder_state={"version": 1},
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=500,
        first_live_trade_ts_ms=FIRST_LIVE_TS,
        first_live_trade_id="j1",
        repair_gap_start_ms=CHECKPOINT_TS + 1,
        repair_gap_end_ms=FIRST_LIVE_TS - 1,
        journal_start_ms=FIRST_LIVE_TS,
        journal_end_ms=BUCKET_END,
        journal_status="journal_finalized",
    )
    result = SimpleNamespace(
        repair_start_ms=CHECKPOINT_TS + 1,
        repair_end_ms=BUCKET_END,
        repair_gap_start_ms=CHECKPOINT_TS + 1,
        repair_gap_end_ms=FIRST_LIVE_TS - 1,
        repair_gap_ms=87,
        journal_start_ms=FIRST_LIVE_TS,
        journal_end_ms=BUCKET_END,
        journal_trade_count=7,
        rest_pages=2,
        rest_raw_trades=4,
        rest_deduped_trades=3,
        fetch_mode="trade_id_anchor",
        fallback_reason=None,
        replayed_rest_trades=3,
        replayed_journal_trades=7,
        range_bars_written=5,
        aggregate_written=True,
    )
    written = []
    monkeypatch.setattr(worker, "now_ms", lambda: BUCKET_END + 4)
    monkeypatch.setattr(worker.os, "getpid", lambda: 4321)

    worker._write_status(
        SimpleNamespace(write=written.append),
        status=MICRO_REPAIR_SUCCESS,
        args=SimpleNamespace(
            exchange="okx",
            symbol="ETH-USDT-PERP",
            range_pct="0.001",
            bucket_start_ms=BUCKET_START,
        ),
        running=False,
        job=job,
        result=result,
        journal_state=_finalized_journal_state(),
    )

    assert written == [
        {
            "pid": 4321,
            "running": False,
            "repair_status": MICRO_REPAIR_SUCCESS,
            "phase": MICRO_REPAIR_SUCCESS,
            "repair_scope": "startup_recovery_current_bucket",
            "exchange": "okx",
            "symbol": "ETH-USDT-PERP",
            "range_pct": "0.001",
            "bucket_start_ms": BUCKET_START,
            "worker_heartbeat_ms": BUCKET_END + 4,
            "heartbeat_ms": BUCKET_END + 4,
            "failure_reason": None,
            "waiting_reason": None,
            "finished_at_ms": BUCKET_END + 4,
            "checkpoint_last_trade_ts_ms": CHECKPOINT_TS,
            "checkpoint_last_trade_id": "cp",
            "first_live_trade_ts_ms": FIRST_LIVE_TS,
            "first_live_trade_id": "j1",
            "repair_gap_start_ms": CHECKPOINT_TS + 1,
            "repair_gap_end_ms": FIRST_LIVE_TS - 1,
            "repair_gap_ms": 87,
            "journal_start_ms": FIRST_LIVE_TS,
            "journal_end_ms": BUCKET_END,
            "journal_trade_count": 7,
            "journal_status": "journal_finalized",
            "journal_dropped_trades": 0,
            "journal_writer_failures": 0,
            "rest_pages": 2,
            "rest_raw_trades": 4,
            "rest_deduped_trades": 3,
            "fetch_mode": "trade_id_anchor",
            "fallback_reason": None,
            "replayed_rest_trades": 3,
            "replayed_journal_trades": 7,
            "range_bars_written": 5,
            "aggregate_written": True,
            "coverage_before": (
                RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value
            ),
            "coverage_after": "COMPLETE",
            "bucket_end_ms": BUCKET_END,
            "missing_gap_ms": 500,
            "aggregates_written": 1,
            "journal_finalized": True,
            "repair_start_ms": CHECKPOINT_TS + 1,
            "repair_end_ms": BUCKET_END,
        }
    ]


def test_worker_repairs_bucket_in_subprocess_mode_without_raw_persistence(
    tmp_path, monkeypatch
) -> None:
    store = _seed(tmp_path / "checkpoint.sqlite3")
    _seed_journal(tmp_path)

    class Provider:
        async def fetch_trades(self, **kwargs):
            assert kwargs["start_time_ms"] == CHECKPOINT_TS + 1
            assert kwargs["end_time_ms"] == FIRST_LIVE_TS - 1
            assert kwargs["end_time_ms"] != BUCKET_END
            return [
                MarketTrade(
                    exchange=ExchangeName.OKX,
                    symbol="ETH-USDT-PERP",
                    raw_symbol="ETH-USDT-SWAP",
                    price=Decimal("100.2"),
                    quantity=Decimal("1"),
                    side=TradeSide.BUY,
                    trade_id="r1",
                    trade_time_ms=CHECKPOINT_TS + 1,
                    source=MarketDataSource.REST,
                )
            ]

    monkeypatch.setattr(
        worker, "create_market_data_feed", lambda *args, **kwargs: Provider()
    )

    assert worker.main(_args(tmp_path)) == 0

    aggregate = store.load_completed_aggregate(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_end_ms=BUCKET_END,
    )
    job = store.load_micro_repair_job(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
    )
    status = RangeBackfillStatusStore(tmp_path / "status.json").read()
    assert aggregate is not None and aggregate.coverage_status == "COMPLETE"
    assert job is not None and job.status == MICRO_REPAIR_SUCCESS
    assert job.first_live_trade_ts_ms == FIRST_LIVE_TS
    assert job.repair_gap_start_ms == CHECKPOINT_TS + 1
    assert job.repair_gap_end_ms == FIRST_LIVE_TS - 1
    assert status is not None
    assert status["repair_status"] == MICRO_REPAIR_SUCCESS
    assert status["repair_gap_start_ms"] == CHECKPOINT_TS + 1
    assert status["repair_gap_end_ms"] == FIRST_LIVE_TS - 1
    assert status["repair_gap_ms"] == 87
    assert status["journal_status"] == "journal_finalized"
    assert status["journal_trade_count"] == 1
    assert status["replayed_rest_trades"] == 1
    assert status["replayed_journal_trades"] == 1
    assert not (tmp_path / "repair.lock").exists()


def test_worker_failure_preserves_degraded_aggregate(tmp_path, monkeypatch) -> None:
    store = _seed(tmp_path / "checkpoint.sqlite3")
    _seed_journal(tmp_path)

    class Provider:
        async def fetch_trades(self, **kwargs):
            raise RuntimeError("REST unavailable")

    monkeypatch.setattr(
        worker, "create_market_data_feed", lambda *args, **kwargs: Provider()
    )

    assert worker.main(_args(tmp_path)) == 1

    aggregate = store.load_completed_aggregate(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_end_ms=BUCKET_END,
    )
    job = store.load_micro_repair_job(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
    )
    assert aggregate is not None
    assert aggregate.coverage_status == (
        RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value
    )
    assert job is not None and job.status == MICRO_REPAIR_FAILED


def test_worker_waits_for_bucket_close_without_holding_repair_lock(
    tmp_path, monkeypatch
) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    journal = SqliteRangeRepairJournalStore(tmp_path / "journal.sqlite3")
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        checkpoint_last_trade_id="cp",
        checkpoint_last_trade_ts_ms=CHECKPOINT_TS,
        builder_state={"version": 1},
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=500,
    )
    journal.open_bucket(
        exchange="okx",
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        checkpoint_last_trade_ts_ms=CHECKPOINT_TS,
        checkpoint_last_trade_id="cp",
        updated_at_ms=CHECKPOINT_TS,
    )
    journal.record_first_live_trade(
        exchange="okx",
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_start_ms=BUCKET_START,
        trade_time_ms=FIRST_LIVE_TS,
        trade_id="j1",
        recorded_at_ms=FIRST_LIVE_TS,
    )
    sleeps = []
    statuses = []
    monkeypatch.setattr(worker, "now_ms", lambda: BUCKET_END)
    monkeypatch.setattr(worker, "_write_status", lambda *args, **kwargs: None)
    def stop_after_wait(value):
        sleeps.append(value)
        raise RuntimeError("stop test loop")
    monkeypatch.setattr(worker.time, "sleep", stop_after_wait)

    with pytest.raises(RuntimeError, match="stop test loop"):
        worker._wait_until_bucket_can_be_repaired(
            store,
            journal,
            RangeBackfillStatusStore(tmp_path / "status.json"),
            args=SimpleNamespace(
                missing_bucket_grace_seconds=120,
                wait_poll_seconds=0.01,
                max_gap_ms=600_000,
            ),
            job=job,
        )

    assert sleeps == [0.1]
    assert not (tmp_path / "repair.lock").exists()


@pytest.mark.parametrize(
    "invalid_status",
    [
        JOURNAL_INVALID_QUEUE_OVERFLOW,
        JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
        JOURNAL_INVALID_DROPPED_TRADE,
        JOURNAL_INVALID_WRITER_ERROR,
    ],
)
def test_worker_invalid_journal_never_overwrites_complete(
    tmp_path, monkeypatch, invalid_status
) -> None:
    store = _seed(tmp_path / "checkpoint.sqlite3")
    _seed_journal(tmp_path, invalid_status=invalid_status)
    monkeypatch.setattr(
        worker,
        "create_market_data_feed",
        lambda *args, **kwargs: pytest.fail(
            "invalid journal must fail before REST provider creation"
        ),
    )

    assert worker.main(_args(tmp_path)) == 1

    aggregate = store.load_completed_aggregate(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.001",
        bucket_end_ms=BUCKET_END,
    )
    assert aggregate is not None
    assert aggregate.coverage_status == (
        RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value
    )
