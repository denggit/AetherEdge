from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

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
        "--status-path",
        str(tmp_path / "status.json"),
        "--lock-path",
        str(tmp_path / "repair.lock"),
        "--max-seconds",
        "1",
        "--wait-poll-seconds",
        "0.01",
    ]


def test_worker_repairs_bucket_in_subprocess_mode_without_raw_persistence(
    tmp_path, monkeypatch
) -> None:
    store = _seed(tmp_path / "checkpoint.sqlite3")

    class Provider:
        async def fetch_trades(self, **kwargs):
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
    assert status is not None
    assert status["repair_status"] == MICRO_REPAIR_SUCCESS
    assert not (tmp_path / "repair.lock").exists()


def test_worker_failure_preserves_degraded_aggregate(tmp_path, monkeypatch) -> None:
    store = _seed(tmp_path / "checkpoint.sqlite3")

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
    clock = iter([BUCKET_END, BUCKET_END + 1])
    sleeps = []
    monkeypatch.setattr(worker, "now_ms", lambda: next(clock))
    monkeypatch.setattr(worker, "_write_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.time, "sleep", sleeps.append)

    ready = worker._wait_until_bucket_can_be_repaired(
        store,
        RangeBackfillStatusStore(tmp_path / "status.json"),
        args=SimpleNamespace(
            missing_bucket_grace_seconds=0,
            wait_poll_seconds=0.01,
        ),
        job=job,
    )

    assert ready is True
    assert sleeps == [0.1]
    assert not (tmp_path / "repair.lock").exists()
