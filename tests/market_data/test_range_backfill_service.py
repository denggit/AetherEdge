from __future__ import annotations

import sqlite3
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from src.market_data.models import RangeBarAggregate, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.backfill.models import BackfillPlan
from src.market_data.backfill.service import BackfillService
from src.market_data.storage import SqliteTradeStore
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.okx.historical_archive import OkxArchiveUnavailableError


H4 = 4 * 60 * 60_000
START = 1_641_081_600_000  # 2022-01-02T00:00:00Z


def _plan(start: int = START) -> BackfillPlan:
    return BackfillPlan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        range_pct="0.002",
        bucket_ms=H4,
        latest_closed_bucket_start_ms=start,
        latest_closed_bucket_end_ms=start + H4 - 1,
        required_bucket_starts=(start,),
        complete_bucket_starts=(),
        missing_bucket_starts=(start,),
        dirty_bucket_starts=(),
        incomplete_coverage_bucket_starts=(),
        continuous_complete_buckets_from_latest=0,
        range_speed_ready=False,
        nearest_missing_bucket_start_ms=start,
        reason="missing",
    )


def _write_zip(raw_root: Path, start: int = START) -> Path:
    path = raw_root / "trades" / "ETH-USDT-SWAP" / "ETH-USDT-SWAP-trades-2022-01-02.zip"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(
            "trades.csv",
            "instrument_name,trade_id,side,price,size,created_time\n"
            f"ETH-USDT-SWAP,a,buy,100,1,{start + 1_000}\n"
            f"ETH-USDT-SWAP,b,buy,100.3,1,{start + 2_000}\n"
            f"ETH-USDT-SWAP,c,sell,100.6,1,{start + H4 - 1_000}\n",
        )
    return path


def test_local_raw_zip_is_used_without_download_and_upserts_aggregate(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_zip(raw_root)
    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        coverage_max_gap_ms=H4,
        edge_tolerance_ms=5_000,
        download_sleep_seconds=0,
    )

    result = service.process_plan(_plan(), max_buckets=1)

    assert result.downloaded_days == 0
    assert result.imported_trades == 3
    assert result.aggregates_upserted == 1
    with sqlite3.connect(tmp_path / "checkpoint.sqlite3") as conn:
        row = conn.execute("SELECT rf_bar_count, coverage_status FROM completed_range_aggregates").fetchone()
    assert row == (1, "COMPLETE")


def test_missing_completed_day_downloads_raw_zip(tmp_path: Path) -> None:
    class Archive:
        def ensure_daily_trades_zip(self, **kwargs):
            path = _write_zip(tmp_path / "raw")
            return type("Meta", (), {"path": str(path)})()

        def iter_daily_trades_zip(self, path, *, raw_symbol, symbol, chunksize):
            yield [
                _trade(START + 1_000, "100", "a"),
                _trade(START + 2_000, "100.3", "b"),
                _trade(START + H4 - 1_000, "100.6", "c"),
            ]

    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        archive=Archive(),  # type: ignore[arg-type]
        coverage_max_gap_ms=H4,
        edge_tolerance_ms=5_000,
        download_sleep_seconds=0,
    )

    result = service.process_plan(_plan(), max_buckets=1)

    assert result.downloaded_days == 1
    assert result.aggregates_upserted == 1


def test_current_utc_day_waits_without_download(tmp_path: Path) -> None:
    class Archive:
        def ensure_daily_trades_zip(self, **_kwargs):
            raise AssertionError("current UTC day should not download daily zip")

    now = datetime(2026, 6, 30, 8, tzinfo=UTC)
    start = int(datetime(2026, 6, 30, 0, tzinfo=UTC).timestamp() * 1000)
    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        archive=Archive(),  # type: ignore[arg-type]
    )

    result = service.process_plan(_plan(start), max_buckets=1, now=now)

    assert result.processed_buckets == 0
    assert result.skipped_buckets == [start]
    assert result.tail_errors


def test_current_day_complete_db_trades_with_fragment_coverage_rebuilds(tmp_path: Path) -> None:
    now = datetime(2026, 6, 30, 8, tzinfo=UTC)
    start = int(datetime(2026, 6, 30, 0, tzinfo=UTC).timestamp() * 1000)
    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        archive=type("Archive", (), {"ensure_daily_trades_zip": lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no zip"))})(),  # type: ignore[arg-type]
        coverage_max_gap_ms=H4,
        edge_tolerance_ms=5_000,
    )
    _save_trades(service.trade_store, [_trade(start + 1_000, "100", "a"), _trade(start + 2_000, "100.3", "b"), _trade(start + H4 - 1_000, "100.6", "c")])
    service.trade_store.mark_coverage(symbol="ETH-USDT-PERP", time_range=TimeRange(start + 1_000, start + 2_000), source="realtime_segment", coverage_status="SEGMENT")
    service.trade_store.mark_coverage(symbol="ETH-USDT-PERP", time_range=TimeRange(start + H4 - 2_000, start + H4 - 1_000), source="realtime_segment", coverage_status="SEGMENT")

    result = service.process_plan(_plan(start), max_buckets=1, now=now)

    assert result.tail_fetch_requested_buckets == []
    assert result.coverage_validated_buckets == [start]
    assert result.aggregates_upserted == 1


def test_current_day_incomplete_db_trades_uses_rest_tail_then_rebuilds(tmp_path: Path) -> None:
    now = datetime(2026, 6, 30, 8, tzinfo=UTC)
    start = int(datetime(2026, 6, 30, 0, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[str, int, int]] = []

    def fetcher(raw_symbol: str, gap_start: int, gap_end: int):
        calls.append((raw_symbol, gap_start, gap_end))
        return [_trade(start + H4 - 1_000, "100.6", "c")]

    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        rest_tail_fetcher=fetcher,
        coverage_max_gap_ms=H4,
        edge_tolerance_ms=5_000,
    )
    _save_trades(service.trade_store, [_trade(start + 1_000, "100", "a"), _trade(start + 2_000, "100.3", "b")])

    result = service.process_plan(_plan(start), max_buckets=1, now=now)

    assert calls == [("ETH-USDT-SWAP", start, start + H4 - 1)]
    assert result.tail_fetch_requested_buckets == [start]
    assert result.tail_fetch_succeeded_buckets == [start]
    assert result.tail_fetch_trades_saved == 1
    assert result.aggregates_upserted == 1


def test_current_day_rest_tail_gap_over_limit_is_skipped(tmp_path: Path) -> None:
    now = datetime(2026, 6, 30, 8, tzinfo=UTC)
    start = int(datetime(2026, 6, 30, 0, tzinfo=UTC).timestamp() * 1000)
    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        rest_tail_fetcher=lambda *_args: (_ for _ in ()).throw(AssertionError("gap too large")),
        max_rest_tail_gap_minutes=1,
    )

    result = service.process_plan(_plan(start), max_buckets=1, now=now)

    assert result.processed_buckets == 0
    assert result.skipped_buckets == [start]
    assert result.tail_fetch_requested_buckets == []
    assert result.tail_errors


def test_daily_zip_404_is_archive_error_not_raise(tmp_path: Path) -> None:
    class Archive:
        def ensure_daily_trades_zip(self, **kwargs):
            raise OkxArchiveUnavailableError(
                raw_symbol=kwargs["raw_symbol"],
                day=kwargs["day"],
                url="https://example.invalid/missing.zip",
                status="not_yet_published",
                message="not yet published",
            )

    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        archive=Archive(),  # type: ignore[arg-type]
    )

    result = service.process_plan(_plan(), max_buckets=1)

    assert result.skipped_buckets == [START]
    assert result.archive_errors == ["not yet published"]


def test_process_plan_respects_max_buckets_per_cycle(tmp_path: Path) -> None:
    now = datetime(2026, 6, 30, 12, tzinfo=UTC)
    first = int(datetime(2026, 6, 30, 0, tzinfo=UTC).timestamp() * 1000)
    second = first + H4
    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        coverage_max_gap_ms=H4,
        edge_tolerance_ms=5_000,
    )
    _save_trades(
        service.trade_store,
        [
            _trade(first + 1_000, "100", "a"),
            _trade(first + 2_000, "100.3", "b"),
            _trade(first + H4 - 1_000, "100.6", "c"),
            _trade(second + 1_000, "101", "d"),
            _trade(second + 2_000, "101.3", "e"),
            _trade(second + H4 - 1_000, "101.6", "f"),
        ],
    )
    plan = _plan(second)
    plan = BackfillPlan(**{**plan.__dict__, "missing_bucket_starts": (second, first), "required_bucket_starts": (first, second)})

    result = service.process_plan(plan, max_buckets=1, now=now)

    assert result.processed_buckets == 1
    assert result.aggregates_upserted == 1


def test_rebuild_does_not_delete_other_complete_buckets(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _write_zip(raw_root)
    service = BackfillService(
        market_db=tmp_path / "market.sqlite3",
        checkpoint_db=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        coverage_max_gap_ms=H4,
        edge_tolerance_ms=5_000,
        download_sleep_seconds=0,
    )
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3").save_completed_aggregate(
        exchange="okx",
        aggregate=_aggregate(START - H4),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=START,
    )

    result = service.process_plan(_plan(), max_buckets=1)

    assert result.aggregates_upserted == 1
    with sqlite3.connect(tmp_path / "checkpoint.sqlite3") as conn:
        starts = [row[0] for row in conn.execute("SELECT bucket_start_ms FROM completed_range_aggregates ORDER BY bucket_start_ms").fetchall()]
    assert starts == [START - H4, START]


def test_sqlite_locked_returns_locked_result(tmp_path: Path) -> None:
    service = BackfillService(market_db=tmp_path / "market.sqlite3", checkpoint_db=tmp_path / "checkpoint.sqlite3")
    service._ensure_raw_trades = lambda **_kwargs: None  # type: ignore[method-assign]
    service.trade_store = type("LockedStore", (), {"load": lambda *_a, **_k: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked"))})()

    result = service.process_plan(_plan(), max_buckets=1)

    assert result.locked is True


def _save_trades(store: SqliteTradeStore, rows: list[MarketTrade]) -> None:
    store.save(rows)


def _aggregate(start: int) -> RangeBarAggregate:
    return RangeBarAggregate(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bucket_start_ms=start,
        bucket_end_ms=start + H4 - 1,
        bar_count=1,
        first_open=Decimal("100"),
        last_close=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("99"),
        buy_notional_sum=Decimal("60"),
        sell_notional_sum=Decimal("40"),
        delta_notional_sum=Decimal("20"),
        notional_sum=Decimal("100"),
    )


def _trade(ts: int, price: str, trade_id: str) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id,
        trade_time_ms=ts,
        event_time_ms=ts,
        source=MarketDataSource.REST,
    )
