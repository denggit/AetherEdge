from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from src.market_data.models import RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from tools import repair_range_history
from tools.repair_range_history import (
    DownloadResult,
    LiveDetector,
    OkxHistoricalTradeDownloader,
    _download_missing_trades,
    _validate_bucket_trade_coverage,
    _find_buckets_with_complete_aggregates,
    _delete_pollution_rows,
    _delete_one_aggregate,
)


H4 = 4 * 60 * 60_000
SYMBOL = "ETH-USDT-PERP"
RAW_SYMBOL = "ETH-USDT-SWAP"


def _args(tmp_path: Path, *extra: str):
    return repair_range_history.parse_args(
        [
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(tmp_path / "checkpoint.sqlite3"),
            "--contract-value",
            "0.01",
            *extra,
        ]
    )


def _trade(price: str, time_ms: int, trade_id: str | None = None) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol=RAW_SYMBOL,
        price=Decimal(price),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=trade_id or str(time_ms),
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
        source=MarketDataSource.REST,
        raw={},
    )


def _closing_trades(bucket_start: int, *, prefix: str = "") -> list[MarketTrade]:
    return [
        _trade("100", bucket_start + 1_000, f"{prefix}a"),
        _trade("100.2", bucket_start + 2_000, f"{prefix}b"),
        _trade("101", bucket_start + 3_000, f"{prefix}c"),
        _trade("101.202", bucket_start + 4_000, f"{prefix}d"),
    ]


def _dense_trades(bucket_start: int, count: int = 120) -> list[MarketTrade]:
    step = H4 // (count + 1)
    return [
        _trade(
            str(100 + i * 0.1),
            bucket_start + (i + 1) * step,
            f"dense_{bucket_start}_{i}",
        )
        for i in range(count)
    ]


def _bar(bar_id: int, end_ms: int, *, symbol: str = SYMBOL, range_pct: str = "0.002") -> RangeBar:
    return RangeBar(
        symbol=symbol,
        range_pct=Decimal(range_pct),
        bar_id=bar_id,
        start_time_ms=end_ms - 100,
        end_time_ms=end_ms,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        buy_notional=Decimal("100"),
        sell_notional=Decimal("0"),
        trade_count=1,
    )


def _aggregate(bucket_start: int, *, symbol: str = SYMBOL, range_pct: str = "0.002") -> RangeBarAggregate:
    return RangeBarAggregate(
        symbol=symbol,
        range_pct=Decimal(range_pct),
        bucket_start_ms=bucket_start,
        bucket_end_ms=bucket_start + H4 - 1,
        bar_count=1,
        first_open=Decimal("100"),
        last_close=Decimal("101"),
        high=Decimal("101"),
        low=Decimal("99"),
        buy_notional_sum=Decimal("10"),
        sell_notional_sum=Decimal("0"),
        delta_notional_sum=Decimal("10"),
        notional_sum=Decimal("10"),
    )


def _completed_rows(path: Path) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return conn.execute(
            """
            SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms, coverage_status
            FROM completed_range_aggregates
            ORDER BY exchange, symbol, range_pct, bucket_end_ms
            """
        ).fetchall()


def _range_rows(path: Path) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return conn.execute(
            "SELECT symbol, range_pct, bar_id, end_time_ms FROM range_bars ORDER BY end_time_ms, bar_id"
        ).fetchall()


def _coverage_rows(path: Path) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return conn.execute(
            "SELECT symbol, start_time_ms, end_time_ms, source FROM trade_coverage ORDER BY start_time_ms"
        ).fetchall()


# ---------------------------------------------------------------------------
# Fake downloaders
# ---------------------------------------------------------------------------

def _fake_full_downloader(raw_symbol: str, bucket_start_ms: int, bucket_end_ms: int, limit: int) -> tuple[list[MarketTrade], int, bool]:
    trades = _dense_trades(bucket_start_ms, count=50)
    return trades, 1, True


def _fake_failing_downloader(raw_symbol: str, bucket_start_ms: int, bucket_end_ms: int, limit: int) -> tuple[list[MarketTrade], int, bool]:
    raise RuntimeError("simulated network error")


def _fake_sparse_downloader(raw_symbol: str, bucket_start_ms: int, bucket_end_ms: int, limit: int) -> tuple[list[MarketTrade], int, bool]:
    trades = [
        _trade("100", bucket_start_ms, f"sparse_a_{bucket_start_ms}"),
        _trade("101", bucket_start_ms + 1000, f"sparse_b_{bucket_start_ms}"),
    ]
    return trades, 1, True


# ---------------------------------------------------------------------------
# Existing tests (adapted for incremental mode)
# ---------------------------------------------------------------------------


def test_dry_run_does_not_modify_db(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    trades = SqliteTradeStore(market_db)
    ranges = SqliteRangeBarStore(market_db)
    checkpoints = SqliteRangeCheckpointStore(checkpoint_db)
    trades.save(_closing_trades(0))
    trades.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    ranges.save([_bar(1, 2_000)])
    checkpoints.save_completed_aggregate(
        exchange="okx",
        aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=H4,
    )

    args = _args(
        tmp_path,
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--delete-existing-aggregates",
        "--delete-existing-range-bars",
        "--dry-run",
        "--start-ms", "0",
        "--end-ms", str(H4 - 1),
    )
    summary, exit_code = repair_range_history.run(args, now_ms=2 * H4)

    assert exit_code == 0
    assert summary.dry_run is True
    assert summary.mode == "incremental"
    assert _range_rows(market_db) == [(SYMBOL, "0.002", 1, 2_000)]
    assert len(_completed_rows(checkpoint_db)) == 1
    assert summary.backup_paths == []


def test_missing_trade_coverage_does_not_rebuild_or_complete(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0))

    args = _args(tmp_path, "--repair-range-bars", "--start-ms", "0", "--end-ms", str(H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=2 * H4)

    assert summary.missing_trade_coverage_buckets == 1
    assert summary.trades_exist_but_coverage_missing == 1
    assert summary.range_bars_written_count == 0
    assert _completed_rows(tmp_path / "checkpoint.sqlite3") == []


def test_complete_coverage_rebuilds_range_bars_from_synthetic_trades(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0) + _closing_trades(H4, prefix="b"))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, 2 * H4 - 1))

    args = _args(tmp_path, "--repair-range-bars", "--start-ms", "0", "--end-ms", str(2 * H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    rows = _range_rows(tmp_path / "market.sqlite3")
    assert summary.range_bars_rebuilt_count == 4
    assert summary.range_bars_written_count == 4
    assert len(rows) == 4


def test_partial_coverage_bucket_is_not_marked_complete(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0) + _closing_trades(H4, prefix="b"))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(H4, H4 + 10_000))

    args = _args(
        tmp_path,
        "--repair-range-bars",
        "--rebuild-aggregates", "false",
        "--start-ms", "0", "--end-ms", str(2 * H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    rows = _completed_rows(tmp_path / "checkpoint.sqlite3")
    assert summary.trade_coverage_complete_buckets == 1
    assert summary.missing_trade_coverage_buckets == 1
    # --rebuild-aggregates false → no aggregates written.
    assert len(rows) == 0


def test_delete_existing_aggregates_is_scoped_and_cleans_pollution(tmp_path: Path) -> None:
    """In incremental mode, --delete-existing-aggregates only affects repaired buckets + pollution."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    # Set up coverage + range bars so the bucket can be repaired.
    SqliteTradeStore(tmp_path / "market.sqlite3").mark_coverage(
        symbol=SYMBOL, time_range=TimeRange(base, base + H4 - 1)
    )
    SqliteRangeBarStore(tmp_path / "market.sqlite3").save([_bar(1, base + 2_000)])
    store = SqliteRangeCheckpointStore(checkpoint_db)
    for exchange, symbol, range_pct, bucket_start in (
        ("okx", SYMBOL, "0.002", base),
        ("binance", SYMBOL, "0.002", base),
        ("okx", "BTC-USDT-PERP", "0.002", base),
        ("okx", SYMBOL, "0.003", base),
    ):
        store.save_completed_aggregate(
            exchange=exchange,
            aggregate=_aggregate(bucket_start, symbol=symbol, range_pct=range_pct),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=2 * H4,
        )
    # Add a pollution row.
    with sqlite3.connect(checkpoint_db) as conn:
        conn.execute(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms, rf_bar_count,
                coverage_status, missing_gap_ms, completed_at_ms
            ) VALUES ('okx', ?, '0.002', 0, 12345, 1, 'COMPLETE', 0, 1)
            """,
            (SYMBOL,),
        )

    args = _args(
        tmp_path,
        "--rebuild-aggregates",
        "--delete-existing-aggregates",
        "--clean-pollution",
        "--start-ms", str(base),
        "--end-ms", str(base + H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=base + 3 * H4)

    rows = _completed_rows(checkpoint_db)
    assert summary.legacy_or_test_polluted_completed_aggregates_detected is True
    # Pollution row deleted.
    assert summary.pollution_rows_deleted == 1
    assert all(not (row[0] == "okx" and row[1] == SYMBOL and row[2] == "0.002" and row[4] == 12345) for row in rows)
    # Other rows (binance, BTC, different range_pct) should be untouched.
    assert ("binance", SYMBOL, "0.002", base, base + H4 - 1, "COMPLETE") in rows
    assert ("okx", "BTC-USDT-PERP", "0.002", base, base + H4 - 1, "COMPLETE") in rows
    assert ("okx", SYMBOL, "0.003", base, base + H4 - 1, "COMPLETE") in rows


def test_delete_existing_range_bars_replaces_only_target_bucket(tmp_path: Path) -> None:
    """In incremental mode, --delete-existing-range-bars replaces only repaired buckets."""
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    range_store = SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    range_store.save([_bar(99, 3_000), _bar(100, H4 + 1_000)])

    args = _args(
        tmp_path,
        "--repair-range-bars",
        "--delete-existing-range-bars",
        "--start-ms", "0",
        "--end-ms", str(H4 - 1),
    )
    repair_range_history.run(args, now_ms=3 * H4)

    rows = _range_rows(market_db)
    # Bar 99 (in repaired bucket 0) should be replaced.
    assert (SYMBOL, "0.002", 99, 3_000) not in rows
    # Bar 100 (in bucket H4, outside repair range) should be untouched.
    assert (SYMBOL, "0.002", 100, H4 + 1_000) in rows
    # Rebuilt bars (from _closing_trades) should be present in bucket 0.
    assert len([row for row in rows if row[3] <= H4 - 1]) == 2


def test_current_unfinished_bucket_is_not_processed(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0) + _closing_trades(H4, prefix="current"))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, 2 * H4 - 1))

    args = _args(tmp_path, "--repair-range-bars", "--start-ms", "0", "--end-ms", str(2 * H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=H4 + 1_000)

    assert summary.end_ms == H4 - 1
    assert summary.bucket_count_target == 1
    assert all(row[3] < H4 for row in _range_rows(tmp_path / "market.sqlite3"))


def test_polluted_1970_bucket_detected_and_deleted_with_flag(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(checkpoint_db)
    with sqlite3.connect(checkpoint_db) as conn:
        conn.execute(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms, rf_bar_count,
                coverage_status, missing_gap_ms, completed_at_ms
            ) VALUES ('okx', ?, '0.002', 0, 14399999, 1, 'COMPLETE', 0, 1)
            """,
            (SYMBOL,),
        )

    args = _args(
        tmp_path,
        "--delete-existing-aggregates",
        "--clean-pollution",
        "--start-ms", str(H4), "--end-ms", str(2 * H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    assert summary.legacy_or_test_polluted_completed_aggregates_detected is True
    assert summary.pollution_rows_deleted == 1
    assert _completed_rows(checkpoint_db) == []


def test_under_min_defaults_to_exit_zero_with_warning(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(tmp_path, "--min-buckets", "2", "--start-ms", "0", "--end-ms", str(H4 - 1))
    summary, exit_code = repair_range_history.run(args, now_ms=2 * H4)

    assert exit_code == 0
    assert "WARNING insufficient_complete_range_history_for_min_periods" in summary.warnings


def test_fail_under_min_returns_exit_code_two(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(tmp_path, "--min-buckets", "2", "--fail-under-min", "--start-ms", "0", "--end-ms", str(H4 - 1))
    _, exit_code = repair_range_history.run(args, now_ms=2 * H4)

    assert exit_code == 2


def test_json_output_is_written(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    output = tmp_path / "repair.json"

    args = _args(tmp_path, "--json-output", str(output), "--start-ms", "0", "--end-ms", str(H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=2 * H4)

    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["symbol"] == SYMBOL
    assert data["mode"] == "incremental"
    assert data["bucket_count_target"] == summary.bucket_count_target


def test_backup_true_generates_market_and_checkpoint_backups(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3").mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    SqliteRangeBarStore(tmp_path / "market.sqlite3").save([_bar(1, 2_000)])
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(tmp_path, "--rebuild-aggregates", "--start-ms", "0", "--end-ms", str(H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=2 * H4)

    backup_names = [Path(path).name for path in summary.backup_paths]
    assert any(name.startswith("market.sqlite3.") and name.endswith(".bak") for name in backup_names)
    assert any(name.startswith("checkpoint.sqlite3.") and name.endswith(".bak") for name in backup_names)
    assert all(Path(path).exists() for path in summary.backup_paths)


def test_tool_does_not_import_or_call_exchange_adapters() -> None:
    source = Path(repair_range_history.__file__).read_text(encoding="utf-8")
    assert "src.platform.exchanges.okx" not in source
    assert "src.platform.exchanges.binance" not in source
    assert "create_execution_client" not in source
    assert "create_market_data_feed" not in source
    assert "fetch_trades" not in source


# ---------------------------------------------------------------------------
# Download tests
# ---------------------------------------------------------------------------


def test_download_missing_trades_false_does_not_call_downloader(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0))

    call_count = 0

    def counting_downloader(raw_symbol, bucket_start_ms, bucket_end_ms, limit):
        nonlocal call_count
        call_count += 1
        return [], 1, False

    args = _args(tmp_path, "--repair-range-bars", "--start-ms", "0", "--end-ms", str(H4 - 1))
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=counting_downloader
    )

    assert exit_code == 0
    assert call_count == 0
    assert summary.download_missing_trades is False


def test_download_on_missing_coverage(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader
    )

    assert exit_code == 0
    assert summary.download_requested_buckets == 1
    assert summary.downloaded_buckets == 1
    assert summary.downloaded_trade_count == 50


def test_download_success_mark_coverage(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader
    )

    assert summary.coverage_validated_buckets == 1
    assert summary.coverage_validation_failed_buckets == 0
    assert summary.trade_coverage_complete_buckets == 1

    rows = _coverage_rows(market_db)
    assert len(rows) == 1
    assert rows[0][0] == SYMBOL
    assert rows[0][1] == 0
    assert rows[0][2] == H4 - 1
    assert rows[0][3] == "historical"


def test_download_success_but_coverage_validation_fails(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_sparse_downloader
    )

    assert summary.coverage_validation_failed_buckets == 1
    assert summary.coverage_validated_buckets == 0
    assert "downloaded_trades_failed_coverage_validation" in summary.warnings
    rows = _coverage_rows(market_db)
    assert len(rows) == 0


def test_download_then_repair_range_bars(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader
    )

    assert summary.range_bars_rebuilt_count > 0
    assert summary.range_bars_written_count > 0
    assert len(_range_rows(market_db)) > 0


def test_download_then_rebuild_aggregates(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--delete-existing-aggregates",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader
    )

    assert summary.aggregates_built_count >= 1
    assert summary.aggregates_written_count >= 1
    assert summary.aggregates_after_count >= 1
    assert len(_completed_rows(checkpoint_db)) >= 1


def test_download_failed_bucket_not_marked_complete(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(checkpoint_db)

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--delete-existing-aggregates",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_failing_downloader
    )

    assert summary.download_failed_buckets == 1
    assert summary.downloaded_buckets == 0
    assert "download_errors_occurred" in summary.warnings
    rows = _completed_rows(checkpoint_db)
    assert all(not (row[0] == "okx" and row[1] == SYMBOL and row[2] == "0.002") for row in rows)


def test_current_unfinished_bucket_not_downloaded(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    downloaded_buckets: list[int] = []

    def tracking_downloader(raw_symbol, bucket_start_ms, bucket_end_ms, limit):
        downloaded_buckets.append(bucket_start_ms)
        return _dense_trades(bucket_start_ms, count=50), 1, True

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--start-ms", "0", "--end-ms", str(4 * H4 - 1),
        "--skip-download-if-live", "false",
    )
    repair_range_history.run(
        args, now_ms=3 * H4 + 500, download_func=tracking_downloader
    )

    current_bucket_ms = 3 * H4
    for bucket_start in downloaded_buckets:
        assert bucket_start < current_bucket_ms


def test_live_running_no_allow_refuses_write(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)
    trade_store.save(_closing_trades(0))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))

    fake_live = LiveDetector(pid_files=(), process_names=())
    with patch.object(fake_live, "is_live", return_value=True):
        args = _args(
            tmp_path,
            "--repair-range-bars",
            "--rebuild-aggregates",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
        )
        summary, exit_code = repair_range_history.run(
            args, now_ms=2 * H4, live_detector=fake_live
        )

    assert exit_code == 3
    assert summary.live_running_detected is True


def test_live_running_with_allow_live_write_proceeds(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)
    trade_store.save(_closing_trades(0))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))

    fake_live = LiveDetector(pid_files=(), process_names=())
    with patch.object(fake_live, "is_live", return_value=True):
        args = _args(
            tmp_path,
            "--repair-range-bars",
            "--rebuild-aggregates",
            "--allow-live-db-write",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
        )
        summary, exit_code = repair_range_history.run(
            args, now_ms=2 * H4, live_detector=fake_live
        )

    assert exit_code == 0
    assert summary.live_running_detected is True
    assert summary.live_db_write_allowed is True
    assert any("live_running_detected_allow_live_db_write_active" in w for w in summary.warnings)


def test_dry_run_allowed_when_live(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)
    trade_store.save(_closing_trades(0))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))

    fake_live = LiveDetector(pid_files=(), process_names=())
    with patch.object(fake_live, "is_live", return_value=True):
        args = _args(
            tmp_path,
            "--dry-run",
            "--repair-range-bars",
            "--rebuild-aggregates",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
        )
        summary, exit_code = repair_range_history.run(
            args, now_ms=2 * H4, live_detector=fake_live
        )

    assert exit_code == 0
    assert summary.dry_run is True
    assert summary.live_running_detected is True


def test_json_output_includes_download_fields(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    output = tmp_path / "repair.json"

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--json-output", str(output),
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    repair_range_history.run(args, now_ms=2 * H4, download_func=_fake_full_downloader)

    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["download_missing_trades"] is True
    assert data["downloaded_buckets"] == 1
    assert data["downloaded_trade_count"] == 50
    assert "buckets_already_complete" in data
    assert "buckets_repaired" in data
    assert "buckets_aggregate_upserted" in data
    assert "mode" in data


def test_idempotent_repeated_download(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary1, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader
    )
    summary2, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader
    )

    rows = _coverage_rows(market_db)
    assert len(rows) == 1
    assert summary2.trade_coverage_complete_buckets == 1
    assert summary2.missing_trade_coverage_buckets == 0

    with sqlite3.connect(market_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM trades WHERE symbol = ?", (SYMBOL,)).fetchone()[0]
    assert count == 50


def test_tool_does_not_import_strategy_or_order_logic() -> None:
    source = Path(repair_range_history.__file__).read_text(encoding="utf-8")
    assert "strategies." not in source
    assert "from strategies" not in source
    assert "src.platform.exchanges.okx" not in source
    assert "src.platform.exchanges.binance" not in source
    assert "env_loader" not in source
    assert "OKX_HISTORY_TRADES_PATH" in source
    assert "/api/v5/market/history-trades" in source


# ---------------------------------------------------------------------------
# Incremental mode tests
# ---------------------------------------------------------------------------


def test_incremental_already_complete_bucket_not_repaired(tmp_path: Path) -> None:
    """Already-complete bucket (has coverage + COMPLETE aggregate) is skipped."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    range_store = SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)

    # Bucket 0: complete coverage + existing COMPLETE aggregate + range bars.
    trade_store.save(_closing_trades(0))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    range_store.save([_bar(1, 2_000)])
    checkpoint_store.save_completed_aggregate(
        exchange="okx",
        aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=H4,
    )

    # Bucket H4: complete coverage but NO aggregate (needs repair).
    trade_store.save(_closing_trades(H4, prefix="b"))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(H4, 2 * H4 - 1))

    args = _args(
        tmp_path,
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--start-ms", "0", "--end-ms", str(2 * H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    # Bucket 0 already complete → skipped.
    assert summary.buckets_already_complete == 1
    assert summary.buckets_skipped_existing_complete == 1
    # Bucket H4 needs repair → repaired.
    assert summary.buckets_repaired == 1
    assert summary.buckets_aggregate_upserted == 1
    # Existing aggregate for bucket 0 still intact.
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 2  # bucket 0 (existing) + bucket H4 (new)


def test_incremental_only_rebuilds_missing_aggregate_buckets(tmp_path: Path) -> None:
    """In incremental mode, only buckets missing aggregates get range bars rebuilt."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    range_store = SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)

    # Bucket 0: complete coverage + COMPLETE aggregate → already complete.
    trade_store.save(_closing_trades(0))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    range_store.save([_bar(1, 2_000)])
    checkpoint_store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=H4,
    )
    # Bucket H4: complete coverage, NO aggregate → needs repair.
    trade_store.save(_closing_trades(H4, prefix="b"))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(H4, 2 * H4 - 1))

    args = _args(
        tmp_path,
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--start-ms", "0", "--end-ms", str(2 * H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    # Only bucket H4 should be repaired.
    assert summary.buckets_repaired == 1
    # Bucket 0's range bars untouched (1 bar).
    # Bucket H4 now has range bars.
    total_range_rows = len(_range_rows(market_db))
    # 1 (existing bar in bucket 0) + 2 (rebuilt bars in bucket H4)
    assert total_range_rows >= 3


def test_incremental_default_does_not_delete_aggregates_window(tmp_path: Path) -> None:
    """Default incremental mode does NOT delete all aggregates in the window."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db).mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    SqliteRangeBarStore(market_db).save([_bar(1, 2_000)])
    store = SqliteRangeCheckpointStore(checkpoint_db)
    # Pre-populate with a valid aggregate in the target range.
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=H4,
    )

    args = _args(
        tmp_path,
        "--rebuild-aggregates",
        # No --delete-existing-aggregates, no --force-rebuild-window.
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    repair_range_history.run(args, now_ms=2 * H4)

    # The existing aggregate should still be there (incremental upsert, not delete).
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1


def test_force_rebuild_window_deletes_aggregates_window(tmp_path: Path) -> None:
    """--mode rebuild-window + --force-rebuild-window + --delete-existing-aggregates deletes the window."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db).mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    SqliteRangeBarStore(market_db).save([_bar(1, 2_000)])
    store = SqliteRangeCheckpointStore(checkpoint_db)
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=H4,
    )

    args = _args(
        tmp_path,
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--rebuild-aggregates",
        "--delete-existing-aggregates",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=2 * H4)

    assert summary.mode == "rebuild-window"
    assert summary.force_rebuild_window is True
    assert summary.deleted_existing_aggregates == 1


def test_clean_pollution_only_deletes_pollution_rows(tmp_path: Path) -> None:
    """--clean-pollution only deletes pollution rows, not valid history."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    store = SqliteRangeCheckpointStore(checkpoint_db)
    # Use a base after POLLUTION_CUTOFF_MS so the valid row isn't treated as pollution.
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    # Valid row (post-cutoff).
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )
    # Pollution row (pre-cutoff).
    with sqlite3.connect(checkpoint_db) as conn:
        conn.execute(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms, rf_bar_count,
                coverage_status, missing_gap_ms, completed_at_ms
            ) VALUES ('okx', ?, '0.002', 0, 12345, 1, 'COMPLETE', 0, 1)
            """,
            (SYMBOL,),
        )

    args = _args(
        tmp_path,
        "--clean-pollution",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert summary.pollution_rows_deleted == 1
    rows = _completed_rows(checkpoint_db)
    # Only the valid row remains.
    assert len(rows) == 1
    assert rows[0][3] == base  # bucket_start_ms


def test_incremental_delete_existing_aggregates_per_bucket(tmp_path: Path) -> None:
    """--delete-existing-aggregates in incremental mode deletes only repaired bucket aggregates before upsert."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    # Use a base after POLLUTION_CUTOFF_MS so rows aren't treated as pollution.
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    SqliteTradeStore(market_db).mark_coverage(
        symbol=SYMBOL, time_range=TimeRange(base, base + 2 * H4 - 1)
    )
    range_store = SqliteRangeBarStore(market_db)
    range_store.save([_bar(1, base + 2_000), _bar(2, base + H4 + 2_000)])
    store = SqliteRangeCheckpointStore(checkpoint_db)
    # Both buckets have aggregates initially.
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base + H4),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + 2 * H4,
    )

    # In incremental mode, both buckets are already complete → nothing to repair.
    # With --delete-existing-aggregates, it should only delete aggregates for repaired buckets (none).
    args = _args(
        tmp_path,
        "--rebuild-aggregates",
        "--delete-existing-aggregates",
        "--start-ms", str(base), "--end-ms", str(base + 2 * H4 - 1),
    )
    repair_range_history.run(args, now_ms=base + 3 * H4)

    # Both aggregates survive because they were already complete and weren't repaired.
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Unit tests: coverage validation
# ---------------------------------------------------------------------------

def test_validate_bucket_coverage_full(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    store = SqliteTradeStore(market_db)
    trades = _dense_trades(0, count=120)
    for t in trades:
        object.__setattr__(t, "symbol", SYMBOL)
    store.save(trades)

    ok, reason = _validate_bucket_trade_coverage(
        db_path=market_db, symbol=SYMBOL, bucket_start_ms=0, bucket_end_ms=H4 - 1,
        edge_tolerance_ms=300_000, max_gap_ms=1_800_000,
    )
    assert ok, f"expected valid coverage, got: {reason}"
    assert reason == "ok"


def test_validate_bucket_coverage_empty(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    ok, reason = _validate_bucket_trade_coverage(
        db_path=market_db, symbol=SYMBOL, bucket_start_ms=0, bucket_end_ms=H4 - 1,
        edge_tolerance_ms=300_000, max_gap_ms=1_800_000,
    )
    assert not ok
    assert "no trades" in reason


def test_validate_bucket_coverage_edge_gap(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    store = SqliteTradeStore(market_db)
    mid_start = H4 // 4
    mid_end = 3 * H4 // 4
    trades = _dense_trades(mid_start, count=50)
    for t in trades:
        object.__setattr__(t, "symbol", SYMBOL)
    store.save(trades)

    ok, reason = _validate_bucket_trade_coverage(
        db_path=market_db, symbol=SYMBOL, bucket_start_ms=0, bucket_end_ms=H4 - 1,
        edge_tolerance_ms=300_000, max_gap_ms=1_800_000,
    )
    assert not ok


def test_validate_bucket_coverage_large_gap(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    store = SqliteTradeStore(market_db)
    trades = [
        _trade("100", 1000, "early_a"),
        _trade("101", 2000, "early_b"),
        _trade("200", H4 - 2000, "late_a"),
        _trade("201", H4 - 1000, "late_b"),
    ]
    for t in trades:
        object.__setattr__(t, "symbol", SYMBOL)
    store.save(trades)

    ok, reason = _validate_bucket_trade_coverage(
        db_path=market_db, symbol=SYMBOL, bucket_start_ms=0, bucket_end_ms=H4 - 1,
        edge_tolerance_ms=300_000, max_gap_ms=1_800_000,
    )
    assert not ok
    assert "max inter-trade gap" in reason


# ---------------------------------------------------------------------------
# Unit tests: _download_missing_trades, LiveDetector, helpers
# ---------------------------------------------------------------------------

def test_download_missing_trades_skips_current_bucket(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    result = _download_missing_trades(
        raw_symbol=RAW_SYMBOL, symbol=SYMBOL,
        missing_bucket_starts=[H4], bucket_ms=H4,
        current_bucket_start_ms=H4,
        download_func=_fake_full_downloader,
        trade_store=trade_store, limit=100,
        coverage_edge_tolerance_ms=300_000, coverage_max_gap_ms=1_800_000,
        max_pages_per_bucket=None,
    )
    assert result.skipped_buckets == 1
    assert result.downloaded_buckets == 0


def test_download_missing_trades_handles_network_error(tmp_path: Path) -> None:
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    result = _download_missing_trades(
        raw_symbol=RAW_SYMBOL, symbol=SYMBOL,
        missing_bucket_starts=[0], bucket_ms=H4,
        current_bucket_start_ms=2 * H4,
        download_func=_fake_failing_downloader,
        trade_store=trade_store, limit=100,
        coverage_edge_tolerance_ms=300_000, coverage_max_gap_ms=1_800_000,
        max_pages_per_bucket=None,
    )
    assert result.failed_buckets == 1
    assert result.downloaded_buckets == 0
    assert len(result.errors) == 1


def test_okx_downloader_instantiation() -> None:
    dl = OkxHistoricalTradeDownloader(
        base_url="https://www.okx.com", timeout_seconds=30, max_retries=3, sleep_seconds=0.5,
    )
    assert dl._base_url == "https://www.okx.com"
    assert dl._timeout == 30
    assert dl._max_retries == 3
    assert dl._sleep_seconds == 0.5


def test_live_detector_no_pid_files() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        detector = LiveDetector(pid_files=(Path(td) / "nonexistent.pid",), process_names=())
        assert detector.is_live() is False


def test_live_detector_invalid_pid_file() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        pid_file = Path(td) / "bad.pid"
        pid_file.write_text("not_a_number")
        detector = LiveDetector(pid_files=(pid_file,), process_names=())
        assert detector.is_live() is False


def test_live_detector_zero_pid() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        pid_file = Path(td) / "zero.pid"
        pid_file.write_text("0")
        detector = LiveDetector(pid_files=(pid_file,), process_names=())
        assert detector.is_live() is False


def test_live_detector_negative_pid() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        pid_file = Path(td) / "neg.pid"
        pid_file.write_text("-1")
        detector = LiveDetector(pid_files=(pid_file,), process_names=())
        assert detector.is_live() is False


# ---------------------------------------------------------------------------
# _find_buckets_with_complete_aggregates unit tests
# ---------------------------------------------------------------------------

def test_find_buckets_with_complete_aggregates(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    store = SqliteRangeCheckpointStore(checkpoint_db)
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=H4,
    )
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(H4),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=2 * H4,
    )

    found = _find_buckets_with_complete_aggregates(
        checkpoint_db, exchange="okx", symbol=SYMBOL, range_pct="0.002",
        bucket_starts=[0, H4, 2 * H4],
    )
    assert found == {0, H4}


def test_find_buckets_empty(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteRangeCheckpointStore(checkpoint_db)
    found = _find_buckets_with_complete_aggregates(
        checkpoint_db, exchange="okx", symbol=SYMBOL, range_pct="0.002",
        bucket_starts=[0, H4],
    )
    assert found == set()


# ---------------------------------------------------------------------------
# _delete_pollution_rows unit test
# ---------------------------------------------------------------------------

def test_delete_pollution_rows_only_deletes_pollution(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    store = SqliteRangeCheckpointStore(checkpoint_db)
    # Use a base after POLLUTION_CUTOFF_MS so the valid row isn't treated as pollution.
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )
    with sqlite3.connect(checkpoint_db) as conn:
        conn.execute(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms, rf_bar_count,
                coverage_status, missing_gap_ms, completed_at_ms
            ) VALUES ('okx', ?, '0.002', 0, 12345, 1, 'COMPLETE', 0, 1)
            """,
            (SYMBOL,),
        )

    count = _delete_pollution_rows(
        checkpoint_db, exchange="okx", symbol=SYMBOL, range_pct="0.002", dry_run=False,
    )
    assert count == 1
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1
    assert rows[0][3] == base


# ---------------------------------------------------------------------------
# _delete_one_aggregate unit test
# ---------------------------------------------------------------------------

def test_delete_one_aggregate_bucket_scoped(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    store = SqliteRangeCheckpointStore(checkpoint_db)
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(0),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=H4,
    )
    store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(H4),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=2 * H4,
    )

    _delete_one_aggregate(
        checkpoint_db, exchange="okx", symbol=SYMBOL, range_pct="0.002",
        bucket_start_ms=0, bucket_ms=H4,
    )
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1
    assert rows[0][3] == H4


# ---------------------------------------------------------------------------
# download-lookback-buckets
# ---------------------------------------------------------------------------

def test_download_lookback_buckets_overrides_start_ms(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(tmp_path, "--download-lookback-buckets", "10", "--min-buckets", "100")
    summary, _ = repair_range_history.run(args, now_ms=10 * H4)

    assert summary.start_ms == 0
    assert summary.end_ms == 10 * H4 - 1


def test_explicit_start_ms_overrides_lookback(tmp_path: Path) -> None:
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-lookback-buckets", "10",
        "--start-ms", str(2 * H4), "--end-ms", str(3 * H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=10 * H4)

    assert summary.start_ms == 2 * H4
    assert summary.end_ms == 3 * H4 - 1
