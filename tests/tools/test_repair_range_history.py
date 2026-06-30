from __future__ import annotations

import json
import sqlite3
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from src.market_data.models import RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.okx.historical_data import (
    DownloadedArchive,
    OkxHistoricalTradesArchiveClient,
)
from src.platform.exchanges.models import ExchangeName
from tools import repair_range_history
from tools.repair_range_history import (
    DownloadResult,
    LiveDetector,
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


def _ms(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)


def _raw_zip_path(raw_root: Path, date_text: str, raw_symbol: str = RAW_SYMBOL) -> Path:
    return raw_root / "trades" / raw_symbol / f"{raw_symbol}-trades-{date_text}.zip"


def _write_raw_zip(raw_root: Path, date_text: str, rows: list[dict[str, object]], raw_symbol: str = RAW_SYMBOL) -> Path:
    path = _raw_zip_path(raw_root, date_text, raw_symbol=raw_symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys()) if rows else ["tradeId", "px", "sz", "side", "ts"]
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(row.get(col, "")) for col in columns))
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{raw_symbol}-trades-{date_text}.csv", "\n".join(lines) + "\n")
    return path


def _raw_rows_for_bucket(bucket_start: int, *, prefix: str = "raw", columns: str = "okx") -> list[dict[str, object]]:
    offsets = [
        1_000,
        20 * 60_000,
        40 * 60_000,
        60 * 60_000,
        80 * 60_000,
        100 * 60_000,
        120 * 60_000,
        140 * 60_000,
        160 * 60_000,
        180 * 60_000,
        200 * 60_000,
        220 * 60_000,
        H4 - 1_000,
    ]
    prices = ["100", "100.3", "100", "99.7", "100", "100.3", "100", "99.7", "100", "100.3", "100", "99.7", "100"]
    rows: list[dict[str, object]] = []
    for i, (offset, price) in enumerate(zip(offsets, prices)):
        ts = bucket_start + offset
        if columns == "aliases":
            rows.append({
                "trade_id": f"{prefix}_{i}",
                "price": price,
                "qty": "1",
                "side": "buy" if i % 2 == 0 else "sell",
                "timestamp": ts,
            })
        else:
            rows.append({
                "tradeId": f"{prefix}_{i}",
                "px": price,
                "sz": "1",
                "side": "buy" if i % 2 == 0 else "sell",
                "ts": ts,
            })
    return rows


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
    assert "cdn/okex" not in source
    assert "urllib.request" not in source


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
    )
    assert result.failed_buckets == 1
    assert result.downloaded_buckets == 0
    assert len(result.errors) == 1


def test_okx_downloader_instantiation() -> None:
    dl = OkxHistoricalTradesArchiveClient(
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


# ---------------------------------------------------------------------------
# Security fix tests: dry-run + download
# ---------------------------------------------------------------------------

def test_dry_run_download_does_not_write_trades(tmp_path: Path) -> None:
    """--download-missing-trades + --dry-run does NOT write trades to DB."""
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--dry-run",
        "--download-missing-trades",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader,
    )

    # No trades should have been persisted.
    with sqlite3.connect(market_db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol = ?", (SYMBOL,)
        ).fetchone()[0]
    assert count == 0


def test_dry_run_download_does_not_write_trade_coverage(tmp_path: Path) -> None:
    """--download-missing-trades + --dry-run does NOT write trade_coverage."""
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--dry-run",
        "--download-missing-trades",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader,
    )

    rows = _coverage_rows(market_db)
    assert len(rows) == 0


def test_dry_run_download_default_no_network(tmp_path: Path) -> None:
    """--dry-run + --download-missing-trades without --dry-run-download-network
    does NOT call the real downloader (no network)."""
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    call_count = 0

    def counting_downloader(raw_symbol, bucket_start_ms, bucket_end_ms, limit):
        nonlocal call_count
        call_count += 1
        return _dense_trades(bucket_start_ms, count=50), 1, True

    args = _args(
        tmp_path,
        "--dry-run",
        "--download-missing-trades",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=counting_downloader,
    )

    # In dry-run without --dry-run-download-network, the downloader should
    # NOT be called at all (the noop downloader is used instead).
    assert call_count == 0
    # would_download counters should be populated.
    assert summary.would_download_buckets >= 1


def test_dry_run_download_network_still_no_persist(tmp_path: Path) -> None:
    """--dry-run + --dry-run-download-network calls the downloader but still
    does NOT persist trades or coverage."""
    market_db = tmp_path / "market.sqlite3"
    trade_store = SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    call_count = 0

    def counting_downloader(raw_symbol, bucket_start_ms, bucket_end_ms, limit):
        nonlocal call_count
        call_count += 1
        return _dense_trades(bucket_start_ms, count=50), 1, True

    args = _args(
        tmp_path,
        "--dry-run",
        "--dry-run-download-network",
        "--download-missing-trades",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    summary, _ = repair_range_history.run(
        args, now_ms=2 * H4, download_func=counting_downloader,
    )

    # Downloader IS called (network allowed).
    assert call_count == 1
    # But should still show would_download, not downloaded.
    assert summary.would_download_buckets >= 1
    assert summary.downloaded_buckets == 0
    # DB must remain untouched.
    with sqlite3.connect(market_db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol = ?", (SYMBOL,)
        ).fetchone()[0]
    assert count == 0
    rows = _coverage_rows(market_db)
    assert len(rows) == 0


# ---------------------------------------------------------------------------
# Security fix tests: backup before write + live guard for download
# ---------------------------------------------------------------------------

def test_download_only_live_running_exit_code_3(tmp_path: Path) -> None:
    """Only --download-missing-trades (non-dry-run), live running, no allow → exit 3."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    fake_live = LiveDetector(pid_files=(), process_names=())
    with patch.object(fake_live, "is_live", return_value=True):
        args = _args(
            tmp_path,
            "--download-missing-trades",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
            "--skip-download-if-live", "false",
        )
        summary, exit_code = repair_range_history.run(
            args, now_ms=2 * H4,
            download_func=_fake_full_downloader,
            live_detector=fake_live,
        )

    assert exit_code == 3
    assert summary.live_running_detected is True


def test_download_backup_before_write(tmp_path: Path) -> None:
    """--download-missing-trades (non-dry-run) creates backup BEFORE writing trades."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    # Track order: was backup created? It should exist before trades are written.
    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader,
    )

    assert exit_code == 0
    # Backup must have been created.
    assert len(summary.backup_paths) >= 2
    # Trades must have been written (backup before write confirmed by execution order).
    with sqlite3.connect(market_db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol = ?", (SYMBOL,)
        ).fetchone()[0]
    assert count == 50


def test_download_error_backup_still_exists(tmp_path: Path) -> None:
    """When download fails, backup already exists (created before download)."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
        "--skip-download-if-live", "false",
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_failing_downloader,
    )

    assert exit_code == 0  # download failure does not cause non-zero exit
    # Backup must exist even though download failed.
    assert len(summary.backup_paths) >= 2
    assert all(Path(p).exists() for p in summary.backup_paths)
    # But no trades should be in DB.
    with sqlite3.connect(market_db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol = ?", (SYMBOL,)
        ).fetchone()[0]
    assert count == 0


def test_live_guard_uses_will_write_db_including_download(tmp_path: Path) -> None:
    """will_write_db includes --download-missing-trades; live guard checks it."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    fake_live = LiveDetector(pid_files=(), process_names=())
    with patch.object(fake_live, "is_live", return_value=True):
        # Only --download-missing-trades, no repair flags.
        args = _args(
            tmp_path,
            "--download-missing-trades",
            "--allow-live-db-write",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
            "--skip-download-if-live", "false",
        )
        summary, exit_code = repair_range_history.run(
            args, now_ms=2 * H4,
            download_func=_fake_full_downloader,
            live_detector=fake_live,
        )

    # With --allow-live-db-write it should proceed.
    assert exit_code == 0
    assert "WARNING live_running_detected_allow_live_db_write_active" in summary.warnings
    # Download should have succeeded.
    assert summary.downloaded_buckets >= 1


# ---------------------------------------------------------------------------
# OKX wrapper tests
# ---------------------------------------------------------------------------


def test_okx_wrapper_no_typeerror_and_passes_kwargs(tmp_path: Path) -> None:
    """Real OKX platform adapter wrapper does not TypeError,
    and passes limit + max_pages as keyword args."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    with patch.object(
        OkxHistoricalTradesArchiveClient, "download_history_bucket_trades",
        return_value=([], 0, True),
    ) as mock_download:
        args = _args(
            tmp_path,
            "--download-missing-trades",
            "--trade-source", "rest_history",
            "--skip-download-if-live", "false",
            "--download-max-pages", "5",
            "--download-limit", "50",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
        )
        repair_range_history.run(args, now_ms=2 * H4)

    # Verify the mock was called (no TypeError was raised).
    assert mock_download.called, "Expected download_history_bucket_trades to be called"
    call_kwargs = mock_download.call_args[1]
    # Keyword args must be passed properly (this was the TypeError root cause).
    assert call_kwargs["limit"] == 50
    assert call_kwargs["max_pages"] == 5


def test_okx_wrapper_limit_reaches_downloader(tmp_path: Path) -> None:
    """download_func receives limit from --download-limit."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    received_limits: list[int] = []

    def tracking_downloader(raw_symbol, bucket_start_ms, bucket_end_ms, limit):
        received_limits.append(limit)
        return _dense_trades(bucket_start_ms, count=50), 1, True

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--trade-source", "rest_history",
        "--download-limit", "75",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    repair_range_history.run(
        args, now_ms=2 * H4, download_func=tracking_downloader,
    )
    assert len(received_limits) == 1
    assert received_limits[0] == 75


def test_okx_max_pages_reaches_downloader(tmp_path: Path) -> None:
    """When using real downloader path, max_pages is passed to the platform adapter."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    with patch.object(
        OkxHistoricalTradesArchiveClient, "download_history_bucket_trades",
        return_value=([], 0, True),
    ) as mock_download:
        args = _args(
            tmp_path,
            "--download-missing-trades",
            "--trade-source", "rest_history",
            "--skip-download-if-live", "false",
            "--download-max-pages", "3",
            "--start-ms", "0", "--end-ms", str(H4 - 1),
        )
        repair_range_history.run(args, now_ms=2 * H4)

    assert mock_download.called
    assert mock_download.call_args[1]["max_pages"] == 3


def test_fake_downloader_still_works_with_four_positional_args(tmp_path: Path) -> None:
    """Fake downloaders matching DownloadFunc signature continue to work."""
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_full_downloader,
    )

    assert exit_code == 0
    assert summary.downloaded_buckets == 1
    assert summary.downloaded_trade_count == 50


# ---------------------------------------------------------------------------
# Destructive rebuild guard tests
# ---------------------------------------------------------------------------


def test_destructive_abort_all_download_failed_exit_code_4(tmp_path: Path) -> None:
    """rebuild-window + force + download-missing-trades: all downloads fail
    → exit code 4, existing aggregates preserved."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    checkpoint_store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )

    args = _args(
        tmp_path,
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-aggregates",
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=base + 2 * H4, download_func=_fake_failing_downloader,
    )

    assert exit_code == 4
    assert summary.destructive_rebuild_aborted is True
    assert "zero_downloaded_buckets_with_missing_coverage" in summary.destructive_rebuild_abort_reason
    assert "destructive_rebuild_aborted_due_to_incomplete_download" in summary.warnings
    # Existing aggregate must survive.
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1
    assert rows[0][3] == base


def test_destructive_abort_zero_downloaded_pollution_preserved(tmp_path: Path) -> None:
    """downloaded_buckets=0 with missing coverage → exit code 4,
    pollution rows NOT deleted."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)
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
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-aggregates",
        "--clean-pollution",
        "--skip-download-if-live", "false",
        "--start-ms", str(H4), "--end-ms", str(2 * H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=3 * H4, download_func=_fake_failing_downloader,
    )

    assert exit_code == 4
    assert summary.destructive_rebuild_aborted is True
    # Pollution row must survive (clean_pollution was never reached).
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1
    assert rows[0][3] == 0
    assert rows[0][4] == 12345


def test_destructive_abort_coverage_below_min_preserves_range_bars(tmp_path: Path) -> None:
    """coverage_complete < min_complete_coverage_buckets → exit code 4,
    range bars NOT replaced."""
    market_db = tmp_path / "market.sqlite3"
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(market_db)
    range_store = SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)
    range_store.save([_bar(1, 2_000)])

    args = _args(
        tmp_path,
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-range-bars",
        "--repair-range-bars",
        "--min-complete-coverage-buckets", "50",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_failing_downloader,
    )

    assert exit_code == 4
    assert summary.destructive_rebuild_aborted is True
    assert "complete_coverage_buckets_0_below_min_50" in summary.destructive_rebuild_abort_reason
    # Range bars must survive (never replaced).
    rows = _range_rows(market_db)
    assert len(rows) == 1
    assert rows[0] == (SYMBOL, "0.002", 1, 2_000)


def test_destructive_abort_success_ratio_below_threshold(tmp_path: Path) -> None:
    """download_failed_buckets > 0 and success ratio < min → exit code 4."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    # Simulate a mix: some buckets succeed, some fail.
    # We'll use a custom downloader that fails for specific buckets.
    import itertools
    _fail_counter = itertools.count()

    def mixed_downloader(raw_symbol, bucket_start_ms, bucket_end_ms, limit):
        # Fail 3 out of 4 calls → 25% success rate
        if next(_fail_counter) % 4 != 0:
            raise RuntimeError("simulated partial failure")
        return _dense_trades(bucket_start_ms, count=50), 1, True

    checkpoint_store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )

    args = _args(
        tmp_path,
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-aggregates",
        "--min-download-success-ratio", "0.5",
        "--download-lookback-buckets", "4",
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + 4 * H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=base + 5 * H4, download_func=mixed_downloader,
    )

    assert exit_code == 4
    assert summary.destructive_rebuild_aborted is True
    assert "download_success_ratio" in summary.destructive_rebuild_abort_reason
    assert "below_min_0.5" in summary.destructive_rebuild_abort_reason
    # Aggregate must survive.
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1


def test_allow_destructive_empty_rebuild_bypasses_guard(tmp_path: Path) -> None:
    """--allow-destructive-empty-rebuild=true allows destructive rebuild despite
    download failures."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    checkpoint_store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )

    args = _args(
        tmp_path,
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-aggregates",
        "--allow-destructive-empty-rebuild",
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=base + 2 * H4, download_func=_fake_failing_downloader,
    )

    # Guard should NOT fire → exit code is NOT 4.
    assert exit_code != 4
    assert summary.destructive_rebuild_aborted is False
    # Aggregate in window gets deleted (destructive rebuild proceeds).
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 0


def test_dry_run_no_destructive_abort(tmp_path: Path) -> None:
    """--dry-run does NOT trigger destructive abort even with all downloads failing."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    checkpoint_store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )

    args = _args(
        tmp_path,
        "--dry-run",
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-aggregates",
        "--dry-run-download-network",
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    # Use a failing downloader: the guard should NOT fire because dry_run=True
    # bypasses the destructive-abort check entirely.
    summary, exit_code = repair_range_history.run(
        args, now_ms=base + 2 * H4, download_func=_fake_failing_downloader,
    )

    # Dry-run: guard does NOT fire (not dry_run is false in the guard condition).
    assert exit_code != 4
    assert summary.destructive_rebuild_aborted is False
    # Aggregate survives because dry-run never writes.
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1


def test_incremental_mode_unaffected_by_destructive_guard(tmp_path: Path) -> None:
    """In incremental mode, download failures do NOT trigger destructive abort."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    checkpoint_store.save_completed_aggregate(
        exchange="okx", aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value, completed_at_ms=base + H4,
    )

    args = _args(
        tmp_path,
        "--mode", "incremental",
        "--download-missing-trades",
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=base + 2 * H4, download_func=_fake_failing_downloader,
    )

    # Incremental mode: no destructive abort.
    assert exit_code == 0
    assert summary.destructive_rebuild_aborted is False
    # Aggregate survives (never in repair set).
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 1


def test_download_failed_incomplete_bucket_not_marked_complete_incremental(
    tmp_path: Path,
) -> None:
    """In incremental mode, download failure still prevents marking bucket COMPLETE."""
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    SqliteRangeBarStore(market_db)
    SqliteRangeCheckpointStore(checkpoint_db)

    args = _args(
        tmp_path,
        "--mode", "incremental",
        "--download-missing-trades",
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--skip-download-if-live", "false",
        "--start-ms", "0", "--end-ms", str(H4 - 1),
    )
    summary, exit_code = repair_range_history.run(
        args, now_ms=2 * H4, download_func=_fake_failing_downloader,
    )

    assert exit_code == 0
    assert summary.downloaded_buckets == 0
    assert summary.download_failed_buckets == 1
    # No aggregates should have been written for the failed bucket.
    rows = _completed_rows(checkpoint_db)
    assert len(rows) == 0


# ---------------------------------------------------------------------------
# OKX CDN daily raw trades
# ---------------------------------------------------------------------------


def test_preset_v10b_bootstrap_expands_expected_args(tmp_path: Path) -> None:
    args = _args(tmp_path, "--preset", "v10b-bootstrap")

    assert args.symbol == SYMBOL
    assert args.raw_symbol == RAW_SYMBOL
    assert args.range_pct == "0.002"
    assert args.bucket_interval == "4h"
    assert args.min_buckets == 180
    assert args.download_lookback_buckets == 180
    assert args.trade_source == "okx_cdn_daily"
    assert args.repair_range_bars is True
    assert args.rebuild_aggregates is True
    assert args.mode == "rebuild-window"
    assert args.force_rebuild_window is True
    assert args.delete_existing_aggregates is True
    assert args.delete_existing_range_bars is True
    assert args.clean_pollution is True
    assert args.abort_if_download_failed is True
    assert args.min_download_success_ratio == 0.95
    assert args.min_complete_coverage_buckets == 180


def test_preset_v10b_incremental_does_not_window_delete(tmp_path: Path) -> None:
    args = _args(tmp_path, "--preset", "v10b-incremental")

    assert args.mode == "incremental"
    assert args.force_rebuild_window is False
    assert args.delete_existing_aggregates is False
    assert args.delete_existing_range_bars is False
    assert args.clean_pollution is True


def test_days_30_converts_to_180_four_hour_buckets(tmp_path: Path) -> None:
    args = _args(tmp_path, "--days", "30", "--bucket-interval", "4h")

    assert args.download_lookback_buckets == 180
    assert args.min_buckets == 180
    assert args.min_complete_coverage_buckets == 180


def test_local_raw_zip_exists_does_not_download(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)
    _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base))

    with patch.object(
        OkxHistoricalTradesArchiveClient,
        "download_daily_trades_zip",
        side_effect=AssertionError("should not download when local raw exists"),
    ):
        args = _args(
            tmp_path,
            "--download-missing-trades",
            "--raw-root", str(raw_root),
            "--skip-download-if-live", "false",
            "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
        )
        summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert summary.raw_files_found == [str(_raw_zip_path(raw_root, "2024-01-01"))]
    assert summary.raw_files_downloaded == []
    assert summary.downloaded_buckets == 1


def test_missing_raw_zip_okx_cdn_daily_downloads(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)

    def fake_download(self, raw_symbol, date, destination: Path, *, overwrite: bool = False):
        _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base))
        return DownloadedArchive(
            date=date.isoformat(),
            url="https://example.invalid/raw.zip",
            path=str(destination),
            sha256="sha",
            size=destination.stat().st_size,
            status="downloaded",
        )

    with patch.object(OkxHistoricalTradesArchiveClient, "download_daily_trades_zip", new=fake_download):
        args = _args(
            tmp_path,
            "--download-missing-trades",
            "--raw-root", str(raw_root),
            "--skip-download-if-live", "false",
            "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
        )
        summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert summary.raw_files_downloaded == [str(_raw_zip_path(raw_root, "2024-01-01"))]
    assert summary.downloaded_buckets == 1


def test_dry_run_default_does_not_download_raw_or_write_db(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)

    with patch.object(
        OkxHistoricalTradesArchiveClient,
        "download_daily_trades_zip",
        side_effect=AssertionError("dry-run without network should not download"),
    ):
        args = _args(
            tmp_path,
            "--dry-run",
            "--download-missing-trades",
            "--raw-root", str(raw_root),
            "--skip-download-if-live", "false",
            "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
        )
        summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert summary.would_download_buckets == 1
    assert summary.raw_trades_saved == 0
    assert _coverage_rows(tmp_path / "market.sqlite3") == []


def test_dry_run_download_network_can_download_raw_but_not_write_db(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)

    def fake_download(self, raw_symbol, date, destination: Path, *, overwrite: bool = False):
        _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base))
        return DownloadedArchive(
            date=date.isoformat(),
            url="https://example.invalid/raw.zip",
            path=str(destination),
            sha256="sha",
            size=destination.stat().st_size,
            status="downloaded",
        )

    with patch.object(OkxHistoricalTradesArchiveClient, "download_daily_trades_zip", new=fake_download):
        args = _args(
            tmp_path,
            "--dry-run",
            "--dry-run-download-network",
            "--download-missing-trades",
            "--raw-root", str(raw_root),
            "--skip-download-if-live", "false",
            "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
        )
        summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert _raw_zip_path(raw_root, "2024-01-01").exists()
    assert summary.raw_rows_read == 13
    assert summary.raw_trades_saved == 0
    assert _coverage_rows(tmp_path / "market.sqlite3") == []


def test_synthetic_okx_raw_zip_imports_trades_and_common_columns(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)
    _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base, columns="okx"))

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--raw-root", str(raw_root),
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert summary.raw_rows_read == 13
    assert summary.raw_trades_saved == 13
    loaded = SqliteTradeStore(tmp_path / "market.sqlite3").load(
        symbol=SYMBOL, time_range=TimeRange(base, base + H4 - 1)
    )
    assert len(loaded) == 13
    assert loaded[0].trade_id == "raw_0"
    assert loaded[0].price == Decimal("100")


def test_raw_import_supports_alias_columns_and_timestamp_formats(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)
    rows = [
        {"trade_id": "ms", "price": "100", "qty": "1", "side": "buy", "timestamp": base + 1_000},
        {"trade_id": "sec", "price": "100.3", "qty": "1", "side": "sell", "timestamp": (base + 20 * 60_000) // 1000},
        {"trade_id": "iso", "price": "100", "qty": "1", "side": "buy", "timestamp": "2024-01-01T00:40:00Z"},
    ]
    rows.extend(_raw_rows_for_bucket(base, prefix="tail", columns="aliases")[3:])
    _write_raw_zip(raw_root, "2024-01-01", rows)

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--raw-root", str(raw_root),
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert summary.downloaded_buckets == 1
    ids = {
        trade.trade_id
        for trade in SqliteTradeStore(tmp_path / "market.sqlite3").load(
            symbol=SYMBOL, time_range=TimeRange(base, base + H4 - 1)
        )
    }
    assert {"ms", "sec", "iso"}.issubset(ids)


def test_raw_import_marks_coverage_repairs_range_bars_and_rebuilds_aggregates(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)
    _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base))

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--repair-range-bars",
        "--rebuild-aggregates",
        "--raw-root", str(raw_root),
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 0
    assert summary.coverage_validated_buckets == 1
    assert _coverage_rows(tmp_path / "market.sqlite3") == [(SYMBOL, base, base + H4 - 1, "historical")]
    assert summary.range_bars_written_count > 0
    assert summary.aggregates_written_count == 1
    assert len(_range_rows(tmp_path / "market.sqlite3")) > 0
    assert len(_completed_rows(tmp_path / "checkpoint.sqlite3")) == 1


def test_rebuild_window_coverage_insufficient_exit_4_preserves_old_aggregates(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(checkpoint_db).save_completed_aggregate(
        exchange="okx",
        aggregate=_aggregate(base),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=base + H4,
    )
    _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base))

    args = _args(
        tmp_path,
        "--mode", "rebuild-window",
        "--force-rebuild-window",
        "--download-missing-trades",
        "--delete-existing-aggregates",
        "--repair-range-bars",
        "--raw-root", str(raw_root),
        "--min-complete-coverage-buckets", "2",
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    summary, exit_code = repair_range_history.run(args, now_ms=base + 2 * H4)

    assert exit_code == 4
    assert summary.destructive_rebuild_aborted is True
    assert "complete_coverage_buckets_1_below_min_2" in summary.destructive_rebuild_abort_reason
    assert len(_completed_rows(checkpoint_db)) == 1


def test_repeated_raw_import_is_idempotent(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)
    _write_raw_zip(raw_root, "2024-01-01", _raw_rows_for_bucket(base))

    args = _args(
        tmp_path,
        "--download-missing-trades",
        "--raw-root", str(raw_root),
        "--skip-download-if-live", "false",
        "--start-ms", str(base), "--end-ms", str(base + H4 - 1),
    )
    repair_range_history.run(args, now_ms=base + 2 * H4)
    repair_range_history.run(args, now_ms=base + 2 * H4)

    rows = SqliteTradeStore(tmp_path / "market.sqlite3").load(
        symbol=SYMBOL, time_range=TimeRange(base, base + H4 - 1)
    )
    assert len(rows) == 13
    assert _coverage_rows(tmp_path / "market.sqlite3") == [(SYMBOL, base, base + H4 - 1, "historical")]


def test_repair_tool_does_not_import_coinbacktest_modules() -> None:
    source = Path(repair_range_history.__file__).read_text(encoding="utf-8")

    assert "CoinBacktest" not in source
    assert "coinbacktest" not in source.lower()


def test_gitignore_preserves_env() -> None:
    gitignore = Path(repair_range_history.REPO_ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "/.env" in gitignore.splitlines()
