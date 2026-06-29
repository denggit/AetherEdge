from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from src.market_data.models import RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from tools import repair_range_history


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
        "--start-ms",
        "0",
        "--end-ms",
        str(H4 - 1),
    )
    summary, exit_code = repair_range_history.run(args, now_ms=2 * H4)

    assert exit_code == 0
    assert summary.dry_run is True
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
    assert len({row[2] for row in rows}) == 4


def test_partial_coverage_bucket_is_not_marked_complete(tmp_path: Path) -> None:
    trade_store = SqliteTradeStore(tmp_path / "market.sqlite3")
    SqliteRangeBarStore(tmp_path / "market.sqlite3")
    SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    trade_store.save(_closing_trades(0) + _closing_trades(H4, prefix="b"))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(0, H4 - 1))
    trade_store.mark_coverage(symbol=SYMBOL, time_range=TimeRange(H4, H4 + 10_000))

    args = _args(tmp_path, "--repair-range-bars", "--start-ms", "0", "--end-ms", str(2 * H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    rows = _completed_rows(tmp_path / "checkpoint.sqlite3")
    assert summary.trade_coverage_complete_buckets == 1
    assert summary.missing_trade_coverage_buckets == 1
    assert len(rows) == 1
    assert rows[0][3] == 0


def test_delete_existing_aggregates_is_scoped_and_cleans_pollution(tmp_path: Path) -> None:
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    base = repair_range_history.POLLUTION_CUTOFF_MS + (
        H4 - repair_range_history.POLLUTION_CUTOFF_MS % H4
    )
    SqliteTradeStore(tmp_path / "market.sqlite3").mark_coverage(symbol=SYMBOL, time_range=TimeRange(base, base + H4 - 1))
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
        "--start-ms",
        str(base),
        "--end-ms",
        str(base + H4 - 1),
    )
    summary, _ = repair_range_history.run(args, now_ms=base + 3 * H4)

    rows = _completed_rows(checkpoint_db)
    assert summary.legacy_or_test_polluted_completed_aggregates_detected is True
    assert summary.polluted_completed_aggregates_deleted == 1
    assert all(not (row[0] == "okx" and row[1] == SYMBOL and row[2] == "0.002" and row[4] == 12_345) for row in rows)
    assert ("binance", SYMBOL, "0.002", base, base + H4 - 1, "COMPLETE") in rows
    assert ("okx", "BTC-USDT-PERP", "0.002", base, base + H4 - 1, "COMPLETE") in rows
    assert ("okx", SYMBOL, "0.003", base, base + H4 - 1, "COMPLETE") in rows


def test_delete_existing_range_bars_replaces_only_target_bucket(tmp_path: Path) -> None:
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
        "--start-ms",
        "0",
        "--end-ms",
        str(H4 - 1),
    )
    repair_range_history.run(args, now_ms=3 * H4)

    rows = _range_rows(market_db)
    assert (SYMBOL, "0.002", 99, 3_000) not in rows
    assert (SYMBOL, "0.002", 100, H4 + 1_000) in rows
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

    args = _args(tmp_path, "--delete-existing-aggregates", "--start-ms", str(H4), "--end-ms", str(2 * H4 - 1))
    summary, _ = repair_range_history.run(args, now_ms=3 * H4)

    assert summary.legacy_or_test_polluted_completed_aggregates_detected is True
    assert summary.polluted_completed_aggregates_deleted == 1
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
