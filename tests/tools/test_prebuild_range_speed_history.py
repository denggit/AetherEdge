from __future__ import annotations

import sqlite3

from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from tools import prebuild_range_speed_history as tool


def test_prebuild_defaults_do_not_require_db_args(monkeypatch) -> None:
    monkeypatch.delenv("AETHER_MARKET_DATA_DB", raising=False)

    args = tool.build_parser().parse_args(["--check-only"])
    request = tool.request_from_args(args)

    assert request.symbol == "ETH-USDT-PERP"
    assert str(request.market_db_path).endswith("aether_market_data.sqlite3")
    assert request.save_raw_trades is True
    assert request.chunk_sleep_seconds == 0.0
    assert request.max_seconds_per_cycle == 0.0
    assert request.max_trades_per_cycle == 0
    assert request.max_buckets_per_cycle == 6
    assert request.max_days_per_cycle == 2


def test_prebuild_buckets_100_takes_effect() -> None:
    args = tool.build_parser().parse_args(["--buckets", "100"])
    request = tool.request_from_args(args)

    assert request.required_buckets == 100
    assert request.lookback_buckets == 100
    assert request.max_buckets_per_cycle == 6


def test_prebuild_batch_args_control_cycle_size() -> None:
    args = tool.build_parser().parse_args(["--buckets", "100", "--batch-buckets", "12", "--batch-days", "3"])
    request = tool.request_from_args(args)

    assert request.required_buckets == 100
    assert request.max_buckets_per_cycle == 12
    assert request.max_days_per_cycle == 3


def test_prebuild_exits_when_live_worker_lock_exists(tmp_path) -> None:
    lock_path = tmp_path / "range.lock"
    status_path = tmp_path / "status.json"
    lock_path.write_text("running", encoding="utf-8")
    RangeBackfillStatusStore(status_path).write(
        {"running": True, "heartbeat_ms": now_ms(), "phase": "sleeping"}
    )

    result = tool.main(
        [
            "--buckets",
            "1",
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(tmp_path / "checkpoint.sqlite3"),
            "--raw-root",
            str(tmp_path / "raw"),
            "--status-path",
            str(status_path),
            "--lock-path",
            str(lock_path),
            "--no-download",
        ]
    )

    assert result == 1


def test_prebuild_no_download_missing_raw_prints_clear_summary(tmp_path, capsys) -> None:
    result = tool.main(
        [
            "--buckets",
            "1",
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(tmp_path / "checkpoint.sqlite3"),
            "--raw-root",
            str(tmp_path / "raw"),
            "--status-path",
            str(tmp_path / "status.json"),
            "--lock-path",
            str(tmp_path / "range.lock"),
            "--no-download",
        ]
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "Range speed prebuild started" in output
    assert "Coverage before |" in output
    assert "status: no_progress" in output
    assert "missing_raw_days:" in output
    assert "failed_downloads:" in output
    assert "hint: raw OKX trades zip missing; run downloader or remove --no-download" in output


def test_check_only_check_raw_prints_raw_diagnostics(tmp_path, capsys) -> None:
    result = tool.main(
        [
            "--check-only",
            "--check-raw",
            "--buckets",
            "1",
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(tmp_path / "checkpoint.sqlite3"),
            "--raw-root",
            str(tmp_path / "raw"),
            "--status-path",
            str(tmp_path / "status.json"),
            "--lock-path",
            str(tmp_path / "range.lock"),
        ]
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "raw_required_days=" in output
    assert "raw_missing_days=" in output
    assert "first_missing_raw_day=" in output


def test_clean_suspicious_deletes_bad_bucket_rows(tmp_path, capsys) -> None:
    checkpoint = tmp_path / "checkpoint.sqlite3"
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            """
            CREATE TABLE completed_range_aggregates (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                range_pct TEXT NOT NULL,
                bucket_start_ms INTEGER NOT NULL,
                bucket_end_ms INTEGER NOT NULL,
                rf_bar_count INTEGER NOT NULL,
                imbalance TEXT,
                close_pos TEXT,
                taker_buy_ratio TEXT,
                micro_return_pct TEXT,
                delta_notional_sum TEXT,
                notional_sum TEXT,
                coverage_status TEXT NOT NULL,
                missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                completed_at_ms INTEGER NOT NULL,
                PRIMARY KEY (exchange, symbol, range_pct, bucket_end_ms)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                rf_bar_count, coverage_status, missing_gap_ms, completed_at_ms
            ) VALUES ('okx', 'ETH-USDT-PERP', '0.002', 0, 14399999, 1, 'COMPLETE', 0, 1)
            """
        )

    result = tool.main(
        [
            "--buckets",
            "1",
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(checkpoint),
            "--raw-root",
            str(tmp_path / "raw"),
            "--status-path",
            str(tmp_path / "status.json"),
            "--lock-path",
            str(tmp_path / "range.lock"),
            "--backup-dir",
            str(tmp_path / "data" / "state" / "backups"),
            "--no-download",
            "--clean-suspicious",
        ]
    )

    output = capsys.readouterr().out
    with sqlite3.connect(checkpoint) as conn:
        count = conn.execute("SELECT COUNT(*) FROM completed_range_aggregates").fetchone()[0]
    backup_dir = tmp_path / "data" / "state" / "backups"
    backups = sorted(backup_dir.glob("checkpoint.*.sqlite3"))
    assert result == 0
    assert "SQLite backup path |" in output
    assert f"backup={backups[0]}" in output
    assert "Suspicious aggregates cleaned | deleted=1" in output
    assert count == 0
    assert len(backups) == 1
    assert not list(backup_dir.glob("*-wal"))
    assert not list(backup_dir.glob("*-shm"))


def test_clean_suspicious_backup_retention_keeps_recent_five(tmp_path, capsys) -> None:
    checkpoint = tmp_path / "checkpoint.sqlite3"
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            """
            CREATE TABLE completed_range_aggregates (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                range_pct TEXT NOT NULL,
                bucket_start_ms INTEGER NOT NULL,
                bucket_end_ms INTEGER NOT NULL,
                rf_bar_count INTEGER NOT NULL,
                imbalance TEXT,
                close_pos TEXT,
                taker_buy_ratio TEXT,
                micro_return_pct TEXT,
                delta_notional_sum TEXT,
                notional_sum TEXT,
                coverage_status TEXT NOT NULL,
                missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                completed_at_ms INTEGER NOT NULL,
                PRIMARY KEY (exchange, symbol, range_pct, bucket_end_ms)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO completed_range_aggregates (
                exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                rf_bar_count, coverage_status, missing_gap_ms, completed_at_ms
            ) VALUES ('okx', 'ETH-USDT-PERP', '0.002', 0, 14399999, 1, 'COMPLETE', 0, 1)
            """
        )
    backup_dir = tmp_path / "data" / "state" / "backups"
    backup_dir.mkdir(parents=True)
    for idx in range(7):
        stale = backup_dir / f"checkpoint.20000101T00000{idx}000000Z.sqlite3"
        stale.write_text("old", encoding="utf-8")

    tool.main(
        [
            "--buckets",
            "1",
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(checkpoint),
            "--raw-root",
            str(tmp_path / "raw"),
            "--status-path",
            str(tmp_path / "status.json"),
            "--lock-path",
            str(tmp_path / "range.lock"),
            "--backup-dir",
            str(backup_dir),
            "--no-download",
            "--clean-suspicious",
        ]
    )

    capsys.readouterr()
    backups = sorted(backup_dir.glob("checkpoint.*.sqlite3"))
    assert len(backups) == 5
