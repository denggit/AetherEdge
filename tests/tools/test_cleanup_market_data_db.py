from __future__ import annotations

import sqlite3

from tools import cleanup_market_data_db as tool


def _create_market_db(path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE trades (trade_key TEXT PRIMARY KEY);
            CREATE TABLE trade_coverage (
                symbol TEXT NOT NULL,
                start_time_ms INTEGER NOT NULL,
                end_time_ms INTEGER NOT NULL,
                source TEXT NOT NULL
            );
            CREATE TABLE klines (id INTEGER PRIMARY KEY);
            CREATE TABLE range_bars (id INTEGER PRIMARY KEY);

            INSERT INTO trades (trade_key) VALUES ('one'), ('two');
            INSERT INTO trade_coverage (
                symbol, start_time_ms, end_time_ms, source
            ) VALUES (
                'ETH-USDT-PERP', 1, 2, 'historical_current_bucket'
            );
            INSERT INTO klines (id) VALUES (1);
            INSERT INTO range_bars (id) VALUES (1);
            """
        )


def _counts(path) -> tuple[int, int, int, int]:
    with sqlite3.connect(path) as conn:
        return (
            int(conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]),
            int(conn.execute("SELECT COUNT(*) FROM trade_coverage").fetchone()[0]),
            int(conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0]),
            int(conn.execute("SELECT COUNT(*) FROM range_bars").fetchone()[0]),
        )


def test_cleanup_dry_run_does_not_delete_or_backup(tmp_path, capsys) -> None:
    db_path = tmp_path / "market.sqlite3"
    _create_market_db(db_path)

    result = tool.main(
        [
            "--db",
            str(db_path),
            "--drop-raw-trades",
            "--backup",
            "--vacuum",
        ]
    )

    output = capsys.readouterr().out
    assert result == 0
    assert _counts(db_path) == (2, 1, 1, 1)
    assert not (tmp_path / "backups").exists()
    assert "Mode: dry-run" in output
    assert "Before cleanup" in output
    assert "After cleanup (dry-run, unchanged)" in output
    assert "trades row count: 2" in output
    assert "trade_coverage row count: 1" in output
    assert "page_count:" in output
    assert "freelist_count:" in output


def test_cleanup_execute_deletes_only_raw_trade_cache_and_creates_backup(
    tmp_path,
    capsys,
) -> None:
    db_path = tmp_path / "market.sqlite3"
    _create_market_db(db_path)

    result = tool.main(
        [
            "--db",
            str(db_path),
            "--drop-raw-trades",
            "--backup",
            "--execute",
        ]
    )

    output = capsys.readouterr().out
    backups = list(
        (tmp_path / "backups").glob(
            "market.sqlite3.before_cleanup.*"
        )
    )
    assert result == 0
    assert _counts(db_path) == (0, 0, 1, 1)
    assert len(backups) == 1
    assert _counts(backups[0]) == (2, 1, 1, 1)
    assert "Backup created:" in output
    assert "Raw trades cache cleared." in output
    assert "After cleanup" in output
