from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@dataclass(frozen=True)
class DatabaseStats:
    file_size: int
    trades_rows: int
    trade_coverage_rows: int
    page_count: int
    freelist_count: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Safely remove optional raw-trade cache rows from the market-data SQLite DB."
    )
    parser.add_argument(
        "--db",
        default="data/market_data/aether_market_data.sqlite3",
        help="Path to the market-data SQLite database.",
    )
    parser.add_argument(
        "--drop-raw-trades",
        action="store_true",
        help="Delete all rows from trades and trade_coverage.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create a consistent SQLite backup before executing changes.",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM after deletion to return free pages to the filesystem.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute requested changes. Without this flag the tool is dry-run only.",
    )
    return parser


def collect_stats(db_path: str | Path) -> DatabaseStats:
    path = Path(db_path)
    with sqlite3.connect(path, timeout=30) as conn:
        return DatabaseStats(
            file_size=path.stat().st_size,
            trades_rows=_table_row_count(conn, "trades"),
            trade_coverage_rows=_table_row_count(conn, "trade_coverage"),
            page_count=int(conn.execute("PRAGMA page_count").fetchone()[0]),
            freelist_count=int(conn.execute("PRAGMA freelist_count").fetchone()[0]),
        )


def print_stats(title: str, stats: DatabaseStats) -> None:
    print(title, flush=True)
    print(
        f"DB file size: {stats.file_size} bytes ({_human_size(stats.file_size)})",
        flush=True,
    )
    print(f"trades row count: {stats.trades_rows}", flush=True)
    print(f"trade_coverage row count: {stats.trade_coverage_rows}", flush=True)
    print(f"page_count: {stats.page_count}", flush=True)
    print(f"freelist_count: {stats.freelist_count}", flush=True)


def backup_database(db_path: str | Path) -> Path:
    source_path = Path(db_path)
    backup_dir = source_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    backup_path = backup_dir / f"{source_path.name}.before_cleanup.{timestamp}"
    with (
        sqlite3.connect(source_path, timeout=30) as source,
        sqlite3.connect(backup_path, timeout=30) as destination,
    ):
        source.backup(destination)
    return backup_path


def delete_raw_trade_cache(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN IMMEDIATE")
        try:
            if _table_exists(conn, "trades"):
                conn.execute("DELETE FROM trades")
            if _table_exists(conn, "trade_coverage"):
                conn.execute("DELETE FROM trade_coverage")
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()


def vacuum_database(db_path: str | Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("VACUUM")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = Path(args.db).expanduser()
    print(
        "WARNING: Stop the AetherEdge live process before cleanup; "
        "this tool will not terminate running processes.",
        flush=True,
    )
    if not db_path.is_file():
        print(f"ERROR: database file does not exist: {db_path}", file=sys.stderr)
        return 2

    before = collect_stats(db_path)
    print_stats("Before cleanup", before)

    if not args.execute:
        print("Mode: dry-run; no database changes or backup were made.", flush=True)
        if args.backup:
            print(
                f"Planned backup directory: {db_path.parent / 'backups'}",
                flush=True,
            )
        if args.drop_raw_trades:
            print("Planned cleanup: DELETE FROM trades; DELETE FROM trade_coverage;", flush=True)
        if args.vacuum:
            print("Planned compaction: VACUUM;", flush=True)
        print_stats("After cleanup (dry-run, unchanged)", collect_stats(db_path))
        return 0

    if not (args.drop_raw_trades or args.vacuum or args.backup):
        print("No action requested; database was not changed.", flush=True)
        print_stats("After cleanup (unchanged)", collect_stats(db_path))
        return 0

    try:
        if args.backup:
            backup_path = backup_database(db_path)
            print(f"Backup created: {backup_path}", flush=True)
        if args.drop_raw_trades:
            delete_raw_trade_cache(db_path)
            print("Raw trades cache cleared.", flush=True)
        if args.vacuum:
            print(
                "Running VACUUM; this can require substantial temporary disk space.",
                flush=True,
            )
            vacuum_database(db_path)
            print("VACUUM completed.", flush=True)
    except sqlite3.Error as exc:
        print(f"ERROR: SQLite cleanup failed: {exc}", file=sys.stderr, flush=True)
        return 1
    except OSError as exc:
        print(f"ERROR: cleanup failed: {exc}", file=sys.stderr, flush=True)
        return 1

    print_stats("After cleanup", collect_stats(db_path))
    return 0


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_row_count(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


def _human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"


if __name__ == "__main__":
    raise SystemExit(main())
