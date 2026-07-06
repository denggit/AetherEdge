#!/usr/bin/env python
"""Inspect persisted MF feature-history coverage without loading full rows."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_MARKET_DB = "data/market_data/aether_market_data.sqlite3"
OKX_TIMEZONE = timezone(timedelta(hours=8))

_FEATURE_TABLES = (
    ("tradebar_1m_features", "open_time_ms", "open_time_ms"),
    ("trade_footprint_1m_features", "open_time_ms", "open_time_ms"),
    ("range_footprint_features", "range_start_ms", "available_time_ms"),
)
_COVERAGE_TABLE = "range_footprint_backfill_coverage"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Report persisted MF feature-history counts and UTC+8 coverage "
            "boundaries without loading full rows."
        )
    )
    parser.add_argument("--market-db", default=DEFAULT_MARKET_DB)
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--exchange", default="okx")
    parser.add_argument("--range-pct", type=_decimal_arg, default="0.002")
    parser.add_argument("--price-step", type=_decimal_arg, default="1")
    return parser


def format_okx_time(value_ms: int | None) -> str:
    if value_ms is None:
        return "unavailable"
    return datetime.fromtimestamp(
        int(value_ms) / 1_000,
        timezone.utc,
    ).astimezone(OKX_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S+08")


def collect_history(
    connection: sqlite3.Connection,
    *,
    symbol: str,
    exchange: str,
    range_pct: str,
    price_step: str,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for table, start_column, end_column in _FEATURE_TABLES:
        filters: tuple[tuple[str, str], ...] = (
            ("exchange", exchange),
            ("symbol", symbol),
        )
        if table == "range_footprint_features":
            filters += (
                ("range_pct", range_pct),
                ("price_step", price_step),
            )
        results[table] = _query_summary(
            connection,
            table=table,
            start_column=start_column,
            end_column=end_column,
            filters=filters,
        )

    results[_COVERAGE_TABLE] = _query_summary(
        connection,
        table=_COVERAGE_TABLE,
        start_column="start_time_ms",
        end_column="end_time_ms",
        filters=(
            ("exchange", exchange),
            ("symbol", symbol),
            ("range_pct", range_pct),
            ("price_step", price_step),
        ),
        include_complete_rows=True,
    )
    return results


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    market_db = Path(args.market_db).expanduser()
    if not market_db.is_absolute():
        market_db = REPO_ROOT / market_db
    market_db = market_db.resolve()

    try:
        with sqlite3.connect(
            market_db.as_uri() + "?mode=ro",
            uri=True,
        ) as connection:
            results = collect_history(
                connection,
                symbol=str(args.symbol),
                exchange=str(args.exchange).lower(),
                range_pct=str(args.range_pct),
                price_step=str(args.price_step),
            )
    except sqlite3.Error as exc:
        print(
            f"unable to inspect market DB {market_db}: {exc}",
            file=sys.stderr,
        )
        return 1

    print(f"market_db={market_db}")
    print(
        "filters "
        f"symbol={args.symbol} "
        f"exchange={str(args.exchange).lower()} "
        f"range_pct={args.range_pct} "
        f"price_step={args.price_step}"
    )
    for table, _, _ in _FEATURE_TABLES:
        summary = results[table]
        print(
            f"{table} "
            f"count={_display(summary.get('count'))} "
            f"start={format_okx_time(summary.get('start_ms'))} "
            f"end={format_okx_time(summary.get('end_ms'))}"
            f"{_error_suffix(summary)}"
        )
    coverage = results[_COVERAGE_TABLE]
    print(
        f"{_COVERAGE_TABLE} "
        f"count={_display(coverage.get('count'))} "
        f"min={format_okx_time(coverage.get('start_ms'))} "
        f"max={format_okx_time(coverage.get('end_ms'))} "
        f"complete_rows={_display(coverage.get('complete_rows'))}"
        f"{_error_suffix(coverage)}"
    )
    return 0


def _query_summary(
    connection: sqlite3.Connection,
    *,
    table: str,
    start_column: str,
    end_column: str,
    filters: tuple[tuple[str, str], ...],
    include_complete_rows: bool = False,
) -> dict[str, Any]:
    where_clause = " AND ".join(
        f"{column} = ?" for column, _ in filters
    )
    complete_expression = (
        ", SUM(CASE WHEN complete = 1 THEN 1 ELSE 0 END)"
        if include_complete_rows
        else ""
    )
    query = (
        f"SELECT COUNT(*), MIN({start_column}), MAX({end_column})"
        f"{complete_expression} "
        f"FROM {table} WHERE {where_clause}"
    )
    try:
        row = connection.execute(
            query,
            tuple(value for _, value in filters),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc).lower():
            raise
        result: dict[str, Any] = {
            "count": None,
            "start_ms": None,
            "end_ms": None,
            "error": "no such table",
        }
        if include_complete_rows:
            result["complete_rows"] = None
        return result

    result = {
        "count": int(row[0] or 0),
        "start_ms": None if row[1] is None else int(row[1]),
        "end_ms": None if row[2] is None else int(row[2]),
    }
    if include_complete_rows:
        result["complete_rows"] = int(row[3] or 0)
    return result


def _decimal_arg(value: str) -> str:
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(
            f"invalid decimal value: {value}"
        ) from exc


def _display(value: Any) -> str:
    return "unavailable" if value is None else str(value)


def _error_suffix(summary: dict[str, Any]) -> str:
    error = summary.get("error")
    return "" if error is None else f" error={error}"


if __name__ == "__main__":
    raise SystemExit(main())
