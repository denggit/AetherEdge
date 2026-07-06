from __future__ import annotations

import sqlite3
from pathlib import Path

from tools import check_mf_feature_history as tool


def test_market_db_and_filters_are_configurable(
    tmp_path: Path,
    capsys,
) -> None:
    market_db = tmp_path / "custom.sqlite3"
    _create_schema(market_db)
    with sqlite3.connect(market_db) as connection:
        connection.execute(
            """
            INSERT INTO tradebar_1m_features
                (exchange, symbol, open_time_ms, close_time_ms)
            VALUES ('okx', 'CUSTOM-PERP', 0, 59999)
            """
        )
        connection.execute(
            """
            INSERT INTO tradebar_1m_features
                (exchange, symbol, open_time_ms, close_time_ms)
            VALUES ('okx', 'OTHER-PERP', 60000, 119999)
            """
        )
        connection.execute(
            """
            INSERT INTO range_footprint_features
                (
                    exchange, symbol, range_pct, price_step,
                    range_start_ms, available_time_ms
                )
            VALUES ('okx', 'CUSTOM-PERP', '0.003', '2', 0, 60000)
            """
        )
        connection.execute(
            """
            INSERT INTO range_footprint_backfill_coverage
                (
                    exchange, symbol, range_pct, price_step,
                    start_time_ms, end_time_ms, complete
                )
            VALUES ('okx', 'CUSTOM-PERP', '0.003', '2', 0, 60000, 1)
            """
        )

    result = tool.main(
        [
            "--market-db",
            str(market_db),
            "--symbol",
            "CUSTOM-PERP",
            "--exchange",
            "OKX",
            "--range-pct",
            "0.0030",
            "--price-step",
            "2.0",
        ]
    )
    output = capsys.readouterr().out

    assert result == 0
    assert f"market_db={market_db.resolve()}" in output
    assert (
        "tradebar_1m_features count=1 "
        "start=1970-01-01 08:00:00+08 "
        "end=1970-01-01 08:00:00+08"
    ) in output
    assert "range_footprint_features count=1" in output
    assert (
        "range_footprint_backfill_coverage count=1 "
        "min=1970-01-01 08:00:00+08 "
        "max=1970-01-01 08:01:00+08 complete_rows=1"
    ) in output


def test_empty_database_with_missing_tables_is_reported(
    tmp_path: Path,
    capsys,
) -> None:
    market_db = tmp_path / "empty.sqlite3"
    sqlite3.connect(market_db).close()

    result = tool.main(["--market-db", str(market_db)])
    output = capsys.readouterr().out

    assert result == 0
    for table in (
        "tradebar_1m_features",
        "trade_footprint_1m_features",
        "range_footprint_features",
        "range_footprint_backfill_coverage",
    ):
        assert f"{table} count=unavailable" in output
        assert "error=no such table" in output


def test_okx_time_formatter_uses_fixed_utc_plus_eight() -> None:
    assert tool.format_okx_time(0) == "1970-01-01 08:00:00+08"
    assert tool.format_okx_time(None) == "unavailable"


def test_queries_only_fetch_aggregate_summaries() -> None:
    connection = _RecordingConnection()

    results = tool.collect_history(
        connection,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        price_step="1",
    )

    assert set(results) == {
        "tradebar_1m_features",
        "trade_footprint_1m_features",
        "range_footprint_features",
        "range_footprint_backfill_coverage",
    }
    assert len(connection.queries) == 4
    for query, _ in connection.queries:
        normalized = " ".join(query.upper().split())
        assert "COUNT(*)" in normalized
        assert "MIN(" in normalized
        assert "MAX(" in normalized
        assert "SELECT *" not in normalized
    assert connection.fetchone_calls == 4


class _RecordingCursor:
    def __init__(self, owner: "_RecordingConnection", row) -> None:
        self.owner = owner
        self.row = row

    def fetchone(self):
        self.owner.fetchone_calls += 1
        return self.row

    def fetchall(self):
        raise AssertionError("full-row fetch is not allowed")


class _RecordingConnection:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[str, ...]]] = []
        self.fetchone_calls = 0

    def execute(self, query: str, parameters: tuple[str, ...]):
        self.queries.append((query, parameters))
        if "complete = 1" in query:
            return _RecordingCursor(self, (2, 0, 60000, 1))
        return _RecordingCursor(self, (2, 0, 60000))


def _create_schema(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE tradebar_1m_features (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open_time_ms INTEGER NOT NULL,
                close_time_ms INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE trade_footprint_1m_features (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open_time_ms INTEGER NOT NULL,
                close_time_ms INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE range_footprint_features (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                range_pct TEXT NOT NULL,
                price_step TEXT NOT NULL,
                range_start_ms INTEGER NOT NULL,
                available_time_ms INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE range_footprint_backfill_coverage (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                range_pct TEXT NOT NULL,
                price_step TEXT NOT NULL,
                start_time_ms INTEGER NOT NULL,
                end_time_ms INTEGER NOT NULL,
                complete INTEGER NOT NULL
            )
            """
        )
