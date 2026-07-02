from __future__ import annotations

import sqlite3
from pathlib import Path

from src.market_data.range_repair import (
    JOURNAL_FINALIZED,
    JOURNAL_INVALID_QUEUE_OVERFLOW,
    RangeRepairJournalWriter,
    RangeRepairTrade,
    SqliteRangeRepairJournalStore,
)


def test_legacy_range_repair_facade_is_removed() -> None:
    assert not Path("src/market_data/range_repair_journal.py").exists()


def test_range_repair_public_api_exports_journal_contracts() -> None:
    from src.market_data.range_repair import (
        DEFAULT_RANGE_REPAIR_JOURNAL_DB,
        JOURNAL_FINALIZED,
        JOURNAL_INVALID_DROPPED_TRADE,
        JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
        JOURNAL_INVALID_PRODUCER_FAILED,
        JOURNAL_INVALID_PRODUCER_STALE,
        JOURNAL_INVALID_QUEUE_OVERFLOW,
        JOURNAL_INVALID_WRITER_ERROR,
        JOURNAL_OPEN,
        RangeRepairJournalState,
        RangeRepairJournalWriter,
        RangeRepairTrade,
        SqliteRangeRepairJournalStore,
        journal_status_is_invalid,
    )

    assert DEFAULT_RANGE_REPAIR_JOURNAL_DB
    assert JOURNAL_OPEN == "journal_open"
    assert JOURNAL_FINALIZED == "journal_finalized"
    assert journal_status_is_invalid(JOURNAL_INVALID_DROPPED_TRADE)
    assert RangeRepairTrade is not None
    assert RangeRepairJournalState is not None
    assert SqliteRangeRepairJournalStore is not None
    assert RangeRepairJournalWriter is not None
    assert JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE
    assert JOURNAL_INVALID_PRODUCER_FAILED
    assert JOURNAL_INVALID_PRODUCER_STALE
    assert JOURNAL_INVALID_QUEUE_OVERFLOW
    assert JOURNAL_INVALID_WRITER_ERROR


def test_range_repair_journal_schema_is_preserved(tmp_path) -> None:
    path = tmp_path / "journal.sqlite3"
    SqliteRangeRepairJournalStore(path)

    with sqlite3.connect(path) as conn:
        objects = {
            (str(row[0]), str(row[1]))
            for row in conn.execute(
                """
                SELECT type, name
                FROM sqlite_master
                WHERE name IN (
                    'range_repair_trades',
                    'range_repair_journal_state',
                    'idx_range_repair_trades_time'
                )
                """
            )
        }

    assert objects == {
        ("table", "range_repair_trades"),
        ("table", "range_repair_journal_state"),
        ("index", "idx_range_repair_trades_time"),
    }


def _trade(ts: int, trade_id: str) -> RangeRepairTrade:
    return RangeRepairTrade(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        trade_time_ms=ts,
        event_time_ms=ts,
        trade_id=trade_id,
        raw_symbol="ETH-USDT-SWAP",
        side="buy",
        price="100",
        quantity="1",
        source="websocket",
        created_at_ms=ts,
    )


def _open(store: SqliteRangeRepairJournalStore) -> None:
    store.open_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        bucket_end_ms=2_000,
        checkpoint_last_trade_ts_ms=1_000,
        checkpoint_last_trade_id="cp",
        updated_at_ms=1_001,
    )


def test_journal_records_first_live_trade_dedupes_and_finalizes(tmp_path) -> None:
    store = SqliteRangeRepairJournalStore(tmp_path / "journal.sqlite3")
    _open(store)

    assert store.record_first_live_trade(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        trade_time_ms=1_088,
        trade_id="first",
        recorded_at_ms=1_090,
    )
    assert (
        store.append_trades(
            [
                _trade(1_100, "2"),
                _trade(1_088, "1"),
                _trade(1_100, "2"),
            ]
        )
        == 2
    )
    state = store.finalize(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        finalized_at_ms=2_001,
    )

    assert state is not None
    assert state.first_live_trade_ts_ms == 1_088
    assert state.status == JOURNAL_FINALIZED
    assert state.valid_for_repair
    assert state.journal_trade_count == 2
    rows = store.load_trades(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        start_time_ms=1_088,
        end_time_ms=2_000,
    )
    assert [(row.trade_time_ms, row.trade_id) for row in rows] == [
        (1_088, "1"),
        (1_100, "2"),
    ]
    with sqlite3.connect(tmp_path / "journal.sqlite3") as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert "trades" not in tables
    assert "trade_coverage" not in tables


def test_writer_queue_overflow_invalidates_bucket_without_blocking(tmp_path) -> None:
    store = SqliteRangeRepairJournalStore(tmp_path / "journal.sqlite3")
    invalidated = []
    writer = RangeRepairJournalWriter(
        store,
        max_pending=1,
        flush_interval_ms=1,
        batch_size=10,
        on_invalidated=(
            lambda key, status, error: invalidated.append(
                (key, status, error)
            )
        ),
    )
    writer.submit_open(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        bucket_end_ms=2_000,
        checkpoint_last_trade_ts_ms=1_000,
        checkpoint_last_trade_id="cp",
        updated_at_ms=1_001,
    )
    assert writer.submit_trade(_trade(1_088, "1"))
    assert writer.submit_trade(_trade(1_100, "2")) is False
    writer.submit_finalize(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        finalized_at_ms=2_001,
    )

    writer.start()
    writer.stop(flush=True)

    state = store.load_state(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
    )
    assert state is not None
    assert state.status == JOURNAL_INVALID_QUEUE_OVERFLOW
    assert state.dropped_trades == 1
    assert state.finalized
    assert not state.valid_for_repair
    assert invalidated
    assert invalidated[0][1] == JOURNAL_INVALID_QUEUE_OVERFLOW


def test_cleanup_removes_expired_journal_only(tmp_path) -> None:
    store = SqliteRangeRepairJournalStore(tmp_path / "journal.sqlite3")
    _open(store)
    store.append_trades([_trade(1_088, "1")])

    assert store.cleanup(older_than_ms=2_001) == (1, 1)
    assert (
        store.load_state(
            exchange="okx",
            symbol="ETH-USDT-PERP",
            range_pct="0.002",
            bucket_start_ms=0,
        )
        is None
    )


def test_second_recovery_gap_in_same_bucket_invalidates_journal(
    tmp_path,
) -> None:
    store = SqliteRangeRepairJournalStore(tmp_path / "journal.sqlite3")
    _open(store)
    store.record_first_live_trade(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        trade_time_ms=1_088,
        trade_id="first",
        recorded_at_ms=1_090,
    )

    store.open_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
        bucket_end_ms=2_000,
        checkpoint_last_trade_ts_ms=1_500,
        checkpoint_last_trade_id="second-checkpoint",
        updated_at_ms=1_501,
    )

    state = store.load_state(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=0,
    )
    assert state is not None
    assert state.status == "journal_invalid_dropped_trade"
    assert state.dropped_trades == 1
