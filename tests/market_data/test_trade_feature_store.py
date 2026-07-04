from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.market_data.models import (
    FixedTimeTradeBar,
    TimeRange,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore

# Use a base that's a multiple of 60_000 (1 minute) so coverage scan aligns.
_MINUTE = 60_000


def _base(i: int = 0) -> int:
    b = 1_700_000_000_000
    aligned = b - (b % _MINUTE)
    return aligned + i * _MINUTE


def _make_bar(
    *,
    exchange: str = "okx",
    symbol: str = "ETH-USDT-PERP",
    open_time_ms: int,
    close_time_ms: int | None = None,
    available_time_ms: int | None = None,
    open_price: str = "1000",
    volume: str = "10",
    buy_volume: str = "6",
    sell_volume: str = "4",
    buy_notional: str = "6000",
    sell_notional: str = "4000",
    trade_count: int = 5,
    quality: str = "COMPLETE",
) -> FixedTimeTradeBar:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    if available_time_ms is None:
        available_time_ms = close_time_ms
    delta_vol = Decimal(buy_volume) - Decimal(sell_volume)
    delta_not = Decimal(buy_notional) - Decimal(sell_notional)
    return FixedTimeTradeBar(
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        available_time_ms=available_time_ms,
        open=Decimal(open_price),
        high=Decimal("1005"),
        low=Decimal("995"),
        close=Decimal("1002"),
        volume=Decimal(volume),
        buy_volume=Decimal(buy_volume),
        sell_volume=Decimal(sell_volume),
        buy_notional=Decimal(buy_notional),
        sell_notional=Decimal(sell_notional),
        delta_volume=delta_vol,
        delta_notional=delta_not,
        abs_delta_notional=abs(delta_not),
        trade_count=trade_count,
        quality=quality,
    )


# ---------------------------------------------------------------------------
# upsert_many
# ---------------------------------------------------------------------------

def test_upsert_many_inserts_and_updates(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    bar1 = _make_bar(open_time_ms=_base(0))
    assert store.upsert_many([bar1]) == 1

    loaded = store.load_recent(symbol="ETH-USDT-PERP", exchange="okx", limit=10)
    assert len(loaded) == 1
    assert loaded[0].open_time_ms == _base(0)

    bar2 = _make_bar(open_time_ms=_base(0), volume="20")
    store.upsert_many([bar2])

    loaded2 = store.load_recent(symbol="ETH-USDT-PERP", exchange="okx", limit=10)
    assert len(loaded2) == 1
    assert loaded2[0].volume == Decimal("20")


# ---------------------------------------------------------------------------
# replace_range
# ---------------------------------------------------------------------------

def test_replace_range_deletes_and_inserts(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    b1 = _make_bar(open_time_ms=_base(0))
    b2 = _make_bar(open_time_ms=_base(1))
    b3 = _make_bar(open_time_ms=_base(3))
    store.upsert_many([b1, b2, b3])

    rng = TimeRange(_base(0), _base(1) + _MINUTE - 1)
    b1_new = _make_bar(open_time_ms=_base(0), volume="99")
    store.replace_range(rng, [b1_new])

    loaded = store.load_range(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        time_range=TimeRange(_base(0), _base(4) + _MINUTE - 1),
    )
    assert len(loaded) == 2
    assert loaded[0].volume == Decimal("99")


# ---------------------------------------------------------------------------
# load_recent
# ---------------------------------------------------------------------------

def test_load_recent_returns_in_order_and_respects_limit(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    bars = [_make_bar(open_time_ms=_base(i)) for i in range(10)]
    store.upsert_many(bars)

    loaded = store.load_recent(symbol="ETH-USDT-PERP", exchange="okx", limit=5)
    assert len(loaded) == 5
    assert loaded[0].open_time_ms < loaded[-1].open_time_ms


# ---------------------------------------------------------------------------
# load_range
# ---------------------------------------------------------------------------

def test_load_range_filters_by_time(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([
        _make_bar(open_time_ms=_base(0)),
        _make_bar(open_time_ms=_base(1)),
        _make_bar(open_time_ms=_base(2)),
    ])

    loaded = store.load_range(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        time_range=TimeRange(_base(0), _base(1) + _MINUTE - 1),
    )
    assert len(loaded) == 2


# ---------------------------------------------------------------------------
# latest_complete_close_time_ms
# ---------------------------------------------------------------------------

def test_latest_complete_close_time_ms(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    assert store.latest_complete_close_time_ms(symbol="ETH-USDT-PERP", exchange="okx") is None

    store.upsert_many([
        _make_bar(open_time_ms=_base(0)),
        _make_bar(open_time_ms=_base(2)),
    ])
    latest = store.latest_complete_close_time_ms(symbol="ETH-USDT-PERP", exchange="okx")
    assert latest is not None
    assert latest >= _base(2)


# ---------------------------------------------------------------------------
# coverage_scan
# ---------------------------------------------------------------------------

def test_coverage_scan_detects_missing_minutes(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([
        _make_bar(open_time_ms=_base(0)),
        _make_bar(open_time_ms=_base(1)),
        _make_bar(open_time_ms=_base(2)),
    ])

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=5,
    )
    assert coverage.complete_minutes == 3
    assert coverage.missing_minutes >= 1
    assert coverage.available is False


def test_coverage_scan_available_when_all_present(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([_make_bar(open_time_ms=_base(i)) for i in range(5)])

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=5,
    )
    assert coverage.available is True
    assert coverage.missing_minutes == 0


def test_coverage_scan_detects_degraded_minutes(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([
        _make_bar(open_time_ms=_base(0), quality="DEGRADED_LOW_TRADE_COUNT"),
        _make_bar(open_time_ms=_base(1)),
    ])

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=2,
    )
    assert coverage.degraded_minutes >= 1


# ---------------------------------------------------------------------------
# Table indices
# ---------------------------------------------------------------------------

def test_table_indices_exist(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    store.upsert_many([_make_bar(open_time_ms=_base(0))])

    conn = store._connect()
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='tradebar_1m_features'"
        )
        indices = [row[0] for row in cursor.fetchall()]
        assert any("close_time" in idx for idx in indices)
        assert any("available_time" in idx for idx in indices)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# No raw trades stored
# ---------------------------------------------------------------------------

def test_trade_feature_store_has_no_raw_trade_table(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    store.upsert_many([_make_bar(open_time_ms=_base(0))])
    conn = store._connect()
    try:
        tables = [
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        assert "trades" not in tables
        assert "raw_trades" not in tables
    finally:
        conn.close()


def test_store_has_no_save_raw_trades_parameter() -> None:
    store = SqliteTradeFeatureStore()
    assert not hasattr(store, "save_raw_trades")


# ---------------------------------------------------------------------------
# WAL mode
# ---------------------------------------------------------------------------

def test_store_uses_wal_mode(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    store.upsert_many([_make_bar(open_time_ms=_base(0))])
    conn = store._connect()
    try:
        row = conn.execute("PRAGMA journal_mode").fetchone()
        assert row is not None
        assert str(row[0]).lower() == "wal"
    finally:
        conn.close()
