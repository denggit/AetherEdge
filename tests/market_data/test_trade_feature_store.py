from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.market_data.models import (
    FixedTimeTradeBar,
    RangeFootprintFeature,
    TimeRange,
    TradeFootprintFeature,
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


def _make_fp(
    *,
    exchange: str = "okx",
    symbol: str = "ETH-USDT-PERP",
    open_time_ms: int,
    close_time_ms: int | None = None,
    available_time_ms: int | None = None,
    delta_notional: str = "2000",
    quality: str = "COMPLETE",
    context_available: bool = True,
) -> TradeFootprintFeature:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    if available_time_ms is None:
        available_time_ms = close_time_ms
    delta = Decimal(delta_notional)
    return TradeFootprintFeature(
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        available_time_ms=available_time_ms,
        delta_notional=delta,
        abs_delta_notional=abs(delta),
        taker_buy_ratio=Decimal("0.6"),
        close_pos=Decimal("0.5"),
        range_pct=Decimal("0.01"),
        return_pct=Decimal("0.002"),
        fp_max_bucket_abs_delta_pressure=Decimal("0"),
        context_available=context_available,
        quality=quality,
    )


def _make_range_fp(
    *,
    available_time_ms: int,
    range_bar_id: int = 1,
    quality: str = "COMPLETE",
    context_available: bool = True,
) -> RangeFootprintFeature:
    return RangeFootprintFeature(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        range_bar_id=range_bar_id,
        range_start_ms=available_time_ms - 1_000,
        range_end_ms=available_time_ms,
        available_time_ms=available_time_ms,
        fp_max_bucket_abs_delta_pressure=(
            Decimal("0.8") if context_available else Decimal("0")
        ),
        fp_low_bucket_delta_pressure=Decimal("-0.2"),
        fp_high_bucket_delta_pressure=Decimal("0.8"),
        fp_delta_pressure=Decimal("0.1"),
        bucket_count=3,
        trade_count=8,
        context_available=context_available,
        quality=quality,
    )


def _write_pair(store: SqliteTradeFeatureStore, open_time_ms: int, *, tb_quality: str = "COMPLETE", fp_quality: str = "COMPLETE", fp_context: bool = True) -> None:
    bar = _make_bar(open_time_ms=open_time_ms, quality=tb_quality)
    fp = _make_fp(open_time_ms=open_time_ms, quality=fp_quality, context_available=fp_context)
    store.upsert_tradebars_many([bar])
    store.upsert_footprints_many([fp])


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

    # Need BOTH tradebar + footprint for latest to be found
    _write_pair(store, _base(0))
    _write_pair(store, _base(2))
    latest = store.latest_complete_close_time_ms(symbol="ETH-USDT-PERP", exchange="okx")
    assert latest is not None
    assert latest >= _base(2)


def test_latest_any_and_earliest_any_times_are_independent(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    store.upsert_tradebars_many(
        [_make_bar(open_time_ms=_base(1)), _make_bar(open_time_ms=_base(3))]
    )
    store.upsert_footprints_many(
        [_make_fp(open_time_ms=_base(2)), _make_fp(open_time_ms=_base(4))]
    )

    assert store.earliest_any_tradebar_open_time_ms(
        symbol="ETH-USDT-PERP", exchange="okx"
    ) == _base(1)
    assert store.earliest_any_footprint_open_time_ms(
        symbol="ETH-USDT-PERP", exchange="okx"
    ) == _base(2)
    assert store.latest_any_tradebar_close_time_ms(
        symbol="ETH-USDT-PERP", exchange="okx"
    ) == _base(3) + _MINUTE - 1
    assert store.latest_any_footprint_close_time_ms(
        symbol="ETH-USDT-PERP", exchange="okx"
    ) == _base(4) + _MINUTE - 1


# ---------------------------------------------------------------------------
# coverage_scan
# ---------------------------------------------------------------------------

def test_coverage_scan_detects_missing_minutes(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    for i in (0, 1, 2):
        _write_pair(store, _base(i))

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=5,
        reference_end_ms=_base(4) + _MINUTE - 1,
        safe_archive_end_ms=_base(4) + _MINUTE - 1,
    )
    assert coverage.complete_minutes == 3
    assert coverage.missing_minutes >= 1
    assert coverage.available is False


def test_coverage_scan_available_when_all_present(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    for i in range(5):
        _write_pair(store, _base(i))
    store.upsert_range_footprints_many(
        [
            _make_range_fp(available_time_ms=_base(0), range_bar_id=1),
            _make_range_fp(
                available_time_ms=_base(4) + _MINUTE - 1,
                range_bar_id=2,
            ),
        ]
    )
    store.mark_range_footprint_coverage(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        start_ms=_base(0),
        end_ms=_base(4) + _MINUTE - 1,
        complete=True,
    )

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=5,
        reference_end_ms=_base(4) + _MINUTE - 1,
        safe_archive_end_ms=_base(4) + _MINUTE - 1,
    )
    assert coverage.available is True
    assert coverage.missing_minutes == 0


def test_store_upsert_and_load_range_footprint_features(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    feature = _make_range_fp(
        available_time_ms=_base(1) + _MINUTE - 1,
        range_bar_id=42,
    )

    assert store.upsert_range_footprints_many([feature]) == 1
    loaded = store.load_range_footprint_features(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        time_range=TimeRange(_base(0), _base(2) + _MINUTE - 1),
    )

    assert loaded == [feature]
    assert (
        store.latest_any_range_footprint_available_time_ms(
            symbol="ETH-USDT-PERP", exchange="okx"
        )
        == feature.available_time_ms
    )


def test_coverage_degraded_range_footprint_is_not_ready(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(2):
        _write_pair(store, _base(i))
    store.upsert_range_footprints_many(
        [
            _make_range_fp(
                available_time_ms=_base(0),
                range_bar_id=1,
            ),
            _make_range_fp(
                available_time_ms=_base(1) + _MINUTE - 1,
                range_bar_id=2,
                quality="MISSING_FOOTPRINT_CONTEXT",
                context_available=False,
            )
        ]
    )
    store.mark_range_footprint_coverage(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        start_ms=_base(0),
        end_ms=_base(1) + _MINUTE - 1,
        complete=True,
    )

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
        safe_archive_end_ms=_base(1) + _MINUTE - 1,
    )

    assert coverage.available is False
    assert coverage.extra["range_footprint_ready"] is False
    assert coverage.extra["degraded_range_footprint_count"] == 1


def test_coverage_scan_detects_degraded_minutes(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    _write_pair(store, _base(0), tb_quality="DEGRADED_LOW_TRADE_COUNT", fp_quality="DEGRADED_LOW_TRADE_COUNT")
    _write_pair(store, _base(1))

    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
        safe_archive_end_ms=_base(1) + _MINUTE - 1,
    )
    assert coverage.degraded_minutes >= 1
    assert coverage.extra["degraded_footprint"] == 1
    assert coverage.extra["degraded_tradebar"] == 1


def test_coverage_extra_splits_tradebar_and_footprint(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    store.upsert_tradebars_many([_make_bar(open_time_ms=_base(0))])
    store.upsert_footprints_many(
        [
            _make_fp(
                open_time_ms=_base(1),
                quality="MISSING_FOOTPRINT_CONTEXT",
                context_available=False,
            )
        ]
    )
    safe_end = _base(1) + _MINUTE - 1
    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        required_minutes=2,
        reference_end_ms=safe_end,
        safe_archive_end_ms=safe_end,
    )

    assert coverage.available is False
    assert coverage.extra["missing_tradebar"] == 1
    assert coverage.extra["missing_footprint"] == 1
    assert coverage.extra["degraded_footprint"] == 1
    assert coverage.extra["latest_any_tradebar_close_time_ms"] is not None
    assert coverage.extra["latest_any_footprint_close_time_ms"] is not None
    assert coverage.extra["safe_archive_end_ms"] == safe_end


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
