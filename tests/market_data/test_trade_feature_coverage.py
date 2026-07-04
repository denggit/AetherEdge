from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from src.market_data.models import (
    FixedTimeTradeBar,
    RangeFootprintFeature,
    TradeFootprintFeature,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import (
    compute_backfill_target,
    mf_feature_coverage_scan,
    resolve_mf_readiness,
    safe_okx_archive_end_ms,
)

_MINUTE = 60_000


def _base(i: int = 0) -> int:
    b = 1_700_000_000_000
    aligned = b - (b % _MINUTE)
    return aligned + i * _MINUTE


def test_safe_okx_archive_end_is_previous_utc8_day() -> None:
    now_ms = int(
        datetime(2026, 7, 4, 4, 0, 0, tzinfo=UTC).timestamp() * 1000
    )
    expected = int(
        datetime(2026, 7, 3, 15, 59, 59, 999000, tzinfo=UTC).timestamp()
        * 1000
    )
    assert safe_okx_archive_end_ms(now_ms) == expected


def _make_bar(open_time_ms: int, close_time_ms: int | None = None, *, quality: str = "COMPLETE") -> FixedTimeTradeBar:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    return FixedTimeTradeBar(
        exchange="okx", symbol="ETH-USDT-PERP", timeframe="1m",
        open_time_ms=open_time_ms, close_time_ms=close_time_ms, available_time_ms=close_time_ms,
        open=Decimal("1000"), high=Decimal("1005"), low=Decimal("995"), close=Decimal("1002"),
        volume=Decimal("10"), buy_volume=Decimal("6"), sell_volume=Decimal("4"),
        buy_notional=Decimal("6000"), sell_notional=Decimal("4000"),
        delta_volume=Decimal("2"), delta_notional=Decimal("2000"), abs_delta_notional=Decimal("2000"),
        trade_count=5, quality=quality,
    )


def _make_fp(open_time_ms: int, close_time_ms: int | None = None, *,
             quality: str = "COMPLETE", context_available: bool = True) -> TradeFootprintFeature:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    delta = Decimal("2000")
    return TradeFootprintFeature(
        exchange="okx", symbol="ETH-USDT-PERP", timeframe="1m",
        open_time_ms=open_time_ms, close_time_ms=close_time_ms, available_time_ms=close_time_ms,
        delta_notional=delta, abs_delta_notional=abs(delta),
        taker_buy_ratio=Decimal("0.6"), close_pos=Decimal("0.5"),
        range_pct=Decimal("0.01"), return_pct=Decimal("0.002"),
        fp_max_bucket_abs_delta_pressure=Decimal("0"),
        context_available=context_available, quality=quality,
    )


def _write_pair(store: SqliteTradeFeatureStore, open_time_ms: int, *,
                tb_quality: str = "COMPLETE", fp_quality: str = "COMPLETE",
                fp_context: bool = True) -> None:
    store.upsert_tradebars_many([_make_bar(open_time_ms, quality=tb_quality)])
    store.upsert_footprints_many([_make_fp(open_time_ms, quality=fp_quality, context_available=fp_context)])


def _write_range_ready(
    store: SqliteTradeFeatureStore,
    available_time_ms: int,
    *,
    start_ms: int | None = None,
) -> None:
    coverage_start = _base(0) if start_ms is None else start_ms
    available_times = [coverage_start]
    if available_time_ms != coverage_start:
        available_times.append(available_time_ms)
    store.upsert_range_footprints_many(
        [
            RangeFootprintFeature(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                range_pct=Decimal("0.002"),
                price_step=Decimal("1"),
                range_bar_id=index,
                range_start_ms=current_ms - 1_000,
                range_end_ms=current_ms,
                available_time_ms=current_ms,
                fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                fp_low_bucket_delta_pressure=Decimal("-0.2"),
                fp_high_bucket_delta_pressure=Decimal("0.8"),
                fp_delta_pressure=Decimal("0.1"),
                bucket_count=3,
                trade_count=9,
            )
            for index, current_ms in enumerate(available_times, start=1)
        ]
    )
    store.mark_range_footprint_coverage(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        start_ms=coverage_start,
        end_ms=available_time_ms,
        complete=True,
    )


def test_mf_feature_coverage_scan_no_data(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
        reference_end_ms=_base(9) + _MINUTE - 1,
    )
    assert coverage.available is False
    assert coverage.complete_minutes == 0
    assert "no_features_stored" in coverage.reason


def test_mf_feature_coverage_scan_complete(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(5):
        _write_pair(store, _base(i))
    _write_range_ready(store, _base(4) + _MINUTE - 1)

    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=5,
        reference_end_ms=_base(4) + _MINUTE - 1,
    )
    assert coverage.available is True
    assert coverage.missing_minutes == 0


def test_mf_feature_coverage_scan_missing_gap(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    _write_pair(store, _base(0))
    _write_pair(store, _base(2))  # skip _base(1)

    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
        reference_end_ms=_base(2) + _MINUTE - 1,
    )
    assert coverage.available is False
    assert coverage.missing_minutes >= 1
    assert coverage.first_missing_range is not None


def test_resolve_mf_readiness_always_signal_false(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(100):
        _write_pair(store, _base(i))
    _write_range_ready(store, _base(99) + _MINUTE - 1)

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=100,
        reference_end_ms=_base(99) + _MINUTE - 1,
    )
    assert readiness.mf_signal_ready is False
    assert readiness.coverage_ready is True
    assert readiness.mf_signal_feature_ready is True
    assert readiness.range_footprint_ready is True


def test_resolve_mf_readiness_degraded_footprint(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    _write_pair(store, _base(0), tb_quality="DEGRADED_LOW_TRADE_COUNT",
                fp_quality="DEGRADED_LOW_TRADE_COUNT")
    _write_pair(store, _base(1))

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
    )
    assert readiness.degraded_footprint is True
    assert readiness.footprint_ready is False
    assert readiness.mf_signal_ready is False


def test_audit_has_required_fields(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP", exchange="okx", store=store, required_minutes=10,
    )
    audit = readiness.audit()
    for key in (
        "tradebar_ready", "fixed_time_footprint_ready",
        "range_footprint_ready", "mf_signal_feature_ready",
        "price_ready", "orderflow_ready", "footprint_ready",
        "coverage_ready", "mf_signal_ready", "coverage",
        "worker_running", "waiting_for_global_lock",
        "degraded_footprint", "current_day_archive_not_ready",
    ):
        assert key in audit


def test_compute_backfill_target_initial_empty_store(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    safe_end = _base(9) + _MINUTE - 1

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=5,
        max_minutes_per_cycle=5,
        safe_archive_end_ms=safe_end,
    )

    assert target is not None
    assert target.reason == "initial_empty_store"
    assert target.end_ms == safe_end
    assert target.start_ms == safe_end - 5 * _MINUTE + 1


def test_compute_backfill_target_returns_none_when_complete(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(5):
        _write_pair(store, _base(i))
    _write_range_ready(store, _base(4) + _MINUTE - 1)

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP", exchange="okx", store=store,
        required_minutes=5,
        max_minutes_per_cycle=5,
        safe_archive_end_ms=_base(4) + _MINUTE - 1,
    )
    assert target is None


def test_coverage_fixed_footprint_does_not_satisfy_range_footprint_ready(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(3):
        _write_pair(store, _base(i))

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
        reference_end_ms=_base(2) + _MINUTE - 1,
    )

    assert readiness.tradebar_ready is True
    assert readiness.fixed_time_footprint_ready is True
    assert readiness.range_footprint_ready is False
    assert readiness.mf_signal_feature_ready is False


def test_coverage_range_footprint_missing_blocks_mf_signal_feature_ready(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(2):
        _write_pair(store, _base(i))

    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
    )

    assert coverage.available is False
    assert coverage.extra["missing_range_footprint_count"] > 0
    assert coverage.extra["range_footprint_ready"] is False


def test_range_context_closed_after_window_start_cannot_seed_earlier_signals(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(2):
        _write_pair(store, _base(i))
    _write_range_ready(
        store,
        _base(1) + _MINUTE - 1,
        start_ms=_base(1) + _MINUTE - 1,
    )
    # Replace the helper's marker with coverage for the full two-minute
    # window while leaving its first context unavailable until the end.
    store.mark_range_footprint_coverage(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        start_ms=_base(0),
        end_ms=_base(1) + _MINUTE - 1,
        complete=True,
    )

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
    )

    assert readiness.range_footprint_ready is False
    assert readiness.mf_signal_feature_ready is False
    assert (
        readiness.coverage.extra[
            "range_footprint_context_seed_available_time_ms"
        ]
        is None
    )


def test_compute_backfill_target_recomputes_degraded_range_footprint(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(2):
        _write_pair(store, _base(i))
    store.upsert_range_footprints_many(
        [
            RangeFootprintFeature(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                range_pct=Decimal("0.002"),
                price_step=Decimal("1"),
                range_bar_id=7,
                range_start_ms=_base(0),
                range_end_ms=_base(1) + _MINUTE - 1,
                available_time_ms=_base(1) + _MINUTE - 1,
                fp_max_bucket_abs_delta_pressure=Decimal("0"),
                fp_low_bucket_delta_pressure=Decimal("0"),
                fp_high_bucket_delta_pressure=Decimal("0"),
                fp_delta_pressure=Decimal("0"),
                bucket_count=0,
                trade_count=2,
                context_available=False,
                quality="MISSING_FOOTPRINT_CONTEXT",
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

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
        max_minutes_per_cycle=2,
        safe_archive_end_ms=_base(1) + _MINUTE - 1,
    )

    assert target is not None
    assert target.reason == "degraded_range_footprint_recompute"


def test_compute_backfill_target_finds_gap(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    _write_pair(store, _base(0))
    _write_pair(store, _base(2))  # gap at _base(1)

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP", exchange="okx", store=store,
        required_minutes=3,
        max_minutes_per_cycle=3,
        safe_archive_end_ms=_base(2) + _MINUTE - 1,
    )
    assert target is not None
    assert target.start_ms > 0
    assert target.reason


def test_compute_backfill_target_prioritizes_missing_footprint(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    store.upsert_tradebars_many([_make_bar(_base(i)) for i in range(5)])

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=5,
        max_minutes_per_cycle=5,
        safe_archive_end_ms=_base(4) + _MINUTE - 1,
    )

    assert target is not None
    assert target.reason == "missing_footprint_for_existing_tradebars"
    assert target.start_ms == _base(0)
    assert target.end_ms == _base(4) + _MINUTE - 1


def test_compute_backfill_target_recomputes_degraded_footprint(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(3):
        _write_pair(store, _base(i))
    store.upsert_footprints_many(
        [
            _make_fp(
                _base(1),
                quality="MISSING_FOOTPRINT_CONTEXT",
                context_available=False,
            )
        ]
    )

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
        max_minutes_per_cycle=3,
        safe_archive_end_ms=_base(2) + _MINUTE - 1,
    )

    assert target is not None
    assert target.reason == "degraded_footprint_recompute"
    assert target.start_ms <= _base(1) <= target.end_ms


def test_compute_backfill_target_gap_after_latest(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(2):
        _write_pair(store, _base(i))
    safe_end = _base(4) + _MINUTE - 1

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=5,
        max_minutes_per_cycle=2,
        safe_archive_end_ms=safe_end,
    )

    assert target is not None
    assert target.reason == "gap_after_latest"
    assert target.start_ms == _base(2)
    assert target.end_ms == _base(3) + _MINUTE - 1


def test_coverage_fails_when_footprint_missing(tmp_path: Path) -> None:
    """Coverage must be NOT_READY when footprint table has no matching rows."""
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    # Only write tradebars for some minutes, and footprints for NONE of them
    store.upsert_tradebars_many([_make_bar(_base(i)) for i in range(5)])

    # latest_complete_close_time_ms joins both tables → None when footprints absent
    coverage = store.coverage_scan(
        symbol="ETH-USDT-PERP", exchange="okx", required_minutes=5,
        reference_end_ms=_base(4) + _MINUTE - 1,
        safe_archive_end_ms=_base(4) + _MINUTE - 1,
    )
    assert coverage.available is False
    assert coverage.extra["missing_footprint"] == 5
    assert coverage.extra["tradebar_complete_minutes"] == 5
    assert coverage.extra["footprint_complete_minutes"] == 0


def test_footprint_store_crud(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    fp = _make_fp(_base(0))
    assert store.upsert_footprints_many([fp]) == 1

    loaded = store.load_recent_footprints(symbol="ETH-USDT-PERP", exchange="okx", limit=10)
    assert len(loaded) == 1
    assert loaded[0].open_time_ms == _base(0)
