from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
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
    resolve_trade_feature_readiness,
    trade_feature_coverage_scan,
    safe_okx_archive_end_ms,
)

_MINUTE = 60_000
_OKX_TIMEZONE = timezone(timedelta(hours=8))


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


def test_safe_okx_archive_end_applies_publish_lag_at_midnight() -> None:
    now_ms = int(
        datetime(
            2026,
            7,
            7,
            0,
            30,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )
    expected = int(
        datetime(
            2026,
            7,
            5,
            23,
            59,
            59,
            999_000,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )

    assert (
        safe_okx_archive_end_ms(
            now_ms,
            archive_publish_lag_hours=8.0,
        )
        == expected
    )


def test_safe_okx_archive_end_advances_after_publish_lag() -> None:
    now_ms = int(
        datetime(
            2026,
            7,
            7,
            9,
            0,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )
    expected = int(
        datetime(
            2026,
            7,
            6,
            23,
            59,
            59,
            999_000,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )

    assert (
        safe_okx_archive_end_ms(
            now_ms,
            archive_publish_lag_hours=8.0,
        )
        == expected
    )


def test_safe_okx_archive_end_zero_lag_preserves_calendar_behavior() -> None:
    now_ms = int(
        datetime(
            2026,
            7,
            7,
            0,
            30,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )
    expected = int(
        datetime(
            2026,
            7,
            6,
            23,
            59,
            59,
            999_000,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )

    assert (
        safe_okx_archive_end_ms(
            now_ms,
            archive_publish_lag_hours=0.0,
        )
        == expected
    )


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


def _write_target_ready(
    store: SqliteTradeFeatureStore,
    *,
    start_ms: int,
    end_ms: int,
    range_bar_id: int,
) -> None:
    open_times = range(
        (start_ms // _MINUTE) * _MINUTE,
        (end_ms // _MINUTE) * _MINUTE + 1,
        _MINUTE,
    )
    values = tuple(open_times)
    store.upsert_tradebars_many(
        [_make_bar(open_ms) for open_ms in values]
    )
    store.upsert_footprints_many(
        [_make_fp(open_ms) for open_ms in values]
    )
    store.upsert_range_footprints_many(
        [
            RangeFootprintFeature(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                range_pct=Decimal("0.002"),
                price_step=Decimal("1"),
                range_bar_id=range_bar_id,
                range_start_ms=start_ms - 1_000,
                range_end_ms=start_ms,
                available_time_ms=start_ms,
                fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                fp_low_bucket_delta_pressure=Decimal("-0.2"),
                fp_high_bucket_delta_pressure=Decimal("0.8"),
                fp_delta_pressure=Decimal("0.1"),
                bucket_count=3,
                trade_count=9,
            )
        ]
    )
    store.mark_range_footprint_coverage(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        price_step="1",
        start_ms=start_ms,
        end_ms=end_ms,
        complete=True,
    )


def test_mf_feature_coverage_scan_no_data(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    coverage = trade_feature_coverage_scan(
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

    coverage = trade_feature_coverage_scan(
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

    coverage = trade_feature_coverage_scan(
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

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=100,
        reference_end_ms=_base(99) + _MINUTE - 1,
    )
    assert readiness.coverage_ready is True
    assert readiness.range_footprint_ready is True


def test_resolve_mf_readiness_degraded_footprint(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    _write_pair(store, _base(0), tb_quality="DEGRADED_LOW_TRADE_COUNT",
                fp_quality="DEGRADED_LOW_TRADE_COUNT")
    _write_pair(store, _base(1))

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
    )
    assert readiness.degraded_footprint is True
    assert readiness.footprint_ready is False


def test_audit_has_required_fields(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP", exchange="okx", store=store, required_minutes=10,
    )
    audit = readiness.audit()
    for key in (
        "tradebar_ready", "fixed_time_footprint_ready",
        "range_footprint_ready",
        "price_ready", "orderflow_ready", "footprint_ready",
        "coverage_ready", "coverage",
        "worker_running", "waiting_for_global_lock",
        "degraded_footprint", "current_day_archive_not_ready",
        "archive_publish_lag_hours",
        "calendar_safe_archive_end_ms", "safe_archive_end_ms",
        "safe_archive_end_okx", "calendar_safe_archive_end_okx",
        "latest_archive_day_deferred",
        "latest_archive_day_deferred_reason",
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


def test_empty_store_oldest_direction_starts_required_window(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    safe_end = _base(172_799) + _MINUTE - 1

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=172_800,
        max_minutes_per_cycle=4_320,
        direction="oldest-to-recent",
        safe_archive_end_ms=safe_end,
    )

    assert target is not None
    assert target.start_ms == _base(0)
    assert target.end_ms == _base(4_319) + _MINUTE - 1


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

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
        reference_end_ms=_base(2) + _MINUTE - 1,
    )

    assert readiness.tradebar_ready is True
    assert readiness.fixed_time_footprint_ready is True
    assert readiness.range_footprint_ready is False


def test_coverage_range_footprint_missing_blocks_mf_signal_feature_ready(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(2):
        _write_pair(store, _base(i))

    coverage = trade_feature_coverage_scan(
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

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
        reference_end_ms=_base(1) + _MINUTE - 1,
    )

    assert readiness.range_footprint_ready is False
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


def test_recent_only_store_targets_older_gap_in_full_batch(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    required = 172_800
    recent = 4_320
    safe_end = _base(required - 1) + _MINUTE - 1
    for start in range(required - recent, required, 720):
        rows = range(start, min(required, start + 720))
        store.upsert_tradebars_many(
            [_make_bar(_base(index)) for index in rows]
        )
        store.upsert_footprints_many(
            [_make_fp(_base(index)) for index in rows]
        )

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=required,
        max_minutes_per_cycle=4_320,
        direction="oldest-to-recent",
        safe_archive_end_ms=safe_end,
    )

    assert target is not None
    assert target.reason == "gap_from_coverage_scan"
    assert target.start_ms == _base(0)
    assert target.end_ms == _base(4_319) + _MINUTE - 1


def test_recent_to_oldest_uses_latest_contiguous_gap(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for index in (*range(4), 8, 9):
        _write_pair(store, _base(index))
    safe_end = _base(9) + _MINUTE - 1

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
        max_minutes_per_cycle=3,
        direction="recent-to-oldest",
        safe_archive_end_ms=safe_end,
    )

    assert target is not None
    assert target.start_ms == _base(5)
    assert target.end_ms == _base(7) + _MINUTE - 1


def test_missing_range_footprint_targets_batch_not_one_minute(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for index in range(10):
        _write_pair(store, _base(index))

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
        max_minutes_per_cycle=4,
        direction="oldest-to-recent",
        safe_archive_end_ms=_base(9) + _MINUTE - 1,
    )

    assert target is not None
    assert target.end_ms - target.start_ms + 1 == 4 * _MINUTE


def test_contiguous_degraded_footprints_target_full_batch(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for index in range(10):
        _write_pair(store, _base(index))
    store.upsert_footprints_many(
        [
            _make_fp(
                _base(index),
                quality="MISSING_FOOTPRINT_CONTEXT",
                context_available=False,
            )
            for index in range(2, 8)
        ]
    )

    target = compute_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
        max_minutes_per_cycle=4,
        direction="oldest-to-recent",
        safe_archive_end_ms=_base(9) + _MINUTE - 1,
    )

    assert target is not None
    assert target.reason == "degraded_footprint_recompute"
    assert target.start_ms == _base(2)
    assert target.end_ms == _base(5) + _MINUTE - 1


def test_120_day_sqlite_backfill_simulation_finishes_in_40_cycles(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    required = 120 * 1_440
    per_cycle = 4_320
    safe_end = _base(required - 1) + _MINUTE - 1
    cycles = 0

    while cycles <= 45:
        target = compute_backfill_target(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            store=store,
            required_minutes=required,
            max_minutes_per_cycle=per_cycle,
            direction="oldest-to-recent",
            safe_archive_end_ms=safe_end,
        )
        if target is None:
            break
        cycles += 1
        _write_target_ready(
            store,
            start_ms=target.start_ms,
            end_ms=target.end_ms,
            range_bar_id=cycles,
        )

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=required,
        reference_end_ms=safe_end,
        range_pct="0.002",
        price_step="1",
    )

    assert cycles == 40
    assert readiness.coverage is not None
    assert readiness.coverage.complete_minutes == required
    assert readiness.coverage_ready is True
    assert readiness.range_footprint_ready is True


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
