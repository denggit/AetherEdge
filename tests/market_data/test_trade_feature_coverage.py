from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.market_data.models import FixedTimeTradeBar
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import (
    mf_feature_coverage_scan,
    resolve_mf_readiness,
)

_MINUTE = 60_000


def _base(i: int = 0) -> int:
    b = 1_700_000_000_000
    aligned = b - (b % _MINUTE)
    return aligned + i * _MINUTE


def _make_bar(open_time_ms: int, close_time_ms: int | None = None, *, quality: str = "COMPLETE") -> FixedTimeTradeBar:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    return FixedTimeTradeBar(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        timeframe="1m",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        available_time_ms=close_time_ms,
        open=Decimal("1000"),
        high=Decimal("1005"),
        low=Decimal("995"),
        close=Decimal("1002"),
        volume=Decimal("10"),
        buy_volume=Decimal("6"),
        sell_volume=Decimal("4"),
        buy_notional=Decimal("6000"),
        sell_notional=Decimal("4000"),
        delta_volume=Decimal("2"),
        delta_notional=Decimal("2000"),
        abs_delta_notional=Decimal("2000"),
        trade_count=5,
        quality=quality,
    )


def test_mf_feature_coverage_scan_no_data(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
    )
    assert coverage.available is False
    assert coverage.complete_minutes == 0
    assert coverage.reason == "no_features_stored"


def test_mf_feature_coverage_scan_complete(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    bars = [_make_bar(_base(i)) for i in range(5)]
    store.upsert_many(bars)

    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=5,
    )
    assert coverage.available is True
    assert coverage.missing_minutes == 0


def test_mf_feature_coverage_scan_missing_gap(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([
        _make_bar(_base(0)),
        _make_bar(_base(2)),  # skip _base(1)
    ])

    coverage = mf_feature_coverage_scan(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
    )
    assert coverage.available is False
    assert coverage.missing_minutes >= 1
    assert coverage.first_missing_range is not None


def test_resolve_mf_readiness_always_signal_false(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([_make_bar(_base(i)) for i in range(100)])

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=100,
    )
    assert readiness.mf_signal_ready is False
    assert readiness.coverage_ready is True


def test_resolve_mf_readiness_degraded_footprint(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    store.upsert_many([
        _make_bar(_base(0), quality="DEGRADED_LOW_TRADE_COUNT"),
        _make_bar(_base(1)),
    ])

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=2,
    )
    assert readiness.degraded_footprint is True
    assert readiness.footprint_ready is False
    assert readiness.mf_signal_ready is False


def test_audit_has_required_fields(tmp_path: Path) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")

    readiness = resolve_mf_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
    )
    audit = readiness.audit()
    for key in (
        "price_ready",
        "orderflow_ready",
        "footprint_ready",
        "coverage_ready",
        "mf_signal_ready",
        "coverage",
        "worker_running",
        "waiting_for_global_lock",
        "degraded_footprint",
        "current_day_archive_not_ready",
    ):
        assert key in audit
