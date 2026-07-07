from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.market_data.models import FixedTimeTradeBar, RangeFootprintFeature
from src.market_data.storage.trade_feature_store import (
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (
    resolve_trade_feature_readiness,
)
from strategies.eth_portfolio_v1.preflight.mf_signal_readiness import (
    compute_mf_signal_backfill_target,
)


_MINUTE = 60_000


def _base(i: int = 0) -> int:
    b = 1_700_000_000_000
    aligned = b - (b % _MINUTE)
    return aligned + i * _MINUTE


def _make_bar(open_time_ms: int) -> FixedTimeTradeBar:
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
        quality="COMPLETE",
    )


def test_mf_signal_target_ignores_fixed_and_range_historical_coverage_gap(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(3):
        store.upsert_tradebars_many([_make_bar(_base(i))])
    store.upsert_range_footprints_many(
        [
            RangeFootprintFeature(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                range_pct=Decimal("0.002"),
                price_step=Decimal("1"),
                range_bar_id=1,
                range_start_ms=_base(2) - 30_000,
                range_end_ms=_base(2) - 1,
                available_time_ms=_base(2) - 1,
                fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                fp_low_bucket_delta_pressure=Decimal("-0.2"),
                fp_high_bucket_delta_pressure=Decimal("0.4"),
                fp_delta_pressure=Decimal("0.1"),
                bucket_count=5,
                trade_count=20,
                context_available=True,
                quality="COMPLETE",
            )
        ]
    )

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
        reference_end_ms=_base(2) + _MINUTE - 1,
    )
    target = compute_mf_signal_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=3,
        max_minutes_per_cycle=3,
        safe_archive_end_ms=_base(2) + _MINUTE - 1,
    )

    assert readiness.coverage_ready is False
    assert target is None


def test_mf_signal_target_missing_context_uses_recent_seed_window(
    tmp_path: Path,
) -> None:
    store = SqliteTradeFeatureStore(path=tmp_path / "test.sqlite3")
    for i in range(10):
        store.upsert_tradebars_many([_make_bar(_base(i))])

    target = compute_mf_signal_backfill_target(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store=store,
        required_minutes=10,
        max_minutes_per_cycle=2,
        safe_archive_end_ms=_base(9) + _MINUTE - 1,
    )

    assert target is not None
    assert target.reason == "missing_range_footprint_context_seed"
    assert target.start_ms == _base(8)
    assert target.end_ms == _base(9) + _MINUTE - 1
