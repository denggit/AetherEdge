from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.market_data.models import (
    FixedTimeTradeBar,
    RangeBar,
    RangeBarAggregate,
    RangeCoverageStatus,
    RangeFootprintFeature,
    TradeFootprintFeature,
)
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteKlineStore, SqliteRangeBarStore
from src.market_data.storage.trade_feature_store import (
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (
    safe_okx_archive_end_ms,
)
from src.platform import ExchangeName
from src.platform.data.models import MarketDataSource, MarketKline
from strategies.eth_portfolio_v1.preflight.readiness import (
    PortfolioV1ReadinessInspector,
)


SYMBOL = "ETH-USDT-PERP"
NOW_MS = 1_800_000_000_000
MINUTE_MS = 60_000
FOUR_HOURS_MS = 4 * 60 * MINUTE_MS
OKX_TIMEZONE = timezone(timedelta(hours=8))


def _seed_ready_stores(
    tmp_path,
    *,
    degraded_range_footprint: bool = False,
    mf_latest_close_ms: int | None = None,
) -> tuple[str, str]:
    market_path = str(tmp_path / "market.sqlite3")
    checkpoint_path = str(tmp_path / "checkpoint.sqlite3")
    latest_close = NOW_MS - MINUTE_MS

    kline_store = SqliteKlineStore(market_path)
    klines = []
    for index in range(2):
        close_ms = latest_close - (1 - index) * FOUR_HOURS_MS
        klines.append(
            MarketKline(
                exchange=ExchangeName.OKX,
                symbol=SYMBOL,
                raw_symbol="ETH-USDT-SWAP",
                interval="4h",
                open_time_ms=close_ms - FOUR_HOURS_MS + 1,
                close_time_ms=close_ms,
                open=Decimal("2000"),
                high=Decimal("2010"),
                low=Decimal("1990"),
                close=Decimal("2005"),
                volume=Decimal("100"),
                source=MarketDataSource.REST,
            )
        )
    kline_store.save(klines)

    range_store = SqliteRangeBarStore(market_path)
    range_store.save(
        (
            RangeBar(
                symbol=SYMBOL,
                range_pct=Decimal("0.002"),
                bar_id=1_800_000_000_001,
                start_time_ms=latest_close - MINUTE_MS,
                end_time_ms=latest_close,
                open=Decimal("2000"),
                high=Decimal("2010"),
                low=Decimal("1990"),
                close=Decimal("2005"),
                volume=Decimal("10"),
                buy_notional=Decimal("110"),
                sell_notional=Decimal("90"),
                trade_count=10,
            ),
        )
    )

    checkpoint = SqliteRangeCheckpointStore(checkpoint_path)
    for index in range(2):
        end_ms = latest_close - (1 - index) * FOUR_HOURS_MS
        checkpoint.save_completed_aggregate(
            exchange="okx",
            aggregate=RangeBarAggregate(
                symbol=SYMBOL,
                range_pct=Decimal("0.002"),
                bucket_start_ms=end_ms - FOUR_HOURS_MS + 1,
                bucket_end_ms=end_ms,
                bar_count=10,
                first_open=Decimal("2000"),
                last_close=Decimal("2005"),
                high=Decimal("2010"),
                low=Decimal("1990"),
                buy_notional_sum=Decimal("110"),
                sell_notional_sum=Decimal("90"),
                delta_notional_sum=Decimal("20"),
                notional_sum=Decimal("200"),
            ),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end_ms,
        )

    feature_store = SqliteTradeFeatureStore(market_path)
    if mf_latest_close_ms is None:
        latest_open = (
            (NOW_MS - 2 * MINUTE_MS) // MINUTE_MS
        ) * MINUTE_MS
    else:
        latest_open = int(mf_latest_close_ms) - MINUTE_MS + 1
    bars = []
    footprints = []
    for index in range(4):
        open_ms = latest_open - (3 - index) * MINUTE_MS
        close_ms = open_ms + MINUTE_MS - 1
        bars.append(
            FixedTimeTradeBar(
                exchange="okx",
                symbol=SYMBOL,
                open_time_ms=open_ms,
                close_time_ms=close_ms,
                available_time_ms=close_ms,
                open=Decimal("2000"),
                high=Decimal("2001"),
                low=Decimal("1999"),
                close=Decimal("2000"),
                volume=Decimal("2"),
                buy_volume=Decimal("1"),
                sell_volume=Decimal("1"),
                buy_notional=Decimal("100"),
                sell_notional=Decimal("100"),
                delta_volume=Decimal("0"),
                delta_notional=Decimal("0"),
                abs_delta_notional=Decimal("0"),
                trade_count=10,
                large_trade_share=Decimal("0.2"),
            )
        )
        footprints.append(
            TradeFootprintFeature(
                exchange="okx",
                symbol=SYMBOL,
                open_time_ms=open_ms,
                close_time_ms=close_ms,
                available_time_ms=close_ms,
                delta_notional=Decimal("0"),
                abs_delta_notional=Decimal("0"),
                taker_buy_ratio=Decimal("0.5"),
                close_pos=Decimal("0.5"),
                range_pct=Decimal("0.001"),
                return_pct=Decimal("0"),
                fp_max_bucket_abs_delta_pressure=Decimal("0.7"),
            )
        )
    feature_store.upsert_tradebars_many(bars)
    feature_store.upsert_footprints_many(footprints)
    coverage_start = bars[-3].open_time_ms
    coverage_end = bars[-1].close_time_ms
    range_available = coverage_start - MINUTE_MS
    feature_store.upsert_range_footprints_many(
        (
            RangeFootprintFeature(
                exchange="okx",
                symbol=SYMBOL,
                range_pct=Decimal("0.002"),
                price_step=Decimal("1"),
                range_bar_id=1,
                range_start_ms=range_available - MINUTE_MS,
                range_end_ms=range_available,
                available_time_ms=range_available,
                fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                fp_low_bucket_delta_pressure=Decimal("-0.2"),
                fp_high_bucket_delta_pressure=Decimal("0.3"),
                fp_delta_pressure=Decimal("0.1"),
                bucket_count=5,
                trade_count=20,
                quality=(
                    "DEGRADED_LOW_TRADE_COUNT"
                    if degraded_range_footprint
                    else "COMPLETE"
                ),
            ),
        )
    )
    feature_store.mark_range_footprint_coverage(
        symbol=SYMBOL,
        exchange="okx",
        range_pct="0.002",
        price_step="1",
        start_ms=coverage_start,
        end_ms=coverage_end,
        complete=True,
    )
    return market_path, checkpoint_path


def _inspect(
    market_path: str,
    checkpoint_path: str,
    *,
    now_ms: int = NOW_MS,
    large_share_min_samples: int = 2,
    readiness_mode: str = "live_freshness",
    archive_publish_lag_hours: float = 8.0,
):
    return PortfolioV1ReadinessInspector(
        symbol=SYMBOL,
        market_data_db_path=market_path,
        range_checkpoint_db_path=checkpoint_path,
        lf_min_records=2,
        range_speed_min_periods=2,
        mf_required_minutes=3,
        large_share_min_samples=large_share_min_samples,
        readiness_mode=readiness_mode,
        archive_publish_lag_hours=archive_publish_lag_hours,
        now_ms=now_ms,
    ).inspect()


def test_lf_closed_kline_enough_warmup_is_ready(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)

    result = _inspect(market, checkpoint)

    assert result.lf["closed_kline_count"] == 2
    assert result.lf["ok"] is True


def test_lf_closed_kline_stale_fails(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)

    result = _inspect(
        market,
        checkpoint,
        now_ms=NOW_MS + 2 * FOUR_HOURS_MS,
    )

    assert "lf_closed_kline_stale" in result.issues


def test_range_aggregate_complete_is_ready(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)

    result = _inspect(market, checkpoint)

    assert result.lf["latest_range_aggregate_status"] == "COMPLETE"
    assert result.lf["range_aggregate_causal_ok"] is True


def test_range_aggregate_missing_fails(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)
    with sqlite3.connect(checkpoint) as conn:
        conn.execute("DELETE FROM completed_range_aggregates")

    result = _inspect(market, checkpoint)

    assert "lf_range_aggregate_missing" in result.issues


def test_mf_tradebar_and_range_footprint_are_ready(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)

    result = _inspect(market, checkpoint)

    assert result.mf["tradebar_ready"] is True
    assert result.mf["range_footprint_ready"] is True
    assert result.mf["ok"] is True


def test_mf_missing_large_share_samples_fails(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)

    result = _inspect(
        market,
        checkpoint,
        large_share_min_samples=10,
    )

    assert "mf_large_share_samples_insufficient" in result.issues


def test_available_time_after_decision_boundary_fails(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(tmp_path)
    with sqlite3.connect(market) as conn:
        conn.execute(
            """
            UPDATE trade_footprint_1m_features
            SET available_time_ms=?
            WHERE open_time_ms=(
                SELECT MAX(open_time_ms)
                FROM trade_footprint_1m_features
            )
            """,
            (NOW_MS + MINUTE_MS,),
        )

    result = _inspect(market, checkpoint)

    assert "mf_fixed_time_footprint_future_available" in result.issues
    assert result.causal["ok"] is False


def test_degraded_range_footprint_fails(tmp_path) -> None:
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        degraded_range_footprint=True,
    )

    result = _inspect(market, checkpoint)

    assert "mf_range_footprint_degraded" in result.issues


def test_historical_preflight_accepts_safe_archive_edge(
    tmp_path,
) -> None:
    safe_end = safe_okx_archive_end_ms(NOW_MS)
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        mf_latest_close_ms=safe_end,
    )

    result = _inspect(
        market,
        checkpoint,
        readiness_mode="historical_preflight",
    )

    assert result.mf["ok"] is True
    assert result.mf["historical_coverage_ready"] is True
    assert result.mf["live_fresh_ready"] is False
    assert result.mf["mf_freshness_mode"] == "historical_preflight"
    assert result.mf["safe_archive_end_ms"] == safe_end
    assert "mf_tradebar_stale" not in result.issues


def test_historical_preflight_rejects_gap_before_safe_archive_edge(
    tmp_path,
) -> None:
    safe_end = safe_okx_archive_end_ms(NOW_MS)
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        mf_latest_close_ms=safe_end - 10 * MINUTE_MS,
    )

    result = _inspect(
        market,
        checkpoint,
        readiness_mode="historical_preflight",
    )

    assert result.mf["ok"] is False
    assert "mf_tradebar_ready_false" in result.issues


def test_live_freshness_mode_rejects_safe_archive_staleness(
    tmp_path,
) -> None:
    safe_end = safe_okx_archive_end_ms(NOW_MS)
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        mf_latest_close_ms=safe_end,
    )

    result = _inspect(
        market,
        checkpoint,
        readiness_mode="live_freshness",
    )

    assert "mf_tradebar_stale" in result.issues
    assert result.mf["live_fresh_ready"] is False


def test_historical_preflight_midnight_accepts_t2_safe_edge(
    tmp_path,
) -> None:
    now_ms = _okx_timestamp_ms(2026, 7, 7, 0, 30)
    safe_end = safe_okx_archive_end_ms(
        now_ms,
        archive_publish_lag_hours=8.0,
    )
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        mf_latest_close_ms=safe_end,
    )

    result = _inspect(
        market,
        checkpoint,
        now_ms=now_ms,
        readiness_mode="historical_preflight",
    )

    assert result.mf["ok"] is True
    assert result.mf["safe_archive_end_okx"] == (
        "2026-07-05 23:59:59+08"
    )
    assert result.mf["calendar_safe_archive_end_okx"] == (
        "2026-07-06 23:59:59+08"
    )
    assert result.mf["latest_archive_day_deferred"] is True
    for issue in (
        "mf_tradebar_ready_false",
        "mf_fixed_time_footprint_ready_false",
        "mf_range_footprint_ready_false",
        "mf_mf_signal_feature_ready_false",
    ):
        assert issue not in result.issues


def test_historical_preflight_midnight_rejects_gap_before_t2(
    tmp_path,
) -> None:
    now_ms = _okx_timestamp_ms(2026, 7, 7, 0, 30)
    safe_end = safe_okx_archive_end_ms(
        now_ms,
        archive_publish_lag_hours=8.0,
    )
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        mf_latest_close_ms=safe_end - 10 * MINUTE_MS,
    )

    result = _inspect(
        market,
        checkpoint,
        now_ms=now_ms,
        readiness_mode="historical_preflight",
    )

    assert result.mf["ok"] is False
    assert "mf_tradebar_ready_false" in result.issues


def test_historical_preflight_after_lag_requires_t1(
    tmp_path,
) -> None:
    midnight_now_ms = _okx_timestamp_ms(2026, 7, 7, 0, 30)
    t2_safe_end = safe_okx_archive_end_ms(
        midnight_now_ms,
        archive_publish_lag_hours=8.0,
    )
    market, checkpoint = _seed_ready_stores(
        tmp_path,
        mf_latest_close_ms=t2_safe_end,
    )

    result = _inspect(
        market,
        checkpoint,
        now_ms=_okx_timestamp_ms(2026, 7, 7, 9, 0),
        readiness_mode="historical_preflight",
    )

    assert result.mf["ok"] is False
    assert result.mf["safe_archive_end_okx"] == (
        "2026-07-06 23:59:59+08"
    )
    assert "mf_tradebar_ready_false" in result.issues


def _okx_timestamp_ms(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
) -> int:
    return int(
        datetime(
            year,
            month,
            day,
            hour,
            minute,
            tzinfo=OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )
