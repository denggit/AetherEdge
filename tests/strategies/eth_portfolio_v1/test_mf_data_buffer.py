from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from src.market_data.storage.trade_feature_store import (
    LargeTradeShareSample,
    SqliteTradeFeatureStore,
)
from strategies.eth_portfolio_v1.domain.mf_data import MfDataBuffer
from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState

from _mf_test_helpers import (
    BASE_MS,
    MINUTE_MS,
    READY,
    bar,
    config,
    historical_large_shares,
    range_footprint,
    setup_bars,
)


def _stub_initial_load(
    buffer: MfDataBuffer,
    monkeypatch,
    *,
    samples: list[LargeTradeShareSample],
    bars,
) -> dict[str, int]:
    limits: dict[str, int] = {}

    def load_large_shares(**kwargs):
        limits["large_share"] = kwargs["limit"]
        return samples

    def load_tradebars(**kwargs):
        limits["tradebars"] = kwargs["limit"]
        return list(bars[-kwargs["limit"] :])

    monkeypatch.setattr(
        buffer._store,
        "load_recent_large_trade_shares",
        load_large_shares,
    )
    monkeypatch.setattr(
        buffer._store,
        "load_recent_tradebars",
        load_tradebars,
    )
    monkeypatch.setattr(
        buffer._store,
        "load_latest_range_footprint_context",
        lambda **kwargs: None,
    )
    return limits


def test_load_initial_separates_large_share_and_decision_queries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars = setup_bars()
    samples = [
        LargeTradeShareSample(
            open_time_ms=BASE_MS - (4 - index) * MINUTE_MS,
            large_trade_share=value,
            quality=quality,
        )
        for index, (value, quality) in enumerate(
            (
                (Decimal("0.10"), "COMPLETE"),
                (Decimal("0.90"), "DEGRADED_LOW_TRADE_COUNT"),
                (Decimal("0.80"), "MISSING"),
                (None, "COMPLETE"),
                (Decimal("0.30"), "COMPLETE"),
            )
        )
    ]
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=4,
        decision_buffer_max_minutes=10,
        large_share_quantile_window_days=90,
    )
    limits = _stub_initial_load(
        buffer,
        monkeypatch,
        samples=samples,
        bars=bars,
    )

    assert buffer.load_initial() == 4

    assert limits == {
        "large_share": 90 * 1_440,
        "tradebars": 4,
    }
    assert buffer.recent_bars() == tuple(bars[-4:])
    assert buffer.large_trade_share_history() == (
        Decimal("0.10"),
        Decimal("0.30"),
    )
    assert buffer.audit()["large_share_samples"] == 2


def test_load_initial_uses_latest_causal_range_context(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "features.sqlite3"
    store = SqliteTradeFeatureStore(path=store_path)
    bars = setup_bars()
    store.upsert_tradebars_many(bars)
    latest = bars[-1]
    causal = replace(
        range_footprint(available_time_ms=latest.open_time_ms - 1),
        range_bar_id=1,
    )
    non_causal = replace(
        range_footprint(
            available_time_ms=latest.open_time_ms + MINUTE_MS
        ),
        range_bar_id=2,
    )
    store.upsert_range_footprints_many([causal, non_causal])
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(store_path),
        decision_buffer_minutes=len(bars),
        decision_buffer_max_minutes=len(bars),
    )

    assert buffer.load_initial() == len(bars)

    contexts = buffer.range_footprints()
    assert contexts == (causal,)
    assert contexts[0].available_time_ms <= latest.open_time_ms


def test_load_initial_skips_non_causal_range_context(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "features.sqlite3"
    store = SqliteTradeFeatureStore(path=store_path)
    bars = setup_bars()
    store.upsert_tradebars_many(bars)
    latest = bars[-1]
    non_causal = replace(
        range_footprint(
            available_time_ms=latest.open_time_ms + MINUTE_MS
        ),
        range_bar_id=3,
    )
    store.upsert_range_footprints_many([non_causal])
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(store_path),
        decision_buffer_minutes=len(bars),
        decision_buffer_max_minutes=len(bars),
    )

    buffer.load_initial()
    decision, audit = evaluate_mf_low_sweep(
        config=config(),
        bars=buffer.recent_bars(),
        range_footprints=buffer.range_footprints(),
        large_share_history=historical_large_shares(),
        readiness={
            **READY,
            "mf_signal_feature_ready": False,
            "range_footprint_context_ready": False,
        },
        sleeve=MfSleeveState(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
        ),
        next_open_price=Decimal("90"),
        next_open_time_ms=latest.close_time_ms + 1,
    )

    assert buffer.range_footprints() == ()
    assert decision is None
    assert audit["reason"] == "data_not_ready"


def test_realtime_large_share_samples_filter_quality_and_expire(
    tmp_path: Path,
) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        large_share_quantile_window_days=1,
    )
    buffer.append_tradebar(bar(index=0, large_share="0.10"))
    buffer.append_tradebar(
        replace(
            bar(index=1, large_share="0.90"),
            quality="DEGRADED_LOW_TRADE_COUNT",
        )
    )
    buffer.append_tradebar(bar(index=1_441, large_share="0.20"))

    assert buffer.large_trade_share_history() == (Decimal("0.20"),)
    assert buffer.audit()["large_share_samples"] == 1


def test_large_share_history_before_excludes_current_signal_bar(
    tmp_path: Path,
) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
    )
    previous = bar(index=0, large_share="0.10")
    current = bar(index=1, large_share="0.90")
    buffer.append_tradebar(previous)
    buffer.append_tradebar(current)

    assert buffer.large_trade_share_history(
        before_open_time_ms=current.open_time_ms
    ) == (Decimal("0.10"),)
    assert buffer.large_trade_share_history() == (
        Decimal("0.10"),
        Decimal("0.90"),
    )


def test_lightweight_load_preserves_quantile_and_swing_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bars = setup_bars()
    old_history = historical_large_shares()
    start_ms = bars[0].open_time_ms - len(old_history) * MINUTE_MS
    samples = [
        LargeTradeShareSample(
            open_time_ms=start_ms + index * MINUTE_MS,
            large_trade_share=value,
            quality="COMPLETE",
        )
        for index, value in enumerate(old_history)
    ]
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=100,
        decision_buffer_max_minutes=100,
        large_share_quantile_window_days=90,
    )
    _stub_initial_load(
        buffer,
        monkeypatch,
        samples=samples,
        bars=bars,
    )
    buffer.load_initial()
    context = range_footprint(
        available_time_ms=bars[-1].open_time_ms - 1
    )

    def evaluate(history):
        return evaluate_mf_low_sweep(
            config=config(),
            bars=buffer.recent_bars(),
            range_footprints=(context,),
            large_share_history=history,
            readiness=READY,
            sleeve=MfSleeveState(
                strategy_id="eth_portfolio_v1",
                symbol="ETH-USDT-PERP",
            ),
            next_open_price=Decimal("90"),
            next_open_time_ms=bars[-1].close_time_ms + 1,
        )

    _, lightweight_audit = evaluate(
        buffer.large_trade_share_history(
            before_open_time_ms=bars[-1].open_time_ms
        )
    )
    _, old_audit = evaluate(old_history)

    for field in (
        "large_share_threshold",
        "large_share_rq80_90d",
        "swing_low",
        "swing_low_age",
        "swing_low_prominence_pct",
        "low_sweep_event",
    ):
        assert lightweight_audit[field] == old_audit[field]
    assert lightweight_audit["large_share_threshold"] == Decimal("0.10")
    assert lightweight_audit["large_share_rq80_90d"] is True
