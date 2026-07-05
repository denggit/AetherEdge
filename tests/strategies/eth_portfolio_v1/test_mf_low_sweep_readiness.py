from __future__ import annotations

from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState

from _mf_test_helpers import READY, config, range_footprint, setup_bars


def _evaluate(*, readiness=READY, cfg=None, bars=None, contexts=None):
    bars = setup_bars() if bars is None else bars
    contexts = (
        [range_footprint(available_time_ms=bars[-1].open_time_ms - 1)]
        if contexts is None
        else contexts
    )
    return evaluate_mf_low_sweep(
        config=cfg or config(),
        bars=bars,
        range_footprints=contexts,
        large_share_history=[
            item.large_trade_share for item in bars[:-1]
        ],
        readiness=readiness,
        sleeve=MfSleeveState(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
            enabled=True,
        ),
    )


def test_data_not_ready_produces_no_mf_signal() -> None:
    decision, audit = _evaluate(
        readiness={
            "mf_signal_feature_ready": False,
            "range_footprint_ready": True,
            "tradebar_ready": True,
        }
    )
    assert decision is None
    assert audit["blocked_reason"] == "data_not_ready"


def test_range_footprint_not_ready_produces_no_mf_signal() -> None:
    decision, audit = _evaluate(
        readiness={
            "mf_signal_feature_ready": True,
            "range_footprint_ready": False,
            "tradebar_ready": True,
        }
    )
    assert decision is None
    assert audit["data_ready"] is False


def test_missing_large_share_threshold_produces_no_mf_signal() -> None:
    decision, audit = _evaluate(cfg=config(large_share_min_samples=20))
    assert decision is None
    assert audit["blocked_reason"] == "missing_feature"
    assert "large_share_rq80_90d" in audit["missing_features"]


def test_future_available_time_produces_no_signal_and_fails_causal_audit() -> None:
    bars = setup_bars()
    future = range_footprint(
        available_time_ms=bars[-1].close_time_ms + 2
    )
    decision, audit = _evaluate(bars=bars, contexts=[future])
    assert decision is None
    assert audit["causal_ok"] is False


def test_ready_but_no_setup_produces_no_signal() -> None:
    bars = setup_bars(
        latest_low="99",
        latest_close="99.5",
        latest_high="101",
    )
    decision, audit = _evaluate(bars=bars)
    assert decision is None
    assert audit["blocked_reason"] == "no_setup"
