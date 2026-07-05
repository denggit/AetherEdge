from __future__ import annotations

from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState

from _mf_test_helpers import (
    READY,
    bar,
    config,
    historical_large_shares,
    range_footprint,
    setup_bars,
)


def _evaluate(*, bars=None, pressure="0.80", history=None, context_ms=None):
    items = setup_bars() if bars is None else bars
    context_time = (
        items[-1].open_time_ms - 1
        if context_ms is None
        else context_ms
    )
    return evaluate_mf_low_sweep(
        config=config(),
        bars=items,
        range_footprints=[
            range_footprint(
                available_time_ms=context_time,
                pressure=pressure,
            )
        ],
        large_share_history=(
            historical_large_shares()
            if history is None
            else history
        ),
        readiness=READY,
        sleeve=MfSleeveState(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
        ),
        next_open_price=items[-1].close,
        next_open_time_ms=items[-1].close_time_ms + 1,
    )


def test_coinbacktest_primary_sample_enters_in_live_mapper() -> None:
    decision, audit = _evaluate()
    assert decision is not None
    assert decision.decision_type == "open"
    assert audit["entry_candidate"] is True
    assert audit["event_variant"] == "fade_close_through"
    assert audit["single_swing"] is True
    assert audit["event_max_swing_age"] == 12
    assert str(audit["event_min_swing_prominence_pct"]) == "0.0030"


def test_reject_shape_is_not_in_primary_event_set() -> None:
    decision, audit = _evaluate(
        bars=setup_bars(
            latest_low="89",
            latest_close="95",
            latest_high="101",
        )
    )
    assert decision is None
    assert audit["low_sweep_event"] is False
    assert audit["single_swing"] is True


def test_wick_shape_does_not_bypass_primary_close_through_event() -> None:
    decision, audit = _evaluate(
        bars=setup_bars(
            latest_low="80",
            latest_close="95",
            latest_high="101",
        )
    )
    assert decision is None
    assert audit["low_sweep_event"] is False


def test_swing_older_than_source_age_grid_does_not_enter() -> None:
    items = [
        bar(
            index=index,
            low=str(value),
            high="102",
            close="100",
        )
        for index, value in enumerate(
            [100, 99, 98, 97, 96, 95, 90, 94, 95, 96]
        )
    ]
    for index in range(len(items), 1_450):
        items.append(
            bar(
                index=index,
                low="96",
                high="102",
                close="100",
            )
        )
    items.append(
        bar(
            index=1_450,
            low="89",
            high="101",
            close="89.5",
            large_share="0.90",
        )
    )
    decision, audit = _evaluate(bars=items)
    assert decision is None
    assert audit["swing_low_age"] == 1_444
    assert audit["low_sweep_event"] is False


def test_large_share_90d_q80_gate_matches_backtest() -> None:
    decision, audit = _evaluate(
        bars=setup_bars(latest_large_share="0.05")
    )
    assert decision is None
    assert audit["large_share_rq80_90d"] is False


def test_footprint_threshold_gate_matches_backtest() -> None:
    decision, audit = _evaluate(pressure="0.59")
    assert decision is None
    assert audit["entry_candidate"] is False


def test_future_range_context_is_rejected() -> None:
    items = setup_bars()
    decision, audit = _evaluate(
        bars=items,
        context_ms=items[-1].close_time_ms + 2,
    )
    assert decision is None
    assert audit["causal_ok"] is False
