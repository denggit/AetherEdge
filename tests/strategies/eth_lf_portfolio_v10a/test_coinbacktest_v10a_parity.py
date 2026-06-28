from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

from strategies.eth_lf_portfolio_v10a.engines.router import (
    MomentumEntryFilterConfig,
    PortfolioRouter,
)
from strategies.eth_lf_portfolio_v10a.features.range_speed import PastOnlyRangeSpeedTracker
from strategies.eth_lf_portfolio_v8.domain.models import EngineSignal, Side


WINDOW_BARS = 4
MIN_PERIODS = 3
FAST_QUANTILE = 0.75


@dataclass(frozen=True)
class MinimalV10ACase:
    name: str
    engine: str
    side: Side
    micro_filter_action: str
    past_rf_bar_counts: tuple[int, ...]
    current_rf_bar_count: int | None
    expected_block: bool


@dataclass(frozen=True)
class ReferenceDecision:
    blocked: bool
    fast_speed_available: bool
    fast_threshold: float | None


def _coinbacktest_v10a_reference(case: MinimalV10ACase) -> ReferenceDecision:
    """Minimal oracle abstracted from the V10/V10A backtest gate functions.

    The production contract uses strict ``current > threshold``. Parity samples
    intentionally avoid equality, so they also agree with the reference file's
    literal ``Series.ge(threshold)`` implementation.
    """

    blocked_by_v10 = (
        case.engine == "MOMENTUM_V3"
        and case.side is Side.LONG
        and case.micro_filter_action == "NOT_ALIGNED_RISK_REDUCED"
    )
    counts = pd.Series(
        [*case.past_rf_bar_counts, case.current_rf_bar_count],
        dtype="float64",
    )
    threshold_value = (
        counts.shift(1)
        .rolling(WINDOW_BARS, min_periods=MIN_PERIODS)
        .quantile(FAST_QUANTILE)
        .iloc[-1]
    )
    available = case.current_rf_bar_count is not None and pd.notna(threshold_value)
    threshold = float(threshold_value) if available else None
    is_fast = bool(
        available
        and threshold is not None
        and float(case.current_rf_bar_count) > threshold
    )
    blocked_by_v10a = (
        case.engine == "MOMENTUM_V3"
        and case.side is Side.SHORT
        and is_fast
    )
    return ReferenceDecision(
        blocked=blocked_by_v10 or blocked_by_v10a,
        fast_speed_available=available,
        fast_threshold=threshold,
    )


def _aetheredge_decision(case: MinimalV10ACase) -> ReferenceDecision:
    tracker = PastOnlyRangeSpeedTracker(
        window_bars=WINDOW_BARS,
        min_periods=MIN_PERIODS,
        fast_quantile=FAST_QUANTILE,
    )
    for count in case.past_rf_bar_counts:
        tracker.evaluate_and_observe(count)
    speed = tracker.evaluate_and_observe(case.current_rf_bar_count)
    router = PortfolioRouter(
        entry_filter_config=MomentumEntryFilterConfig(
            range_speed_rolling_window_bars=WINDOW_BARS,
            range_speed_min_periods=MIN_PERIODS,
            range_speed_fast_quantile=FAST_QUANTILE,
        )
    )
    routed = router.select(
        [
            EngineSignal(
                side=case.side,
                engine=case.engine,
                priority=100,
                metadata={"micro_filter_action": case.micro_filter_action},
            )
        ],
        range_speed=speed,
    )
    blocked = bool(
        routed.metadata["blocked_by_v10_momentum_long_not_aligned"]
        or routed.metadata["blocked_by_v10a_momentum_short_fast_speed"]
    )
    return ReferenceDecision(
        blocked=blocked,
        fast_speed_available=bool(routed.metadata["v10a_fast_speed_available"]),
        fast_threshold=routed.metadata["rf_bar_count_fast_threshold"],
    )


PARITY_CASES = (
    MinimalV10ACase(
        name="v10_momentum_long_not_aligned_blocks",
        engine="MOMENTUM_V3",
        side=Side.LONG,
        micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
        past_rf_bar_counts=(),
        current_rf_bar_count=None,
        expected_block=True,
    ),
    MinimalV10ACase(
        name="v10_momentum_long_neutral_does_not_block",
        engine="MOMENTUM_V3",
        side=Side.LONG,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(),
        current_rf_bar_count=None,
        expected_block=False,
    ),
    MinimalV10ACase(
        name="v10_bear_not_aligned_does_not_block",
        engine="BEAR_V3_ONLY",
        side=Side.SHORT,
        micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
        past_rf_bar_counts=(),
        current_rf_bar_count=None,
        expected_block=False,
    ),
    MinimalV10ACase(
        name="v10a_momentum_short_above_past_q75_blocks",
        engine="MOMENTUM_V3",
        side=Side.SHORT,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(2, 3, 4),
        current_rf_bar_count=9,
        expected_block=True,
    ),
    MinimalV10ACase(
        name="v10a_momentum_short_below_past_q75_does_not_block",
        engine="MOMENTUM_V3",
        side=Side.SHORT,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(2, 3, 8),
        current_rf_bar_count=3,
        expected_block=False,
    ),
    MinimalV10ACase(
        name="v10a_momentum_long_fast_does_not_block",
        engine="MOMENTUM_V3",
        side=Side.LONG,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(2, 3, 4),
        current_rf_bar_count=9,
        expected_block=False,
    ),
    MinimalV10ACase(
        name="v10a_bear_short_fast_does_not_block",
        engine="BEAR_V3_ONLY",
        side=Side.SHORT,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(2, 3, 4),
        current_rf_bar_count=9,
        expected_block=False,
    ),
    MinimalV10ACase(
        name="v10a_insufficient_history_does_not_block",
        engine="MOMENTUM_V3",
        side=Side.SHORT,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(1, 2),
        current_rf_bar_count=99,
        expected_block=False,
    ),
    MinimalV10ACase(
        name="v10a_missing_current_count_does_not_block",
        engine="MOMENTUM_V3",
        side=Side.SHORT,
        micro_filter_action="NEUTRAL",
        past_rf_bar_counts=(1, 2, 3),
        current_rf_bar_count=None,
        expected_block=False,
    ),
)


@pytest.mark.parametrize("case", PARITY_CASES, ids=lambda case: case.name)
def test_aetheredge_matches_coinbacktest_v10a_minimal_cases(case: MinimalV10ACase) -> None:
    reference = _coinbacktest_v10a_reference(case)
    actual = _aetheredge_decision(case)

    assert reference.blocked is case.expected_block
    assert actual.blocked is reference.blocked
    assert actual.fast_speed_available is reference.fast_speed_available
    if reference.fast_threshold is None:
        assert actual.fast_threshold is None
    else:
        assert actual.fast_threshold == pytest.approx(reference.fast_threshold)
