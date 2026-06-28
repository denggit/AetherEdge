from __future__ import annotations

from decimal import Decimal

import pytest
import pandas as pd

from strategies.eth_lf_portfolio_v8.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextEngine
from strategies.eth_lf_portfolio_v10a.engines.router import (
    MomentumEntryFilterConfig,
    PortfolioRouter,
)
from strategies.eth_lf_portfolio_v10a.features.range_speed import (
    PastOnlyRangeSpeedTracker,
    RangeSpeedEvaluation,
)
from strategies.eth_lf_portfolio_v10a.strategy import Strategy
from strategies.eth_lf_portfolio_v8.domain.models import (
    BarReadyContext,
    ClosedKlineContext,
    EngineSignal,
    MicroDecision,
    RangeAggregateContext,
    RoutedSignal,
    Side,
)


def _candidate(
    *,
    engine: str,
    side: Side,
    priority: int = 100,
    micro_filter_action: str = "NEUTRAL",
) -> EngineSignal:
    return EngineSignal(
        side=side,
        engine=engine,
        priority=priority,
        metadata={"micro_filter_action": micro_filter_action},
    )


def _speed(*, count: int | None, threshold: float | None, available: bool, fast: bool) -> RangeSpeedEvaluation:
    return RangeSpeedEvaluation(
        rf_bar_count=count,
        fast_threshold=threshold,
        is_fast_range_speed=fast,
        available=available,
        historical_periods=100 if available else 0,
    )


def _aggregate(
    *,
    bar_count: int = 10,
    coverage_status: str = "COMPLETE",
    imbalance: Decimal = Decimal("0"),
    close_pos: Decimal = Decimal("0.5"),
) -> RangeAggregateContext:
    return RangeAggregateContext(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        timeframe="4h",
        bucket_start_ms=0,
        bucket_end_ms=1,
        range_pct=Decimal("0.002"),
        bar_count=bar_count,
        first_open=Decimal("100"),
        last_close=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        buy_notional_sum=Decimal("50"),
        sell_notional_sum=Decimal("50"),
        delta_notional_sum=Decimal("0"),
        notional_sum=Decimal("100"),
        micro_return_pct=Decimal("0"),
        imbalance=imbalance,
        taker_buy_ratio=Decimal("0.5"),
        close_pos=close_pos,
        coverage_status=coverage_status,
    )


def _context(
    *,
    routed_signal: RoutedSignal | None = None,
    aggregate: RangeAggregateContext | None = None,
    engine_features: dict | None = None,
) -> BarReadyContext:
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=0,
            close_time_ms=1,
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("1"),
        ),
        range_aggregate=aggregate,
        micro=MicroDecision(
            signal_side=Side.FLAT,
            context_available=False,
            aligned=False,
            contra=False,
            entry_risk_scale=Decimal("1"),
            action="NO_SIGNAL",
        ),
        global_risk_scale=Decimal("1.3"),
        routed_signal=routed_signal or RoutedSignal.flat(),
        engine_features=engine_features or {},
    )


@pytest.mark.parametrize(
    ("candidate", "blocked"),
    [
        (
            _candidate(
                engine="MOMENTUM_V3",
                side=Side.LONG,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            ),
            True,
        ),
        (_candidate(engine="MOMENTUM_V3", side=Side.LONG, micro_filter_action="NEUTRAL"), False),
        (
            _candidate(
                engine="MOMENTUM_V3",
                side=Side.SHORT,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            ),
            False,
        ),
        (
            _candidate(
                engine="BULL_RECLAIM_V2",
                side=Side.LONG,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            ),
            False,
        ),
        (
            _candidate(
                engine="BEAR_V3_ONLY",
                side=Side.SHORT,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            ),
            False,
        ),
    ],
)
def test_v10_blocks_only_momentum_long_not_aligned(candidate: EngineSignal, blocked: bool) -> None:
    routed = PortfolioRouter().select([candidate])

    assert routed.metadata["blocked_by_v10_momentum_long_not_aligned"] is blocked
    assert (routed.side is Side.FLAT) is blocked


def test_v10_router_evaluates_micro_action_for_raw_momentum_candidate() -> None:
    router = PortfolioRouter(
        engines=(MomentumV3Engine(),),
        micro_evaluator=MicroContextEngine(),
    )
    context = _context(
        aggregate=_aggregate(),
        engine_features={"momentum": {"signal": 1}},
    )

    routed = router.evaluate(context)

    assert routed.side is Side.FLAT
    assert routed.metadata["blocked_by_v10_momentum_long_not_aligned"] is True
    assert routed.metadata["v10_momentum_long_micro_filter_action"] == "NOT_ALIGNED_RISK_REDUCED"


def test_v10a_blocks_momentum_short_above_past_q75() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=4, min_periods=2, fast_quantile=0.75)
    tracker.evaluate_and_observe(1)
    tracker.evaluate_and_observe(2)
    speed = tracker.evaluate_and_observe(3)

    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert speed.fast_threshold == pytest.approx(1.75)
    assert routed.side is Side.FLAT
    assert routed.metadata["blocked_by_v10a_momentum_short_fast_speed"] is True


def test_v10a_blocks_momentum_short_at_threshold_equality() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=4, min_periods=3, fast_quantile=0.75)
    tracker.evaluate_and_observe(4)
    tracker.evaluate_and_observe(4)
    tracker.evaluate_and_observe(4)
    speed = tracker.evaluate_and_observe(4)

    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert speed.fast_threshold == pytest.approx(4.0)
    assert speed.is_fast_range_speed is True
    assert routed.side is Side.FLAT
    assert routed.metadata["blocked_by_v10a_momentum_short_fast_speed"] is True


def test_complete_coverage_blocks_at_normal_threshold() -> None:
    tracker = PastOnlyRangeSpeedTracker(
        window_bars=4, min_periods=3, fast_quantile=0.75
    )
    tracker.warmup((20, 20, 20))

    speed = tracker.evaluate_and_observe(20, coverage_status="COMPLETE")
    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert routed.side is Side.FLAT


def test_degraded_minor_blocks_only_at_threshold_times_margin() -> None:
    tracker = PastOnlyRangeSpeedTracker(
        window_bars=4, min_periods=3, fast_quantile=0.75
    )
    tracker.warmup((20, 20, 20))

    near = tracker.evaluate_and_observe(
        20,
        coverage_status="RECOVERED_DEGRADED_MINOR",
        degraded_fast_margin=1.05,
    )
    at_margin = tracker.evaluate_and_observe(
        21,
        coverage_status="RECOVERED_DEGRADED_MINOR",
        degraded_fast_margin=1.05,
    )

    near_route = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=near,
    )
    margin_route = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=at_margin,
    )
    assert near_route.side is Side.SHORT
    assert margin_route.side is Side.FLAT
    assert margin_route.metadata["v10a_fast_speed_degraded_margin"] == pytest.approx(1.05)


@pytest.mark.parametrize(
    "coverage_status",
    ["COLD_START_PARTIAL", "RECOVERED_INCOMPLETE"],
)
def test_incomplete_coverage_makes_fast_speed_unavailable(
    coverage_status: str,
) -> None:
    tracker = PastOnlyRangeSpeedTracker(
        window_bars=4, min_periods=3, fast_quantile=0.75
    )
    tracker.warmup((1, 1, 1))
    speed = tracker.evaluate_and_observe(
        999, coverage_status=coverage_status
    )

    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert routed.side is Side.SHORT
    assert routed.metadata["v10a_fast_speed_available"] is False
    assert routed.metadata["v10a_fast_speed_unavailable_reason"] == coverage_status


@pytest.mark.parametrize(
    "coverage_status",
    ["COLD_START_PARTIAL", "RECOVERED_INCOMPLETE"],
)
def test_micro_context_falls_back_neutral_for_incomplete_coverage(
    coverage_status: str,
) -> None:
    decision = MicroContextEngine().evaluate(
        signal_side=Side.LONG,
        aggregate=_aggregate(
            coverage_status=coverage_status,
            imbalance=Decimal("-0.10"),
            close_pos=Decimal("0.20"),
        ),
    )

    assert decision.action == "NEUTRAL"
    assert decision.context_available is False
    assert decision.entry_risk_scale == Decimal("1")


def test_micro_context_degraded_minor_keeps_only_downside_filtering() -> None:
    engine = MicroContextEngine()
    contra = engine.evaluate(
        signal_side=Side.LONG,
        aggregate=_aggregate(
            coverage_status="RECOVERED_DEGRADED_MINOR",
            imbalance=Decimal("-0.10"),
            close_pos=Decimal("0.20"),
        ),
    )
    aligned = engine.evaluate(
        signal_side=Side.LONG,
        aggregate=_aggregate(
            coverage_status="RECOVERED_DEGRADED_MINOR",
            imbalance=Decimal("0.10"),
            close_pos=Decimal("0.80"),
        ),
    )

    assert contra.action == "CONTRA_RISK_REDUCED"
    assert contra.entry_risk_scale < Decimal("1")
    assert aligned.action == "NEUTRAL"
    assert aligned.aligned is False


def test_v10a_does_not_block_momentum_short_below_threshold() -> None:
    speed = _speed(count=3, threshold=4.0, available=True, fast=False)

    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert routed.side is Side.SHORT
    assert routed.metadata["blocked_by_v10a_momentum_short_fast_speed"] is False


@pytest.mark.parametrize(
    "candidate",
    [
        _candidate(engine="MOMENTUM_V3", side=Side.LONG),
        _candidate(engine="BEAR_V3_ONLY", side=Side.SHORT),
    ],
)
def test_v10a_fast_speed_does_not_block_other_engine_or_side(candidate: EngineSignal) -> None:
    routed = PortfolioRouter().select(
        [candidate],
        range_speed=_speed(count=10, threshold=5.0, available=True, fast=True),
    )

    assert routed.side is candidate.side
    assert routed.metadata["blocked_by_v10a_momentum_short_fast_speed"] is False


def test_range_speed_threshold_excludes_current_bucket() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=3, min_periods=2, fast_quantile=0.75)
    tracker.evaluate_and_observe(1)
    tracker.evaluate_and_observe(1)

    current = tracker.evaluate_and_observe(100)
    following = tracker.evaluate_and_observe(1)

    assert current.fast_threshold == pytest.approx(1.0)
    assert current.is_fast_range_speed is True
    assert following.fast_threshold == pytest.approx(50.5)


def test_range_speed_matches_shifted_pandas_rolling_quantile() -> None:
    counts = [3, 9, 4, 4, 10, 2, 8]
    tracker = PastOnlyRangeSpeedTracker(window_bars=4, min_periods=3, fast_quantile=0.75)
    actual = [tracker.evaluate_and_observe(count) for count in counts]
    expected = pd.Series(counts, dtype="float64").shift(1).rolling(4, min_periods=3).quantile(0.75)

    for result, threshold in zip(actual, expected, strict=True):
        if pd.isna(threshold):
            assert result.available is False
            assert result.fast_threshold is None
        else:
            assert result.available is True
            assert result.fast_threshold == pytest.approx(float(threshold))
            assert result.is_fast_range_speed is (float(result.rf_bar_count) >= float(threshold))


def test_range_speed_insufficient_history_falls_back_without_block() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=4, min_periods=3, fast_quantile=0.75)
    tracker.evaluate_and_observe(1)
    tracker.evaluate_and_observe(2)
    speed = tracker.evaluate_and_observe(100)

    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert speed.available is False
    assert routed.side is Side.SHORT
    assert routed.metadata["v10a_fast_speed_available"] is False


def test_range_speed_missing_count_falls_back_without_block() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=4, min_periods=2, fast_quantile=0.75)
    tracker.evaluate_and_observe(1)
    tracker.evaluate_and_observe(2)
    speed = tracker.evaluate_and_observe(None)

    routed = PortfolioRouter().select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=speed,
    )

    assert speed.available is False
    assert routed.side is Side.SHORT
    assert routed.metadata["v10a_fast_speed_available"] is False
    assert routed.metadata["blocked_by_v10a_momentum_short_fast_speed"] is False


def test_router_continues_to_next_candidate_after_momentum_is_blocked() -> None:
    momentum = _candidate(
        engine="MOMENTUM_V3",
        side=Side.LONG,
        priority=100,
        micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
    )
    bear = _candidate(engine="BEAR_V3_ONLY", side=Side.SHORT, priority=50)

    routed = PortfolioRouter().select([momentum, bear])

    assert routed.engine == "BEAR_V3_ONLY"
    assert routed.side is Side.SHORT
    assert routed.metadata["blocked_by_v10_momentum_long_not_aligned"] is True


def test_router_returns_flat_when_only_candidate_is_blocked() -> None:
    routed = PortfolioRouter().select(
        [
            _candidate(
                engine="MOMENTUM_V3",
                side=Side.LONG,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            )
        ]
    )

    assert routed.side is Side.FLAT
    assert routed.metadata["blocked_by_v10_momentum_long_not_aligned"] is True


def test_filter_switches_restore_v9e_candidate_behavior() -> None:
    router = PortfolioRouter(
        entry_filter_config=MomentumEntryFilterConfig(
            enable_momentum_long_not_aligned_block=False,
            enable_momentum_short_fast_speed_block=False,
        )
    )

    long_route = router.select(
        [
            _candidate(
                engine="MOMENTUM_V3",
                side=Side.LONG,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            )
        ]
    )
    short_route = router.select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=_speed(count=10, threshold=5.0, available=True, fast=True),
    )

    assert long_route.side is Side.LONG
    assert short_route.side is Side.SHORT


def test_disabling_v10a_keeps_v10_long_gate_enabled() -> None:
    router = PortfolioRouter(
        entry_filter_config=MomentumEntryFilterConfig(
            enable_momentum_long_not_aligned_block=True,
            enable_momentum_short_fast_speed_block=False,
        )
    )

    long_route = router.select(
        [
            _candidate(
                engine="MOMENTUM_V3",
                side=Side.LONG,
                micro_filter_action="NOT_ALIGNED_RISK_REDUCED",
            )
        ]
    )
    short_route = router.select(
        [_candidate(engine="MOMENTUM_V3", side=Side.SHORT)],
        range_speed=_speed(count=10, threshold=5.0, available=True, fast=True),
    )

    assert long_route.side is Side.FLAT
    assert short_route.side is Side.SHORT


def test_decision_audit_exposes_v10a_fields_when_speed_is_unavailable() -> None:
    strategy = Strategy()
    routed = RoutedSignal(
        side=Side.SHORT,
        engine="MOMENTUM_V3",
        priority=100,
        metadata={
            "blocked_by_v10_momentum_long_not_aligned": False,
            "blocked_by_v10a_momentum_short_fast_speed": False,
            "v10a_fast_speed_available": False,
            "rf_bar_count_fast_threshold": None,
            "is_fast_range_speed": False,
            "range_speed_historical_periods": 2,
        },
    )
    context = _context(routed_signal=routed, engine_features={"momentum": {"signal": -1}})

    audit = strategy._build_decision_audit(context, [])

    assert audit["v10a_fast_speed_available"] is False
    assert audit["blocked_by_v10a_momentum_short_fast_speed"] is False
    assert audit["range_speed_rolling_window_bars"] == 1080
    assert audit["range_speed_min_periods"] == 100
    assert audit["range_speed_fast_quantile"] == pytest.approx(0.75)
