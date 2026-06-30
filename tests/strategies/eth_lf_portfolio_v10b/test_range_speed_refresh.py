from __future__ import annotations

from strategies.eth_lf_portfolio_v10b.features.range_speed import PastOnlyRangeSpeedTracker
from strategies.eth_lf_portfolio_v10b.strategy import Strategy


def test_warmup_append_and_replace_clears_old_values() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=5, min_periods=2, fast_quantile=0.5)

    tracker.warmup((1, 2, 3))
    count = tracker.replace_history((7, 8))

    assert count == 2
    assert tracker.history == (7.0, 8.0)


def test_strategy_replace_range_speed_history_updates_status() -> None:
    strategy = Strategy()
    min_periods = strategy.config.entry_filters.range_speed_min_periods

    count = strategy.replace_range_speed_history(range(min_periods))
    status = strategy.range_speed_history_status()

    assert count == min_periods
    assert status["available"] is True


def test_evaluate_and_observe_remains_past_only_after_replace() -> None:
    tracker = PastOnlyRangeSpeedTracker(window_bars=5, min_periods=2, fast_quantile=1.0)
    tracker.replace_history((1, 2))

    result = tracker.evaluate_and_observe(1)

    assert result.fast_threshold == 2.0
    assert result.is_fast_range_speed is False
