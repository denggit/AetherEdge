from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class RangeSpeedEvaluation:
    """Past-only range-speed state for one completed 4H bucket."""

    rf_bar_count: int | None
    fast_threshold: float | None
    is_fast_range_speed: bool
    available: bool
    historical_periods: int


class PastOnlyRangeSpeedTracker:
    """Evaluate the current count against a rolling quantile of earlier buckets."""

    def __init__(self, *, window_bars: int, min_periods: int, fast_quantile: float) -> None:
        if window_bars <= 0:
            raise ValueError("range speed rolling window must be positive")
        if min_periods <= 0 or min_periods > window_bars:
            raise ValueError("range speed min_periods must be in [1, window_bars]")
        if not 0.0 <= fast_quantile <= 1.0:
            raise ValueError("range speed fast_quantile must be in [0, 1]")
        self.window_bars = int(window_bars)
        self.min_periods = int(min_periods)
        self.fast_quantile = float(fast_quantile)
        self._history: deque[float | None] = deque(maxlen=self.window_bars)

    def evaluate_and_observe(self, rf_bar_count: int | None) -> RangeSpeedEvaluation:
        """Compare before appending, equivalent to ``shift(1).rolling(...).quantile``."""

        past_values = [value for value in self._history if value is not None and math.isfinite(value)]
        available = rf_bar_count is not None and len(past_values) >= self.min_periods
        threshold = _linear_quantile(past_values, self.fast_quantile) if available else None
        is_fast = bool(available and threshold is not None and float(rf_bar_count) > threshold)
        result = RangeSpeedEvaluation(
            rf_bar_count=rf_bar_count,
            fast_threshold=threshold,
            is_fast_range_speed=is_fast,
            available=available,
            historical_periods=len(past_values),
        )
        self._history.append(None if rf_bar_count is None else float(rf_bar_count))
        return result

    @property
    def history(self) -> tuple[float | None, ...]:
        return tuple(self._history)


def _linear_quantile(values: list[float], quantile: float) -> float:
    """Match pandas' default linear interpolation for a non-empty finite sample."""

    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return float(ordered[lower_index])
    fraction = position - lower_index
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    return float(lower + (upper - lower) * fraction)
