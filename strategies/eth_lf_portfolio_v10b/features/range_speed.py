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
    coverage_status: str = "COMPLETE"
    unavailable_reason: str | None = None
    degraded_fast_margin: float = 1.0


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

    def evaluate_and_observe(
        self,
        rf_bar_count: int | None,
        *,
        coverage_status: str = "COMPLETE",
        degraded_fast_margin: float = 1.05,
    ) -> RangeSpeedEvaluation:
        """Compare before appending, equivalent to ``shift(1).rolling(...).quantile``."""

        coverage = str(coverage_status).strip().upper()
        usable_coverage = coverage in {
            "COMPLETE",
            "RECOVERED_DEGRADED_MINOR",
        }
        past_values = [value for value in self._history if value is not None and math.isfinite(value)]
        available = (
            usable_coverage
            and rf_bar_count is not None
            and len(past_values) >= self.min_periods
        )
        threshold = _linear_quantile(past_values, self.fast_quantile) if available else None
        configured_degraded_margin = max(
            1.0, float(degraded_fast_margin)
        )
        applied_margin = (
            configured_degraded_margin
            if coverage == "RECOVERED_DEGRADED_MINOR"
            else 1.0
        )
        is_fast = bool(
            available
            and threshold is not None
            and float(rf_bar_count) >= threshold * applied_margin
        )
        unavailable_reason = None
        if not usable_coverage:
            unavailable_reason = coverage
        elif rf_bar_count is None:
            unavailable_reason = "rf_bar_count_missing"
        elif len(past_values) < self.min_periods:
            unavailable_reason = "insufficient_complete_history"
        result = RangeSpeedEvaluation(
            rf_bar_count=rf_bar_count,
            fast_threshold=threshold,
            is_fast_range_speed=is_fast,
            available=available,
            historical_periods=len(past_values),
            coverage_status=coverage,
            unavailable_reason=unavailable_reason,
            degraded_fast_margin=configured_degraded_margin,
        )
        if coverage == "COMPLETE":
            self._history.append(None if rf_bar_count is None else float(rf_bar_count))
        return result

    def warmup(self, rf_bar_counts: list[int] | tuple[int, ...]) -> int:
        """Load already-completed, COMPLETE buckets in caller-provided order."""

        for value in rf_bar_counts:
            self._history.append(float(value))
        return len(rf_bar_counts)

    @property
    def history(self) -> tuple[float | None, ...]:
        return tuple(self._history)

    @property
    def complete_history_count(self) -> int:
        return sum(
            value is not None and math.isfinite(value)
            for value in self._history
        )


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
