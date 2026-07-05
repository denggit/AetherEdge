from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from strategies.eth_portfolio_v1.domain.mf_live_policy import (
    MF_LIVE_EXIT_VARIANT,
    validate_mf_exit_variant,
)


COINBACKTEST_PORTFOLIO_SOURCE = (
    r"D:\Code_Project\CoinBacktest\backtest\portfolio"
    r"\eth_portfolio_V1_lf_v10b_low_sweep_mf_backtest.py"
)
COINBACKTEST_CHILD_SOURCE = (
    "backtest/mf/low_sweep/low_sweep_V1_a0_footprint_backtest.py"
)
MF_VARIANT_NAME = (
    "A0_fp_abs_delta_high__single_swing__next_open__time48__no_stop"
)
MF_ENGINE_NAME = "MF_LOW_SWEEP_TIME48"
MF_POSITION_ID_PREFIX = "mf-low-sweep-time48-"
MF_RANGE_FOOTPRINT_EVENT_TYPE = "range_footprint_feature"

# Source mapping:
# - portfolio wrapper _build_mf_args/_make_mf_variant
# - child formal_variant_specs
# - research build_low_sweep_events/build_fixed_candidate_masks/
#   build_candidate_layer_masks/build_support_mask
MF_ENTRY_REQUIRED_FIELDS = (
    "spike_pct",
    "close_pos",
    "large_trade_share",
    "large_share_rq80_90d",
    "swing_low",
    "swing_low_age",
    "swing_low_prominence_pct",
    "single_swing",
    "fp_max_bucket_abs_delta_pressure",
    "fp_abs_delta_high_threshold",
    "signal_side",
)


@dataclass(frozen=True)
class MfLowSweepConfig:
    enabled: bool = False
    position_fraction: Decimal = Decimal("0.10")
    footprint_abs_delta_threshold: Decimal = Decimal("0.60")
    spike_threshold: Decimal = Decimal("0.0100")
    close_pos_max: Decimal = Decimal("0.30")
    large_share_quantile: Decimal = Decimal("0.80")
    large_share_window_days: int = 90
    large_share_min_samples: int = 43_200
    pivot_left: int = 6
    pivot_right: int = 3
    min_swing_age: int = 3
    max_swing_age: int = 1_440
    min_swing_prominence_pct: Decimal = Decimal("0.0015")
    holding_minutes: int = 48
    decision_buffer_minutes: int = 4_320
    decision_buffer_max_minutes: int = 10_080
    range_pct: Decimal = Decimal("0.002")
    range_price_step: Decimal = Decimal("1")
    exit_variant: str = MF_LIVE_EXIT_VARIANT

    def __post_init__(self) -> None:
        validate_mf_exit_variant(self.exit_variant)
        if not Decimal("0") < self.position_fraction <= Decimal("1"):
            raise ValueError("mf.position_fraction must be within (0, 1]")
        if not Decimal("0") <= self.footprint_abs_delta_threshold <= Decimal("1"):
            raise ValueError(
                "mf.footprint_abs_delta_threshold must be within [0, 1]"
            )
        if self.holding_minutes != 48:
            raise ValueError("MF live holding_minutes must be 48")
        if self.pivot_left <= 0 or self.pivot_right <= 0:
            raise ValueError("MF pivot windows must be positive")
        if self.max_swing_age < self.min_swing_age:
            raise ValueError("MF max_swing_age must be >= min_swing_age")
        if self.large_share_min_samples <= 0:
            raise ValueError("MF large_share_min_samples must be positive")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "MfLowSweepConfig":
        raw = dict(value or {})
        return cls(
            enabled=_config_bool(raw.get("enabled"), default=False),
            position_fraction=Decimal(
                str(raw.get("position_fraction", "0.10"))
            ),
            footprint_abs_delta_threshold=Decimal(
                str(raw.get("footprint_abs_delta_threshold", "0.60"))
            ),
            spike_threshold=Decimal(str(raw.get("spike_threshold", "0.0100"))),
            close_pos_max=Decimal(str(raw.get("close_pos_max", "0.30"))),
            large_share_quantile=Decimal(
                str(raw.get("large_share_quantile", "0.80"))
            ),
            large_share_window_days=int(
                raw.get("large_share_window_days", 90)
            ),
            large_share_min_samples=int(
                raw.get("large_share_min_samples", 43_200)
            ),
            pivot_left=int(raw.get("pivot_left", 6)),
            pivot_right=int(raw.get("pivot_right", 3)),
            min_swing_age=int(raw.get("min_swing_age", 3)),
            max_swing_age=int(raw.get("max_swing_age", 1_440)),
            min_swing_prominence_pct=Decimal(
                str(raw.get("min_swing_prominence_pct", "0.0015"))
            ),
            holding_minutes=int(raw.get("holding_minutes", 48)),
            decision_buffer_minutes=int(
                raw.get("decision_buffer_minutes", 4_320)
            ),
            decision_buffer_max_minutes=int(
                raw.get("decision_buffer_max_minutes", 10_080)
            ),
            range_pct=Decimal(str(raw.get("range_pct", "0.002"))),
            range_price_step=Decimal(
                str(raw.get("range_price_step", "1"))
            ),
            exit_variant=str(raw.get("exit_variant", MF_LIVE_EXIT_VARIANT)),
        )


@dataclass(frozen=True)
class MfSignalDecision:
    decision_type: str
    signal_time_ms: int
    decision_time_ms: int
    entry_execution_time_ms: int
    position_id: str
    reference_price: Decimal
    reason: str
    audit: Mapping[str, Any] = field(default_factory=dict)


def _config_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n", ""}:
            return False
        return default
    return bool(value)


__all__ = [
    "COINBACKTEST_CHILD_SOURCE",
    "COINBACKTEST_PORTFOLIO_SOURCE",
    "MF_ENGINE_NAME",
    "MF_ENTRY_REQUIRED_FIELDS",
    "MF_POSITION_ID_PREFIX",
    "MF_RANGE_FOOTPRINT_EVENT_TYPE",
    "MF_VARIANT_NAME",
    "MfLowSweepConfig",
    "MfSignalDecision",
]
