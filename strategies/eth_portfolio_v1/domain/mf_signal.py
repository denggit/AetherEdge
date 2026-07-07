from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import os
from pathlib import Path
from typing import Any, Mapping

from strategies.eth_portfolio_v1.domain.mf_live_policy import (
    MF_LIVE_EXIT_VARIANT,
    validate_mf_exit_variant,
)


COINBACKTEST_PORTFOLIO_RELATIVE_SOURCE = (
    "backtest/portfolio/"
    "eth_portfolio_V1_lf_v10b_low_sweep_mf_backtest.py"
)
COINBACKTEST_PORTFOLIO_SOURCE = os.getenv(
    "AETHER_COINBACKTEST_PORTFOLIO_SOURCE",
    COINBACKTEST_PORTFOLIO_RELATIVE_SOURCE,
)
COINBACKTEST_CHILD_SOURCE = (
    "backtest/mf/low_sweep/low_sweep_V1_a0_footprint_backtest.py"
)
COINBACKTEST_RESEARCH_SOURCES = (
    "research/low_sweep_a_upgrade_research.py",
    "research/low_sweep_panic_reversal_strategy_probe.py",
    "research/focused_low_sweep_reversal_event_lab.py",
)
MF_VARIANT_NAME = (
    "A0_fp_abs_delta_high__single_swing__next_open__time48__no_stop"
)
MF_ENGINE_NAME = "MF_LOW_SWEEP_TIME48"
MF_POSITION_ID_PREFIX = "mf-low-sweep-time48-"
MF_RANGE_FOOTPRINT_EVENT_TYPE = "range_footprint_feature"
MF_READINESS_EVENT_TYPE = "trade_feature_readiness"

# Source mapping:
# - portfolio wrapper _build_mf_args/_make_mf_variant
# - child formal_variant_specs
# - research build_low_sweep_events/build_fixed_candidate_masks/
#   build_candidate_layer_masks/build_support_mask/build_canonical_events
MF_EVENT_SPIKE_THRESHOLDS = tuple(
    Decimal(value) for value in ("0.0060", "0.0080", "0.0100", "0.0120")
)
MF_EVENT_BREAKOUT_THRESHOLDS = tuple(
    Decimal(value) for value in ("0.0000", "0.0005")
)
MF_EVENT_MAX_SWING_AGES = (12, 24, 48, 96, 240, 1_440)
MF_EVENT_MIN_PROMINENCE_PCTS = tuple(
    Decimal(value) for value in ("0.0015", "0.0030")
)
MF_EVENT_VARIANTS = ("fade_close_through",)
MF_PIVOT_LEFT = 6
MF_PIVOT_RIGHT = 3
MF_MIN_SWING_AGE = 3
MF_CLOSE_THROUGH_BUFFER_PCT = Decimal("0")
MF_A0_SPIKE_THRESHOLD = Decimal("0.0100")
MF_A_CLOSE_POS_MAX = Decimal("0.30")
MF_LARGE_SHARE_QUANTILE = Decimal("0.80")
MF_LARGE_SHARE_WINDOW_DAYS = 90
MF_LARGE_SHARE_WINDOW_SAMPLES = 129_600
MF_LARGE_SHARE_MIN_SAMPLES = 43_200
MF_FOOTPRINT_ABS_DELTA_THRESHOLD = Decimal("0.60")
MF_TIME_EXIT_BARS = 48

MF_ENTRY_REQUIRED_FIELDS = (
    "spike_pct",
    "close_pos",
    "large_trade_share",
    "large_share_rq80_90d",
    "swing_low",
    "swing_low_age",
    "swing_low_prominence_pct",
    "low_sweep_event",
    "event_variant",
    "single_swing",
    "fp_max_bucket_abs_delta_pressure",
    "fp_abs_delta_high_threshold",
    "signal_side",
)


@dataclass(frozen=True)
class MfLowSweepConfig:
    enabled: bool = True
    margin_fraction: Decimal = Decimal("0.10")
    available_margin_buffer: Decimal = Decimal("0.95")
    position_fraction: Decimal | None = None
    footprint_abs_delta_threshold: Decimal = (
        MF_FOOTPRINT_ABS_DELTA_THRESHOLD
    )
    spike_threshold: Decimal = MF_A0_SPIKE_THRESHOLD
    close_pos_max: Decimal = MF_A_CLOSE_POS_MAX
    large_share_quantile: Decimal = MF_LARGE_SHARE_QUANTILE
    large_share_window_days: int = MF_LARGE_SHARE_WINDOW_DAYS
    large_share_min_samples: int = MF_LARGE_SHARE_MIN_SAMPLES
    pivot_left: int = MF_PIVOT_LEFT
    pivot_right: int = MF_PIVOT_RIGHT
    min_swing_age: int = MF_MIN_SWING_AGE
    max_swing_age: int = MF_EVENT_MAX_SWING_AGES[-1]
    min_swing_prominence_pct: Decimal = (
        MF_EVENT_MIN_PROMINENCE_PCTS[0]
    )
    holding_minutes: int = MF_TIME_EXIT_BARS
    decision_buffer_minutes: int = 4_320
    decision_buffer_max_minutes: int = 10_080
    range_pct: Decimal = Decimal("0.002")
    range_price_step: Decimal = Decimal("1")
    exit_variant: str = MF_LIVE_EXIT_VARIANT

    def __post_init__(self) -> None:
        validate_mf_exit_variant(self.exit_variant)
        if self.position_fraction is not None:
            legacy_fraction = Decimal(str(self.position_fraction))
            if legacy_fraction != self.margin_fraction and self.margin_fraction != Decimal("0.10"):
                raise ValueError(
                    "mf.position_fraction and mf.margin_fraction are ambiguous"
                )
            object.__setattr__(self, "margin_fraction", legacy_fraction)
        object.__setattr__(self, "position_fraction", self.margin_fraction)
        if not Decimal("0") < self.margin_fraction <= Decimal("1"):
            raise ValueError("mf.margin_fraction must be within (0, 1]")
        if not Decimal("0") < self.available_margin_buffer <= Decimal("1"):
            raise ValueError(
                "mf.available_margin_buffer must be within (0, 1]"
            )
        if not Decimal("0") <= self.footprint_abs_delta_threshold <= Decimal("1"):
            raise ValueError(
                "mf.footprint_abs_delta_threshold must be within [0, 1]"
            )
        exact_values = {
            "footprint_abs_delta_threshold": (
                self.footprint_abs_delta_threshold,
                MF_FOOTPRINT_ABS_DELTA_THRESHOLD,
            ),
            "spike_threshold": (
                self.spike_threshold,
                MF_A0_SPIKE_THRESHOLD,
            ),
            "close_pos_max": (self.close_pos_max, MF_A_CLOSE_POS_MAX),
            "large_share_quantile": (
                self.large_share_quantile,
                MF_LARGE_SHARE_QUANTILE,
            ),
            "large_share_window_days": (
                self.large_share_window_days,
                MF_LARGE_SHARE_WINDOW_DAYS,
            ),
            "large_share_min_samples": (
                self.large_share_min_samples,
                MF_LARGE_SHARE_MIN_SAMPLES,
            ),
            "pivot_left": (self.pivot_left, MF_PIVOT_LEFT),
            "pivot_right": (self.pivot_right, MF_PIVOT_RIGHT),
            "min_swing_age": (
                self.min_swing_age,
                MF_MIN_SWING_AGE,
            ),
            "max_swing_age": (
                self.max_swing_age,
                MF_EVENT_MAX_SWING_AGES[-1],
            ),
            "min_swing_prominence_pct": (
                self.min_swing_prominence_pct,
                MF_EVENT_MIN_PROMINENCE_PCTS[0],
            ),
            "holding_minutes": (
                self.holding_minutes,
                MF_TIME_EXIT_BARS,
            ),
            "range_pct": (self.range_pct, Decimal("0.002")),
            "range_price_step": (
                self.range_price_step,
                Decimal("1"),
            ),
        }
        mismatched = [
            name
            for name, (actual, expected) in exact_values.items()
            if actual != expected
        ]
        if mismatched:
            raise ValueError(
                "MF CoinBacktest parity parameters cannot be overridden: "
                + ", ".join(mismatched)
            )
        if self.decision_buffer_minutes < self.max_swing_age + self.pivot_left:
            raise ValueError(
                "mf.decision_buffer_minutes is too small for swing parity"
            )
        if (
            self.decision_buffer_minutes
            > self.decision_buffer_max_minutes
        ):
            raise ValueError(
                "mf.decision_buffer_minutes must not exceed "
                "decision_buffer_max_minutes"
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "MfLowSweepConfig":
        raw = dict(value or {})
        if "margin_fraction" in raw and "position_fraction" in raw:
            margin_fraction_value = Decimal(str(raw["margin_fraction"]))
            legacy_fraction_value = Decimal(str(raw["position_fraction"]))
            if margin_fraction_value != legacy_fraction_value:
                raise ValueError(
                    "mf.position_fraction and mf.margin_fraction are ambiguous"
                )
        margin_fraction = raw.get(
            "margin_fraction",
            raw.get("position_fraction", "0.10"),
        )
        return cls(
            enabled=_config_bool(raw.get("enabled"), default=True),
            margin_fraction=Decimal(str(margin_fraction)),
            available_margin_buffer=Decimal(
                str(raw.get("available_margin_buffer", "0.95"))
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


def resolve_coinbacktest_source_root(
    portfolio_source: str | os.PathLike[str] | None = None,
) -> Path | None:
    """Return the CoinBacktest root when local provenance files exist.

    The default provenance path is repository-relative so AetherEdge remains
    portable across Windows/Linux servers.  Developers who keep CoinBacktest
    elsewhere can set ``AETHER_COINBACKTEST_PORTFOLIO_SOURCE`` to an absolute
    path; live runtime does not depend on this optional mapping.
    """

    path = Path(portfolio_source or COINBACKTEST_PORTFOLIO_SOURCE)
    if not path.is_absolute():
        env_root = os.getenv("AETHER_COINBACKTEST_ROOT")
        if not env_root:
            return None
        path = Path(env_root) / path
    try:
        return path.parents[2] if path.is_file() else None
    except IndexError:
        return None


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
    "COINBACKTEST_PORTFOLIO_RELATIVE_SOURCE",
    "COINBACKTEST_PORTFOLIO_SOURCE",
    "COINBACKTEST_RESEARCH_SOURCES",
    "MF_A0_SPIKE_THRESHOLD",
    "MF_A_CLOSE_POS_MAX",
    "MF_CLOSE_THROUGH_BUFFER_PCT",
    "MF_ENGINE_NAME",
    "MF_ENTRY_REQUIRED_FIELDS",
    "MF_EVENT_BREAKOUT_THRESHOLDS",
    "MF_EVENT_MAX_SWING_AGES",
    "MF_EVENT_MIN_PROMINENCE_PCTS",
    "MF_EVENT_SPIKE_THRESHOLDS",
    "MF_EVENT_VARIANTS",
    "MF_FOOTPRINT_ABS_DELTA_THRESHOLD",
    "MF_LARGE_SHARE_MIN_SAMPLES",
    "MF_LARGE_SHARE_QUANTILE",
    "MF_LARGE_SHARE_WINDOW_DAYS",
    "MF_LARGE_SHARE_WINDOW_SAMPLES",
    "MF_MIN_SWING_AGE",
    "MF_PIVOT_LEFT",
    "MF_PIVOT_RIGHT",
    "MF_POSITION_ID_PREFIX",
    "MF_RANGE_FOOTPRINT_EVENT_TYPE",
    "MF_READINESS_EVENT_TYPE",
    "MF_TIME_EXIT_BARS",
    "MF_VARIANT_NAME",
    "MfLowSweepConfig",
    "MfSignalDecision",
    "resolve_coinbacktest_source_root",
]
