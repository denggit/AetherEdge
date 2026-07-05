from __future__ import annotations

import math
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.market_data.models import FixedTimeTradeBar, RangeFootprintFeature
from strategies.eth_portfolio_v1.domain.mf_signal import (
    MF_POSITION_ID_PREFIX,
    MfLowSweepConfig,
    MfSignalDecision,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState


_MINUTE_MS = 60_000


def evaluate_mf_low_sweep(
    *,
    config: MfLowSweepConfig,
    bars: Sequence[FixedTimeTradeBar],
    range_footprints: Sequence[RangeFootprintFeature],
    large_share_history: Sequence[Decimal] | None = None,
    readiness: Mapping[str, Any],
    sleeve: MfSleeveState,
) -> tuple[MfSignalDecision | None, dict[str, Any]]:
    """Map the frozen CoinBacktest bar indices onto live closed-bar events.

    CoinBacktest enters at ``signal_pos + 1`` open and plans the fixed exit at
    ``signal_pos + 48`` close. Live emits the market entry only after the signal
    bar is closed, then emits the scoped close after 48 completed holding
    minutes. Neither decision reads the next bar's high, low, or close.
    """

    latest = bars[-1] if bars else None
    signal_time_ms = (
        0
        if latest is None
        else max(int(latest.close_time_ms) + 1, int(latest.available_time_ms))
    )
    audit = _base_audit(
        config=config,
        readiness=readiness,
        sleeve=sleeve,
        signal_time_ms=signal_time_ms,
    )
    if latest is None:
        _set_reason(audit, "missing_feature")
        audit["missing_features"] = ["tradebar"]
        return None, audit

    audit.update(
        {
            "signal_time_ms": signal_time_ms,
            "decision_time_ms": signal_time_ms,
            "entry_execution_time_ms": signal_time_ms,
            "used_tradebar_close_time_ms": latest.close_time_ms,
            "used_tradebar_available_time_ms": latest.available_time_ms,
        }
    )

    data_ready = all(
        bool(readiness.get(field, False))
        for field in (
            "mf_signal_feature_ready",
            "range_footprint_ready",
            "tradebar_ready",
        )
    )
    audit["data_ready"] = data_ready
    audit["signal_feature_ready"] = bool(
        readiness.get("mf_signal_feature_ready", False)
    )
    if not config.enabled:
        _set_reason(audit, "disabled")
        return None, audit
    if not data_ready:
        _set_reason(audit, "data_not_ready")
        return None, audit

    tradebar_causal = int(latest.available_time_ms) <= signal_time_ms
    context = _latest_range_context(
        range_footprints,
        # CoinBacktest merge_asof uses end_ts <= the 1m signal-bar timestamp.
        # Its trade-bar timestamp is the bar's left/open label.
        cutoff_ms=int(latest.open_time_ms),
        range_pct=config.range_pct,
        price_step=config.range_price_step,
    )
    if context is not None:
        audit["used_range_footprint_available_time_ms"] = (
            context.available_time_ms
        )
        audit["fp_max_bucket_abs_delta_pressure"] = str(
            context.fp_max_bucket_abs_delta_pressure
        )
    range_causal = bool(
        context is not None
        and context.available_time_ms <= signal_time_ms
        and context.available_time_ms <= latest.open_time_ms
    )
    audit["causal_ok"] = bool(tradebar_causal and range_causal)
    if not audit["causal_ok"]:
        _set_reason(audit, "invalid_causal_feature")
        if context is None:
            audit["missing_features"] = ["range_footprint"]
        return None, audit

    if sleeve.pending_open or sleeve.pending_close:
        _set_reason(audit, sleeve.state_label)
        return None, audit

    holding_minutes, holding_bars = _holding_age(
        sleeve=sleeve,
        latest=latest,
    )
    audit["holding_minutes"] = holding_minutes
    audit["holding_bars"] = holding_bars
    audit["time48_due"] = bool(
        sleeve.active and holding_minutes >= config.holding_minutes
    )
    if sleeve.active:
        if not audit["time48_due"]:
            _set_reason(audit, "holding")
            return None, audit
        audit["exit_signal"] = True
        audit["exit_reason"] = "mf_time48_exit"
        _set_reason(audit, "mf_time48_exit", blocked=False)
        return (
            MfSignalDecision(
                decision_type="close",
                signal_time_ms=signal_time_ms,
                decision_time_ms=signal_time_ms,
                entry_execution_time_ms=(
                    sleeve.entry_execution_time_ms or signal_time_ms
                ),
                position_id=str(sleeve.position_id),
                reference_price=latest.close,
                reason="mf_time48_exit",
                audit=dict(audit),
            ),
            audit,
        )

    decision_bars = _contiguous_suffix(bars)
    features, missing = _entry_features(
        bars=decision_bars,
        context=context,
        config=config,
        large_share_history=large_share_history,
    )
    audit.update(features)
    audit["missing_features"] = missing
    if missing:
        _set_reason(audit, "missing_feature")
        return None, audit

    entry_candidate = bool(
        features["spike_pct"] >= config.spike_threshold
        and features["close_pos"] <= config.close_pos_max
        and features["large_trade_share"]
        >= features["large_share_threshold"]
        and features["single_swing"]
        and features["fp_max_bucket_abs_delta_pressure"]
        >= config.footprint_abs_delta_threshold
    )
    audit["entry_candidate"] = entry_candidate
    if not entry_candidate:
        _set_reason(audit, "no_setup")
        return None, audit

    position_id = f"{MF_POSITION_ID_PREFIX}{signal_time_ms}"
    audit["entry_signal"] = True
    audit["position_id"] = position_id
    _set_reason(audit, "mf_low_sweep_entry", blocked=False)
    return (
        MfSignalDecision(
            decision_type="open",
            signal_time_ms=signal_time_ms,
            decision_time_ms=signal_time_ms,
            entry_execution_time_ms=signal_time_ms,
            position_id=position_id,
            reference_price=latest.close,
            reason="mf_low_sweep_entry",
            audit=dict(audit),
        ),
        audit,
    )


def _entry_features(
    *,
    bars: Sequence[FixedTimeTradeBar],
    context: RangeFootprintFeature,
    config: MfLowSweepConfig,
    large_share_history: Sequence[Decimal] | None,
) -> tuple[dict[str, Any], list[str]]:
    latest = bars[-1]
    missing: list[str] = []
    spike_pct: Decimal | None = None
    close_pos: Decimal | None = None
    if len(bars) >= 2 and latest.low > 0:
        spike_pct = bars[-2].close / latest.low - Decimal("1")
    else:
        missing.append("spike_pct")
    span = latest.high - latest.low
    if span > 0:
        close_pos = (latest.close - latest.low) / span
    else:
        missing.append("close_pos")

    history = (
        list(large_share_history)
        if large_share_history is not None
        else [bar.large_trade_share for bar in bars[:-1]]
    )
    threshold = _historical_quantile(
        values=history,
        quantile=config.large_share_quantile,
        window_samples=config.large_share_window_days * 1_440,
        min_samples=config.large_share_min_samples,
    )
    if threshold is None:
        missing.append("large_share_rq80_90d")

    swing = _latest_confirmed_swing(bars, config)
    if swing is None:
        missing.extend(
            [
                "swing_low",
                "swing_low_age",
                "swing_low_prominence_pct",
                "single_swing",
            ]
        )
        swing_low = None
        swing_age = None
        swing_prominence = None
        single_swing = False
    else:
        swing_low, swing_age, swing_prominence = swing
        single_swing = bool(
            config.min_swing_age <= swing_age <= config.max_swing_age
            and swing_prominence >= config.min_swing_prominence_pct
            and latest.low <= swing_low
            and latest.close <= swing_low
        )

    features: dict[str, Any] = {
        "spike_pct": spike_pct,
        "close_pos": close_pos,
        "large_trade_share": latest.large_trade_share,
        "large_share_threshold": threshold,
        "large_share_rq80_90d": (
            None
            if threshold is None
            else latest.large_trade_share >= threshold
        ),
        "swing_low": swing_low,
        "swing_low_age": swing_age,
        "swing_low_prominence_pct": swing_prominence,
        "single_swing": single_swing,
        "fp_max_bucket_abs_delta_pressure": (
            context.fp_max_bucket_abs_delta_pressure
        ),
        "fp_abs_delta_high_threshold": config.footprint_abs_delta_threshold,
        "signal_side": "long",
    }
    return features, list(dict.fromkeys(missing))


def _latest_confirmed_swing(
    bars: Sequence[FixedTimeTradeBar],
    config: MfLowSweepConfig,
) -> tuple[Decimal, int, Decimal] | None:
    current = len(bars) - 1
    left = config.pivot_left
    right = config.pivot_right
    first = max(left, current - config.max_swing_age)
    last = current - right - 1
    for center in range(last, first - 1, -1):
        low = bars[center].low
        left_lows = [bars[i].low for i in range(center - left, center)]
        right_lows = [
            bars[i].low for i in range(center + 1, center + right + 1)
        ]
        if not (low < min(left_lows) and low <= min(right_lows)):
            continue
        local_high = max(
            bars[i].high
            for i in range(center - left, center + right + 1)
        )
        prominence = local_high / low - Decimal("1")
        return low, current - center, prominence
    return None


def _historical_quantile(
    *,
    values: Sequence[Decimal],
    quantile: Decimal,
    window_samples: int,
    min_samples: int,
) -> Decimal | None:
    ordered = [
        value
        for value in values[-window_samples:]
        if value.is_finite()
    ]
    if len(ordered) < min_samples:
        return None
    ordered.sort()
    position = Decimal(len(ordered) - 1) * quantile
    lower = int(math.floor(float(position)))
    upper = int(math.ceil(float(position)))
    if lower == upper:
        return ordered[lower]
    weight = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _latest_range_context(
    features: Sequence[RangeFootprintFeature],
    *,
    cutoff_ms: int,
    range_pct: Decimal,
    price_step: Decimal,
) -> RangeFootprintFeature | None:
    eligible = [
        feature
        for feature in features
        if feature.range_pct == range_pct
        and feature.price_step == price_step
        and feature.available_time_ms <= cutoff_ms
        and feature.context_available
        and str(feature.quality).upper() == "COMPLETE"
    ]
    return (
        max(
            eligible,
            key=lambda feature: (
                feature.available_time_ms,
                feature.range_bar_id,
            ),
        )
        if eligible
        else None
    )


def _holding_age(
    *,
    sleeve: MfSleeveState,
    latest: FixedTimeTradeBar,
) -> tuple[int, int]:
    if not sleeve.active or sleeve.entry_execution_time_ms is None:
        return 0, 0
    completed_through_ms = int(latest.close_time_ms) + 1
    elapsed_ms = max(
        0, completed_through_ms - int(sleeve.entry_execution_time_ms)
    )
    completed_minutes = elapsed_ms // _MINUTE_MS
    return int(completed_minutes), int(completed_minutes)


def _contiguous_suffix(
    bars: Sequence[FixedTimeTradeBar],
) -> tuple[FixedTimeTradeBar, ...]:
    if not bars:
        return ()
    start = len(bars) - 1
    while start > 0:
        previous = bars[start - 1]
        current = bars[start]
        if (
            current.open_time_ms - previous.open_time_ms != _MINUTE_MS
            or current.open_time_ms != previous.close_time_ms + 1
        ):
            break
        start -= 1
    return tuple(bars[start:])


def _base_audit(
    *,
    config: MfLowSweepConfig,
    readiness: Mapping[str, Any],
    sleeve: MfSleeveState,
    signal_time_ms: int,
) -> dict[str, Any]:
    return {
        "enabled": config.enabled,
        "data_ready": False,
        "signal_feature_ready": bool(
            readiness.get("mf_signal_feature_ready", False)
        ),
        "entry_candidate": False,
        "entry_signal": False,
        "exit_signal": False,
        "exit_reason": None,
        "blocked_reason": None,
        "reason": None,
        "missing_features": [],
        "sleeve_state": sleeve.state_label,
        "position_id": sleeve.position_id,
        "holding_minutes": 0,
        "holding_bars": 0,
        "time48_due": False,
        "causal_ok": None,
        "signal_time_ms": signal_time_ms,
        "decision_time_ms": signal_time_ms,
        "entry_execution_time_ms": (
            sleeve.entry_execution_time_ms or signal_time_ms
        ),
        "used_tradebar_close_time_ms": None,
        "used_tradebar_available_time_ms": None,
        "used_range_footprint_available_time_ms": None,
        "fp_max_bucket_abs_delta_pressure": None,
        "large_trade_share": None,
        "large_share_threshold": None,
        "single_swing": False,
        "exit_variant": "time48",
    }


def _set_reason(
    audit: dict[str, Any], reason: str, *, blocked: bool = True
) -> None:
    audit["blocked_reason"] = reason if blocked else None
    audit["reason"] = reason


__all__ = ["evaluate_mf_low_sweep"]
