from __future__ import annotations

import math
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.market_data.models import FixedTimeTradeBar, RangeFootprintFeature
from strategies.eth_portfolio_v1.domain.mf_signal import (
    MF_CLOSE_THROUGH_BUFFER_PCT,
    MF_EVENT_BREAKOUT_THRESHOLDS,
    MF_EVENT_MAX_SWING_AGES,
    MF_EVENT_MIN_PROMINENCE_PCTS,
    MF_EVENT_SPIKE_THRESHOLDS,
    MF_EVENT_VARIANTS,
    MF_POSITION_ID_PREFIX,
    MfLowSweepConfig,
    MfSignalDecision,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState


_MINUTE_MS = 60_000
MF_READINESS_GATE_FIELDS = (
    "mf_signal_feature_ready",
    "range_footprint_ready",
    "tradebar_ready",
    "fixed_time_footprint_ready",
    "coverage_ready",
    "large_share_samples_ready",
)


def mf_readiness_gates(
    readiness: Mapping[str, Any],
) -> dict[str, bool]:
    return {
        field: bool(readiness.get(field, False))
        for field in MF_READINESS_GATE_FIELDS
    }


def evaluate_mf_low_sweep(
    *,
    config: MfLowSweepConfig,
    bars: Sequence[FixedTimeTradeBar],
    range_footprints: Sequence[RangeFootprintFeature],
    large_share_history: Sequence[Decimal] | None = None,
    readiness: Mapping[str, Any],
    sleeve: MfSleeveState,
    next_open_price: Decimal | None = None,
    next_open_time_ms: int | None = None,
) -> tuple[MfSignalDecision | None, dict[str, Any]]:
    """Map the frozen CoinBacktest bar indices onto live closed-bar events.

    CoinBacktest enters at ``signal_pos + 1`` open and plans the fixed exit at
    ``signal_pos + 48`` close. Live emits the market entry only after the signal
    bar is closed, then emits the scoped close after 48 completed holding
    minutes. Neither decision reads the next bar's high, low, or close.
    """

    latest = bars[-1] if bars else None
    decision_time_ms = (
        0
        if latest is None
        else max(
            int(latest.close_time_ms) + 1,
            int(latest.available_time_ms),
            int(next_open_time_ms or 0),
        )
    )
    audit = _base_audit(
        config=config,
        readiness=readiness,
        sleeve=sleeve,
        signal_time_ms=decision_time_ms,
    )
    if latest is None:
        _set_reason(audit, "missing_feature")
        audit["missing_features"] = ["tradebar"]
        return None, audit

    audit.update(
        {
            "signal_time_ms": decision_time_ms,
            "decision_time_ms": decision_time_ms,
            "source_signal_bar_time_ms": latest.open_time_ms,
            "entry_execution_time_ms": (
                next_open_time_ms or decision_time_ms
            ),
            "entry_tradebar_open_time_ms": (
                latest.open_time_ms + _MINUTE_MS
            ),
            "used_tradebar_close_time_ms": latest.close_time_ms,
            "used_tradebar_available_time_ms": latest.available_time_ms,
        }
    )

    data_ready = all(audit["readiness_gates"].values())
    audit["data_ready"] = data_ready
    if not data_ready:
        _set_reason(audit, "data_not_ready")
        return None, audit
    if not config.enabled:
        _set_reason(audit, "disabled")
        return None, audit

    tradebar_causal = int(latest.available_time_ms) <= decision_time_ms
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
        and context.available_time_ms <= decision_time_ms
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
                signal_time_ms=decision_time_ms,
                decision_time_ms=decision_time_ms,
                entry_execution_time_ms=(
                    sleeve.entry_execution_time_ms or decision_time_ms
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
        and features["large_share_rq80_90d"]
        and features["low_sweep_event"]
        and features["single_swing"]
        and features["fp_max_bucket_abs_delta_pressure"]
        >= config.footprint_abs_delta_threshold
    )
    audit["entry_candidate"] = entry_candidate
    if not entry_candidate:
        _set_reason(audit, "no_setup")
        return None, audit

    expected_entry_open_ms = latest.open_time_ms + _MINUTE_MS
    next_open_causal = bool(
        next_open_price is not None
        and next_open_price.is_finite()
        and next_open_price > 0
        and next_open_time_ms is not None
        and expected_entry_open_ms
        <= int(next_open_time_ms)
        < expected_entry_open_ms + _MINUTE_MS
        and int(next_open_time_ms) <= decision_time_ms
    )
    audit["next_open_causal"] = next_open_causal
    if not next_open_causal:
        audit["causal_ok"] = False
        audit["missing_features"] = ["next_open_execution"]
        _set_reason(audit, "invalid_causal_feature")
        return None, audit

    entry_execution_time_ms = int(next_open_time_ms)
    position_id = f"{MF_POSITION_ID_PREFIX}{entry_execution_time_ms}"
    audit["entry_signal"] = True
    audit["position_id"] = position_id
    audit["entry_execution_time_ms"] = entry_execution_time_ms
    audit["entry_reference_price"] = next_open_price
    _set_reason(audit, "mf_low_sweep_entry", blocked=False)
    return (
        MfSignalDecision(
            decision_type="open",
            signal_time_ms=decision_time_ms,
            decision_time_ms=decision_time_ms,
            entry_execution_time_ms=entry_execution_time_ms,
            position_id=position_id,
            reference_price=next_open_price,
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

    enough_swing_history = (
        len(bars) >= config.pivot_left + config.pivot_right + 2
    )
    swing = (
        _latest_confirmed_swing(bars, config)
        if enough_swing_history
        else None
    )
    if not enough_swing_history:
        missing.extend(
            [
                "swing_low",
                "swing_low_age",
                "swing_low_prominence_pct",
            ]
        )
    if swing is None:
        swing_low = None
        swing_age = None
        swing_prominence = None
        low_sweep_event = False
        canonical_event: Mapping[str, Any] = {}
    else:
        swing_low, swing_age, swing_prominence = swing
        canonical_event = _canonical_low_sweep_event(
            latest=latest,
            spike_pct=spike_pct,
            swing_low=swing_low,
            swing_age=swing_age,
            swing_prominence=swing_prominence,
            config=config,
        )
        low_sweep_event = bool(canonical_event)
    # CoinBacktest build_support_mask("single_swing") is an all-True mask.
    # Event shape belongs exclusively to build_low_sweep_events.
    single_swing = True

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
        "low_sweep_event": low_sweep_event,
        "event_variant": canonical_event.get("event_variant"),
        "event_spike_threshold_pct": canonical_event.get(
            "event_spike_threshold_pct"
        ),
        "event_breakout_threshold_pct": canonical_event.get(
            "event_breakout_threshold_pct"
        ),
        "event_max_swing_age": canonical_event.get(
            "event_max_swing_age"
        ),
        "event_min_swing_prominence_pct": canonical_event.get(
            "event_min_swing_prominence_pct"
        ),
        "event_specificity_score": canonical_event.get(
            "event_specificity_score"
        ),
        "single_swing": single_swing,
        "fp_max_bucket_abs_delta_pressure": (
            context.fp_max_bucket_abs_delta_pressure
        ),
        "fp_abs_delta_high_threshold": config.footprint_abs_delta_threshold,
        "signal_side": "long",
    }
    return features, list(dict.fromkeys(missing))


def _canonical_low_sweep_event(
    *,
    latest: FixedTimeTradeBar,
    spike_pct: Decimal | None,
    swing_low: Decimal,
    swing_age: int,
    swing_prominence: Decimal,
    config: MfLowSweepConfig,
) -> Mapping[str, Any]:
    """Exact row-level port of build_low_sweep_events/canonical selection."""

    if spike_pct is None:
        return {}
    candidates: list[dict[str, Any]] = []
    for event_spike in MF_EVENT_SPIKE_THRESHOLDS:
        for breakout in MF_EVENT_BREAKOUT_THRESHOLDS:
            for max_age in MF_EVENT_MAX_SWING_AGES:
                for min_prominence in MF_EVENT_MIN_PROMINENCE_PCTS:
                    base = bool(
                        config.min_swing_age <= swing_age <= max_age
                        and swing_prominence >= min_prominence
                        and spike_pct >= event_spike
                        and latest.low
                        <= swing_low * (Decimal("1") - breakout)
                    )
                    if not base:
                        continue
                    for variant in MF_EVENT_VARIANTS:
                        if variant != "fade_close_through":
                            continue
                        variant_match = (
                            latest.close
                            <= swing_low
                            * (
                                Decimal("1")
                                - MF_CLOSE_THROUGH_BUFFER_PCT
                            )
                        )
                        if not variant_match:
                            continue
                        score = (
                            event_spike * Decimal("10000")
                            + min_prominence * Decimal("10000")
                            + breakout * Decimal("10000")
                            - Decimal(max_age) * Decimal("0.01")
                        )
                        candidates.append(
                            {
                                "event_variant": variant,
                                "event_spike_threshold_pct": event_spike,
                                "event_breakout_threshold_pct": breakout,
                                "event_max_swing_age": max_age,
                                "event_min_swing_prominence_pct": (
                                    min_prominence
                                ),
                                "event_specificity_score": score,
                            }
                        )
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda item: item["event_specificity_score"],
    )


def _latest_confirmed_swing(
    bars: Sequence[FixedTimeTradeBar],
    config: MfLowSweepConfig,
) -> tuple[Decimal, int, Decimal] | None:
    current = len(bars) - 1
    left = config.pivot_left
    right = config.pivot_right
    first = left
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
    entry_bar_open_ms = (
        sleeve.entry_tradebar_open_time_ms
        if sleeve.entry_tradebar_open_time_ms is not None
        else (
            int(sleeve.entry_execution_time_ms) // _MINUTE_MS
        )
        * _MINUTE_MS
    )
    if latest.open_time_ms < entry_bar_open_ms:
        return 0, 0
    completed_bars = (
        int(latest.open_time_ms) - int(entry_bar_open_ms)
    ) // _MINUTE_MS + 1
    return int(completed_bars), int(completed_bars)


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
    readiness_gates = mf_readiness_gates(readiness)
    return {
        "enabled": config.enabled,
        "data_ready": False,
        "signal_feature_ready": readiness_gates[
            "mf_signal_feature_ready"
        ],
        "readiness_source": readiness.get("source", "unavailable"),
        "readiness_reason": readiness.get("reason"),
        "readiness_gates": readiness_gates,
        "missing_readiness_gates": [
            field for field, ready in readiness_gates.items() if not ready
        ],
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
        "entry_tradebar_open_time_ms": (
            sleeve.entry_tradebar_open_time_ms
        ),
        "source_signal_bar_time_ms": None,
        "used_tradebar_close_time_ms": None,
        "used_tradebar_available_time_ms": None,
        "used_range_footprint_available_time_ms": None,
        "fp_max_bucket_abs_delta_pressure": None,
        "large_trade_share": None,
        "large_share_threshold": None,
        "single_swing": False,
        "low_sweep_event": False,
        "event_variant": None,
        "next_open_causal": None,
        "exit_variant": "time48",
    }


def _set_reason(
    audit: dict[str, Any], reason: str, *, blocked: bool = True
) -> None:
    audit["blocked_reason"] = reason if blocked else None
    audit["reason"] = reason


__all__ = [
    "MF_READINESS_GATE_FIELDS",
    "evaluate_mf_low_sweep",
    "mf_readiness_gates",
]
