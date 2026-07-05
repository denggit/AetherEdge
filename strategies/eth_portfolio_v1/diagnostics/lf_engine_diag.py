from __future__ import annotations

import math
from decimal import Decimal
from numbers import Integral, Real
from typing import Any, Mapping

import pandas as pd


_MOMENTUM_FIELDS = (
    "signal",
    "long_signal",
    "short_signal",
    "close_gt_entry_high",
    "close_lt_entry_low",
    "vol_ok",
    "atr_ok",
    "adx",
    "adx_long_ok",
    "adx_short_ok",
    "short_enabled",
    "d1_bull",
    "d1_bear",
    "ema20_gt_ema50",
    "ema20_lt_ema50",
    "close_gt_ema50",
    "close_lt_ema50",
    "close_gt_open",
    "close_lt_open",
    "risk_mult",
    "quality_mult",
)
_BULL_FIELDS = (
    "signal",
    "long_signal",
    "recent_pullback",
    "reclaim",
    "macro_bull_ok",
    "range_ok",
    "volume_ok",
    "not_extended",
    "quality_bucket_a",
    "quality_bucket_b",
    "risk_mult",
    "quality_mult",
)
_BEAR_FIELDS = (
    "signal",
    "short_signal",
    "bear_permission_v3",
    "four_h_bear",
    "weekly_bear",
    "breakdown",
    "permission_continuation",
    "risk_mult",
    "quality_mult",
)
_BOOL_FIELDS = frozenset(
    {
        "long_signal",
        "short_signal",
        "close_gt_entry_high",
        "close_lt_entry_low",
        "vol_ok",
        "atr_ok",
        "adx_long_ok",
        "adx_short_ok",
        "short_enabled",
        "d1_bull",
        "d1_bear",
        "ema20_gt_ema50",
        "ema20_lt_ema50",
        "close_gt_ema50",
        "close_lt_ema50",
        "close_gt_open",
        "close_lt_open",
        "recent_pullback",
        "reclaim",
        "macro_bull_ok",
        "range_ok",
        "volume_ok",
        "not_extended",
        "quality_bucket_a",
        "quality_bucket_b",
        "bear_permission_v3",
        "four_h_bear",
        "weekly_bear",
        "breakdown",
        "permission_continuation",
    }
)


def build_lf_engine_diag(
    engine_features: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    """Build a JSON-safe explanation from the already-computed LF feature rows."""

    rows = engine_features if isinstance(engine_features, Mapping) else {}
    momentum = _extract_row(rows.get("momentum"), _MOMENTUM_FIELDS)
    bull = _extract_row(rows.get("bull"), _BULL_FIELDS)
    bear = _extract_row(rows.get("bear"), _BEAR_FIELDS)

    momentum["missing"] = _momentum_missing(momentum)
    bull["missing"] = _bull_missing(bull)
    bear["missing"] = _bear_missing(bear)
    return {
        "momentum": momentum,
        "bull": bull,
        "bear": bear,
    }


def format_lf_engine_diag(engine_diag: Mapping[str, Mapping[str, Any]]) -> str:
    """Render engine diagnostics as a compact multi-line log payload."""

    lines = ["engine_diag:"]
    for engine in ("momentum", "bull", "bear"):
        values = engine_diag.get(engine, {})
        lines.append(f"  {engine}:")
        for key, value in values.items():
            if key == "missing":
                rendered = _format_missing(engine, value)
            elif key == "missing_fields":
                rendered = ",".join(str(item) for item in value) if value else "[]"
            else:
                rendered = _format_value(value)
            lines.append(f"    {key}={rendered}")
    return "\n".join(lines)


def _extract_row(
    row: Mapping[str, Any] | None,
    fields: tuple[str, ...],
) -> dict[str, Any]:
    source = row if isinstance(row, Mapping) else {}
    values: dict[str, Any] = {}
    missing_fields: list[str] = []
    for field in fields:
        if field not in source or _is_missing(source[field]):
            values[field] = None
            missing_fields.append(field)
            continue
        value = _json_scalar(source[field])
        if field == "signal":
            value = _coerce_int(value)
        elif field in _BOOL_FIELDS:
            value = _coerce_bool(value)
        values[field] = value
    values["diag_status"] = "partial" if missing_fields else "ok"
    values["missing_fields"] = missing_fields
    return values


def _momentum_missing(values: Mapping[str, Any]) -> list[str]:
    if values.get("signal") in {-1, 1}:
        return []

    missing: list[str] = []
    _append_false(
        missing,
        values,
        (
            ("close_gt_entry_high", "long:no_12bar_breakout"),
            ("close_gt_open", "long:close_not_green"),
            ("close_gt_ema50", "long:close_not_above_ema50"),
            ("ema20_gt_ema50", "long:ema20_not_above_ema50"),
            ("vol_ok", "long:volume_not_ok"),
            ("atr_ok", "long:atr_not_ok"),
            ("d1_bull", "long:d1_not_bull"),
            ("adx_long_ok", "long:adx_not_in_long_range"),
            ("close_lt_entry_low", "short:no_12bar_breakdown"),
            ("close_lt_open", "short:close_not_red"),
            ("close_lt_ema50", "short:close_not_below_ema50"),
            ("ema20_lt_ema50", "short:ema20_not_below_ema50"),
            ("vol_ok", "short:volume_not_ok"),
            ("atr_ok", "short:atr_not_ok"),
            ("d1_bear", "short:d1_not_bear"),
            ("short_enabled", "short:short_disabled"),
            ("adx_short_ok", "short:adx_not_in_short_range"),
        ),
    )
    return missing


def _bull_missing(values: Mapping[str, Any]) -> list[str]:
    if values.get("signal") == 1:
        return []

    missing: list[str] = []
    _append_false(
        missing,
        values,
        (
            ("recent_pullback", "recent_pullback_missing"),
            ("reclaim", "reclaim_missing"),
            ("macro_bull_ok", "macro_bull_not_ok"),
            ("range_ok", "range_not_ok"),
            ("volume_ok", "volume_not_ok"),
            ("not_extended", "extended"),
            ("quality_bucket_a", "quality_bucket_a_false"),
            ("quality_bucket_b", "quality_bucket_b_false"),
        ),
    )
    return missing


def _bear_missing(values: Mapping[str, Any]) -> list[str]:
    if values.get("signal") == -1:
        return []

    missing: list[str] = []
    regime_values = (
        values.get("bear_permission_v3"),
        values.get("four_h_bear"),
        values.get("weekly_bear"),
    )
    if all(value is False for value in regime_values):
        missing.append("not_bear_regime")
    _append_false(
        missing,
        values,
        (
            ("bear_permission_v3", "bear_permission_false"),
            ("four_h_bear", "four_h_bear_false"),
            ("weekly_bear", "weekly_bear_false"),
            ("breakdown", "breakdown_false"),
            ("permission_continuation", "permission_continuation_false"),
            ("short_signal", "short_signal_false"),
        ),
    )
    return missing


def _append_false(
    missing: list[str],
    values: Mapping[str, Any],
    rules: tuple[tuple[str, str], ...],
) -> None:
    for field, reason in rules:
        if values.get(field) is False:
            missing.append(reason)


def _format_missing(engine: str, value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "[]"
    if engine != "momentum":
        return ",".join(str(item) for item in value)

    grouped: dict[str, list[str]] = {"long": [], "short": []}
    other: list[str] = []
    for item in value:
        label = str(item)
        side, separator, reason = label.partition(":")
        if separator and side in grouped:
            grouped[side].append(reason)
        else:
            other.append(label)
    parts = [
        f"{side}:{','.join(reasons)}"
        for side, reasons in grouped.items()
        if reasons
    ]
    parts.extend(other)
    return " | ".join(parts)


def _format_value(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)


def _json_scalar(value: Any) -> bool | int | float | str | None:
    if _is_missing(value):
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except (TypeError, ValueError):
            pass
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        number = float(value)
        return round(number, 6) if math.isfinite(number) else None
    if isinstance(value, str):
        return value
    return str(value)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n", ""}:
            return False
        return None
    return bool(value)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        result = pd.isna(value)
        return bool(result)
    except (TypeError, ValueError):
        return False
