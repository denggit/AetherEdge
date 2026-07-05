from __future__ import annotations

import json
from decimal import Decimal

import numpy as np
import pandas as pd

from strategies.eth_portfolio_v1.diagnostics.lf_engine_diag import (
    _coerce_bool,
    build_lf_engine_diag,
    format_lf_engine_diag,
)


def _momentum(**overrides):
    row = {
        "signal": 0,
        "long_signal": False,
        "short_signal": False,
        "close_gt_entry_high": False,
        "close_lt_entry_low": False,
        "vol_ok": True,
        "atr_ok": True,
        "adx": 18.3,
        "adx_long_ok": True,
        "adx_short_ok": True,
        "short_enabled": False,
        "d1_bull": False,
        "d1_bear": False,
        "ema20_gt_ema50": True,
        "ema20_lt_ema50": False,
        "close_gt_ema50": True,
        "close_lt_ema50": False,
        "close_gt_open": True,
        "close_lt_open": False,
        "risk_mult": Decimal("1.20"),
        "quality_mult": Decimal("0.50"),
    }
    row.update(overrides)
    return row


def _bull(**overrides):
    row = {
        "signal": 0,
        "long_signal": False,
        "recent_pullback": True,
        "reclaim": True,
        "macro_bull_ok": True,
        "range_ok": True,
        "volume_ok": True,
        "not_extended": True,
        "quality_bucket_a": False,
        "quality_bucket_b": False,
        "risk_mult": Decimal("1"),
        "quality_mult": Decimal("0.20"),
    }
    row.update(overrides)
    return row


def _bear(**overrides):
    row = {
        "signal": 0,
        "short_signal": False,
        "bear_permission_v3": True,
        "four_h_bear": True,
        "weekly_bear": True,
        "breakdown": True,
        "permission_continuation": True,
        "risk_mult": Decimal("1"),
        "quality_mult": Decimal("1"),
    }
    row.update(overrides)
    return row


def _features(*, momentum=None, bull=None, bear=None):
    return {
        "momentum": _momentum() if momentum is None else momentum,
        "bull": _bull() if bull is None else bull,
        "bear": _bear() if bear is None else bear,
    }


def test_momentum_flat_signal_explains_long_and_short_missing_reasons() -> None:
    diag = build_lf_engine_diag(_features())["momentum"]

    assert "long:no_12bar_breakout" in diag["missing"]
    assert "long:d1_not_bull" in diag["missing"]
    assert "short:no_12bar_breakdown" in diag["missing"]
    assert "short:short_disabled" in diag["missing"]
    assert diag["diag_status"] == "ok"


def test_momentum_selected_signal_has_no_missing_reasons() -> None:
    diag = build_lf_engine_diag(
        _features(
            momentum=_momentum(
                signal=1,
                long_signal=True,
                close_gt_entry_high=True,
                d1_bull=True,
            )
        )
    )["momentum"]

    assert diag["signal"] == 1
    assert diag["missing"] == []


def test_bull_missing_reports_macro_regime_failure() -> None:
    diag = build_lf_engine_diag(
        _features(bull=_bull(macro_bull_ok=False))
    )["bull"]

    assert "macro_bull_not_ok" in diag["missing"]


def test_bear_missing_is_readable_when_permission_and_4h_regime_fail() -> None:
    diag = build_lf_engine_diag(
        _features(
            bear=_bear(
                bear_permission_v3=False,
                four_h_bear=False,
                weekly_bear=False,
                permission_continuation=False,
            )
        )
    )["bear"]

    assert "not_bear_regime" in diag["missing"]
    assert "bear_permission_false" in diag["missing"]
    assert "four_h_bear_false" in diag["missing"]


def test_missing_fields_are_partial_and_do_not_raise() -> None:
    diag = build_lf_engine_diag(
        {"momentum": {"signal": 0}, "bull": {}, "bear": {}}
    )

    assert diag["momentum"]["diag_status"] == "partial"
    assert "long_signal" in diag["momentum"]["missing_fields"]
    assert diag["bull"]["diag_status"] == "partial"
    assert diag["bear"]["diag_status"] == "partial"


def test_decimal_numpy_and_pandas_scalars_are_json_safe() -> None:
    diag = build_lf_engine_diag(
        _features(
            momentum=_momentum(
                signal=np.int64(0),
                long_signal=np.bool_(False),
                vol_ok=np.bool_(True),
                adx=np.float64(18.34567891),
                risk_mult=Decimal("1.23456789"),
                quality_mult=pd.NA,
            )
        )
    )

    encoded = json.dumps(diag)

    assert '"risk_mult": "1.23456789"' in encoded
    assert diag["momentum"]["long_signal"] is False
    assert diag["momentum"]["vol_ok"] is True
    assert diag["momentum"]["adx"] == 18.345679
    assert diag["momentum"]["quality_mult"] is None


def test_pretty_text_contains_all_three_engine_sections() -> None:
    text = format_lf_engine_diag(build_lf_engine_diag(_features()))

    assert text.startswith("engine_diag:")
    assert "\n  momentum:" in text
    assert "\n  bull:" in text
    assert "\n  bear:" in text
    assert "missing=long:no_12bar_breakout" in text


def test_coerce_bool_string_false_is_false() -> None:
    assert _coerce_bool("False") is False
    assert _coerce_bool("false") is False
    assert _coerce_bool("0") is False
    assert _coerce_bool("no") is False
    assert _coerce_bool("n") is False
    assert _coerce_bool("") is False
    assert _coerce_bool("true") is True
    assert _coerce_bool("True") is True
    assert _coerce_bool("1") is True
    assert _coerce_bool("yes") is True
    assert _coerce_bool("y") is True


def test_coerce_bool_unknown_string_is_not_true() -> None:
    assert _coerce_bool("not-a-bool") is None
