from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pandas as pd

from tools.v9c_parity_check import (
    ParityConfig,
    _values_equal,
    build_coinbacktest_namespace,
    compare_config,
    compare_signal_audits,
    normalize_coinbacktest_signal_audit,
)


def test_build_coinbacktest_namespace_uses_v9c_champion_defaults():
    ns = build_coinbacktest_namespace(ParityConfig(start_date="2025-01-01", end_date="2025-02-01"))
    assert ns.preset == "turbo"
    assert ns.bear_preset == "high"
    assert ns.bull_preset == "high"
    assert ns.priority_mode == "reclaim_first"
    assert ns.global_risk_scale == 1.30
    assert ns.micro_filter_mode == "soft"
    assert ns.micro_min_range_bars == 5
    assert ns.micro_contra_risk_scale == 0.50
    assert ns.micro_not_aligned_risk_scale == 0.50


def test_normalize_coinbacktest_signal_audit_outputs_stable_columns():
    idx = pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 04:00:00"])
    features = pd.DataFrame(
        {
            "signal": [1, 0],
            "selected_engine": ["BULL_RECLAIM_V2", "NONE"],
            "selected_priority": [150, 0],
            "risk_mult": [1.2, 1.0],
            "quality_mult": [1.1, 1.0],
            "micro_entry_risk_scale": [0.5, 1.0],
            "micro_filter_action": ["soft_reduce", "none"],
            "micro_context_available": [True, False],
            "micro_aligned": [False, False],
            "micro_contra": [True, False],
            "momentum_signal": [0, 0],
            "bear_signal": [0, 0],
            "bull_signal": [1, 0],
            "rf_bar_count": [7, 0],
            "rf_imbalance": [-0.1, float("nan")],
            "rf_close_pos": [0.2, float("nan")],
            "atr": [100.0, 101.0],
            "open": [1000.0, 1001.0],
            "high": [1010.0, 1011.0],
            "low": [990.0, 991.0],
            "close": [1005.0, 1006.0],
        },
        index=idx,
    )
    audit = normalize_coinbacktest_signal_audit(features)
    assert list(audit["side"]) == ["long", "flat"]
    assert audit.loc[0, "selected_engine"] == "BULL_RECLAIM_V2"
    assert audit.loc[0, "micro_context_available"] is True or bool(audit.loc[0, "micro_context_available"]) is True
    assert "open_time_ms" in audit.columns


def test_compare_signal_audits_detects_mismatches():
    coin = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00:00", "2025-01-01T04:00:00"],
            "signal": [1, -1],
            "side": ["long", "short"],
            "selected_engine": ["BULL_RECLAIM_V2", "BEAR_V3_ONLY"],
            "risk_mult": [1.0, 1.0],
        }
    )
    ae = pd.DataFrame(
        {
            "time": ["2025-01-01T00:00:00", "2025-01-01T04:00:00"],
            "signal": [1, 1],
            "side": ["long", "long"],
            "selected_engine": ["BULL_RECLAIM_V2", "MOMENTUM_V3"],
            "risk_mult": [1.0, 1.2],
        }
    )
    result, mismatches = compare_signal_audits(
        coin,
        ae,
        columns=["time", "signal", "side", "selected_engine", "risk_mult"],
    )
    assert result.matched is False
    assert result.mismatched_rows == 1
    assert result.column_mismatches["signal"] == 1
    assert result.column_mismatches["selected_engine"] == 1
    assert not mismatches.empty


def test_compare_config_reports_expected_mismatch_shape():
    coin = {
        "priority_order": ["BULL_RECLAIM_V2", "MOMENTUM_V3", "BEAR_V3_ONLY"],
        "global_risk_scale": "1.3",
        "micro_context": {"mode": "soft", "min_range_bars": 5},
        "engine_execution_params": {"MOMENTUM_V3": {"unit_risk_per_trade": "0.032"}},
    }
    ae = {
        "priority_order": ["BULL_RECLAIM_V2", "MOMENTUM_V3", "BEAR_V3_ONLY"],
        "global_risk_scale": "1.3",
        "micro_context": {"mode": "soft", "min_range_bars": 5},
        "engine_execution_params": {"MOMENTUM_V3": {"unit_risk_per_trade": "0.026"}},
    }
    result = compare_config(coin, ae)
    assert result.matched is False
    assert "engine_execution_params.MOMENTUM_V3" in result.mismatches


def test_values_equal_handles_float_tolerance_and_nan():
    assert _values_equal(Decimal("1.0000000001"), Decimal("1.0"), float_tolerance=1e-6)
    assert _values_equal(float("nan"), float("nan"), float_tolerance=1e-9)
    assert not _values_equal("long", "short", float_tolerance=1e-9)
