from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

import tools.v9c_signal_parity_check as parity
from tools.v9c_signal_parity_check import (
    FINGERPRINT_FILENAME,
    MISMATCH_CONTEXT_FILENAME,
    MISMATCH_FILENAME,
    REPLAY_AUDIT_FILENAME,
    SUMMARY_FILENAME,
    build_range_context_from_rf_columns,
    build_parser,
    build_signal_mismatch_context,
    compare_signal_audits,
    detect_feature_warmup,
    main,
    replay_aetheredge_signal_audit,
    strategy_fingerprint,
    timestamp_to_open_close_ms,
    validate_coin_audit_columns,
)


def test_validate_coin_audit_requires_expected_columns() -> None:
    df = _coin_audit_df().drop(columns=["timestamp", "selected_engine"])

    with pytest.raises(ValueError, match="Missing required columns") as excinfo:
        validate_coin_audit_columns(df)

    assert "timestamp" in str(excinfo.value)
    assert "selected_engine" in str(excinfo.value)


def test_timestamp_to_open_close_ms() -> None:
    open_time_ms, close_time_ms = timestamp_to_open_close_ms("2023-01-01 04:00:00")

    assert open_time_ms == 1672545600000
    assert close_time_ms == open_time_ms + 4 * 60 * 60 * 1000 - 1


def test_build_range_context_from_rf_columns() -> None:
    aggregate = build_range_context_from_rf_columns(_coin_audit_df().iloc[0].to_dict())

    assert aggregate is not None
    assert aggregate.bar_count == 37
    assert aggregate.imbalance == Decimal("0.2")
    assert aggregate.taker_buy_ratio == Decimal("0.6")
    assert aggregate.close_pos == Decimal("0.7")
    assert aggregate.micro_return_pct == Decimal("0.01")


def test_build_range_context_missing_bar_count_returns_none() -> None:
    row = _coin_audit_df().iloc[0].to_dict()
    row["rf_bar_count"] = None

    assert build_range_context_from_rf_columns(row) is None


def test_compare_signal_audits_detects_selected_engine_mismatch() -> None:
    coin_df = _coin_audit_df(selected_engine="BULL_RECLAIM_V2")
    ae_df = _coin_audit_df(selected_engine="MOMENTUM_V3")

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.mismatch_count == 1
    assert result.action_critical_mismatch_count == 1
    assert result.passed is False
    assert result.mismatches.iloc[0]["field"] == "selected_engine"
    assert result.mismatches.iloc[0]["category"] == "action_critical"
    assert result.mismatch_fields == {"selected_engine": 1}


def test_detect_feature_warmup_finds_first_valid_row() -> None:
    ae_df = pd.DataFrame(
        [
            {
                "timestamp": "2023-01-01 00:00:00",
                "atr": None,
                "atr_pct": None,
                "adx": None,
                "momentum_long_exit_channel": None,
                "momentum_short_exit_channel": None,
                "bull_long_exit_channel": None,
            },
            {
                "timestamp": "2023-01-01 04:00:00",
                "atr": None,
                "atr_pct": None,
                "adx": None,
                "momentum_long_exit_channel": None,
                "momentum_short_exit_channel": None,
                "bull_long_exit_channel": None,
            },
            {
                "timestamp": "2023-01-01 08:00:00",
                "atr": 10.0,
                "atr_pct": 0.01,
                "adx": 25.0,
                "momentum_long_exit_channel": None,
                "momentum_short_exit_channel": None,
                "bull_long_exit_channel": 100.0,
            },
        ]
    )

    result = detect_feature_warmup(ae_df)

    assert result["first_valid_ae_feature_index"] == 2
    assert result["first_valid_ae_feature_timestamp"] == "2023-01-01 08:00:00"
    assert result["recommended_skip_warmup_bars"] == 2
    assert result["ae_feature_invalid_rows"] == 2
    assert result["ae_feature_valid_rows"] == 1


def test_detect_feature_warmup_handles_no_valid_rows() -> None:
    ae_df = pd.DataFrame(
        [
            {
                "timestamp": "2023-01-01 00:00:00",
                "atr": None,
                "atr_pct": None,
                "adx": None,
                "momentum_long_exit_channel": None,
                "momentum_short_exit_channel": None,
                "bull_long_exit_channel": None,
            },
            {
                "timestamp": "2023-01-01 04:00:00",
                "atr": 10.0,
                "atr_pct": 0.01,
                "adx": 25.0,
                "momentum_long_exit_channel": None,
                "momentum_short_exit_channel": None,
                "bull_long_exit_channel": None,
            },
        ]
    )

    result = detect_feature_warmup(ae_df)

    assert result["first_valid_ae_feature_index"] is None
    assert result["first_valid_ae_feature_timestamp"] is None
    assert result["recommended_skip_warmup_bars"] == len(ae_df)
    assert result["ae_feature_valid_rows"] == 0
    assert result["ae_feature_invalid_rows"] == len(ae_df)


def test_compare_signal_audits_respects_float_tolerance() -> None:
    coin_df = _coin_audit_df(signal=1, selected_engine="MOMENTUM_V3", selected_priority=10, risk_mult=1.0)
    ae_df = _coin_audit_df(signal=1, selected_engine="MOMENTUM_V3", selected_priority=10, risk_mult=1.0 + 1e-10)

    within = compare_signal_audits(coin_df, ae_df, tolerance=1e-9, skip_warmup_bars=0)
    assert within.mismatch_count == 0

    ae_df = _coin_audit_df(signal=1, selected_engine="MOMENTUM_V3", selected_priority=10, risk_mult=1.0 + 1e-5)
    outside = compare_signal_audits(coin_df, ae_df, tolerance=1e-9, skip_warmup_bars=0)
    assert outside.mismatch_count == 1
    assert outside.signal_scoped_mismatch_count == 1
    assert outside.passed is False
    assert outside.mismatches.iloc[0]["category"] == "signal_scoped"
    assert outside.mismatches.iloc[0]["field"] == "risk_mult"


def test_no_signal_micro_neutral_vs_no_signal_is_not_action_mismatch() -> None:
    coin_df = _coin_audit_df(signal=0, micro_filter_action="NEUTRAL", micro_context_available=True)
    ae_df = _coin_audit_df(signal=0, micro_filter_action="NO_SIGNAL", micro_context_available=False)

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.action_critical_mismatch_count == 0
    assert result.signal_scoped_mismatch_count == 0
    assert result.passed is True


def test_no_signal_risk_quality_mismatch_is_ignored_for_pass_fail() -> None:
    coin_df = _coin_audit_df(signal=0, risk_mult=1.2, quality_mult=1.1)
    ae_df = _coin_audit_df(signal=0, risk_mult=1.0, quality_mult=1.0)

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.passed is True
    assert result.signal_scoped_mismatch_count == 0


def test_signal_mismatch_is_action_critical() -> None:
    coin_df = _coin_audit_df(signal=1)
    ae_df = _coin_audit_df(signal=0)

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.action_critical_mismatch_count == 1
    assert result.passed is False
    assert result.mismatches.iloc[0]["category"] == "action_critical"


def test_selected_engine_mismatch_is_action_critical() -> None:
    coin_df = _coin_audit_df(signal=1, selected_engine="BULL_RECLAIM_V2", selected_priority=30)
    ae_df = _coin_audit_df(signal=1, selected_engine="MOMENTUM_V3", selected_priority=30)

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.action_critical_mismatch_count == 1
    assert result.passed is False
    assert result.mismatches.iloc[0]["category"] == "action_critical"


def test_signal_scoped_fields_compare_when_signal_exists() -> None:
    coin_df = _coin_audit_df(signal=1, selected_engine="MOMENTUM_V3", selected_priority=10, micro_entry_risk_scale=0.5)
    ae_df = _coin_audit_df(signal=1, selected_engine="MOMENTUM_V3", selected_priority=10, micro_entry_risk_scale=1.0)

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.signal_scoped_mismatch_count == 1
    assert result.passed is False
    assert result.mismatches.iloc[0]["category"] == "signal_scoped"


def test_diagnostic_mismatch_does_not_fail_parity() -> None:
    coin_df = _coin_audit_df(rf_delta_sum=100.0)
    ae_df = _coin_audit_df(rf_delta_sum=101.0)

    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    assert result.diagnostic_mismatch_count == 1
    assert result.passed is True
    assert result.mismatches.iloc[0]["category"] == "diagnostic"


def test_strategy_fingerprint_hash_is_stable() -> None:
    first = strategy_fingerprint()
    second = strategy_fingerprint()

    assert first["fingerprint_hash"] == second["fingerprint_hash"]
    assert first["strategy_id"] == "eth_lf_portfolio_v9c_reclaim_priority"


def test_parser_exposes_logging_options() -> None:
    parser = build_parser()
    help_text = parser.format_help()

    assert "--log-every-rows" in help_text
    assert "--quiet" in help_text
    assert "--auto-skip-feature-warmup" in help_text


def test_replay_logs_progress(caplog) -> None:
    df = pd.concat(
        [
            _coin_audit_df(timestamp="2023-01-01 04:00:00"),
            _coin_audit_df(timestamp="2023-01-01 08:00:00"),
        ],
        ignore_index=True,
    )
    caplog.set_level(logging.INFO, logger="v9c_signal_parity")

    replay_aetheredge_signal_audit(df, log_every_rows=1)

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Replay progress" in messages
    assert "row=1/2" in messages
    assert "row=2/2" in messages


def test_compare_logs_progress(caplog) -> None:
    coin_df = pd.concat(
        [
            _coin_audit_df(timestamp="2023-01-01 04:00:00"),
            _coin_audit_df(timestamp="2023-01-01 08:00:00"),
        ],
        ignore_index=True,
    )
    ae_df = coin_df.copy()
    caplog.set_level(logging.INFO, logger="v9c_signal_parity")

    compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0, log_every_rows=1)

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "Compare progress" in messages
    assert "row=1/2" in messages
    assert "row=2/2" in messages


def test_cli_writes_expected_outputs(tmp_path: Path) -> None:
    coin_audit = tmp_path / "coin_signal_audit.csv"
    out_dir = tmp_path / "out"
    _coin_audit_df(rf_bar_count=0).to_csv(coin_audit, index=False)

    exit_code = main(
        [
            "--coin-audit",
            str(coin_audit),
            "--out-dir",
            str(out_dir),
            "--skip-warmup-bars",
            "0",
        ]
    )

    assert exit_code == 0
    assert (out_dir / REPLAY_AUDIT_FILENAME).exists()
    assert (out_dir / MISMATCH_FILENAME).exists()
    assert (out_dir / MISMATCH_CONTEXT_FILENAME).exists()
    assert (out_dir / SUMMARY_FILENAME).exists()
    assert (out_dir / FINGERPRINT_FILENAME).exists()
    summary = json.loads((out_dir / SUMMARY_FILENAME).read_text(encoding="utf-8"))
    assert summary["coin_rows"] == 1
    assert summary["aetheredge_rows"] == 1
    assert summary["requested_skip_warmup_bars"] == 0
    assert summary["effective_skip_warmup_bars"] == 0
    assert summary["skip_warmup_bars"] == 0
    assert summary["auto_skip_feature_warmup"] is False
    assert "feature_warmup" in summary


def test_cli_writes_signal_mismatch_context_csv(tmp_path: Path) -> None:
    coin_audit = tmp_path / "coin_signal_audit.csv"
    out_dir = tmp_path / "out"
    _coin_audit_df(rf_bar_count=0).to_csv(coin_audit, index=False)

    exit_code = main(
        [
            "--coin-audit",
            str(coin_audit),
            "--out-dir",
            str(out_dir),
            "--skip-warmup-bars",
            "0",
        ]
    )

    assert exit_code == 0
    assert (out_dir / MISMATCH_CONTEXT_FILENAME).exists()


def test_cli_quiet_still_prints_summary(tmp_path: Path, capsys) -> None:
    coin_audit = tmp_path / "coin_signal_audit.csv"
    out_dir = tmp_path / "out"
    _coin_audit_df(rf_bar_count=0).to_csv(coin_audit, index=False)

    exit_code = main(
        [
            "--coin-audit",
            str(coin_audit),
            "--out-dir",
            str(out_dir),
            "--skip-warmup-bars",
            "0",
            "--quiet",
        ]
    )

    assert exit_code == 0
    out = capsys.readouterr().out
    summary = json.loads(out)
    assert summary["coin_rows"] == 1
    assert summary["aetheredge_rows"] == 1


def test_auto_skip_feature_warmup_uses_max_requested_and_recommended(tmp_path: Path, monkeypatch) -> None:
    coin_audit = tmp_path / "coin_signal_audit.csv"
    out_dir = tmp_path / "out"
    _three_row_coin_audit_df(rf_bar_count=0).to_csv(coin_audit, index=False)
    monkeypatch.setattr(
        parity,
        "detect_feature_warmup",
        lambda _df: {
            "first_valid_ae_feature_index": 2,
            "first_valid_ae_feature_timestamp": "2023-01-01 12:00:00",
            "recommended_skip_warmup_bars": 2,
            "ae_feature_valid_rows": 1,
            "ae_feature_invalid_rows": 2,
        },
    )

    exit_code = main(
        [
            "--coin-audit",
            str(coin_audit),
            "--out-dir",
            str(out_dir),
            "--skip-warmup-bars",
            "1",
            "--auto-skip-feature-warmup",
            "--log-every-rows",
            "0",
        ]
    )

    assert exit_code == 0
    summary = json.loads((out_dir / SUMMARY_FILENAME).read_text(encoding="utf-8"))
    assert summary["requested_skip_warmup_bars"] == 1
    assert summary["effective_skip_warmup_bars"] == 2
    assert summary["skip_warmup_bars"] == 2
    assert summary["auto_skip_feature_warmup"] is True
    assert summary["feature_warmup"]["recommended_skip_warmup_bars"] == 2


def test_without_auto_skip_uses_requested_skip(tmp_path: Path, monkeypatch) -> None:
    coin_audit = tmp_path / "coin_signal_audit.csv"
    out_dir = tmp_path / "out"
    _three_row_coin_audit_df(rf_bar_count=0).to_csv(coin_audit, index=False)
    monkeypatch.setattr(
        parity,
        "detect_feature_warmup",
        lambda _df: {
            "first_valid_ae_feature_index": 2,
            "first_valid_ae_feature_timestamp": "2023-01-01 12:00:00",
            "recommended_skip_warmup_bars": 2,
            "ae_feature_valid_rows": 1,
            "ae_feature_invalid_rows": 2,
        },
    )

    exit_code = main(
        [
            "--coin-audit",
            str(coin_audit),
            "--out-dir",
            str(out_dir),
            "--skip-warmup-bars",
            "1",
            "--log-every-rows",
            "0",
        ]
    )

    assert exit_code == 0
    summary = json.loads((out_dir / SUMMARY_FILENAME).read_text(encoding="utf-8"))
    assert summary["requested_skip_warmup_bars"] == 1
    assert summary["effective_skip_warmup_bars"] == 1
    assert summary["skip_warmup_bars"] == 1
    assert summary["auto_skip_feature_warmup"] is False
    assert summary["feature_warmup"]["recommended_skip_warmup_bars"] == 2


def test_mismatch_context_marks_warmup_invalid() -> None:
    coin_df = _three_row_coin_audit_df(signal=1)
    ae_df = _three_row_coin_audit_df(signal=0)
    result = compare_signal_audits(coin_df, ae_df, skip_warmup_bars=0)

    context = build_signal_mismatch_context(
        coin_df,
        ae_df,
        result,
        recommended_skip_warmup_bars=2,
    )

    assert int(context.iloc[0]["row_index"]) == 0
    assert context.iloc[0]["warmup_invalid"] is True


def _coin_audit_df(**overrides) -> pd.DataFrame:
    row = {
        "timestamp": "2023-01-01 04:00:00",
        "open": 1000.0,
        "high": 1010.0,
        "low": 990.0,
        "close": 1005.0,
        "volume": 123.0,
        "signal": 0,
        "selected_engine": "NONE",
        "selected_priority": 0,
        "risk_mult": 1.0,
        "quality_mult": 1.0,
        "momentum_signal": 0,
        "bear_signal": 0,
        "bull_signal": 0,
        "micro_context_available": False,
        "micro_aligned": False,
        "micro_contra": False,
        "micro_entry_risk_scale": 1.0,
        "micro_filter_action": "NO_SIGNAL",
        "rf_bar_count": 37,
        "rf_micro_return_pct": 0.01,
        "rf_close_pos": 0.7,
        "rf_delta_sum": 100.0,
        "rf_imbalance": 0.2,
        "rf_taker_buy_ratio": 0.6,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _three_row_coin_audit_df(**overrides) -> pd.DataFrame:
    rows = []
    for timestamp in ["2023-01-01 04:00:00", "2023-01-01 08:00:00", "2023-01-01 12:00:00"]:
        row_overrides = dict(overrides)
        row_overrides["timestamp"] = timestamp
        rows.append(_coin_audit_df(**row_overrides))
    return pd.concat(rows, ignore_index=True)
