from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from tools.v9c_signal_parity_check import (
    FINGERPRINT_FILENAME,
    MISMATCH_FILENAME,
    REPLAY_AUDIT_FILENAME,
    SUMMARY_FILENAME,
    build_range_context_from_rf_columns,
    build_parser,
    compare_signal_audits,
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
    assert result.mismatches.iloc[0]["field"] == "selected_engine"
    assert result.mismatch_fields == {"selected_engine": 1}


def test_compare_signal_audits_respects_float_tolerance() -> None:
    coin_df = _coin_audit_df(risk_mult=1.0)
    ae_df = _coin_audit_df(risk_mult=1.0 + 1e-10)

    within = compare_signal_audits(coin_df, ae_df, tolerance=1e-9, skip_warmup_bars=0)
    assert within.mismatch_count == 0

    ae_df = _coin_audit_df(risk_mult=1.0 + 1e-5)
    outside = compare_signal_audits(coin_df, ae_df, tolerance=1e-9, skip_warmup_bars=0)
    assert outside.mismatch_count == 1
    assert outside.mismatches.iloc[0]["field"] == "risk_mult"


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
    assert (out_dir / SUMMARY_FILENAME).exists()
    assert (out_dir / FINGERPRINT_FILENAME).exists()
    summary = json.loads((out_dir / SUMMARY_FILENAME).read_text(encoding="utf-8"))
    assert summary["coin_rows"] == 1
    assert summary["aetheredge_rows"] == 1


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
