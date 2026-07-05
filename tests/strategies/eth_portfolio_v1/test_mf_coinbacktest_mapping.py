from __future__ import annotations

from pathlib import Path

from strategies.eth_portfolio_v1.domain.mf_signal import (
    COINBACKTEST_PORTFOLIO_SOURCE,
    MF_ENTRY_REQUIRED_FIELDS,
    MF_VARIANT_NAME,
)


def test_coinbacktest_portfolio_source_path_is_recorded() -> None:
    assert COINBACKTEST_PORTFOLIO_SOURCE.endswith(
        "eth_portfolio_V1_lf_v10b_low_sweep_mf_backtest.py"
    )
    assert Path(COINBACKTEST_PORTFOLIO_SOURCE).is_file()


def test_only_time48_leg_is_mapped() -> None:
    assert MF_VARIANT_NAME == (
        "A0_fp_abs_delta_high__single_swing__next_open__time48__no_stop"
    )
    forbidden = (
        "mfe_" + "lock",
        "comfort_" + "leg",
        "profit_" + "lock",
    )
    assert all(token not in MF_VARIANT_NAME.lower() for token in forbidden)


def test_a0_required_field_mapping_is_complete() -> None:
    assert set(MF_ENTRY_REQUIRED_FIELDS) == {
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
    }


def test_single_swing_next_open_and_time48_mapping_notes_exist() -> None:
    source = (
        Path(__file__).resolve().parents[3]
        / "strategies"
        / "eth_portfolio_v1"
        / "domain"
        / "mf_low_sweep.py"
    ).read_text(encoding="utf-8")
    assert "build_support_mask" in (
        Path(__file__).resolve().parents[3]
        / "strategies"
        / "eth_portfolio_v1"
        / "domain"
        / "mf_signal.py"
    ).read_text(encoding="utf-8")
    assert "signal-bar timestamp" in source
    assert "holding_minutes" in source
