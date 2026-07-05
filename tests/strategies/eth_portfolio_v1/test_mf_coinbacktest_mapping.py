from __future__ import annotations

from pathlib import Path

from strategies.eth_portfolio_v1.domain.mf_signal import (
    COINBACKTEST_CHILD_SOURCE,
    COINBACKTEST_PORTFOLIO_SOURCE,
    COINBACKTEST_RESEARCH_SOURCES,
    MF_ENTRY_REQUIRED_FIELDS,
    MF_EVENT_BREAKOUT_THRESHOLDS,
    MF_EVENT_MAX_SWING_AGES,
    MF_EVENT_MIN_PROMINENCE_PCTS,
    MF_EVENT_SPIKE_THRESHOLDS,
    MF_EVENT_VARIANTS,
    MF_LARGE_SHARE_MIN_SAMPLES,
    MF_LARGE_SHARE_WINDOW_SAMPLES,
    MF_MIN_SWING_AGE,
    MF_PIVOT_LEFT,
    MF_PIVOT_RIGHT,
    MF_TIME_EXIT_BARS,
    MF_VARIANT_NAME,
)


def test_coinbacktest_portfolio_source_path_is_recorded() -> None:
    assert COINBACKTEST_PORTFOLIO_SOURCE.endswith(
        "eth_portfolio_V1_lf_v10b_low_sweep_mf_backtest.py"
    )
    assert Path(COINBACKTEST_PORTFOLIO_SOURCE).is_file()
    source_root = Path(COINBACKTEST_PORTFOLIO_SOURCE).parents[2]
    assert (source_root / COINBACKTEST_CHILD_SOURCE).is_file()
    assert all(
        (source_root / source).is_file()
        for source in COINBACKTEST_RESEARCH_SOURCES
    )


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
        "low_sweep_event",
        "event_variant",
        "single_swing",
        "fp_max_bucket_abs_delta_pressure",
        "fp_abs_delta_high_threshold",
        "signal_side",
    }


def test_exact_event_and_swing_parameters_match_source_chain() -> None:
    assert MF_PIVOT_LEFT == 6
    assert MF_PIVOT_RIGHT == 3
    assert MF_MIN_SWING_AGE == 3
    assert MF_EVENT_MAX_SWING_AGES == (12, 24, 48, 96, 240, 1440)
    assert tuple(map(str, MF_EVENT_MIN_PROMINENCE_PCTS)) == (
        "0.0015",
        "0.0030",
    )
    assert tuple(map(str, MF_EVENT_SPIKE_THRESHOLDS)) == (
        "0.0060",
        "0.0080",
        "0.0100",
        "0.0120",
    )
    assert tuple(map(str, MF_EVENT_BREAKOUT_THRESHOLDS)) == (
        "0.0000",
        "0.0005",
    )
    assert MF_EVENT_VARIANTS == ("fade_close_through",)
    assert MF_LARGE_SHARE_WINDOW_SAMPLES == 129_600
    assert MF_LARGE_SHARE_MIN_SAMPLES == 43_200


def test_single_swing_next_open_and_time48_mapping_notes_exist() -> None:
    source = (
        Path(__file__).resolve().parents[3]
        / "strategies"
        / "eth_portfolio_v1"
        / "domain"
        / "mf_low_sweep.py"
    ).read_text(encoding="utf-8")
    mapping_source = (
        Path(__file__).resolve().parents[3]
        / "strategies"
        / "eth_portfolio_v1"
        / "domain"
        / "mf_signal.py"
    ).read_text(encoding="utf-8")
    assert "build_support_mask" in mapping_source
    assert 'build_support_mask("single_swing") is an all-True mask' in source
    assert "build_low_sweep_events" in source
    assert "build_canonical_events" in mapping_source
    assert "signal-bar timestamp" in source
    assert MF_TIME_EXIT_BARS == 48
    assert "signal_pos + 48" in source
