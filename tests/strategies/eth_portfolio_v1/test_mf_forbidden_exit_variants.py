from __future__ import annotations

from pathlib import Path

import pytest

from strategies.eth_portfolio_v1.domain.mf_live_policy import (
    MF_LIVE_EXIT_VARIANT,
    R007_MF_EXIT_VARIANT,
    validate_mf_exit_variant,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_mf_live_forbids_unapproved_exit_variants() -> None:
    base = "mfe_" + "lock"
    forbidden_variants = (
        base,
        base + "_15_05",
        base + "_15_05_time48",
        "comfort_" + "leg",
        "profit_" + "lock",
    )
    for variant in forbidden_variants:
        with pytest.raises(ValueError, match="not allowed"):
            validate_mf_exit_variant(variant)

    forbidden_tokens = (
        base,
        base + "_15_05",
        base + "_15_05_time48",
        "comfort_" + "leg",
        "profit_" + "lock",
    )
    text_suffixes = {
        ".cfg",
        ".ini",
        ".json",
        ".md",
        ".py",
        ".toml",
        ".txt",
        ".yaml",
        ".yml",
    }
    for path in PROJECT_ROOT.rglob("*"):
        if (
            not path.is_file()
            or path.suffix.lower() not in text_suffixes
            or any(
                part == ".git"
                or part == ".idea"
                or part == "__pycache__"
                or part.startswith(".pytest")
                for part in path.parts
            )
        ):
            continue
        text = path.read_text(encoding="utf-8").lower()
        for token in forbidden_tokens:
            assert token not in text, (
                f"{path} contains forbidden MF exit token"
            )

    r008_sources = (
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "domain" / "mf_data.py",
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "domain" / "mf_live_policy.py",
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "domain" / "mf_low_sweep.py",
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "domain" / "mf_signal.py",
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "domain" / "mf_sleeve.py",
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "execution" / "mf_signal_mapper.py",
    )
    standalone_forbidden = "m" + "fe"
    for path in r008_sources:
        assert standalone_forbidden not in path.read_text(
            encoding="utf-8"
        ).lower()


def test_mf_exit_variant_time48_only_guard() -> None:
    assert R007_MF_EXIT_VARIANT == "none"
    assert MF_LIVE_EXIT_VARIANT == "time48"
    assert validate_mf_exit_variant("time48") == "time48"
    with pytest.raises(ValueError, match="not allowed"):
        validate_mf_exit_variant("none")
    with pytest.raises(ValueError, match="not allowed"):
        validate_mf_exit_variant("time72")
