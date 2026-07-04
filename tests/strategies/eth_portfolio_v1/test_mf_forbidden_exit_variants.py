from __future__ import annotations

from pathlib import Path

import pytest

from strategies.eth_portfolio_v1.domain.mf_live_policy import (
    R007_MF_EXIT_VARIANT,
    validate_mf_exit_variant,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_mf_live_forbids_mfe_lock_exit_variants() -> None:
    forbidden_variants = (
        "mfe_lock",
        "mfe_lock_15_05",
        "mfe_lock_15_05_time48",
        "comfort_leg",
        "profit_lock",
    )
    for variant in forbidden_variants:
        with pytest.raises(ValueError, match="not allowed"):
            validate_mf_exit_variant(variant)

    protected_sources = (
        PROJECT_ROOT / "strategies" / "eth_portfolio_v1",
        PROJECT_ROOT / "src" / "market_data",
        PROJECT_ROOT / "src" / "runtime",
        PROJECT_ROOT / "tools" / "mf_feature_backfill_worker.py",
    )
    forbidden_tokens = ("mfe_" + "lock", "comfort_" + "leg", "profit_" + "lock")
    for source in protected_sources:
        paths = source.rglob("*.py") if source.is_dir() else (source,)
        for path in paths:
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8").lower()
            for token in forbidden_tokens:
                assert token not in text, f"{path} contains forbidden MF exit token"


def test_mf_exit_variant_time48_only_guard() -> None:
    assert R007_MF_EXIT_VARIANT == "none"
    assert validate_mf_exit_variant("none") == "none"
    assert validate_mf_exit_variant("time48") == "time48"
    with pytest.raises(ValueError, match="not allowed"):
        validate_mf_exit_variant("time72")
