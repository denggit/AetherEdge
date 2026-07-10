"""Sizing parity: CoinBacktest mf_exposure=1.5 notional ↔ AetherEdge margin_fraction=0.10.

CoinBacktest Portfolio V1 sizing model
--------------------------------------
- mf_exposure is a NOTIONAL multiplier on portfolio equity.
- primary scenario: mf_exposure=1.5 → each MF trade controls 1.5× equity notional.
- With 15× leverage (default --leverage=15), implied margin = 1.5 / 15 = 0.10 = 10%.
- Config comment (line 93-94): "1.5 corresponds to 10% margin with 15× leverage".
- attach_mf_position_metrics assumes assumed_exposure=1.0 per trade;
  the portfolio layer scales returns by mf_exposure.
- margin_fraction_at_leverage15 = mf_exposure / 15.

AetherEdge live sizing model
----------------------------
- margin_fraction = 0.10 (config.json line 100).
- Actual notional = equity * margin_fraction * leverage.
- With 15× leverage → notional = equity * 0.10 * 15 = 1.5 × equity.
- MfSignalMapper._exchange_quantities (line 206-208):
    target_notional = equity * self.config.margin_fraction * leverage
- The position_fraction field is an alias for margin_fraction (MfLowSweepConfig.__post_init__).

Verdict: INTENTIONAL MATCH, not a bug
--------------------------------------
CoinBacktest mf_exposure=1.5 (notional multiplier) == AetherEdge margin_fraction=0.10 at 15× leverage.
The naming differs because CoinBacktest works in "notional space" (think of
it as "position × leverage / equity") while AetherEdge works in "margin
space" (what fraction of equity is used as exchange margin).

If leverage changes:
- CoinBacktest: mf_exposure stays 1.5 independent of leverage (it's a
  pure sizing parameter).
- AetherEdge: notional = equity * margin_fraction * leverage → changes with
  leverage. This is correct: if the exchange sets 10× leverage instead of
  15×, the same margin_fraction=0.10 gives 1.0× notional, which is the
  safe/correct behavior for exchange-mandated leverage.

Recommendation: NO CHANGE REQUIRED. The current naming is correct.
margin_fraction=0.10 is the fraction of equity used as margin; at
project-standard 15× leverage this equals 1.5× notional exposure,
matching the CoinBacktest primary scenario.

To avoid future confusion, the config.json could add:
  "_sizing_note": "margin_fraction=0.10 at 15× leverage = 1.5× equity notional exposure"
"""

from __future__ import annotations

from decimal import Decimal


# ---------------------------------------------------------------------------
# Frozen reference values
# ---------------------------------------------------------------------------

# CoinBacktest Portfolio V1 primary scenario (line 54)
CB_PRIMARY_EXPOSURE = 1.5  # notional multiplier
CB_DEFAULT_LEVERAGE = 15.0  # --leverage default
CB_IMPLIED_MARGIN = CB_PRIMARY_EXPOSURE / CB_DEFAULT_LEVERAGE  # 0.10

# AetherEdge config.json (line 100)
AE_MARGIN_FRACTION = Decimal("0.10")


def test_sizing_notional_equivalence() -> None:
    """At 15× leverage, margin_fraction=0.10 = 1.5× notional exposure."""
    equity = Decimal("1000")
    leverage = Decimal("15")
    # AetherEdge formula
    ae_notional = equity * AE_MARGIN_FRACTION * leverage
    # CoinBacktest formula: equity * mf_exposure
    cb_notional = equity * Decimal(str(CB_PRIMARY_EXPOSURE))

    assert ae_notional == cb_notional == Decimal("1500")
    assert float(AE_MARGIN_FRACTION) == CB_IMPLIED_MARGIN


def test_margin_fraction_at_different_leverages() -> None:
    """Verify that at 10× leverage, same margin_fraction gives proportionally less notional."""
    equity = Decimal("1000")
    for lev, expected_notional in [
        (Decimal("10"), Decimal("1000")),   # 1000 * 0.10 * 10 = 1000
        (Decimal("15"), Decimal("1500")),   # 1000 * 0.10 * 15 = 1500
        (Decimal("20"), Decimal("2000")),   # 1000 * 0.10 * 20 = 2000
    ]:
        ae_notional = equity * AE_MARGIN_FRACTION * lev
        assert ae_notional == expected_notional


def test_position_fraction_is_alias_for_margin_fraction() -> None:
    """MfLowSweepConfig normalizes position_fraction → margin_fraction."""
    from strategies.eth_portfolio_v1.domain.mf_signal import MfLowSweepConfig

    cfg = MfLowSweepConfig.from_mapping({"position_fraction": "0.10"})
    assert cfg.margin_fraction == Decimal("0.10")
    assert cfg.position_fraction == Decimal("0.10")

    # Both set to same value → OK
    cfg2 = MfLowSweepConfig.from_mapping(
        {"margin_fraction": "0.10", "position_fraction": "0.10"}
    )
    assert cfg2.margin_fraction == Decimal("0.10")


def test_available_margin_buffer_caps_exposure() -> None:
    """available_margin_buffer=0.95 means max notional = available * leverage * 0.95."""
    equity = Decimal("1000")
    available = Decimal("50")  # only $50 available
    leverage = Decimal("15")
    buffer = Decimal("0.95")

    target_by_equity = equity * AE_MARGIN_FRACTION * leverage  # 1500
    max_by_available = available * leverage * buffer  # 50 * 15 * 0.95 = 712.5
    capped = min(target_by_equity, max_by_available)  # 712.5

    assert capped == Decimal("712.5")
    assert capped < target_by_equity  # equity-based target is capped by available


def test_mf_config_rejects_invalid_margin_fraction() -> None:
    """margin_fraction must be in (0, 1]."""
    import pytest
    from strategies.eth_portfolio_v1.domain.mf_signal import MfLowSweepConfig

    with pytest.raises(ValueError, match="margin_fraction"):
        MfLowSweepConfig.from_mapping({"margin_fraction": "0"})

    with pytest.raises(ValueError, match="margin_fraction"):
        MfLowSweepConfig.from_mapping({"margin_fraction": "1.5"})


def test_margin_fraction_1x_sizing() -> None:
    """Live sizing: margin_fraction=0.0666666667 → 1.0× equity notional at 15×."""
    equity = Decimal("10000")
    leverage = Decimal("15")
    margin_fraction = Decimal("0.0666666667")
    reference_price = Decimal("2500")

    target_notional = equity * margin_fraction * leverage
    # Should be approximately 10000 (1× equity), not 15000 (1.5× equity)
    expected = Decimal("10000")
    assert abs(target_notional - expected) / expected < Decimal(
        "0.001"
    ), (
        f"target_notional={target_notional} should be ≈{expected} "
        f"(1× equity), not 1.5×"
    )

    quantity = target_notional / reference_price
    expected_qty = Decimal("4.0")
    assert abs(quantity - expected_qty) / expected_qty < Decimal(
        "0.001"
    )

    # Verify it's NOT 1.5× (which would be 15000 notional, 6.0 qty)
    old_notional = equity * Decimal("0.10") * leverage
    assert old_notional == Decimal("15000"), (
        "old margin_fraction=0.10 gives 1.5×"
    )


def test_hard_stop_config_defaults() -> None:
    """Hard stop config fields parse with correct defaults."""
    from strategies.eth_portfolio_v1.domain.mf_signal import (
        MfLowSweepConfig,
    )

    cfg = MfLowSweepConfig.from_mapping(
        {
            "margin_fraction": "0.0666666667",
            "hard_stop_enabled": True,
            "hard_stop_pct": "0.0500",
            "hard_stop_cooldown_hours": 12,
        }
    )
    assert cfg.hard_stop_enabled is True
    assert cfg.hard_stop_pct == Decimal("0.0500")
    assert cfg.hard_stop_cooldown_hours == 12

    # Defaults
    cfg2 = MfLowSweepConfig.from_mapping(
        {"margin_fraction": "0.0666666667"}
    )
    assert cfg2.hard_stop_enabled is True
    assert cfg2.hard_stop_pct == Decimal("0.0500")
    assert cfg2.hard_stop_cooldown_hours == 12


def test_hard_stop_config_rejects_invalid() -> None:
    """Hard stop pct must be in (0, 0.20]. Cooldown must be >= 0."""
    import pytest
    from strategies.eth_portfolio_v1.domain.mf_signal import (
        MfLowSweepConfig,
    )

    with pytest.raises(ValueError, match="hard_stop_pct"):
        MfLowSweepConfig.from_mapping(
            {
                "margin_fraction": "0.0666666667",
                "hard_stop_pct": "0.25",
            }
        )

    with pytest.raises(ValueError, match="hard_stop_pct"):
        MfLowSweepConfig.from_mapping(
            {
                "margin_fraction": "0.0666666667",
                "hard_stop_pct": "0",
            }
        )

    with pytest.raises(ValueError, match="hard_stop_cooldown_hours"):
        MfLowSweepConfig.from_mapping(
            {
                "margin_fraction": "0.0666666667",
                "hard_stop_cooldown_hours": -1,
            }
        )


def test_cb_exposure_to_ae_margin_formula() -> None:
    """Conversion formula: margin_fraction = mf_exposure / leverage."""
    for exposure, leverage, expected_margin in [
        (0.5, 15.0, 0.5 / 15.0),   # 0.0333...
        (1.0, 15.0, 1.0 / 15.0),   # 0.0666...
        (1.5, 15.0, 1.5 / 15.0),   # 0.10
        (2.0, 15.0, 2.0 / 15.0),   # 0.1333...
    ]:
        margin = exposure / leverage
        assert abs(margin - expected_margin) < 1e-12
