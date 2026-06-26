from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.execution.range_exit import RangeExitConfig, evaluate_range_exit
from strategies.eth_lf_portfolio_v8.strategy import V8Config


def test_peak_r_below_min_mfe_does_not_exit() -> None:
    decision = _decision(max_fav=Decimal("115"), close=Decimal("105"), rf_imbalance=Decimal("-0.10"))

    assert decision.should_exit is False
    assert decision.metadata["range_exit_triggered"] is False


def test_hold_bars_below_min_hold_does_not_exit() -> None:
    decision = _decision(hold_bars=1, max_fav=Decimal("140"), close=Decimal("114"), rf_imbalance=Decimal("-0.10"))

    assert decision.should_exit is False


def test_giveback_below_threshold_does_not_exit() -> None:
    decision = _decision(max_fav=Decimal("140"), close=Decimal("125"), rf_imbalance=Decimal("-0.10"))

    assert decision.should_exit is False
    assert Decimal(decision.metadata["range_exit_giveback_frac"]) == Decimal("0.375")


def test_micro_context_unavailable_does_not_exit() -> None:
    decision = _decision(
        max_fav=Decimal("140"),
        close=Decimal("114"),
        micro_context_available=False,
        rf_imbalance=Decimal("-0.10"),
    )

    assert decision.should_exit is False


def test_require_reversal_without_hostile_context_does_not_exit() -> None:
    decision = _decision(max_fav=Decimal("140"), close=Decimal("114"), rf_imbalance=Decimal("0.00"), rf_close_pos=Decimal("0.50"))

    assert decision.should_exit is False
    assert decision.metadata["range_exit_reversal"] is False


def test_long_hostile_imbalance_triggers_exit() -> None:
    decision = _decision(max_fav=Decimal("140"), close=Decimal("114"), rf_imbalance=Decimal("-0.05"), rf_close_pos=Decimal("0.50"))

    assert decision.should_exit is True
    assert decision.reason == "RANGE_EXIT_NEXT_OPEN"
    assert decision.metadata["range_exit_reversal"] is True


def test_long_bad_close_pos_triggers_exit() -> None:
    decision = _decision(max_fav=Decimal("140"), close=Decimal("114"), rf_imbalance=Decimal("0.00"), rf_close_pos=Decimal("0.35"))

    assert decision.should_exit is True
    assert decision.metadata["range_exit_reason"] == "RANGE_EXIT_NEXT_OPEN"


def test_short_hostile_imbalance_triggers_exit() -> None:
    decision = _decision(
        side=Side.SHORT,
        max_fav=Decimal("60"),
        close=Decimal("86"),
        rf_imbalance=Decimal("0.05"),
        rf_close_pos=Decimal("0.50"),
    )

    assert decision.should_exit is True
    assert Decimal(decision.metadata["range_exit_peak_r"]) == Decimal("4")
    assert Decimal(decision.metadata["range_exit_current_r"]) == Decimal("1.4")


def test_short_bad_close_pos_triggers_exit() -> None:
    decision = _decision(
        side=Side.SHORT,
        max_fav=Decimal("60"),
        close=Decimal("86"),
        rf_imbalance=Decimal("0.00"),
        rf_close_pos=Decimal("0.65"),
    )

    assert decision.should_exit is True


def test_delay_bars_above_zero_config_fails() -> None:
    path = Path(".tmp_pytest/v9e_delay_config.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(Path("strategies/eth_lf_portfolio_v8/config.json").read_text(encoding="utf-8"))
    data["range_exit"]["delay_bars"] = 1
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(ValueError, match="delay_bars"):
        V8Config.from_file(path)


def _decision(
    *,
    side: Side = Side.LONG,
    avg_entry: Decimal = Decimal("100"),
    risk_per_coin: Decimal = Decimal("10"),
    max_fav: Decimal,
    hold_bars: int = 3,
    close: Decimal,
    micro_context_available: bool = True,
    rf_imbalance: Decimal | None = None,
    rf_close_pos: Decimal | None = None,
):
    return evaluate_range_exit(
        side=side,
        avg_entry=avg_entry,
        risk_per_coin=risk_per_coin,
        max_fav=max_fav,
        hold_bars=hold_bars,
        close=close,
        micro_context_available=micro_context_available,
        rf_imbalance=rf_imbalance,
        rf_close_pos=rf_close_pos,
        config=RangeExitConfig(),
    )
