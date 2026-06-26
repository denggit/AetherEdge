"""Minimal CoinBacktest V9E range-exit canonical snapshot.

This helper is extracted from the CoinBacktest V9E `_range_exit_signal`
formula. It intentionally does not import the CoinBacktest runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CanonicalRangeExitConfig:
    min_mfe_r: Decimal = Decimal("2.0")
    giveback_frac: Decimal = Decimal("0.65")
    min_hold_bars: int = 2
    contra_imbalance: Decimal = Decimal("0.05")
    bad_close_pos: Decimal = Decimal("0.35")
    require_reversal: bool = True


def range_exit_signal(
    *,
    side: int,
    avg_entry: Decimal,
    risk_per_coin: Decimal,
    max_fav: Decimal,
    hold_bars: int,
    close: Decimal,
    micro_context_available: bool,
    rf_imbalance: Decimal | None,
    rf_close_pos: Decimal | None,
    cfg: CanonicalRangeExitConfig = CanonicalRangeExitConfig(),
) -> tuple[bool, str, dict[str, object]]:
    meta: dict[str, object] = {
        "range_exit_triggered": False,
        "range_exit_peak_r": None,
        "range_exit_current_r": None,
        "range_exit_giveback_frac": None,
        "range_exit_reversal": False,
        "range_exit_reason": "",
    }
    if side not in (1, -1) or risk_per_coin <= 0:
        return False, "", meta
    if hold_bars < cfg.min_hold_bars:
        return False, "", meta
    if side == 1:
        peak_r = (max_fav - avg_entry) / risk_per_coin
        current_r = (close - avg_entry) / risk_per_coin
    else:
        peak_r = (avg_entry - max_fav) / risk_per_coin
        current_r = (avg_entry - close) / risk_per_coin
    giveback_frac = (peak_r - current_r) / max(abs(peak_r), Decimal("1e-12"))
    meta.update(
        {
            "range_exit_peak_r": peak_r,
            "range_exit_current_r": current_r,
            "range_exit_giveback_frac": giveback_frac,
        }
    )
    if peak_r < cfg.min_mfe_r or giveback_frac < cfg.giveback_frac or not micro_context_available:
        return False, "", meta
    if side == 1:
        reversal = bool(
            (rf_imbalance is not None and rf_imbalance <= -cfg.contra_imbalance)
            or (rf_close_pos is not None and rf_close_pos <= cfg.bad_close_pos)
        )
    else:
        reversal = bool(
            (rf_imbalance is not None and rf_imbalance >= cfg.contra_imbalance)
            or (rf_close_pos is not None and rf_close_pos >= Decimal("1") - cfg.bad_close_pos)
        )
    meta["range_exit_reversal"] = reversal
    if cfg.require_reversal and not reversal:
        return False, "", meta
    meta["range_exit_triggered"] = True
    meta["range_exit_reason"] = "RANGE_EXIT_NEXT_OPEN"
    return True, "RANGE_EXIT_NEXT_OPEN", meta
