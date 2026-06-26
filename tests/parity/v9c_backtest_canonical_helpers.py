"""Minimal CoinBacktest V9C canonical snapshot helpers.

These helpers intentionally do not import the CoinBacktest runtime. They are a
small, deterministic snapshot of the V9C formulas extracted from the
CoinBacktest backtest implementation and used to guard AetherEdge live strategy
semantics.

If CoinBacktest V9C changes, update these helpers and the parity report
together, then rerun the relevant historical backtest before accepting the new
canonical baseline.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CanonicalExecConfig:
    unit_risk_per_trade: Decimal
    max_total_notional_mult: Decimal
    min_risk_mult: Decimal = Decimal("0.35")
    max_risk_mult: Decimal = Decimal("10.0")
    breakeven_after_r: Decimal = Decimal("1.0")
    breakeven_lock_r: Decimal = Decimal("0.10")
    lock_after_2r: Decimal = Decimal("1.7")
    lock_2r: Decimal = Decimal("0.70")
    lock_after_3r: Decimal = Decimal("2.8")
    lock_3r: Decimal = Decimal("1.50")


def same_bar_stop_update_allows_add() -> bool:
    return True


def initial_stop(*, side: int, entry_price: Decimal, atr: Decimal, initial_atr_mult: Decimal) -> Decimal:
    risk_per_coin = atr * initial_atr_mult
    return entry_price - risk_per_coin if side == 1 else entry_price + risk_per_coin


def unit_qty(
    *,
    capital: Decimal,
    entry_price: Decimal,
    stop_dist: Decimal,
    current_qty: Decimal,
    cfg: CanonicalExecConfig,
    risk_mult: Decimal,
) -> Decimal:
    if stop_dist <= 0:
        return Decimal("0")
    clipped_risk = max(cfg.min_risk_mult, min(risk_mult, cfg.max_risk_mult))
    risk_qty = capital * cfg.unit_risk_per_trade * clipped_risk / stop_dist
    max_total_qty = capital * cfg.max_total_notional_mult / entry_price
    remaining_qty = max(Decimal("0"), max_total_qty - current_qty)
    return max(Decimal("0"), min(risk_qty, remaining_qty))


def protected_stop(
    *,
    first_entry: Decimal,
    avg_entry: Decimal,
    side: int,
    risk_per_coin: Decimal,
    max_fav: Decimal,
    cfg: CanonicalExecConfig,
) -> Decimal | None:
    if risk_per_coin <= 0:
        return None
    fav_r = (max_fav - first_entry) / risk_per_coin if side == 1 else (first_entry - max_fav) / risk_per_coin
    lock_r: Decimal | None = None
    avg_lock_r: Decimal | None = None
    if fav_r >= cfg.lock_after_3r:
        lock_r = cfg.lock_3r
        avg_lock_r = Decimal("0.50")
    elif fav_r >= cfg.lock_after_2r:
        lock_r = cfg.lock_2r
        avg_lock_r = Decimal("0.00")
    elif fav_r >= cfg.breakeven_after_r:
        lock_r = cfg.breakeven_lock_r
        avg_lock_r = None
    if lock_r is None:
        return None
    first_based = first_entry + Decimal(side) * lock_r * risk_per_coin
    if avg_lock_r is None:
        return first_based
    avg_based = avg_entry + Decimal(side) * avg_lock_r * risk_per_coin
    return max(first_based, avg_based) if side == 1 else min(first_based, avg_based)


def trailing_stop(*, side: int, current_stop: Decimal, close: Decimal, atr: Decimal, trailing_atr_mult: Decimal) -> Decimal:
    candidate = close - trailing_atr_mult * atr if side == 1 else close + trailing_atr_mult * atr
    return max(current_stop, candidate) if side == 1 else min(current_stop, candidate)


def stop_candidate(*, side: int, trailing: Decimal, protected: Decimal | None) -> Decimal:
    if protected is None:
        return trailing
    return max(trailing, protected) if side == 1 else min(trailing, protected)


def add_trigger_price(*, side: int, first_entry: Decimal, units: int, add_every_r: Decimal, risk_per_coin: Decimal) -> Decimal:
    trigger_r = Decimal(units) * add_every_r
    return first_entry + trigger_r * risk_per_coin if side == 1 else first_entry - trigger_r * risk_per_coin
