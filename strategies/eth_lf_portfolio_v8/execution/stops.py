from __future__ import annotations

from decimal import Decimal

from strategies.eth_lf_portfolio_v8.domain.models import Side


def initial_stop_from_risk(*, side: Side, entry_price: Decimal, risk_per_coin: Decimal) -> Decimal:
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    if risk_per_coin <= 0:
        raise ValueError("risk_per_coin must be positive")
    if side is Side.LONG:
        return entry_price - risk_per_coin
    if side is Side.SHORT:
        return entry_price + risk_per_coin
    raise ValueError("side must be long or short")


def protected_stop(
    *,
    first_entry: Decimal,
    avg_entry: Decimal,
    side: Side,
    risk_per_coin: Decimal,
    max_fav: Decimal,
    breakeven_after_r: Decimal = Decimal("1.0"),
    breakeven_lock_r: Decimal = Decimal("0.10"),
    lock_after_2r: Decimal = Decimal("1.7"),
    lock_2r: Decimal = Decimal("0.70"),
    lock_after_3r: Decimal = Decimal("2.8"),
    lock_3r: Decimal = Decimal("1.50"),
) -> Decimal | None:
    if risk_per_coin <= 0:
        return None
    if side is Side.LONG:
        fav_r = (max_fav - first_entry) / risk_per_coin
    elif side is Side.SHORT:
        fav_r = (first_entry - max_fav) / risk_per_coin
    else:
        return None
    lock_r: Decimal | None = None
    avg_lock_r: Decimal | None = None
    if fav_r >= lock_after_3r:
        lock_r = lock_3r
        avg_lock_r = Decimal("0.50")
    elif fav_r >= lock_after_2r:
        lock_r = lock_2r
        avg_lock_r = Decimal("0.00")
    elif fav_r >= breakeven_after_r:
        lock_r = breakeven_lock_r
        avg_lock_r = None
    if lock_r is None:
        return None
    direction = Decimal("1") if side is Side.LONG else Decimal("-1")
    first_based = first_entry + direction * lock_r * risk_per_coin
    if avg_lock_r is None:
        return first_based
    avg_based = avg_entry + direction * avg_lock_r * risk_per_coin
    return max(first_based, avg_based) if side is Side.LONG else min(first_based, avg_based)


def is_better_stop(*, side: Side, current_stop: Decimal | None, candidate: Decimal | None) -> bool:
    if candidate is None or candidate <= 0:
        return False
    if current_stop is None or current_stop <= 0:
        return True
    if side is Side.LONG:
        return candidate > current_stop
    if side is Side.SHORT:
        return candidate < current_stop
    return False
