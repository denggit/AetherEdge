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
