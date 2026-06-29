from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RiskSizingConfig:
    risk_pct: Decimal = Decimal("0.01")
    max_total_notional_mult: Decimal | None = None


class V8RiskSizer:
    """Risk-based base-asset quantity calculator.

    This mirrors the live intent: strategy outputs base asset quantity; exchange
    native conversion remains in order_management.
    """

    def __init__(self, config: RiskSizingConfig | None = None) -> None:
        self.config = config or RiskSizingConfig()

    def unit_qty(
        self,
        *,
        equity: Decimal,
        entry_price: Decimal,
        stop_price: Decimal,
        risk_mult: Decimal = Decimal("1"),
        quality_mult: Decimal = Decimal("1"),
        micro_entry_risk_scale: Decimal = Decimal("1"),
        global_risk_scale: Decimal = Decimal("1"),
        current_qty: Decimal = Decimal("0"),
    ) -> Decimal:
        risk_per_coin = abs(entry_price - stop_price)
        if equity <= 0:
            raise ValueError("equity must be positive")
        if entry_price <= 0:
            raise ValueError("entry_price must be positive")
        if risk_per_coin <= 0:
            raise ValueError("stop_price must differ from entry_price")
        risk_budget = equity * self.config.risk_pct * risk_mult * quality_mult * micro_entry_risk_scale * global_risk_scale
        qty = risk_budget / risk_per_coin
        if self.config.max_total_notional_mult is not None:
            max_qty = (equity * self.config.max_total_notional_mult) / entry_price
            remaining_qty = max(Decimal("0"), max_qty - current_qty)
            qty = min(qty, remaining_qty)
        return qty
