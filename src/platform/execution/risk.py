from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from src.platform.exchanges.models import AmendOrderRequest, InstrumentRule, OrderRequest, OrderType


class RiskCheckError(ValueError):
    pass


@dataclass(frozen=True)
class ExecutionRiskLimits:
    max_order_notional: Decimal | None = None
    allow_live_trading: bool = False
    require_client_order_id: bool = False


class ExecutionRiskGate:
    def __init__(self, limits: ExecutionRiskLimits | None = None) -> None:
        self._limits = limits or ExecutionRiskLimits()

    @property
    def limits(self) -> ExecutionRiskLimits:
        return self._limits

    def validate_order(self, request: OrderRequest, rule: InstrumentRule | None = None) -> None:
        if self._limits.require_client_order_id and not request.client_order_id:
            raise RiskCheckError("client_order_id is required by risk gate")
        if request.order_type == OrderType.LIMIT and request.price is None:
            raise RiskCheckError("limit order requires price")
        if rule is not None:
            if rule.min_quantity is not None and request.quantity < rule.min_quantity:
                raise RiskCheckError(f"quantity {request.quantity} is below min_quantity {rule.min_quantity}")
            if rule.max_quantity is not None and request.quantity > rule.max_quantity:
                raise RiskCheckError(f"quantity {request.quantity} is above max_quantity {rule.max_quantity}")
            notional = _estimate_notional(request.quantity, request.price)
            if rule.min_notional is not None and notional is not None and notional < rule.min_notional:
                raise RiskCheckError(f"notional {notional} is below min_notional {rule.min_notional}")
        if self._limits.max_order_notional is not None:
            notional = _estimate_notional(request.quantity, request.price)
            if notional is not None and notional > self._limits.max_order_notional:
                raise RiskCheckError(f"notional {notional} exceeds max_order_notional {self._limits.max_order_notional}")

    def validate_amend(self, request: AmendOrderRequest, rule: InstrumentRule | None = None) -> None:
        if request.new_quantity is not None and rule is not None:
            if rule.min_quantity is not None and request.new_quantity < rule.min_quantity:
                raise RiskCheckError(f"new_quantity {request.new_quantity} is below min_quantity {rule.min_quantity}")
            if rule.max_quantity is not None and request.new_quantity > rule.max_quantity:
                raise RiskCheckError(f"new_quantity {request.new_quantity} is above max_quantity {rule.max_quantity}")


def _estimate_notional(quantity: Decimal, price: Decimal | None) -> Decimal | None:
    if price is None:
        return None
    return quantity * price
