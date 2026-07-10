from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_DOWN

from src.platform.exchanges.models import AmendOrderRequest, InstrumentRule, OrderRequest, StopMarketOrderRequest


def round_to_step(value: Decimal, step: Decimal | None) -> Decimal:
    if step is None or step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def normalize_stop_trigger_price(
    trigger_price: Decimal,
    rule: InstrumentRule | None,
) -> Decimal:
    """Return the trigger price the venue will receive for a stop order.

    This is the single price-normalization primitive shared by stop order
    submission and recovery validation.  Keeping it here prevents recovery
    code from growing a second, subtly different tick-rounding policy.
    """

    return round_to_step(
        trigger_price,
        None if rule is None else rule.price_tick,
    )


def normalize_order_request(request: OrderRequest, rule: InstrumentRule | None) -> OrderRequest:
    if rule is None:
        return request
    quantity = round_to_step(request.quantity, rule.quantity_step)
    price = round_to_step(request.price, rule.price_tick) if request.price is not None else None
    return replace(request, quantity=quantity, price=price)


def normalize_amend_order_request(request: AmendOrderRequest, rule: InstrumentRule | None) -> AmendOrderRequest:
    if rule is None:
        return request
    new_quantity = round_to_step(request.new_quantity, rule.quantity_step) if request.new_quantity is not None else None
    new_price = round_to_step(request.new_price, rule.price_tick) if request.new_price is not None else None
    return replace(request, new_quantity=new_quantity, new_price=new_price)


def normalize_stop_market_order_request(request: StopMarketOrderRequest, rule: InstrumentRule | None) -> StopMarketOrderRequest:
    if rule is None:
        return request
    quantity = round_to_step(request.quantity, rule.quantity_step) if request.quantity is not None else None
    trigger_price = normalize_stop_trigger_price(request.trigger_price, rule)
    return replace(request, quantity=quantity, trigger_price=trigger_price)
