from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.platform.execution import ExecutionClient
import asyncio

from src.platform.exchanges.models import Order, OrderQuery, OrderStatus, StopOrderQuery
from src.planner import PlannedExecution, PlannedExecutionAction


_TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}


class OrderStatusSynchronizer:
    """Fetch the exchange's post-submit order state and enrich the journal result.

    Exchange REST placement responses are often only acknowledgements. For live
    state machines we need the actual filled quantity, average fill price, and
    any fee fields returned by the venue. This synchronizer intentionally stays
    in order-management, not in strategies.
    """

    def __init__(self, *, terminal_retry_delays_seconds: tuple[float, ...] = (1.0, 2.0, 3.0)) -> None:
        self.terminal_retry_delays_seconds = terminal_retry_delays_seconds

    async def sync_after_submit(self, *, client: ExecutionClient, item: PlannedExecution, order: Order) -> Order:
        try:
            if item.action is PlannedExecutionAction.PLACE_ORDER:
                return await self._sync_regular_order(client, order)
            if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
                return await self._sync_stop_order(client, order)
            if item.action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS:
                return await self._sync_open_stop_orders_after_cancel(client, order)
        except Exception as exc:
            return _merge_raw(order, {"order_status_sync_error": str(exc)})
        return order

    async def _sync_regular_order(self, client: ExecutionClient, order: Order) -> Order:
        fetch = getattr(client, "fetch_order_status", None)
        if not callable(fetch):
            return order
        query = OrderQuery(symbol=order.symbol, order_id=order.order_id, client_order_id=order.client_order_id)
        synced = await fetch(query)
        for delay in self.terminal_retry_delays_seconds:
            if synced.status in _TERMINAL:
                break
            await asyncio.sleep(delay)
            synced = await fetch(query)
        return _prefer_synced(order, synced, raw_key="synced_order")

    async def _sync_stop_order(self, client: ExecutionClient, order: Order) -> Order:
        fetch = getattr(client, "fetch_stop_order_status", None)
        if not callable(fetch):
            return order
        query = StopOrderQuery(symbol=order.symbol, stop_order_id=order.order_id, client_order_id=order.client_order_id)
        synced = await fetch(query)
        return _prefer_synced(order, synced, raw_key="synced_stop_order")

    async def _sync_open_stop_orders_after_cancel(self, client: ExecutionClient, order: Order) -> Order:
        fetch = getattr(client, "fetch_open_stop_orders", None)
        if not callable(fetch):
            return order
        open_stop_orders = await fetch()
        return _merge_raw(order, {"synced_open_stop_orders": [dict(item.raw) for item in open_stop_orders]})


def _prefer_synced(original: Order, synced: Order, *, raw_key: str) -> Order:
    raw = {**dict(original.raw), raw_key: dict(synced.raw)}
    return replace(
        original,
        order_id=synced.order_id or original.order_id,
        client_order_id=synced.client_order_id or original.client_order_id,
        status=synced.status or original.status,
        side=synced.side or original.side,
        order_type=synced.order_type or original.order_type,
        price=synced.price if synced.price is not None else original.price,
        quantity=synced.quantity if synced.quantity is not None else original.quantity,
        filled_quantity=synced.filled_quantity if synced.filled_quantity is not None else original.filled_quantity,
        raw=raw,
    )


def _merge_raw(order: Order, values: Mapping[str, Any]) -> Order:
    return replace(order, raw={**dict(order.raw), **dict(values)})


def extract_avg_fill_price(order: Order) -> Decimal | None:
    """Extract average fill price from normalized or raw order payloads."""
    raw = _flatten_raw(order.raw)
    for key in ("avg_fill_price", "avgPx", "avgPrice", "fillPx", "priceAvg"):
        value = _decimal_or_none(raw.get(key))
        if value is not None and value > 0:
            return value
    cum_quote = _decimal_or_none(raw.get("cumQuote")) or _decimal_or_none(raw.get("cummulativeQuoteQty"))
    executed_qty = _decimal_or_none(raw.get("executedQty")) or order.filled_quantity
    if cum_quote is not None and executed_qty is not None and executed_qty > 0:
        return cum_quote / executed_qty
    if order.price is not None and order.price > 0:
        return order.price
    return None


def extract_fee(order: Order) -> tuple[Decimal | None, str | None]:
    raw = _flatten_raw(order.raw)
    fee = _decimal_or_none(raw.get("fee"))
    fee_asset = _str_or_none(raw.get("feeCcy"))
    if fee is not None:
        return fee, fee_asset
    fee = _decimal_or_none(raw.get("n"))
    fee_asset = _str_or_none(raw.get("N"))
    if fee is not None:
        return fee, fee_asset
    fills = raw.get("fills")
    if isinstance(fills, list):
        total = Decimal("0")
        asset: str | None = None
        found = False
        for fill in fills:
            if not isinstance(fill, Mapping):
                continue
            commission = _decimal_or_none(fill.get("commission"))
            if commission is None:
                continue
            total += commission
            asset = _str_or_none(fill.get("commissionAsset")) or asset
            found = True
        if found:
            return total, asset
    return None, None


def _flatten_raw(raw: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(raw)
    for nested_key in ("synced_order", "synced_stop_order"):
        nested = raw.get(nested_key)
        if isinstance(nested, Mapping):
            result.update(nested)
    return result


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)
