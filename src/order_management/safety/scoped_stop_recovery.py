from __future__ import annotations

from collections.abc import Iterable, Sequence

from src.platform.exchanges.models import Order


_RAW_POSITION_ID_KEYS = (
    "position_id",
    "positionId",
    "positionID",
)
_RAW_CLIENT_ID_KEYS = (
    "clientAlgoId",
    "algoClOrdId",
    "clOrdId",
    "clientOrderId",
    "newClientOrderId",
)


def order_matches_position_scope(
    order: Order,
    *,
    position_id: str,
    known_order_ids: Iterable[str | None] = (),
) -> bool:
    """Return whether an order is provably owned by one logical position.

    Strategy ownership alone is deliberately insufficient: a multi-sleeve strategy can
    hold multiple sleeves under the same strategy, so recovery must have an
    exact position reference from the plan, raw exchange payload, or order ID.
    """

    expected = _normalized(position_id)
    if not expected:
        return False

    raw = dict(order.raw)
    raw_position_ids = tuple(
        _normalized(raw.get(key))
        for key in _RAW_POSITION_ID_KEYS
        if raw.get(key) not in (None, "")
    )
    if raw_position_ids:
        return expected in raw_position_ids

    known = {
        normalized
        for value in known_order_ids
        if (normalized := _normalized(value))
    }
    identifiers = [
        order.order_id,
        order.client_order_id,
        *(raw.get(key) for key in _RAW_CLIENT_ID_KEYS),
    ]
    for value in identifiers:
        normalized = _normalized(value)
        if not normalized:
            continue
        if normalized in known or expected in normalized:
            return True
    return False


def filter_orders_for_position_scope(
    orders: Sequence[Order],
    *,
    position_id: str,
    known_order_ids: Iterable[str | None] = (),
) -> tuple[Order, ...]:
    return tuple(
        order
        for order in orders
        if order_matches_position_scope(
            order,
            position_id=position_id,
            known_order_ids=known_order_ids,
        )
    )


def _normalized(value: object) -> str:
    return str(value or "").strip().lower()


__all__ = [
    "filter_orders_for_position_scope",
    "order_matches_position_scope",
]
