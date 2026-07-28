from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from src.platform.data.models import OrderBookLevel
from src.platform.data.order_book.errors import LocalOrderBookError


class LocalOrderBook:
    """Transactionally maintain a bounded price-indexed order book."""

    def __init__(self, *, max_depth: int = 400) -> None:
        if type(max_depth) is not int or max_depth <= 0:
            raise ValueError("max_depth must be a positive integer")
        self._max_depth = max_depth
        self._retained_depth = max_depth * 4
        self._bids: dict[Decimal, OrderBookLevel] = {}
        self._asks: dict[Decimal, OrderBookLevel] = {}

    def reset(
        self,
        *,
        bids: Sequence[OrderBookLevel],
        asks: Sequence[OrderBookLevel],
    ) -> None:
        checked_bids = self._validated(bids)
        checked_asks = self._validated(asks)
        next_bids = {
            level.price: level for level in checked_bids if level.quantity != 0
        }
        next_asks = {
            level.price: level for level in checked_asks if level.quantity != 0
        }
        self._bids = self._trim(next_bids, reverse=True)
        self._asks = self._trim(next_asks, reverse=False)

    def apply_updates(
        self,
        *,
        bids: Sequence[OrderBookLevel],
        asks: Sequence[OrderBookLevel],
    ) -> None:
        checked_bids = self._validated(bids)
        checked_asks = self._validated(asks)
        next_bids = dict(self._bids)
        next_asks = dict(self._asks)
        self._apply(next_bids, checked_bids)
        self._apply(next_asks, checked_asks)
        self._bids = self._trim(next_bids, reverse=True)
        self._asks = self._trim(next_asks, reverse=False)

    def snapshot(
        self,
        *,
        depth: int,
    ) -> tuple[tuple[OrderBookLevel, ...], tuple[OrderBookLevel, ...]]:
        if type(depth) is not int or depth <= 0:
            raise ValueError("depth must be a positive integer")
        limit = min(depth, self._max_depth)
        bids = tuple(
            self._bids[price]
            for price in sorted(self._bids, reverse=True)[:limit]
        )
        asks = tuple(
            self._asks[price]
            for price in sorted(self._asks)[:limit]
        )
        return bids, asks

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()

    @property
    def empty(self) -> bool:
        return not self._bids and not self._asks

    @staticmethod
    def _validated(
        levels: Sequence[OrderBookLevel],
    ) -> tuple[OrderBookLevel, ...]:
        checked: list[OrderBookLevel] = []
        for level in levels:
            if not isinstance(level, OrderBookLevel):
                raise LocalOrderBookError(
                    f"order book level must be OrderBookLevel: {level!r}"
                )
            if not isinstance(level.price, Decimal):
                raise LocalOrderBookError(
                    f"order book price must be Decimal: {level.price!r}"
                )
            if not isinstance(level.quantity, Decimal):
                raise LocalOrderBookError(
                    f"order book quantity must be Decimal: {level.quantity!r}"
                )
            if level.quantity < 0:
                raise LocalOrderBookError(
                    f"order book quantity must not be negative: {level.quantity!r}"
                )
            if (
                level.order_count is not None
                and (
                    type(level.order_count) is not int
                    or level.order_count < 0
                )
            ):
                raise LocalOrderBookError(
                    "order_count must be a non-negative integer or None: "
                    f"{level.order_count!r}"
                )
            checked.append(level)
        return tuple(checked)

    @staticmethod
    def _apply(
        side: dict[Decimal, OrderBookLevel],
        levels: Sequence[OrderBookLevel],
    ) -> None:
        for level in levels:
            if level.quantity == 0:
                side.pop(level.price, None)
            else:
                side[level.price] = level

    def _trim(
        self,
        side: dict[Decimal, OrderBookLevel],
        *,
        reverse: bool,
    ) -> dict[Decimal, OrderBookLevel]:
        prices = sorted(side, reverse=reverse)[: self._retained_depth]
        return {price: side[price] for price in prices}


__all__ = ["LocalOrderBook"]
