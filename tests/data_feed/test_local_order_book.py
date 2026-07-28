from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform.data.models import OrderBookLevel
from src.platform.data.order_book import LocalOrderBook, LocalOrderBookError


def _level(
    price: str,
    quantity: str,
    order_count: int | None = None,
) -> OrderBookLevel:
    return OrderBookLevel(
        price=Decimal(price),
        quantity=Decimal(quantity),
        order_count=order_count,
    )


def test_local_order_book_sorts_updates_and_deletes_levels() -> None:
    book = LocalOrderBook()
    book.reset(
        bids=[_level("100", "1", 2), _level("101", "2", 3)],
        asks=[_level("103", "1", 1), _level("102", "2", 2)],
    )
    book.apply_updates(
        bids=[_level("101", "5", 7), _level("100", "0", 0)],
        asks=[_level("101.5", "3", 4)],
    )

    bids, asks = book.snapshot(depth=400)

    assert bids == (_level("101", "5", 7),)
    assert asks == (
        _level("101.5", "3", 4),
        _level("102", "2", 2),
        _level("103", "1", 1),
    )


def test_local_order_book_ignores_deletion_of_missing_price() -> None:
    book = LocalOrderBook()
    book.reset(bids=[_level("100", "1")], asks=[])
    book.apply_updates(bids=[_level("99", "0", 0)], asks=[])
    assert book.snapshot(depth=400)[0] == (_level("100", "1"),)


def test_local_order_book_trims_both_sides_to_400() -> None:
    book = LocalOrderBook()
    book.reset(
        bids=[_level(str(index), "1") for index in range(500)],
        asks=[_level(str(index), "1") for index in range(500)],
    )
    bids, asks = book.snapshot(depth=400)
    assert len(bids) == 400
    assert len(asks) == 400
    assert bids[0].price == Decimal("499")
    assert asks[0].price == Decimal("0")


def test_local_order_book_rejects_update_atomically() -> None:
    book = LocalOrderBook()
    book.reset(
        bids=[_level("100", "1")],
        asks=[_level("101", "1")],
    )
    before = book.snapshot(depth=400)
    with pytest.raises(LocalOrderBookError):
        book.apply_updates(
            bids=[_level("100", "2")],
            asks=[OrderBookLevel(price=Decimal("101"), quantity="bad")],  # type: ignore[arg-type]
        )
    assert book.snapshot(depth=400) == before


def test_order_book_level_two_argument_constructor_is_compatible() -> None:
    level = OrderBookLevel(Decimal("1.1"), Decimal("2.2"))
    assert level.order_count is None
