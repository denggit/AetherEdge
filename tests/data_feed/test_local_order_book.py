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


def test_bid_trimmed_from_internal_state_does_not_revive() -> None:
    book = LocalOrderBook(max_depth=3)
    book.reset(
        bids=[
            _level("100", "1"),
            _level("99", "1"),
            _level("98", "1"),
        ],
        asks=[],
    )

    book.apply_updates(bids=[_level("101", "1")], asks=[])
    assert book.snapshot(depth=3)[0] == (
        _level("101", "1"),
        _level("100", "1"),
        _level("99", "1"),
    )

    book.apply_updates(bids=[_level("101", "0")], asks=[])
    assert book.snapshot(depth=3)[0] == (
        _level("100", "1"),
        _level("99", "1"),
    )


def test_ask_trimmed_from_internal_state_does_not_revive() -> None:
    book = LocalOrderBook(max_depth=3)
    book.reset(
        bids=[],
        asks=[
            _level("101", "1"),
            _level("102", "1"),
            _level("103", "1"),
        ],
    )

    book.apply_updates(bids=[], asks=[_level("100", "1")])
    assert book.snapshot(depth=3)[1] == (
        _level("100", "1"),
        _level("101", "1"),
        _level("102", "1"),
    )

    book.apply_updates(bids=[], asks=[_level("100", "0")])
    assert book.snapshot(depth=3)[1] == (
        _level("101", "1"),
        _level("102", "1"),
    )


def test_continuous_boundary_updates_match_depth_bounded_reference() -> None:
    book = LocalOrderBook(max_depth=3)
    reference_bids = {
        Decimal("100"): _level("100", "1"),
        Decimal("99"): _level("99", "1"),
        Decimal("98"): _level("98", "1"),
    }
    reference_asks = {
        Decimal("101"): _level("101", "1"),
        Decimal("102"): _level("102", "1"),
        Decimal("103"): _level("103", "1"),
    }
    book.reset(
        bids=list(reference_bids.values()),
        asks=list(reference_asks.values()),
    )

    updates = (
        ([_level("101", "2")], [_level("100", "2")]),
        ([_level("101", "0")], [_level("100", "0")]),
        ([_level("97", "3")], [_level("104", "3")]),
        ([_level("99", "4", 7)], [_level("102", "4", 8)]),
        ([_level("98", "0")], [_level("103", "0")]),
    )
    for bids, asks in updates:
        book.apply_updates(bids=bids, asks=asks)
        _apply_reference(reference_bids, bids, reverse=True, depth=3)
        _apply_reference(reference_asks, asks, reverse=False, depth=3)
        actual_bids, actual_asks = book.snapshot(depth=3)
        assert actual_bids == tuple(
            reference_bids[price]
            for price in sorted(reference_bids, reverse=True)
        )
        assert actual_asks == tuple(
            reference_asks[price]
            for price in sorted(reference_asks)
        )


def _apply_reference(
    side: dict[Decimal, OrderBookLevel],
    updates: list[OrderBookLevel],
    *,
    reverse: bool,
    depth: int,
) -> None:
    for level in updates:
        if level.quantity == 0:
            side.pop(level.price, None)
        else:
            side[level.price] = level
    retained = sorted(side, reverse=reverse)[:depth]
    retained_values = {price: side[price] for price in retained}
    side.clear()
    side.update(retained_values)


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
