from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.platform.data.rest.okx import (
    OkxFullOrderBookError,
    OkxFullOrderBookRestClient,
)


class _Requester:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.calls = []

    async def request(self, method, path, *, params=None):
        self.calls.append((method, path, params))
        return self.payload


def test_full_order_book_rest_maps_depth_order_count_and_timestamp() -> None:
    requester = _Requester(
        {
            "code": "0",
            "msg": "",
            "data": [
                {
                    "bids": [["99", "2", "3"], ["100", "1", "4"]],
                    "asks": [["102", "5", "6"], ["101", "4", "7"]],
                    "ts": "1234",
                }
            ],
        }
    )
    client = OkxFullOrderBookRestClient(requester=requester)
    event = asyncio.run(
        client.fetch_full_order_book(
            symbol="ETH-USDT-PERP",
            depth=5000,
        )
    )

    assert requester.calls == [
        (
            "GET",
            "/api/v5/market/books-full",
            {"instId": "ETH-USDT-SWAP", "sz": 5000},
        )
    ]
    assert [item.price for item in event.bids] == [
        Decimal("100"),
        Decimal("99"),
    ]
    assert [item.price for item in event.asks] == [
        Decimal("101"),
        Decimal("102"),
    ]
    assert event.asks[0].order_count == 7
    assert event.event_time_ms == 1234
    assert event.requested_depth == 5000
    assert "bids" not in event.raw


@pytest.mark.parametrize("depth", [0, 5001])
def test_full_order_book_rest_rejects_invalid_depth(depth: int) -> None:
    client = OkxFullOrderBookRestClient(requester=_Requester({}))
    with pytest.raises(ValueError):
        asyncio.run(
            client.fetch_full_order_book(
                symbol="ETH-USDT-PERP",
                depth=depth,
            )
        )


def test_full_order_book_rest_rejects_error_and_malformed_level() -> None:
    error_client = OkxFullOrderBookRestClient(
        requester=_Requester({"code": "50011", "msg": "limited"})
    )
    with pytest.raises(OkxFullOrderBookError, match="endpoint="):
        asyncio.run(
            error_client.fetch_full_order_book(
                symbol="ETH-USDT-PERP"
            )
        )

    malformed_client = OkxFullOrderBookRestClient(
        requester=_Requester(
            {
                "code": "0",
                "data": [
                    {"bids": [["1", "2"]], "asks": [], "ts": "1"}
                ],
            }
        )
    )
    with pytest.raises(OkxFullOrderBookError):
        asyncio.run(
            malformed_client.fetch_full_order_book(
                symbol="ETH-USDT-PERP"
            )
        )
