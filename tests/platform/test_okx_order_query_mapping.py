"""Tests for order ID validation and exchange-specific query mapping.

Covers:
- OKX ordId / algoId must be numeric
- OKX clOrdId / algoClOrdId are client IDs
- Binance orderId must be numeric
- Binance origClientOrderId is client ID
- resolve_query_params behavior
"""

from __future__ import annotations

import pytest

from src.order_management.reconciliation.validation import (
    is_valid_exchange_order_id,
    is_valid_client_order_id,
    is_fake_order_id,
    resolve_query_params,
)
from src.platform.exchanges.models import ExchangeName


# ── Fake ID detection ──

@pytest.mark.parametrize(
    "value,expected",
    [
        ("okx-order-1", True),
        ("okx-order-abcde", True),
        ("okx-stop-1", True),
        ("okx-stop-12345", True),
        ("binance-order-1", True),
        ("binance-stop-1", True),
        ("okx-1", True),
        ("binance-1", True),
        ("test-abc", True),
        ("Test-XYZ", True),  # case insensitive
        ("fake-123", True),
        ("dummy-x", True),
        ("DUMMY-Y", True),
        # Valid IDs should NOT match
        ("1234567890", False),
        ("AEOKOLabc123def", False),
        (None, False),
        ("", False),
        ("none", True),  # sentinel check
        ("null", True),
        ("N/A", True),
    ],
)
def test_is_fake_order_id(value, expected):
    assert is_fake_order_id(value) == expected


# ── Exchange order ID validation ──

@pytest.mark.parametrize("exchange", [ExchangeName.OKX, ExchangeName.BINANCE])
def test_valid_numeric_order_id(exchange):
    assert is_valid_exchange_order_id(exchange, "1234567890") is True


@pytest.mark.parametrize("exchange", [ExchangeName.OKX, ExchangeName.BINANCE])
def test_invalid_non_numeric_order_id(exchange):
    assert is_valid_exchange_order_id(exchange, "okx-order-1") is False
    assert is_valid_exchange_order_id(exchange, "binance-order-1") is False
    assert is_valid_exchange_order_id(exchange, "abc") is False


@pytest.mark.parametrize("exchange", [ExchangeName.OKX, ExchangeName.BINANCE])
def test_invalid_empty_or_none_order_id(exchange):
    assert is_valid_exchange_order_id(exchange, None) is False
    assert is_valid_exchange_order_id(exchange, "") is False
    assert is_valid_exchange_order_id(exchange, "none") is False
    assert is_valid_exchange_order_id(exchange, "null") is False


# ── Client order ID validation ──

def test_valid_client_order_id():
    assert is_valid_client_order_id("AEOKOLabc123def456") is True
    assert is_valid_client_order_id("AEBNOLabc123") is True


def test_invalid_client_order_id():
    assert is_valid_client_order_id(None) is False
    assert is_valid_client_order_id("") is False
    assert is_valid_client_order_id("none") is False
    assert is_valid_client_order_id("null") is False


# ── resolve_query_params ──

def test_resolve_with_valid_exchange_order_id():
    """Valid exchange order ID — use it with client_order_id as additional param."""
    oid, cid = resolve_query_params(ExchangeName.OKX, "1234567890", "AEOKOLabc123")
    assert oid == "1234567890"
    assert cid == "AEOKOLabc123"


def test_resolve_fake_exchange_order_id_with_valid_client():
    """Fake exchange order ID + valid client_order_id — use only client_order_id."""
    oid, cid = resolve_query_params(ExchangeName.OKX, "okx-order-1", "AEOKOLabc123")
    assert oid is None
    assert cid == "AEOKOLabc123"


def test_resolve_fake_exchange_order_id_without_client():
    """Fake exchange order ID + no valid client — both None, skip query."""
    oid, cid = resolve_query_params(ExchangeName.OKX, "okx-order-1", None)
    assert oid is None
    assert cid is None


def test_resolve_none_order_id_with_client():
    """No exchange order ID but valid client_order_id — use client."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, None, "AEBNOLabc123")
    assert oid is None
    assert cid == "AEBNOLabc123"


def test_resolve_both_invalid():
    """Both IDs invalid — skip query."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "binance-1", None)
    assert oid is None
    assert cid is None


def test_resolve_binance_fake_with_valid_client():
    """Binance fake orderId + valid origClientOrderId — use only client."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "binance-order-1", "AEBNOLabc123")
    assert oid is None
    assert cid == "AEBNOLabc123"


def test_resolve_binance_numeric_order_id():
    """Binance numeric orderId + valid client."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "987654321", "AEBNOLabc123")
    assert oid == "987654321"
    assert cid == "AEBNOLabc123"


def test_okx_numeric_stop_order_id():
    """OKX algoId must be numeric."""
    assert is_valid_exchange_order_id(ExchangeName.OKX, "1234567890") is True
    assert is_valid_exchange_order_id(ExchangeName.OKX, "okx-stop-1") is False


# ── Real adapter query param mapping tests (AE-V9C-LIVE-BOOTSTRAP-013) ──


from typing import Any, Mapping

import pytest

from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.models import (
    ExchangeConfig,
    OrderQuery,
    StopOrderQuery,
)
from src.platform.exchanges.okx.client import OkxExchangeClient
from src.platform.exchanges.ports import HttpClient


class FakeHttpClient(HttpClient):
    """Captures REST params without making real HTTP calls."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_seconds: float = 30,
    ) -> Any:
        self.requests.append({
            "method": method,
            "url": url,
            "params": dict(params or {}),
            "json_body": dict(json_body or {}),
            "headers": dict(headers or {}),
        })
        # Return a fake order response
        if "/api/v5/trade/order-algo" in url:
            return {"code": "0", "data": [{"algoId": "99999", "algoClOrdId": "AE-test", "state": "live", "instId": "ETH-USDT-SWAP", "sz": "1"}]}
        return {"code": "0", "data": [{"ordId": "99999", "clOrdId": "AE-test", "state": "filled", "instId": "ETH-USDT-SWAP", "sz": "1", "px": "3000", "accFillSz": "1", "side": "buy", "ordType": "limit"}]}


def _make_okx_client(http: FakeHttpClient) -> OkxExchangeClient:
    return OkxExchangeClient(
        config=ExchangeConfig(
            api_key="test-key",
            api_secret="test-secret",
            passphrase="test-pass",
            sandbox=True,
        ),
        http_client=http,
    )


@pytest.mark.asyncio
async def test_okx_fetch_order_status_uses_clordid_when_ordid_fake():
    """Fake ordId + valid clOrdId → adapter sends clOrdId only, no ordId."""
    http = FakeHttpClient()
    client = _make_okx_client(http)

    query = OrderQuery(
        symbol="ETH-USDT-PERP",
        order_id="okx-order-1",
        client_order_id="AEOKOL210BAA0585046BF3",
    )

    order = await client.fetch_order_status(query)
    assert order is not None
    assert len(http.requests) == 1
    req = http.requests[0]

    assert "/api/v5/trade/order" in req["url"]
    assert "ordId" not in req["params"], (
        f"ordId should NOT be sent for fake order_id, got params={req['params']}"
    )
    assert req["params"].get("clOrdId") == "AEOKOL210BAA0585046BF3", (
        f"clOrdId should be sent, got params={req['params']}"
    )


@pytest.mark.asyncio
async def test_okx_fetch_stop_status_uses_algoclordid_when_algoid_fake():
    """Fake algoId + valid algoClOrdId → adapter sends algoClOrdId only."""
    http = FakeHttpClient()
    client = _make_okx_client(http)

    query = StopOrderQuery(
        symbol="ETH-USDT-PERP",
        stop_order_id="okx-stop-1",
        client_order_id="AEOKSL210BAA0585046BF3",
    )

    order = await client.fetch_stop_order_status(query)
    assert order is not None
    assert len(http.requests) == 1
    req = http.requests[0]

    assert "/api/v5/trade/order-algo" in req["url"]
    assert "algoId" not in req["params"], (
        f"algoId should NOT be sent for fake stop_order_id, got params={req['params']}"
    )
    assert req["params"].get("algoClOrdId") == "AEOKSL210BAA0585046BF3", (
        f"algoClOrdId should be sent, got params={req['params']}"
    )


@pytest.mark.asyncio
async def test_okx_fetch_order_status_raises_when_both_ids_invalid():
    """Both order_id and client_order_id invalid → ExchangeConfigError."""
    http = FakeHttpClient()
    client = _make_okx_client(http)

    query = OrderQuery(
        symbol="ETH-USDT-PERP",
        order_id="okx-order-1",
        client_order_id=None,
    )

    with pytest.raises(ExchangeConfigError, match="valid ordId or clOrdId"):
        await client.fetch_order_status(query)

    # No HTTP call should have been made
    assert len(http.requests) == 0


@pytest.mark.asyncio
async def test_okx_fetch_order_status_uses_ordid_when_valid_numeric():
    """Valid numeric ordId → use ordId directly."""
    http = FakeHttpClient()
    client = _make_okx_client(http)

    query = OrderQuery(
        symbol="ETH-USDT-PERP",
        order_id="1234567890",
        client_order_id="AEOKOLabc123",
    )

    order = await client.fetch_order_status(query)
    assert order is not None
    assert len(http.requests) == 1
    req = http.requests[0]

    assert req["params"].get("ordId") == "1234567890", (
        f"ordId should be sent, got params={req['params']}"
    )
    assert req["params"].get("clOrdId") == "AEOKOLabc123", (
        f"clOrdId should also be sent as supplement, got params={req['params']}"
    )
