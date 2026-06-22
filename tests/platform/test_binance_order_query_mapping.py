"""Tests for Binance order query parameter validation.

Covers:
- Binance orderId must be numeric
- Binance origClientOrderId is client ID
- resolve_query_params behavior for Binance
"""

from __future__ import annotations

import pytest

from src.order_management.reconciliation.validation import (
    is_valid_exchange_order_id,
    resolve_query_params,
)
from src.platform.exchanges.models import ExchangeName


def test_binance_numeric_order_id_valid():
    assert is_valid_exchange_order_id(ExchangeName.BINANCE, "987654321") is True


def test_binance_non_numeric_order_id_invalid():
    assert is_valid_exchange_order_id(ExchangeName.BINANCE, "binance-order-1") is False
    assert is_valid_exchange_order_id(ExchangeName.BINANCE, "binance-1") is False
    assert is_valid_exchange_order_id(ExchangeName.BINANCE, "binance-stop-1") is False


def test_binance_order_id_none_invalid():
    assert is_valid_exchange_order_id(ExchangeName.BINANCE, None) is False


def test_binance_resolve_numeric_with_client():
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "123456789", "client123")
    assert oid == "123456789"
    assert cid == "client123"


def test_binance_resolve_fake_with_client():
    """Binance: fake orderId -> use only origClientOrderId."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "binance-order-1", "AEBNOLabc123")
    assert oid is None
    assert cid == "AEBNOLabc123"


def test_binance_resolve_fake_no_client():
    """Binance: fake orderId + no client -> both None, skip."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "binance-stop-1", None)
    assert oid is None
    assert cid is None


def test_binance_resolve_zero_length_algo_id():
    """Binance algoId with valid client -> use client."""
    oid, cid = resolve_query_params(ExchangeName.BINANCE, "binance-1", "AEBNSPabc")
    assert oid is None
    assert cid == "AEBNSPabc"


# ── Real adapter query param mapping tests (AE-V9C-LIVE-BOOTSTRAP-013) ──


from typing import Any, Mapping

import pytest

from src.platform.exchanges.binance.client import BinanceExchangeClient
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.models import (
    ExchangeConfig,
    OrderQuery,
    StopOrderQuery,
)
from src.platform.exchanges.ports import HttpClient


class FakeBinanceHttpClient(HttpClient):
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
        if "algoOrder" in url:
            return {"algoId": "99999", "clientAlgoId": "AE-test", "algoStatus": "WORKING", "symbol": "ETHUSDT", "side": "BUY", "triggerPrice": "3000"}
        return {"orderId": "99999", "clientOrderId": "AE-test", "status": "FILLED", "symbol": "ETHUSDT", "side": "BUY", "type": "LIMIT", "price": "3000", "origQty": "1", "executedQty": "1"}


def _make_binance_client(http: FakeBinanceHttpClient) -> BinanceExchangeClient:
    return BinanceExchangeClient(
        config=ExchangeConfig(
            api_key="test-key",
            api_secret="test-secret",
            sandbox=True,
        ),
        http_client=http,
    )


@pytest.mark.asyncio
async def test_binance_fetch_order_status_uses_orig_client_order_id_when_orderid_fake():
    """Fake orderId + valid origClientOrderId → adapter sends origClientOrderId only."""
    http = FakeBinanceHttpClient()
    client = _make_binance_client(http)

    query = OrderQuery(
        symbol="ETH-USDT-PERP",
        order_id="binance-order-1",
        client_order_id="AEBIOL210BAA0585046BF3",
    )

    order = await client.fetch_order_status(query)
    assert order is not None
    assert len(http.requests) == 1
    req = http.requests[0]

    assert "/fapi/v1/order" in req["url"]
    assert "orderId" not in req["params"], (
        f"orderId should NOT be sent for fake order_id, got params={req['params']}"
    )
    assert req["params"].get("origClientOrderId") == "AEBIOL210BAA0585046BF3", (
        f"origClientOrderId should be sent, got params={req['params']}"
    )


@pytest.mark.asyncio
async def test_binance_fetch_stop_status_uses_client_algo_id_when_algoid_fake():
    """Fake algoId + valid clientAlgoId → adapter sends clientAlgoId only."""
    http = FakeBinanceHttpClient()
    client = _make_binance_client(http)

    query = StopOrderQuery(
        symbol="ETH-USDT-PERP",
        stop_order_id="binance-stop-1",
        client_order_id="AEBISL210BAA0585046BF3",
    )

    order = await client.fetch_stop_order_status(query)
    assert order is not None
    assert len(http.requests) == 1
    req = http.requests[0]

    assert "/fapi/v1/algoOrder" in req["url"]
    assert "algoId" not in req["params"], (
        f"algoId should NOT be sent for fake stop_order_id, got params={req['params']}"
    )
    assert req["params"].get("clientAlgoId") == "AEBISL210BAA0585046BF3", (
        f"clientAlgoId should be sent, got params={req['params']}"
    )


@pytest.mark.asyncio
async def test_binance_fetch_order_status_raises_when_both_ids_invalid():
    """Both order_id and client_order_id invalid → ExchangeConfigError."""
    http = FakeBinanceHttpClient()
    client = _make_binance_client(http)

    query = OrderQuery(
        symbol="ETH-USDT-PERP",
        order_id="binance-order-1",
        client_order_id=None,
    )

    with pytest.raises(ExchangeConfigError, match="valid orderId or origClientOrderId"):
        await client.fetch_order_status(query)

    # No HTTP call should have been made
    assert len(http.requests) == 0


@pytest.mark.asyncio
async def test_binance_fetch_order_status_uses_orderid_when_valid_numeric():
    """Valid numeric orderId → use orderId directly."""
    http = FakeBinanceHttpClient()
    client = _make_binance_client(http)

    query = OrderQuery(
        symbol="ETH-USDT-PERP",
        order_id="987654321",
        client_order_id="AEBIOLabc123",
    )

    order = await client.fetch_order_status(query)
    assert order is not None
    assert len(http.requests) == 1
    req = http.requests[0]

    assert req["params"].get("orderId") == "987654321", (
        f"orderId should be sent, got params={req['params']}"
    )
    assert req["params"].get("origClientOrderId") == "AEBIOLabc123", (
        f"origClientOrderId should also be sent, got params={req['params']}"
    )
