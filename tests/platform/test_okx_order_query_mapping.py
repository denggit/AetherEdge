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
