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
