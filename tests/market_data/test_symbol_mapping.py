from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.storage import SqliteKlineStore
from src.market_data.models import TimeRange
from src.platform.data.models import MarketKline
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.symbols import (
    CANONICAL_ETH_USDT_PERP,
    OKX_ETH_USDT_SWAP,
    BINANCE_ETH_USDT_PERP,
    to_canonical_symbol,
    to_exchange_symbol,
)
from src.platform.markets import get_market_profile


class TestCanonicalRawSymbolMapping:
    """Verify that canonical ↔ raw symbol mapping works correctly across the
    platform and market_data layers."""

    def test_canonical_eth_usdt_perp_to_okx_swap(self):
        assert CANONICAL_ETH_USDT_PERP == "ETH-USDT-PERP"
        assert OKX_ETH_USDT_SWAP == "ETH-USDT-SWAP"

    def test_canonical_to_okx_via_to_exchange_symbol(self):
        raw = to_exchange_symbol(ExchangeName.OKX, "ETH-USDT-PERP")
        assert raw == "ETH-USDT-SWAP"

    def test_okx_raw_back_to_canonical(self):
        canonical = to_canonical_symbol(ExchangeName.OKX, "ETH-USDT-SWAP")
        assert canonical == "ETH-USDT-PERP"

    def test_canonical_to_binance_via_to_exchange_symbol(self):
        raw = to_exchange_symbol(ExchangeName.BINANCE, "ETH-USDT-PERP")
        assert raw == "ETHUSDT"

    def test_binance_raw_back_to_canonical(self):
        canonical = to_canonical_symbol(ExchangeName.BINANCE, "ETHUSDT")
        assert canonical == "ETH-USDT-PERP"

    def test_market_profile_raw_symbol(self):
        profile = get_market_profile("ETH-USDT-PERP")
        assert profile.raw_symbol(ExchangeName.OKX) == "ETH-USDT-SWAP"
        assert profile.raw_symbol(ExchangeName.BINANCE) == "ETHUSDT"
        assert profile.symbol == "ETH-USDT-PERP"

    def test_unknown_raw_symbol_raises(self):
        with pytest.raises(ValueError, match="Unsupported raw symbol mapping"):
            to_canonical_symbol(ExchangeName.OKX, "NO-SUCH-SYMBOL")

    def test_unknown_exchange_for_known_symbol_raises_in_raw_symbol(self):
        profile = get_market_profile("ETH-USDT-PERP")
        # The ExchangeName enum only accepts valid members ("okx", "binance").
        # Passing a raw string that isn't a valid ExchangeName raises ValueError
        # from the enum constructor. This is expected behaviour.
        with pytest.raises(ValueError):
            profile.raw_symbol("bybit")


class TestKlineStorePreservesCanonicalSymbol:
    """Verify that the KlineStore saves and loads with the canonical symbol,
    regardless of what raw_symbol was used in the API request."""

    def test_save_and_load_preserves_canonical_symbol(self, tmp_path):
        store = SqliteKlineStore(tmp_path / "market.sqlite3")

        kline = MarketKline(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",       # canonical
            raw_symbol="ETH-USDT-SWAP",   # raw (what OKX API uses)
            interval="4h",
            open_time_ms=0,
            close_time_ms=4 * 60 * 60_000 - 1,
            open=Decimal("3000"),
            high=Decimal("3100"),
            low=Decimal("2900"),
            close=Decimal("3050"),
            volume=Decimal("100"),
            is_closed=True,
        )
        store.save([kline])

        loaded = store.load(
            symbol="ETH-USDT-PERP",
            interval="4h",
            time_range=TimeRange(0, 4 * 60 * 60_000),
        )
        assert len(loaded) == 1
        assert loaded[0].symbol == "ETH-USDT-PERP"
        assert loaded[0].raw_symbol == "ETH-USDT-SWAP"
        assert loaded[0].exchange == ExchangeName.OKX

    def test_load_with_wrong_symbol_returns_empty(self, tmp_path):
        store = SqliteKlineStore(tmp_path / "market.sqlite3")
        kline = MarketKline(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            interval="4h",
            open_time_ms=0,
            close_time_ms=4 * 60 * 60_000 - 1,
            open=Decimal("3000"), high=Decimal("3100"), low=Decimal("2900"),
            close=Decimal("3050"), volume=Decimal("100"), is_closed=True,
        )
        store.save([kline])

        # Loading with the raw symbol should return nothing.
        loaded = store.load(
            symbol="ETH-USDT-SWAP",
            interval="4h",
            time_range=TimeRange(0, 4 * 60 * 60_000),
        )
        assert len(loaded) == 0


class TestBinanceSymbolMapping:
    def test_binance_eth_usdt_perp_symbol(self):
        assert BINANCE_ETH_USDT_PERP == "ETHUSDT"

    def test_binance_raw_to_canonical_roundtrip(self):
        profile = get_market_profile("ETH-USDT-PERP")
        raw_bn = profile.raw_symbol(ExchangeName.BINANCE)
        canonical = to_canonical_symbol(ExchangeName.BINANCE, raw_bn)
        assert canonical == "ETH-USDT-PERP"
