from decimal import Decimal

import pytest

from src.platform import ExchangeName, create_account_client, create_execution_client, create_market_data_feed, get_market_profile
from src.platform.exchanges.symbols import to_exchange_symbol
from src.platform.markets import MarketProfile, register_market_profile


class FakeClient:
    exchange = ExchangeName.OKX

    async def fetch_balance(self, asset="USDT"):
        raise AssertionError("not used")

    async def fetch_positions(self, symbol=None):
        self.last_symbol = symbol
        return []


def test_default_market_profile_is_eth_usdt_perp():
    profile = get_market_profile()

    assert profile.symbol == "ETH-USDT-PERP"
    assert profile.base_asset == "ETH"
    assert profile.raw_symbol(ExchangeName.OKX) == "ETH-USDT-SWAP"
    assert profile.raw_symbol(ExchangeName.BINANCE) == "ETHUSDT"
    assert profile.contract_value(ExchangeName.OKX) == Decimal("0.1")


def test_can_register_future_market_without_changing_exchange_code():
    register_market_profile(
        MarketProfile(
            symbol="SOL-USDT-PERP",
            base_asset="SOL",
            quote_asset="USDT",
            exchange_symbols={ExchangeName.OKX: "SOL-USDT-SWAP", ExchangeName.BINANCE: "SOLUSDT"},
            contract_value_by_exchange={ExchangeName.OKX: Decimal("1"), ExchangeName.BINANCE: Decimal("1")},
        )
    )

    assert to_exchange_symbol(ExchangeName.OKX, "SOL-USDT-PERP") == "SOL-USDT-SWAP"
    assert to_exchange_symbol(ExchangeName.BINANCE, "SOL-USDT-PERP") == "SOLUSDT"


def test_platform_clients_are_bound_to_configured_market_symbol():
    data = create_market_data_feed("okx", symbol="ETH-USDT-PERP", exchange_client=FakeClient(), enable_trade_stream=False, enable_order_book_stream=False)
    execution = create_execution_client("okx", symbol="ETH-USDT-PERP", exchange_client=FakeClient(), validate_orders=False)
    account = create_account_client("okx", symbol="ETH-USDT-PERP", exchange_client=FakeClient())

    assert data.symbol == "ETH-USDT-PERP"
    assert execution.symbol == "ETH-USDT-PERP"
    assert account.symbol == "ETH-USDT-PERP"
    assert data.market_profile.contract_value(ExchangeName.OKX) == Decimal("0.1")


def test_account_client_rejects_position_query_for_unbound_symbol():
    account = create_account_client("okx", symbol="ETH-USDT-PERP", exchange_client=FakeClient())

    with pytest.raises(ValueError):
        import asyncio

        asyncio.run(account.fetch_positions("SOL-USDT-PERP"))
