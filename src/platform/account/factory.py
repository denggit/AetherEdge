from __future__ import annotations

from src.platform.account.ports import AccountClient
from src.platform.account.service import ExchangeAccountService
from src.platform.exchanges.factory import create_exchange_client
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.exchanges.ports import ExchangeAccountClient, HttpClient
from src.platform.markets import MarketProfile, get_market_profile


def create_account_client(
    exchange: ExchangeName | str,
    config: ExchangeConfig | None = None,
    *,
    symbol: str | None = None,
    market_profile: MarketProfile | None = None,
    exchange_client: ExchangeAccountClient | None = None,
    http_client: HttpClient | None = None,
) -> AccountClient:
    profile = market_profile or get_market_profile(symbol)
    client = exchange_client or create_exchange_client(exchange, config or ExchangeConfig.from_env(exchange), http_client=http_client)
    return ExchangeAccountService(client, symbol=profile.symbol, market_profile=profile)
