from __future__ import annotations

from src.platform.account.ports import AccountClient
from src.platform.account.service import ExchangeAccountService
from src.platform.exchanges.factory import create_exchange_client
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.exchanges.ports import ExchangeAccountClient, HttpClient


def create_account_client(
    exchange: ExchangeName | str,
    config: ExchangeConfig | None = None,
    *,
    exchange_client: ExchangeAccountClient | None = None,
    http_client: HttpClient | None = None,
) -> AccountClient:
    client = exchange_client or create_exchange_client(exchange, config or ExchangeConfig(), http_client=http_client)
    return ExchangeAccountService(client)
