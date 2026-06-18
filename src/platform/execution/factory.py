from __future__ import annotations

from src.platform.execution.ports import ExecutionClient
from src.platform.execution.service import ExchangeExecutionService
from src.platform.exchanges.factory import create_exchange_client
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.exchanges.ports import ExchangeExecutionClient, HttpClient


def create_execution_client(
    exchange: ExchangeName | str,
    config: ExchangeConfig | None = None,
    *,
    exchange_client: ExchangeExecutionClient | None = None,
    http_client: HttpClient | None = None,
) -> ExecutionClient:
    client = exchange_client or create_exchange_client(exchange, config or ExchangeConfig(), http_client=http_client)
    return ExchangeExecutionService(client)
