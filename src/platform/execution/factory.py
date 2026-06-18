from __future__ import annotations

from src.platform.execution.ports import ExecutionClient
from src.platform.execution.risk import ExecutionRiskLimits
from src.platform.execution.service import ExchangeExecutionService
from src.platform.exchanges.factory import create_exchange_client
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.exchanges.ports import ExchangeExecutionClient, HttpClient
from src.platform.markets import MarketProfile, get_market_profile


def create_execution_client(
    exchange: ExchangeName | str,
    config: ExchangeConfig | None = None,
    *,
    symbol: str | None = None,
    market_profile: MarketProfile | None = None,
    exchange_client: ExchangeExecutionClient | None = None,
    http_client: HttpClient | None = None,
    risk_limits: ExecutionRiskLimits | None = None,
    validate_orders: bool = True,
) -> ExecutionClient:
    profile = market_profile or get_market_profile(symbol)
    cfg = config or ExchangeConfig.from_env(exchange)
    client = exchange_client or create_exchange_client(exchange, cfg, http_client=http_client)
    return ExchangeExecutionService(
        client,
        symbol=profile.symbol,
        market_profile=profile,
        risk_limits=risk_limits,
        validate_orders=validate_orders,
        sandbox=cfg.sandbox,
        live_trading_enabled=cfg.live_trading_enabled,
    )
