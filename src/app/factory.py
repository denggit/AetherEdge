from __future__ import annotations

import os

from src.app.alerts import AsyncAlertDispatcher, EmailAlertSink, NoopAlertSink
from src.app.config import AppConfig
from src.app.context import AppContext
from src.platform import create_execution_client, create_market_data_feed
from src.platform.execution import MultiExchangeExecutionClient
from src.platform.exchanges.models import ExchangeConfig
from src.platform.state import SqliteStateStore
from src.planner import ExecutionPlanner
from src.strategy import load_strategy


def build_app_context(config: AppConfig) -> AppContext:
    data_exchange_config = ExchangeConfig.from_env(config.data_exchange)
    platform_cache_db = os.getenv("AETHER_PLATFORM_MARKET_DATA_DB", "data/cache/market_data.sqlite3")
    data = create_market_data_feed(
        config.data_exchange,
        symbol=config.symbol,
        config=data_exchange_config,
        sqlite_path=platform_cache_db,
    )
    execution_clients = [create_execution_client(exchange, symbol=config.symbol) for exchange in config.exchanges]
    execution = execution_clients[0] if len(execution_clients) == 1 else MultiExchangeExecutionClient(execution_clients)
    sink = EmailAlertSink() if config.enable_email_alerts else NoopAlertSink()
    return AppContext(
        data=data,
        execution=execution,
        state_store=SqliteStateStore(config.state_db_path),
        strategy=load_strategy(config.strategy),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(sink=sink, maxsize=config.alert_queue_maxsize),
    )
