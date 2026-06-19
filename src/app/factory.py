from __future__ import annotations

from src.app.alerts import AsyncAlertDispatcher, EmailAlertSink, NoopAlertSink
from src.app.config import AppConfig
from src.app.context import AppContext
from src.platform import create_execution_client, create_market_data_feed
from src.platform.execution import MultiExchangeExecutionClient
from src.platform.state import SqliteStateStore
from src.planner import ExecutionPlanner
from src.strategy import load_strategy


def build_app_context(config: AppConfig) -> AppContext:
    data = create_market_data_feed(config.data_exchange, symbol=config.symbol)
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
