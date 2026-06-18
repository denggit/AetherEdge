from __future__ import annotations

from src.platform.account import create_account_client, create_account_event_stream
from src.platform.data import create_market_data_feed
from src.platform.execution import create_execution_client
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.runtime.config import RuntimeConfig
from src.platform.runtime.context import RuntimeContext
from src.platform.state import SqliteStateStore


def build_runtime_context(
    config: RuntimeConfig,
    *,
    exchange_config: ExchangeConfig | None = None,
) -> RuntimeContext:
    """Factory that wires the platform ports together.

    Tests can bypass this and inject a RuntimeContext directly into
    PlatformRuntime. Production code should prefer this factory.
    """

    exchange = config.exchange if isinstance(config.exchange, ExchangeName) else ExchangeName(config.exchange)
    resolved_exchange_config = exchange_config or ExchangeConfig.from_env(exchange)
    data = create_market_data_feed(exchange, symbol=config.symbol, config=resolved_exchange_config)
    execution = create_execution_client(exchange, symbol=config.symbol, config=resolved_exchange_config)
    account = create_account_client(exchange, symbol=config.symbol, config=resolved_exchange_config)
    state_store = SqliteStateStore(config.state_db_path)
    event_stream = None
    if config.enable_private_event_stream:
        event_stream = create_account_event_stream(
            exchange,
            symbol=config.symbol,
            config=resolved_exchange_config,
            reconnect=config.reconnect_private_stream,
            reconnect_delay_seconds=config.reconnect_delay_seconds,
            max_reconnects=config.max_reconnects,
        )
    return RuntimeContext(
        data=data,
        execution=execution,
        account=account,
        state_store=state_store,
        account_event_stream=event_stream,
    )
