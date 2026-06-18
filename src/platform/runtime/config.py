from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.platform.exchanges.models import ExchangeName
from src.platform.markets.registry import DEFAULT_MARKET_SYMBOL


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime wiring config.

    This config describes which platform interfaces to compose. It does not
    contain strategy parameters or trading rules.
    """

    exchange: ExchangeName
    symbol: str = DEFAULT_MARKET_SYMBOL
    asset: str = "USDT"
    state_db_path: str | Path = "data/state/aether_state.sqlite3"
    enable_private_event_stream: bool = True
    save_startup_snapshot: bool = True
    reconnect_private_stream: bool = True
    reconnect_delay_seconds: float = 1.0
    max_reconnects: int | None = None
