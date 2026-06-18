from __future__ import annotations

from dataclasses import dataclass

from src.platform.account.ports import AccountClient
from src.platform.account.stream import AccountEventStream
from src.platform.data.ports import MarketDataFeed
from src.platform.execution.ports import ExecutionClient
from src.platform.state.ports import StateStore


@dataclass(frozen=True)
class RuntimeContext:
    """Dependency-injected platform components for one exchange + one market."""

    data: MarketDataFeed
    execution: ExecutionClient
    account: AccountClient
    state_store: StateStore
    account_event_stream: AccountEventStream | None = None
