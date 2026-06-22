from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from src.platform.account.ports import AccountClient
from src.platform.execution.ports import ExecutionClient
from src.platform.state.ports import StateStore


@dataclass(frozen=True)
class SyncExchangeContext:
    account: AccountClient
    execution: ExecutionClient
    state_store: StateStore


@dataclass(frozen=True)
class KnownOrderRef:
    """Cleaned reference to a known exchange order.

    At least one of *order_id* or *client_order_id* must be non‑None after
    cleaning; the caller should skip refs where both are ``None``.
    """

    order_id: str | None = None
    client_order_id: str | None = None


@dataclass(frozen=True)
class SyncResult:
    exchange: str
    sync_type: str
    request_count: int
    duration_ms: int
    success: bool
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
