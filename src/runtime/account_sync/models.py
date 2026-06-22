from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from src.platform.account.ports import AccountClient
from src.platform.execution.ports import ExecutionClient
from src.platform.state.ports import StateStore


class KnownOrderRefStatus(str, Enum):
    """Lifecycle status for a known order reference.

    ACTIVE: ref is valid and should be queried.
    INVALID_FORMAT: exchange order ID is non-numeric or matches fake patterns.
    NOT_FOUND: exchange returned "order not found" for this ref.
    STALE_RECONCILED: ref was cleaned up by startup reconciliation.
    """

    ACTIVE = "active"
    INVALID_FORMAT = "invalid_format"
    NOT_FOUND = "not_found"
    STALE_RECONCILED = "stale_reconciled"


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

    *status* tracks the lifecycle of this ref so stale or malformed refs
    are not repeatedly queried against the exchange.
    """

    order_id: str | None = None
    client_order_id: str | None = None
    status: KnownOrderRefStatus = KnownOrderRefStatus.ACTIVE


@dataclass(frozen=True)
class SyncResult:
    exchange: str
    sync_type: str
    request_count: int
    duration_ms: int
    success: bool
    error: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
