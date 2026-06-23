from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName


class PositionPlanStatus(str, Enum):
    ACTIVE = "active"
    CLOSED = "closed"
    MASTER_ACTIVE_PLAN_UNKNOWN = "master_active_plan_unknown"
    MANUAL_REQUIRED = "manual_required"
    MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED = "master_closed_follower_close_required"


class LegRole(str, Enum):
    MASTER = "master"
    FOLLOWER = "follower"


class LegSyncStatus(str, Enum):
    PLANNED = "planned"
    OPEN = "open"
    SYNCED = "synced"
    MISSING = "missing"
    UNDERFILLED = "underfilled"
    OVERFILLED = "overfilled"
    REVERSE_POSITION = "reverse_position"
    PLAN_UNKNOWN = "plan_unknown"
    TOPUP_PENDING = "topup_pending"
    TOPUP_SUBMITTED = "topup_submitted"
    TOPUP_FAILED = "topup_failed"
    MANUAL_REQUIRED = "manual_required"
    CLOSED = "closed"
    FOLLOWER_ENTRY_FAILED = "follower_entry_failed"
    FOLLOWER_CLOSE_FAILED = "follower_close_failed"
    STALE_RECONCILED = "stale_reconciled"


@dataclass(frozen=True)
class PositionPlan:
    position_id: str
    strategy_id: str
    entry_engine: str
    side: str
    status: PositionPlanStatus | str
    canonical_stop_price: Decimal | None
    master_exchange: ExchangeName
    master_target_qty_base: Decimal
    master_filled_qty_base: Decimal = Decimal("0")
    created_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LegPlan:
    position_id: str
    exchange: ExchangeName
    role: LegRole | str
    target_qty_base: Decimal
    filled_qty_base: Decimal = Decimal("0")
    entry_order_id: str | None = None
    entry_client_order_id: str | None = None
    stop_order_id: str | None = None
    stop_client_order_id: str | None = None
    stop_price: Decimal | None = None
    sync_status: LegSyncStatus | str = LegSyncStatus.PLANNED
    created_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    metadata: Mapping[str, Any] = field(default_factory=dict)
