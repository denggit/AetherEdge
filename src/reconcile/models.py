from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName


class ReconcileSeverity(IntEnum):
    INFO = 10
    WARNING = 20
    CRITICAL = 30


class ReconcileCategory(str, Enum):
    MISSING_LOCAL_ORDER = "missing_local_order"
    MISSING_EXCHANGE_ORDER = "missing_exchange_order"
    ORDER_STATUS_MISMATCH = "order_status_mismatch"
    MISSING_LOCAL_STOP_ORDER = "missing_local_stop_order"
    MISSING_EXCHANGE_STOP_ORDER = "missing_exchange_stop_order"
    STOP_ORDER_STATUS_MISMATCH = "stop_order_status_mismatch"
    POSITION_MISMATCH = "position_mismatch"
    MISSING_LOCAL_SNAPSHOT = "missing_local_snapshot"


@dataclass(frozen=True)
class ReconcileIssue:
    exchange: ExchangeName
    symbol: str
    severity: ReconcileSeverity
    category: ReconcileCategory
    message: str
    entity_id: str | None = None
    local: Mapping[str, Any] = field(default_factory=dict)
    remote: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReconcileReport:
    exchange: ExchangeName
    symbol: str
    checked_at_ms: int
    issues: list[ReconcileIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.issues

    @property
    def has_warnings(self) -> bool:
        return any(issue.severity >= ReconcileSeverity.WARNING for issue in self.issues)

    def issues_at_or_above(self, severity: ReconcileSeverity) -> list[ReconcileIssue]:
        return [issue for issue in self.issues if issue.severity >= severity]
