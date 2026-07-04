from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


class StrategyPositionStatus(str, Enum):
    FLAT = "flat"
    ACTIVE = "active"
    CLOSING = "closing"
    UNKNOWN = "unknown"


class StrategyPositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"
    BOTH = "both"
    FLAT = "flat"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class StrategyPositionSnapshot:
    """Exchange-agnostic view of one strategy-owned logical position."""

    strategy_id: str
    position_id: str
    symbol: str
    side: StrategyPositionSide
    status: StrategyPositionStatus
    base_quantity: Decimal = Decimal("0")
    average_entry_price: Decimal | None = None
    stop_price: Decimal | None = None
    sleeve_id: str | None = None
    engine: str | None = None
    entry_time_ms: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise ValueError("strategy_id must be non-empty")
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValueError("symbol must be non-empty")
        if not isinstance(self.side, StrategyPositionSide):
            raise ValueError("side must be StrategyPositionSide")
        if not isinstance(self.status, StrategyPositionStatus):
            raise ValueError("status must be StrategyPositionStatus")
        if self.status == StrategyPositionStatus.ACTIVE:
            if not isinstance(self.position_id, str) or not self.position_id.strip():
                raise ValueError("active position must have a non-empty position_id")
            try:
                quantity_is_valid = self.base_quantity.is_finite() and self.base_quantity >= Decimal("0")
            except (AttributeError, InvalidOperation, TypeError):
                quantity_is_valid = False
            if not quantity_is_valid:
                raise ValueError("active position base_quantity must be a non-negative Decimal")
        try:
            json.dumps(dict(self.metadata))
        except (TypeError, ValueError) as exc:
            raise ValueError("metadata must contain only JSON-serializable values") from exc


@runtime_checkable
class StrategyPositionProvider(Protocol):
    """Optional strategy extension exposing logical position snapshots."""

    def position_snapshots(self) -> Sequence[StrategyPositionSnapshot]:
        ...


__all__ = [
    "StrategyPositionProvider",
    "StrategyPositionSide",
    "StrategyPositionSnapshot",
    "StrategyPositionStatus",
]
