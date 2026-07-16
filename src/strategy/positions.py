from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from src.strategy.contracts import StrategyPositionContractError


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
            raise StrategyPositionContractError("strategy_id must be non-empty")
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise StrategyPositionContractError("symbol must be non-empty")
        if not isinstance(self.side, StrategyPositionSide):
            raise StrategyPositionContractError("side must be StrategyPositionSide")
        if not isinstance(self.status, StrategyPositionStatus):
            raise StrategyPositionContractError("status must be StrategyPositionStatus")
        if self.status == StrategyPositionStatus.ACTIVE:
            if not isinstance(self.position_id, str) or not self.position_id.strip():
                raise StrategyPositionContractError(
                    "active position must have a non-empty position_id"
                )
            if self.side not in {
                StrategyPositionSide.LONG,
                StrategyPositionSide.SHORT,
            }:
                raise StrategyPositionContractError(
                    "active position side must be LONG or SHORT"
                )
            try:
                quantity_is_valid = (
                    isinstance(self.base_quantity, Decimal)
                    and self.base_quantity.is_finite()
                    and self.base_quantity > Decimal("0")
                )
            except (AttributeError, InvalidOperation, TypeError):
                quantity_is_valid = False
            if not quantity_is_valid:
                raise StrategyPositionContractError(
                    "active position base_quantity must be a positive finite Decimal"
                )
        try:
            json.dumps(dict(self.metadata))
        except (TypeError, ValueError) as exc:
            raise StrategyPositionContractError(
                "metadata must contain only JSON-serializable values"
            ) from exc


def format_strategy_position_snapshot_contexts(
    snapshots: Sequence[StrategyPositionSnapshot],
) -> str:
    """Format public ownership fields for position contract diagnostics."""

    return (
        f"strategy_ids={[snapshot.strategy_id for snapshot in snapshots]} | "
        f"symbols={[snapshot.symbol for snapshot in snapshots]} | "
        f"sleeve_ids={[snapshot.sleeve_id for snapshot in snapshots]}"
    )


@runtime_checkable
class StrategyPositionProvider(Protocol):
    """Optional strategy extension exposing logical position snapshots."""

    def position_snapshots(self) -> Sequence[StrategyPositionSnapshot]:
        ...


__all__ = [
    "format_strategy_position_snapshot_contexts",
    "StrategyPositionContractError",
    "StrategyPositionProvider",
    "StrategyPositionSide",
    "StrategyPositionSnapshot",
    "StrategyPositionStatus",
]
