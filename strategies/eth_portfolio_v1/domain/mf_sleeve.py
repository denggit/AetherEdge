from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
from strategies.eth_portfolio_v1.domain.mf_signal import MF_ENGINE_NAME
from strategies.eth_portfolio_v1.domain.recovery import merged_plan_metadata
from strategies.eth_portfolio_v1.domain.sleeves import MF_RESERVED_SLEEVE_ID


@dataclass
class MfSleeveState:
    strategy_id: str
    symbol: str
    enabled: bool = True
    sleeve_id: str = MF_RESERVED_SLEEVE_ID
    position_id: str | None = None
    quantity: Decimal = Decimal("0")
    average_entry_price: Decimal | None = None
    entry_signal_time_ms: int | None = None
    entry_execution_time_ms: int | None = None
    entry_tradebar_open_time_ms: int | None = None
    pending_open: bool = False
    pending_close: bool = False

    @property
    def active(self) -> bool:
        return (
            self.position_id is not None
            and self.quantity > 0
            and not self.pending_open
        )

    @property
    def state_label(self) -> str:
        if self.pending_open:
            return "pending_open"
        if self.pending_close:
            return "pending_close"
        if self.active:
            return "active"
        return "flat"

    def reserve_open(
        self,
        *,
        position_id: str,
        quantity: Decimal,
        signal_time_ms: int,
        entry_execution_time_ms: int,
        tradebar_open_time_ms: int,
    ) -> None:
        if self.position_id is not None:
            raise ValueError("MF sleeve already owns a position")
        if quantity <= 0:
            raise ValueError("MF open quantity must be positive")
        self.position_id = str(position_id)
        self.quantity = Decimal(quantity)
        self.entry_signal_time_ms = int(signal_time_ms)
        self.entry_execution_time_ms = int(entry_execution_time_ms)
        self.entry_tradebar_open_time_ms = int(tradebar_open_time_ms)
        self.pending_open = True
        self.pending_close = False

    def confirm_open(
        self,
        *,
        quantity: Decimal,
        average_entry_price: Decimal,
        entry_time_ms: int,
    ) -> None:
        if self.position_id is None:
            return
        if quantity > 0:
            self.quantity = Decimal(quantity)
        if average_entry_price > 0:
            self.average_entry_price = Decimal(average_entry_price)
        self.entry_execution_time_ms = int(entry_time_ms)
        self.pending_open = False
        self.pending_close = False

    def reject_open(self) -> None:
        if self.pending_open:
            self.clear()

    def reserve_close(self) -> None:
        if self.active:
            self.pending_close = True

    def reject_close(self) -> None:
        self.pending_close = False

    def confirm_close(self) -> None:
        self.clear()

    def restore_from_plan(self, payload: Mapping[str, Any]) -> bool:
        position = dict(payload.get("position", {}))
        metadata = merged_plan_metadata(payload)
        position_id = str(position.get("position_id") or "")
        if not position_id:
            return False
        quantity = Decimal(
            str(
                position.get("master_filled_qty_base")
                or position.get("master_target_qty_base")
                or "0"
            )
        )
        if quantity <= 0:
            return False
        average_entry_price = _positive_decimal(
            metadata.get("average_entry_price")
        )
        signal_time_ms = _positive_int(metadata.get("signal_time_ms"))
        execution_time_ms = _positive_int(
            metadata.get("entry_execution_time_ms")
        )
        tradebar_open_time_ms = _positive_int(
            metadata.get("entry_tradebar_open_time_ms")
        )
        if (
            average_entry_price is None
            or signal_time_ms is None
            or execution_time_ms is None
            or tradebar_open_time_ms is None
        ):
            return False
        self.position_id = position_id
        self.quantity = quantity
        self.average_entry_price = average_entry_price
        self.entry_signal_time_ms = signal_time_ms
        self.entry_execution_time_ms = execution_time_ms
        self.entry_tradebar_open_time_ms = tradebar_open_time_ms
        self.pending_open = False
        self.pending_close = False
        return True

    def clear(self) -> None:
        self.position_id = None
        self.quantity = Decimal("0")
        self.average_entry_price = None
        self.entry_signal_time_ms = None
        self.entry_execution_time_ms = None
        self.entry_tradebar_open_time_ms = None
        self.pending_open = False
        self.pending_close = False

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        if not self.active or self.position_id is None:
            return ()
        status = (
            StrategyPositionStatus.CLOSING
            if self.pending_close
            else StrategyPositionStatus.ACTIVE
        )
        return (
            StrategyPositionSnapshot(
                strategy_id=self.strategy_id,
                position_id=self.position_id,
                symbol=self.symbol,
                side=StrategyPositionSide.LONG,
                status=status,
                base_quantity=self.quantity,
                average_entry_price=self.average_entry_price,
                stop_price=None,
                sleeve_id=self.sleeve_id,
                engine=MF_ENGINE_NAME,
                entry_time_ms=self.entry_execution_time_ms,
                metadata={
                    "exit_variant": "time48",
                    "stop_scope": self.position_id,
                    "protective_stop_required": False,
                    "entry_execution_time_ms": self.entry_execution_time_ms,
                    "entry_tradebar_open_time_ms": (
                        self.entry_tradebar_open_time_ms
                    ),
                },
            ),
        )


def _positive_decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _positive_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


__all__ = ["MfSleeveState"]
