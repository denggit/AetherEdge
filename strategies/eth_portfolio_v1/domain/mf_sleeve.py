from __future__ import annotations

from dataclasses import dataclass, field
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
    exchange_quantities: dict[str, Decimal] = field(default_factory=dict)
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
        exchange_quantities: Mapping[str, Decimal] | None = None,
    ) -> None:
        if self.position_id is not None:
            raise ValueError("MF sleeve already owns a position")
        if quantity <= 0:
            raise ValueError("MF open quantity must be positive")
        self.position_id = str(position_id)
        self.quantity = Decimal(quantity)
        self.exchange_quantities = _positive_decimal_mapping(
            exchange_quantities or {}
        )
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
        exchange_quantities: Mapping[str, Decimal] | None = None,
        master_exchange: str | None = None,
    ) -> None:
        if self.position_id is None:
            return
        if quantity > 0:
            self.quantity = Decimal(quantity)
        if exchange_quantities:
            self.exchange_quantities = _positive_decimal_mapping(
                exchange_quantities
            )
        if master_exchange and quantity > 0:
            self.exchange_quantities[str(master_exchange).strip().lower()] = (
                Decimal(quantity)
            )
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

    def confirm_close(
        self,
        *,
        closed_exchanges: tuple[str, ...] | None = None,
    ) -> None:
        if closed_exchanges is None:
            self.clear()
            return
        closed = {
            str(exchange).strip().lower()
            for exchange in closed_exchanges
            if str(exchange).strip()
        }
        if not closed:
            return
        self.exchange_quantities = {
            exchange: quantity
            for exchange, quantity in self.exchange_quantities.items()
            if exchange not in closed and quantity > 0
        }
        if not self.exchange_quantities:
            self.clear()
            return
        self.pending_close = True

    def restore_from_plan(self, payload: Mapping[str, Any]) -> bool:
        position = dict(payload.get("position", {}))
        metadata = merged_plan_metadata(payload)
        position_id = str(position.get("position_id") or "")
        if not position_id:
            return False
        exchange_quantities = _exchange_quantities_from_plan(
            payload,
            metadata=metadata,
        )
        master_exchange = str(position.get("master_exchange") or "").strip().lower()
        quantity = _plan_master_quantity(
            position=position,
            exchange_quantities=exchange_quantities,
            master_exchange=master_exchange,
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
        self.exchange_quantities = exchange_quantities
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
        self.exchange_quantities = {}
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
                    "active_exchanges": sorted(self.exchange_quantities),
                    "exchange_quantities_base": _string_decimal_mapping(
                        self.exchange_quantities
                    ),
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


def _positive_decimal_mapping(
    values: Mapping[str, Decimal],
) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    for key, value in values.items():
        exchange = str(key).strip().lower()
        if not exchange:
            continue
        decimal = _positive_decimal(value)
        if decimal is not None:
            out[exchange] = decimal
    return out


def _string_decimal_mapping(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {
        exchange: str(quantity)
        for exchange, quantity in values.items()
        if quantity > 0
    }


def _exchange_quantities_from_plan(
    payload: Mapping[str, Any],
    *,
    metadata: Mapping[str, Any],
) -> dict[str, Decimal]:
    quantities: dict[str, Decimal] = {}
    for raw_leg in payload.get("legs", ()):
        if not isinstance(raw_leg, Mapping):
            continue
        exchange = str(raw_leg.get("exchange") or "").strip().lower()
        quantity = _positive_decimal(
            raw_leg.get("filled_qty_base")
            or raw_leg.get("target_qty_base")
        )
        if exchange and quantity is not None:
            quantities[exchange] = quantity
    if quantities:
        return quantities
    raw = metadata.get("exchange_quantities_base")
    if isinstance(raw, Mapping):
        return _positive_decimal_mapping(raw)
    return {}


def _plan_master_quantity(
    *,
    position: Mapping[str, Any],
    exchange_quantities: Mapping[str, Decimal],
    master_exchange: str,
) -> Decimal:
    if master_exchange in exchange_quantities:
        return exchange_quantities[master_exchange]
    for quantity in exchange_quantities.values():
        if quantity > 0:
            return quantity
    return Decimal(
        str(
            position.get("master_filled_qty_base")
            or position.get("master_target_qty_base")
            or "0"
        )
    )


__all__ = ["MfSleeveState"]
