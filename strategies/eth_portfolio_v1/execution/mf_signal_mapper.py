from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from src.signals import SignalAction, SignalOrderType, TradeSignal
from strategies.eth_portfolio_v1.domain.mf_signal import (
    MF_ENGINE_NAME,
    MF_VARIANT_NAME,
    MfLowSweepConfig,
    MfSignalDecision,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.domain.sleeves import MF_RESERVED_SLEEVE_ID


@dataclass(frozen=True)
class MfSizingInput:
    equity: Decimal | None
    available_equity: Decimal | None


class MfSignalMapper:
    def __init__(
        self,
        *,
        strategy_id: str,
        symbol: str,
        config: MfLowSweepConfig,
        target_exchanges: tuple[str, ...] = (),
    ) -> None:
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.config = config
        self.target_exchanges = tuple(target_exchanges)

    def map_open(
        self,
        decision: MfSignalDecision,
        *,
        sizing: MfSizingInput,
    ) -> TradeSignal | None:
        if (
            sizing.equity is None
            or sizing.equity <= 0
            or sizing.available_equity is None
            or sizing.available_equity <= 0
            or decision.reference_price <= 0
        ):
            return None
        target_notional = sizing.equity * self.config.position_fraction
        target_notional = min(target_notional, sizing.available_equity)
        if target_notional <= 0:
            return None
        quantity = target_notional / decision.reference_price
        metadata = self._metadata(decision)
        metadata["sizing_input"] = {
            "equity": str(sizing.equity),
            "available_equity": (
                None
                if sizing.available_equity is None
                else str(sizing.available_equity)
            ),
            "position_fraction": str(self.config.position_fraction),
            "target_notional": str(target_notional),
            "reference_price": str(decision.reference_price),
        }
        return TradeSignal(
            symbol=self.symbol,
            action=SignalAction.OPEN_LONG,
            quantity=quantity,
            order_type=SignalOrderType.MARKET,
            client_order_id=f"mf-open-{decision.signal_time_ms}",
            reason=decision.reason,
            metadata=metadata,
            created_time_ms=decision.decision_time_ms,
        )

    def map_close(
        self,
        decision: MfSignalDecision,
        *,
        sleeve: MfSleeveState,
    ) -> TradeSignal | None:
        if (
            not sleeve.active
            or sleeve.quantity <= 0
            or sleeve.position_id != decision.position_id
        ):
            return None
        metadata = self._metadata(decision)
        metadata.update(
            {
                "reduce_only": True,
                "close_scope": "mf_sleeve_only",
                "quantity_scope": "mf_sleeve_quantity",
            }
        )
        return TradeSignal(
            symbol=self.symbol,
            action=SignalAction.CLOSE_LONG,
            quantity=sleeve.quantity,
            order_type=SignalOrderType.MARKET,
            client_order_id=f"mf-close-{decision.signal_time_ms}",
            reason=decision.reason,
            metadata=metadata,
            created_time_ms=decision.decision_time_ms,
        )

    def _metadata(self, decision: MfSignalDecision) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "strategy_id": self.strategy_id,
            "sleeve_id": MF_RESERVED_SLEEVE_ID,
            "position_id": decision.position_id,
            "engine": MF_ENGINE_NAME,
            "variant_name": MF_VARIANT_NAME,
            "entry_mode": "next_open",
            "exit_variant": "time48",
            "signal_time_ms": decision.signal_time_ms,
            "decision_time_ms": decision.decision_time_ms,
            "entry_execution_time_ms": decision.entry_execution_time_ms,
            "entry_tradebar_open_time_ms": decision.audit.get(
                "entry_tradebar_open_time_ms"
            ),
            "time48_holding_minutes": self.config.holding_minutes,
            "quantity_scope": "mf_sleeve_quantity",
            "stop_scope": decision.position_id,
            "protective_stop_required": False,
            "audit": _json_safe_mapping(decision.audit),
        }
        if self.target_exchanges:
            metadata["target_exchanges"] = list(self.target_exchanges)
        return metadata


def _json_safe_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, Decimal):
            result[str(key)] = str(item)
        elif isinstance(item, Mapping):
            result[str(key)] = _json_safe_mapping(item)
        elif isinstance(item, (list, tuple)):
            result[str(key)] = [
                str(entry) if isinstance(entry, Decimal) else entry
                for entry in item
            ]
        else:
            result[str(key)] = item
    return result


__all__ = ["MfSignalMapper", "MfSizingInput"]
