from __future__ import annotations

from dataclasses import dataclass, field
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
    equity_by_exchange: Mapping[str, Decimal] = field(default_factory=dict)
    available_equity_by_exchange: Mapping[str, Decimal] = field(default_factory=dict)
    leverage_by_exchange: Mapping[str, Decimal] = field(default_factory=dict)
    margin_mode_by_exchange: Mapping[str, str] = field(default_factory=dict)


class MfSignalMapper:
    def __init__(
        self,
        *,
        strategy_id: str,
        symbol: str,
        config: MfLowSweepConfig,
        target_exchanges: tuple[str, ...] = (),
        master_exchange: str = "",
    ) -> None:
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.config = config
        self.target_exchanges = tuple(
            str(exchange).strip().lower()
            for exchange in target_exchanges
            if str(exchange).strip()
        )
        self.master_exchange = str(master_exchange).strip().lower()

    def map_open(
        self,
        decision: MfSignalDecision,
        *,
        sizing: MfSizingInput,
    ) -> TradeSignal | None:
        if decision.reference_price <= 0:
            return None
        quantities, sizing_metadata = self._exchange_quantities(
            decision=decision,
            sizing=sizing,
        )
        master_exchange = self._master_exchange(quantities)
        quantity = quantities.get(master_exchange, Decimal("0"))
        if quantity <= 0:
            return None
        metadata = self._metadata(decision)
        metadata.update(
            {
                "execution_purpose": "normal_entry",
                "target_exchanges": sorted(quantities),
                "exchange_quantities_base": _decimal_mapping(quantities),
                **sizing_metadata,
            }
        )
        metadata["sizing_input"] = dict(sizing_metadata)
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
                "execution_purpose": "normal_close",
                "reduce_only": True,
                "close_scope": "mf_sleeve_only",
                "quantity_scope": "mf_sleeve_quantity",
            }
        )
        exchange_quantities = sleeve.exchange_quantities
        if exchange_quantities:
            metadata["target_exchanges"] = sorted(exchange_quantities)
            metadata["exchange_quantities_base"] = _decimal_mapping(
                exchange_quantities
            )
        quantity = _canonical_quantity(
            exchange_quantities,
            master_exchange=self._master_exchange(exchange_quantities),
            fallback=sleeve.quantity,
        )
        return TradeSignal(
            symbol=self.symbol,
            action=SignalAction.CLOSE_LONG,
            quantity=quantity,
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
            "fixed_time_exit_holding_minutes": self.config.holding_minutes,
            "quantity_scope": "mf_sleeve_quantity",
            "stop_scope": decision.position_id,
            "protective_stop_required": (
                self.config.hard_stop_enabled
            ),
            "mf_hard_stop_enabled": self.config.hard_stop_enabled,
            "mf_hard_stop_pct": (
                str(self.config.hard_stop_pct)
                if self.config.hard_stop_enabled
                else None
            ),
            "mf_hard_stop_cooldown_hours": (
                self.config.hard_stop_cooldown_hours
            ),
            "unconfirmed_master_close_policy": "manual_required",
            "audit": _json_safe_mapping(decision.audit),
        }
        if self.target_exchanges:
            metadata["target_exchanges"] = list(self.target_exchanges)
        return metadata

    def _exchange_quantities(
        self,
        *,
        decision: MfSignalDecision,
        sizing: MfSizingInput,
    ) -> tuple[dict[str, Decimal], dict[str, Any]]:
        equity_by_exchange = _normalized_decimal_mapping(
            sizing.equity_by_exchange
        )
        available_by_exchange = _normalized_decimal_mapping(
            sizing.available_equity_by_exchange
        )
        leverage_by_exchange = _normalized_decimal_mapping(
            sizing.leverage_by_exchange
        )
        if not equity_by_exchange and sizing.equity is not None:
            master = self._master_exchange({})
            equity_by_exchange[master] = sizing.equity
        if not available_by_exchange and sizing.available_equity is not None:
            master = self._master_exchange({})
            available_by_exchange[master] = sizing.available_equity
        exchanges = (
            set(self.target_exchanges)
            if self.target_exchanges
            else set(equity_by_exchange) | set(available_by_exchange)
        )
        master_exchange = self._master_exchange(equity_by_exchange)
        exchanges.add(master_exchange)

        quantities: dict[str, Decimal] = {}
        target_notional_by_exchange: dict[str, Decimal] = {}
        sizing_equity_by_exchange: dict[str, Decimal] = {}
        available_equity_by_exchange: dict[str, Decimal] = {}
        used_leverage_by_exchange: dict[str, Decimal] = {}
        margin_mode_by_exchange = _normalized_string_mapping(
            sizing.margin_mode_by_exchange
        )

        for exchange in sorted(exchanges):
            equity = equity_by_exchange.get(exchange)
            available = available_by_exchange.get(exchange)
            leverage = leverage_by_exchange.get(exchange)
            if (
                equity is None
                or equity <= 0
                or available is None
                or available <= 0
                or leverage is None
                or leverage <= 0
            ):
                continue
            target_notional = (
                equity * self.config.margin_fraction * leverage
            )
            max_notional_by_available = (
                available * leverage * self.config.available_margin_buffer
            )
            target_notional = min(
                target_notional,
                max_notional_by_available,
            )
            if target_notional <= 0:
                continue
            quantities[exchange] = target_notional / decision.reference_price
            target_notional_by_exchange[exchange] = target_notional
            sizing_equity_by_exchange[exchange] = equity
            available_equity_by_exchange[exchange] = available
            used_leverage_by_exchange[exchange] = leverage

        if master_exchange not in quantities:
            return {}, {}
        metadata = {
            "margin_fraction": str(self.config.margin_fraction),
            "available_margin_buffer": str(
                self.config.available_margin_buffer
            ),
            "reference_price": str(decision.reference_price),
            "target_notional_by_exchange": _decimal_mapping(
                target_notional_by_exchange
            ),
            "sizing_equity_by_exchange": _decimal_mapping(
                sizing_equity_by_exchange
            ),
            "available_equity_by_exchange": _decimal_mapping(
                available_equity_by_exchange
            ),
            "leverage_by_exchange": _decimal_mapping(
                used_leverage_by_exchange
            ),
            "margin_mode_by_exchange": {
                exchange: margin_mode_by_exchange[exchange]
                for exchange in sorted(quantities)
                if exchange in margin_mode_by_exchange
            },
        }
        return quantities, metadata

    def _master_exchange(self, quantities: Mapping[str, Decimal]) -> str:
        if self.master_exchange:
            return self.master_exchange
        if self.target_exchanges:
            return self.target_exchanges[0]
        if quantities:
            return sorted(quantities)[0]
        return "master"


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


def _normalized_decimal_mapping(
    values: Mapping[str, Decimal],
) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    for key, value in values.items():
        normalized = str(key).strip().lower()
        if not normalized:
            continue
        try:
            decimal = Decimal(str(value))
        except Exception:
            continue
        if decimal.is_finite():
            out[normalized] = decimal
    return out


def _normalized_string_mapping(values: Mapping[str, str]) -> dict[str, str]:
    return {
        str(key).strip().lower(): str(value).strip().lower()
        for key, value in values.items()
        if str(key).strip() and str(value).strip()
    }


def _decimal_mapping(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {
        str(exchange): str(quantity)
        for exchange, quantity in values.items()
        if quantity > 0
    }


def _canonical_quantity(
    values: Mapping[str, Decimal],
    *,
    master_exchange: str,
    fallback: Decimal,
) -> Decimal:
    master = values.get(master_exchange)
    if master is not None and master > 0:
        return master
    for exchange in sorted(values):
        quantity = values[exchange]
        if quantity > 0:
            return quantity
    return fallback


__all__ = ["MfSignalMapper", "MfSizingInput"]
