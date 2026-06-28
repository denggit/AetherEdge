from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.signals import SignalAction, SignalOrderType, TradeSignal
from strategies.eth_lf_portfolio_v8.domain.models import Side, V8DecisionType, V8TradeDecision


@dataclass(frozen=True)
class SignalMapperConfig:
    strategy_id: str = "eth_lf_portfolio_v8"
    target_exchanges: tuple[str, ...] = ()
    extra_metadata: Mapping[str, Any] = field(default_factory=dict)


class V8SignalMapper:
    def __init__(self, config: SignalMapperConfig | None = None) -> None:
        self.config = config or SignalMapperConfig()

    def map_decision(self, decision: V8TradeDecision) -> list[TradeSignal]:
        if decision.decision_type is V8DecisionType.NONE:
            return []
        metadata = {
            "strategy_id": self.config.strategy_id,
            "engine": decision.engine,
            "bar_close_time_ms": decision.bar_close_time_ms,
            "entry_risk_scale": str(decision.entry_risk_scale),
            "risk_mult": str(decision.risk_mult),
            "quality_mult": str(decision.quality_mult),
            **dict(self.config.extra_metadata),
            **dict(decision.metadata),
        }
        if self.config.target_exchanges:
            metadata["target_exchanges"] = list(self.config.target_exchanges)

        if decision.decision_type in {V8DecisionType.OPEN, V8DecisionType.ADD}:
            if decision.quantity is None or decision.quantity <= 0:
                raise ValueError("open/add decision requires positive quantity")
            return [
                TradeSignal(
                    symbol=decision.symbol,
                    action=_open_action(decision.side),
                    quantity=decision.quantity,
                    order_type=SignalOrderType.MARKET,
                    reason=decision.reason,
                    metadata={**metadata, "decision_type": decision.decision_type.value},
                )
            ]

        if decision.decision_type is V8DecisionType.CLOSE:
            if decision.quantity is None or decision.quantity <= 0:
                raise ValueError("close decision requires positive quantity")
            return [
                TradeSignal(
                    symbol=decision.symbol,
                    action=_close_action(decision.side),
                    quantity=decision.quantity,
                    order_type=SignalOrderType.MARKET,
                    reason=decision.reason,
                    metadata={**metadata, "decision_type": decision.decision_type.value, "reduce_only": True},
                )
            ]

        if decision.decision_type is V8DecisionType.PLACE_STOP:
            if decision.quantity is None or decision.quantity <= 0:
                raise ValueError("stop decision requires positive quantity")
            if decision.stop_price is None or decision.stop_price <= 0:
                raise ValueError("stop decision requires positive stop_price")
            return [
                TradeSignal(
                    symbol=decision.symbol,
                    action=_stop_action(decision.side),
                    quantity=decision.quantity,
                    order_type=SignalOrderType.MARKET,
                    trigger_price=decision.stop_price,
                    reason=decision.reason,
                    metadata={**metadata, "decision_type": decision.decision_type.value, "reduce_only": True},
                )
            ]
        raise ValueError(f"unsupported decision type: {decision.decision_type}")

    def map_many(self, decisions: Sequence[V8TradeDecision]) -> list[TradeSignal]:
        signals: list[TradeSignal] = []
        for decision in decisions:
            signals.extend(self.map_decision(decision))
        return signals


def _open_action(side: Side) -> SignalAction:
    if side is Side.LONG:
        return SignalAction.OPEN_LONG
    if side is Side.SHORT:
        return SignalAction.OPEN_SHORT
    raise ValueError("open side must be long or short")


def _close_action(side: Side) -> SignalAction:
    if side is Side.LONG:
        return SignalAction.CLOSE_LONG
    if side is Side.SHORT:
        return SignalAction.CLOSE_SHORT
    raise ValueError("close side must be long or short")


def _stop_action(side: Side) -> SignalAction:
    if side is Side.LONG:
        return SignalAction.PLACE_STOP_LOSS_LONG
    if side is Side.SHORT:
        return SignalAction.PLACE_STOP_LOSS_SHORT
    raise ValueError("stop side must be long or short")
