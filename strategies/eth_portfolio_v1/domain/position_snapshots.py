from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.domain.position_state import V8PositionState
from strategies.eth_portfolio_v1.domain.sleeves import LF_SLEEVE_ID


@dataclass(frozen=True)
class LfSleeveSnapshotAdapter:
    """Adapt the legacy LF state to the runtime's generic position snapshot."""

    strategy_id: str
    symbol: str

    def build_active(
        self,
        position: V8PositionState,
    ) -> StrategyPositionSnapshot | None:
        if not position.in_pos:
            return None
        if not isinstance(position.position_id, str) or not position.position_id.strip():
            return None

        side = _snapshot_side(position.side)
        if side is None:
            return None

        quantity = position.qty
        if not isinstance(quantity, Decimal) or not quantity.is_finite() or quantity < 0:
            return None

        return StrategyPositionSnapshot(
            strategy_id=self.strategy_id,
            sleeve_id=LF_SLEEVE_ID,
            position_id=position.position_id,
            symbol=self.symbol,
            side=side,
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=quantity,
            average_entry_price=position.avg_entry,
            stop_price=position.stop_price,
            engine=position.entry_engine or None,
            entry_time_ms=position.entry_time_ms,
            metadata={
                "active_exchanges": sorted(position.open_legs),
                "protective_stop_required": True,
                "stop_scope": position.position_id,
                "stop_order_ids": [
                    value
                    for leg in position.legs.values()
                    for value in (
                        leg.stop_order_id,
                        leg.stop_client_order_id,
                    )
                    if value
                ],
            },
        )


def _snapshot_side(side: Side) -> StrategyPositionSide | None:
    if side is Side.LONG:
        return StrategyPositionSide.LONG
    if side is Side.SHORT:
        return StrategyPositionSide.SHORT
    return None


__all__ = ["LfSleeveSnapshotAdapter"]
