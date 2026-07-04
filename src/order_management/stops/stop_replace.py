from __future__ import annotations

from src.order_management.stops.stop_scope import StopScope
from src.signals.models import SignalAction, TradeSignal


class ScopedStopReplaceService:
    """Build scoped stop replacement signals without executing them.

    R002 must orchestrate the safety-critical boundary between the returned
    signals: place the new reduce-only, quantity-scoped stop; verify that the
    new stop exists; only then dispatch the scoped cancel for the old stop.
    A global ``cancel_all_stop_orders`` call is never a valid default here.
    """

    def build_cancel_signal(self, scope: StopScope) -> TradeSignal:
        metadata = {
            "strategy_id": scope.strategy_id,
            "sleeve_id": scope.sleeve_id,
            "position_id": scope.position_id,
            "position_side": None if scope.position_side is None else scope.position_side.value,
            "target_exchanges": (
                None
                if scope.target_exchanges is None
                else [exchange.value for exchange in scope.target_exchanges]
            ),
            "stop_order_id": scope.stop_order_id,
            "stop_client_order_id": scope.stop_client_order_id,
            "reason": "scoped_stop_replace_cancel_old",
        }
        return TradeSignal(
            symbol=scope.symbol,
            action=SignalAction.CANCEL_STOP_ORDER,
            client_order_id=scope.stop_client_order_id,
            reason="scoped_stop_replace_cancel_old",
            metadata=metadata,
        )

    def build_replace_signals(
        self,
        scope: StopScope,
        new_stop_signal: TradeSignal,
    ) -> list[TradeSignal]:
        if new_stop_signal.action not in {
            SignalAction.PLACE_STOP_LOSS_LONG,
            SignalAction.PLACE_STOP_LOSS_SHORT,
        }:
            raise ValueError("new_stop_signal must place a stop-loss order")
        if new_stop_signal.symbol != scope.symbol:
            raise ValueError("new_stop_signal symbol must match stop scope")

        # This list represents the two orchestration stages, not an atomic batch.
        # R002 must verify stage 1 at the venue before dispatching stage 2.
        return [new_stop_signal, self.build_cancel_signal(scope)]
