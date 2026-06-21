from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

from src.order_management.models import OrderIntent
from src.platform.exchanges.models import ExchangeName
from src.signals.models import TradeSignal


@dataclass(frozen=True)
class LiveOrderIntentFactory:
    """Create durable order intents from strategy signals."""

    strategy_id: str
    target_exchanges: tuple[ExchangeName, ...]

    def create(self, signal: TradeSignal, *, source: str = "", event_time_ms: int | None = None, metadata: Mapping[str, Any] | None = None) -> OrderIntent:
        intent_id = _intent_id(
            strategy_id=self.strategy_id,
            signal=signal,
            source=source,
            event_time_ms=event_time_ms,
        )
        return OrderIntent(
            intent_id=intent_id,
            strategy_id=self.strategy_id,
            signal=signal,
            target_exchanges=self.target_exchanges,
            metadata={
                "source": source,
                "event_time_ms": event_time_ms,
                "signal_action": signal.action.value,
                "signal_created_time_ms": signal.created_time_ms,
                "canonical_quantity": None if signal.quantity is None else str(signal.quantity),
                **dict(metadata or {}),
            },
        )


def _intent_id(*, strategy_id: str, signal: TradeSignal, source: str, event_time_ms: int | None) -> str:
    raw = "|".join(
        [
            strategy_id,
            signal.symbol,
            signal.action.value,
            str(signal.created_time_ms),
            str(event_time_ms or ""),
            source,
            str(signal.quantity or ""),
            str(signal.price or ""),
            str(signal.trigger_price or ""),
        ]
    )
    return "intent-" + hashlib.blake2b(raw.encode("utf-8"), digest_size=12).hexdigest()
