from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from src.order_management.models import OrderIntent
from src.platform.exchanges.models import ExchangeName
from src.signals.models import TradeSignal


@dataclass(frozen=True)
class LiveOrderIntentFactory:
    """Create durable order intents from strategy signals.

    Strategy signals may optionally include ``metadata['target_exchanges']`` to
    narrow execution to one or more configured exchanges. This is required by
    master/follower flows such as stop replacement where the strategy must
    cancel/replace stops only on the leg that just filled.
    """

    strategy_id: str
    target_exchanges: tuple[ExchangeName, ...]

    def create(self, signal: TradeSignal, *, source: str = "", event_time_ms: int | None = None, metadata: Mapping[str, Any] | None = None) -> OrderIntent:
        targets = self._resolve_target_exchanges(signal)
        intent_id = _intent_id(
            strategy_id=self.strategy_id,
            signal=signal,
            source=source,
            event_time_ms=event_time_ms,
            target_exchanges=targets,
        )
        return OrderIntent(
            intent_id=intent_id,
            strategy_id=self.strategy_id,
            signal=signal,
            target_exchanges=targets,
            metadata={
                "source": source,
                "event_time_ms": event_time_ms,
                "signal_action": signal.action.value,
                "signal_created_time_ms": signal.created_time_ms,
                "canonical_quantity": None if signal.quantity is None else str(signal.quantity),
                "target_exchanges": [exchange.value for exchange in targets],
                **dict(metadata or {}),
            },
        )

    def _resolve_target_exchanges(self, signal: TradeSignal) -> tuple[ExchangeName, ...]:
        raw = signal.metadata.get("target_exchanges") if signal.metadata else None
        if raw is None:
            return self.target_exchanges
        values = _normalize_target_values(raw)
        if not values:
            raise ValueError("signal target_exchanges cannot be empty")
        allowed = {exchange.value: exchange for exchange in self.target_exchanges}
        resolved: list[ExchangeName] = []
        seen: set[ExchangeName] = set()
        for value in values:
            if value not in allowed:
                raise ValueError(f"signal target exchange {value!r} is not configured for this runtime")
            exchange = allowed[value]
            if exchange not in seen:
                resolved.append(exchange)
                seen.add(exchange)
        if not resolved:
            raise ValueError("signal target_exchanges resolved to empty set")
        return tuple(resolved)


def _normalize_target_values(raw: Any) -> list[str]:
    if isinstance(raw, str):
        items: Iterable[Any] = raw.split(",")
    elif isinstance(raw, ExchangeName):
        items = (raw.value,)
    elif isinstance(raw, Iterable):
        items = raw
    else:
        raise ValueError("target_exchanges must be a string or iterable")
    values: list[str] = []
    for item in items:
        if isinstance(item, ExchangeName):
            value = item.value
        else:
            value = str(item)
        value = value.strip().lower()
        if value:
            values.append(value)
    return values


# ── Stable intent identity ───────────────────────────────────────────────────


def _intent_id(
    *,
    strategy_id: str,
    signal: TradeSignal,
    source: str,
    event_time_ms: int | None,
    target_exchanges: tuple[ExchangeName, ...],
) -> str:
    """Generate a stable intent_id from business keys.

    Does NOT use ``signal.created_time_ms``, ``time.time()``, UUID, or any
    value that changes across replays of the same logical business event.
    """
    metadata = signal.metadata or {}

    # 1. Resolve stable canonical event time from metadata.
    canonical_time = _resolve_canonical_event_time(metadata)

    # 2. Resolve operation identity from stable business metadata.
    op_identity = _operation_identity(metadata)

    # 3. Prefer operation identity (most specific), then canonical time, then
    #    the event_time_ms parameter.
    if op_identity:
        stable_time = op_identity
    elif canonical_time is not None:
        stable_time = str(canonical_time)
    else:
        stable_time = str(event_time_ms or "")

    # Target exchanges sorted for identity (set semantics).
    sorted_exchanges = sorted(exchange.value for exchange in target_exchanges)

    raw = "|".join(
        [
            strategy_id,
            signal.symbol,
            signal.action.value,
            source,
            stable_time,
            ",".join(sorted_exchanges),
        ]
    )
    return "intent-" + hashlib.blake2b(raw.encode("utf-8"), digest_size=12).hexdigest()


def _resolve_canonical_event_time(metadata: Mapping[str, Any]) -> int | None:
    """Extract a stable event timestamp from signal metadata.

    Priority order follows the canonical event identity chain:
    explicit event_time_ms -> bar_close_time_ms -> signal_time_ms ->
    master_close_event_time_ms -> entry_execution_time_ms ->
    candidate_open_ms -> created_time_ms.

    Returns ``None`` when no canonical time is available.
    """
    for key in (
        "event_time_ms",
        "bar_close_time_ms",
        "signal_time_ms",
        "master_close_event_time_ms",
        "entry_execution_time_ms",
        "candidate_open_ms",
        "created_time_ms",
    ):
        value = metadata.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


def _operation_identity(metadata: Mapping[str, Any]) -> str:
    """Derive a stable operation identity string from business metadata.

    Only fields that describe *which business operation* this intent represents
    are included.  Audit, logging, price-protection, and equity-snapshot fields
    are intentionally excluded so they cannot accidentally create new identities.

    Returns an empty string when no operation-identity fields are present.
    """
    parts: list[str] = []
    for key in (
        "position_id",
        "execution_purpose",
        "operation_key",
        "operation_sequence",
        "retry_generation",
        "position_generation",
        "decision_type",
        "stop_identifier",
        "stop_replace_stage",
        "stop_client_order_id",
        "stop_order_id",
    ):
        value = metadata.get(key)
        if value is not None:
            parts.append(f"{key}={value}")
    return ";".join(parts)
