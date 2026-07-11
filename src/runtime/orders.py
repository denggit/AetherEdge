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

    def create(
        self,
        signal: TradeSignal,
        *,
        source: str = "",
        event_time_ms: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> OrderIntent:
        targets = self._resolve_target_exchanges(signal)
        # Effective metadata: signal.metadata overridden by explicit metadata.
        effective = {**dict(signal.metadata or {}), **dict(metadata or {})}
        intent_id = _intent_id(
            strategy_id=self.strategy_id,
            signal=signal,
            source=source,
            event_time_ms=event_time_ms,
            target_exchanges=targets,
            effective_metadata=effective,
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
    effective_metadata: Mapping[str, Any],
) -> str:
    """Generate a stable intent_id from business keys.

    Does NOT use ``signal.created_time_ms``, ``time.time()``, UUID, or any
    value that changes across replays of the same logical business event.

    Identity is composed from ALL applicable fields (not chosen between
    alternatives):

    * Core fields: strategy_id, symbol, action, source.
    * Canonical event identity when available (bar close time, signal time,
      master close event, etc.).
    * Operation identity fields when present (position_id, execution_purpose,
      operation_key, generation, sequence, …).
    * Target exchange set (sorted).

    For durable recovery operations without a canonical event time, the
    operation identity fields alone provide sufficient identity.
    """
    # 1. Resolve stable canonical event time from metadata.
    canonical_time = _resolve_canonical_event_time(effective_metadata)

    # 2. Build operation identity parts from stable business metadata.
    op_parts = _operation_identity_parts(effective_metadata)

    # 3. Fail closed: require at least one stable identity anchor.
    has_event = canonical_time is not None or event_time_ms is not None
    if not has_event and not op_parts:
        raise ValueError(
            f"order intent has no stable identity "
            f"(source={source!r}, action={signal.action.value})"
        )

    # 4. Target exchanges sorted for identity (set semantics).
    sorted_exchanges = sorted(exchange.value for exchange in target_exchanges)

    # 5. Combine ALL applicable fields into a single deterministic string.
    parts: list[str] = [
        strategy_id,
        signal.symbol,
        signal.action.value,
        source,
        ",".join(sorted_exchanges),
    ]
    if canonical_time is not None:
        parts.append(f"evt={canonical_time}")
    elif event_time_ms is not None:
        parts.append(f"evt={event_time_ms}")
    if op_parts:
        parts.append(f"op={op_parts}")

    raw = "|".join(parts)
    return "intent-" + hashlib.blake2b(raw.encode("utf-8"), digest_size=12).hexdigest()


def _resolve_canonical_event_time(metadata: Mapping[str, Any]) -> int | None:
    """Extract a stable event timestamp from signal metadata.

    Priority order follows the canonical event identity chain:
    explicit event_time_ms -> bar_close_time_ms -> signal_time_ms ->
    master_close_event_time_ms -> entry_execution_time_ms ->
    candidate_open_ms.

    ``created_time_ms`` is intentionally excluded — it is an audit field
    and must never participate in identity generation.

    Returns ``None`` when no canonical time is available.
    """
    for key in (
        "event_time_ms",
        "bar_close_time_ms",
        "signal_time_ms",
        "master_close_event_time_ms",
        "entry_execution_time_ms",
        "candidate_open_ms",
    ):
        value = metadata.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


def _operation_identity_parts(metadata: Mapping[str, Any]) -> str:
    """Derive stable operation identity fields from business metadata.

    Only fields that describe *which business operation* this intent
    represents are included.  Audit, logging, price-protection, and
    equity-snapshot fields are intentionally excluded so they cannot
    accidentally create new identities.

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
        "master_close_generation",
    ):
        value = metadata.get(key)
        if value is not None:
            parts.append(f"{key}={value}")
    return ";".join(parts)
