from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
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

    * Core fields: strategy_id, symbol, action (NOT source).
    * Canonical event identity when available (bar close time, signal time,
      master close event, etc.).
    * Operation identity fields when present (position_id, execution_purpose,
      operation_key, generation, sequence, …).
    * Target exchange set (sorted).

    ``source`` is preserved in journal metadata for diagnostics but never
    enters the identity digest so that ``closed_kline`` and
    ``startup_catchup`` produce the same ID for the same bar event.
    """
    # 1. Resolve stable canonical event time from metadata.
    canonical_time = _resolve_canonical_event_time(effective_metadata)

    # 2. Build operation identity parts from stable business metadata.
    op_parts = _operation_identity_parts(effective_metadata)

    # 3. Validate minimum identity fields.
    has_event = canonical_time is not None or event_time_ms is not None
    _validate_identity_fields(
        signal.action.value, source, effective_metadata,
        has_event=has_event, has_op_parts=bool(op_parts),
    )

    # 4. Fail closed: require at least one stable identity anchor.
    if not has_event and not op_parts:
        raise ValueError(
            f"order intent has no stable identity "
            f"(source={source!r}, action={signal.action.value})"
        )

    # 5. Target exchanges sorted for identity (set semantics).
    sorted_exchanges = sorted(exchange.value for exchange in target_exchanges)

    # 6. Combine ALL applicable fields into a single deterministic string.
    #    NOTE: source is intentionally excluded from identity.
    parts: list[str] = [
        strategy_id,
        signal.symbol,
        signal.action.value,
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

    Each value is passed through the canonical serializer to ensure
    deterministic output regardless of Python type or dict ordering.

    Returns an empty string when no operation-identity fields are present.
    """
    parts: list[str] = []
    for key in (
        "position_id",
        "execution_purpose",
        "operation_key",
        "operation_sequence",
        "retry_generation",
        "decision_type",
        "stop_identifier",
        "stop_replace_stage",
        "stop_client_order_id",
        "stop_order_id",
        "master_close_generation",
        "follower_close_generation",
        "topup_generation",
        "stop_generation",
    ):
        raw_value = metadata.get(key)
        if raw_value is None:
            continue
        canonical = _canonical_value(raw_value)
        if canonical is None:
            continue
        # Strip whitespace — empty strings treated as absent.
        stripped = canonical.strip()
        if not stripped:
            continue
        parts.append(f"{key}={stripped}")
    return ";".join(parts)


def _validate_identity_fields(
    action: str,
    source: str,
    metadata: Mapping[str, Any],
    *,
    has_event: bool = False,
    has_op_parts: bool = False,
) -> None:
    """Validate minimum field combinations for identity.

    Rules:
    * ``position_id`` must be non-empty when present.
    * ``execution_purpose`` alone cannot constitute identity.
    * At least one discriminator beyond ``execution_purpose`` is required
      when operation identity fields are present without event time.

    Raises ``ValueError`` with action/source/missing field only — never
    the full metadata payload.
    """
    # Reject empty identity fields.
    for key in ("position_id", "execution_purpose", "operation_key"):
        raw = metadata.get(key)
        if raw is not None:
            text = str(raw).strip()
            if not text:
                raise ValueError(
                    f"order intent has empty {key} "
                    f"(action={action!r}, source={source!r})"
                )

    # If execution_purpose is present, verify additional discriminators exist.
    has_purpose = _is_present(metadata.get("execution_purpose"))
    if not has_purpose and not has_op_parts:
        return  # No operation identity to validate.

    has_position = _is_present(metadata.get("position_id"))
    has_event_key = any(
        metadata.get(k) is not None
        for k in ("bar_close_time_ms", "signal_time_ms", "event_time_ms",
                   "candidate_open_ms", "master_close_event_time_ms",
                   "entry_execution_time_ms")
    )
    has_generation = any(
        _is_present(metadata.get(k))
        for k in ("retry_generation", "operation_sequence",
                   "follower_close_generation", "topup_generation",
                   "stop_generation", "master_close_generation")
    )
    has_operation_key = _is_present(metadata.get("operation_key"))
    has_stop_identity = any(
        _is_present(metadata.get(k))
        for k in ("stop_client_order_id", "stop_order_id", "stop_identifier")
    )
    has_decision_type = _is_present(metadata.get("decision_type"))

    has_extra = has_event or has_event_key or has_generation or has_operation_key or has_stop_identity or has_decision_type

    # position_id alone is not enough.
    if has_position and not has_extra:
        raise ValueError(
            f"order intent has insufficient operation identity "
            f"(action={action!r}, source={source!r}, "
            f"missing=generation_or_event_or_key)"
        )

    # execution_purpose alone is not enough (even with position_id check above).
    if has_purpose and not has_position and not has_extra:
        raise ValueError(
            f"order intent has insufficient operation identity "
            f"(action={action!r}, source={source!r}, "
            f"missing=position_id_or_generation)"
        )


def _is_present(value: Any) -> bool:
    """Return True when value is non-None and non-empty after strip."""
    if value is None:
        return False
    return bool(str(value).strip())


def _canonical_value(value: Any) -> str | None:
    """Serialize an identity value to a canonical deterministic string.

    Supports: ``None``, ``str``, ``bool``, ``int``, ``Decimal``, ``Enum``,
    ``list``, ``tuple``, ``set``, ``Mapping``.

    * Mapping: sorted by normalised key.
    * set: sorted.
    * Enum: uses ``.value``.
    * Decimal: stable decimal string via ``normalize()``.
    * dict insertion order does not affect output.
    * Python ``hash()`` is never used.
    * Unsupported types raise ``TypeError`` (fail closed).

    Returns ``None`` for ``None`` input.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if isinstance(value, (list, tuple)):
        items = [_canonical_value(v) for v in value]
        return "[" + ",".join(item if item is not None else "" for item in items) + "]"
    if isinstance(value, set):
        items = sorted(
            (_canonical_value(v) for v in value),
            key=lambda x: x if x is not None else "",
        )
        return "{" + ",".join(item if item is not None else "" for item in items) + "}"
    if isinstance(value, Mapping):
        pairs = sorted(
            (
                str(k).strip().lower(),
                _canonical_value(v),
            )
            for k, v in value.items()
        )
        return "{" + ",".join(
            f"{k}={v if v is not None else ''}" for k, v in pairs
        ) + "}"
    raise TypeError(
        f"unsupported identity value type: {type(value).__name__}"
    )
