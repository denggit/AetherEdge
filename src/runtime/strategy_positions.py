from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


_MISSING = object()
_LEGACY_POSITION_FIELDS = (
    "in_pos",
    "position_id",
    "side",
    "qty",
    "avg_entry",
    "stop_price",
)


def resolve_strategy_position_snapshots(strategy: object) -> tuple[StrategyPositionSnapshot, ...]:
    """Resolve provider snapshots first, then a single legacy position."""

    provider = _safe_getattr(strategy, "position_snapshots")
    if callable(provider):
        return tuple(provider())

    position = _safe_getattr(strategy, "position")
    if position is _MISSING or position is None:
        return ()

    fields = {name: _safe_getattr(position, name) for name in _LEGACY_POSITION_FIELDS}
    if any(value is _MISSING for value in fields.values()) or not fields["in_pos"]:
        return ()

    strategy_id = _strategy_identity_value(strategy, "strategy_id")
    symbol = _strategy_identity_value(strategy, "symbol")
    position_id = _non_empty_string(fields["position_id"])
    base_quantity = _non_negative_decimal(fields["qty"])
    if strategy_id is None or symbol is None or position_id is None or base_quantity is None:
        return ()

    return (
        StrategyPositionSnapshot(
            strategy_id=strategy_id,
            position_id=position_id,
            symbol=symbol,
            side=_resolve_side(fields["side"]),
            status=StrategyPositionStatus.ACTIVE,
            base_quantity=base_quantity,
            average_entry_price=_optional_decimal(fields["avg_entry"]),
            stop_price=_optional_decimal(fields["stop_price"]),
            sleeve_id=_optional_string(_safe_getattr(position, "sleeve_id")),
            engine=_legacy_engine(position),
            entry_time_ms=_optional_int(_safe_getattr(position, "entry_time_ms")),
        ),
    )


def _safe_getattr(value: object, name: str) -> Any:
    try:
        return getattr(value, name)
    except Exception:
        return _MISSING


def _strategy_identity_value(strategy: object, name: str) -> str | None:
    value = _safe_getattr(strategy, name)
    normalized = _non_empty_string(value)
    if normalized is not None:
        return normalized

    config = _safe_getattr(strategy, "config")
    if config is _MISSING or config is None:
        return None
    return _non_empty_string(_safe_getattr(config, name))


def _non_empty_string(value: object) -> str | None:
    if value is _MISSING or value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _optional_string(value: object) -> str | None:
    return _non_empty_string(value)


def _non_negative_decimal(value: object) -> Decimal | None:
    converted = _optional_decimal(value)
    if converted is None:
        return None
    try:
        if not converted.is_finite() or converted < Decimal("0"):
            return None
    except InvalidOperation:
        return None
    return converted


def _optional_decimal(value: object) -> Decimal | None:
    if value is _MISSING or value is None:
        return None
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return converted if converted.is_finite() else None


def _optional_int(value: object) -> int | None:
    if value is _MISSING or value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _legacy_engine(position: object) -> str | None:
    entry_engine = _optional_string(_safe_getattr(position, "entry_engine"))
    if entry_engine is not None:
        return entry_engine
    return _optional_string(_safe_getattr(position, "engine"))


def _resolve_side(value: object) -> StrategyPositionSide:
    if isinstance(value, StrategyPositionSide):
        return value

    enum_value = _safe_getattr(value, "value")
    if enum_value is not _MISSING:
        value = enum_value

    if isinstance(value, str):
        normalized = value.strip().lower()
        return {
            "long": StrategyPositionSide.LONG,
            "short": StrategyPositionSide.SHORT,
            "both": StrategyPositionSide.BOTH,
            "flat": StrategyPositionSide.FLAT,
        }.get(normalized, StrategyPositionSide.UNKNOWN)
    if isinstance(value, int) and not isinstance(value, bool):
        return {
            1: StrategyPositionSide.LONG,
            -1: StrategyPositionSide.SHORT,
            0: StrategyPositionSide.FLAT,
        }.get(value, StrategyPositionSide.UNKNOWN)
    return StrategyPositionSide.UNKNOWN


__all__ = ["resolve_strategy_position_snapshots"]
