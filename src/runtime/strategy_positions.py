from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


@dataclass(frozen=True)
class StrategyPositionSnapshotIndex:
    """Stable, non-deduplicating index over strategy logical positions."""

    snapshots: tuple[StrategyPositionSnapshot, ...]

    @property
    def active(self) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.status is StrategyPositionStatus.ACTIVE
        )

    def by_position_id(self, position_id: str) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.position_id == position_id
        )

    def by_symbol(self, symbol: str) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.symbol == symbol
        )

    def by_symbol_side(
        self,
        symbol: str,
        side: StrategyPositionSide,
    ) -> tuple[StrategyPositionSnapshot, ...]:
        return tuple(
            snapshot
            for snapshot in self.snapshots
            if snapshot.symbol == symbol and snapshot.side is side
        )

    def single_active_or_none_for_legacy(self) -> StrategyPositionSnapshot | None:
        """Return the sole active snapshot, never an arbitrary first snapshot."""

        active = self.active
        return active[0] if len(active) == 1 else None


_MISSING = object()
_LEGACY_POSITION_FIELDS = (
    "in_pos",
    "position_id",
    "side",
    "qty",
    "avg_entry",
    "stop_price",
)


def resolve_strategy_position_snapshots(
    strategy: object,
    *,
    legacy_strategy_id: str | None = None,
    legacy_symbol: str | None = None,
    legacy_base_quantity: Decimal | None = None,
) -> tuple[StrategyPositionSnapshot, ...]:
    """Resolve provider snapshots first, then a single legacy position."""

    provider = _declared_attribute(strategy, "position_snapshots")
    if callable(provider):
        return tuple(provider())

    position = _safe_getattr(strategy, "position")
    if position is _MISSING or position is None:
        return ()

    fields = {name: _safe_getattr(position, name) for name in _LEGACY_POSITION_FIELDS}
    if any(value is _MISSING for value in fields.values()) or not fields["in_pos"]:
        return ()

    strategy_id = _strategy_identity_value(strategy, "strategy_id") or _identity_string(legacy_strategy_id)
    symbol = _strategy_identity_value(strategy, "symbol") or _identity_string(legacy_symbol)
    position_id = _identity_string(fields["position_id"])
    base_quantity = _non_negative_decimal(fields["qty"])
    if base_quantity is None:
        base_quantity = _non_negative_decimal(legacy_base_quantity)
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
            engine=_legacy_engine(position),
            entry_time_ms=_optional_int(_safe_getattr(position, "entry_time_ms")),
            metadata=_legacy_metadata(position),
        ),
    )


def resolve_strategy_position_snapshot_index(
    strategy: object,
    *,
    legacy_strategy_id: str | None = None,
    legacy_symbol: str | None = None,
    legacy_base_quantity: Decimal | None = None,
) -> StrategyPositionSnapshotIndex:
    return StrategyPositionSnapshotIndex(
        resolve_strategy_position_snapshots(
            strategy,
            legacy_strategy_id=legacy_strategy_id,
            legacy_symbol=legacy_symbol,
            legacy_base_quantity=legacy_base_quantity,
        )
    )


def _safe_getattr(value: object, name: str) -> Any:
    try:
        return getattr(value, name)
    except Exception:
        return _MISSING


def _declared_attribute(value: object, name: str) -> Any:
    try:
        instance_vars = vars(value)
    except TypeError:
        instance_vars = {}
    if name in instance_vars:
        return _safe_getattr(value, name)
    try:
        declared_on_type = any(name in cls.__dict__ for cls in type(value).__mro__)
    except (AttributeError, TypeError):
        declared_on_type = False
    return _safe_getattr(value, name) if declared_on_type else _MISSING


def _strategy_identity_value(strategy: object, name: str) -> str | None:
    value = _safe_getattr(strategy, name)
    normalized = _identity_string(value)
    if normalized is not None:
        return normalized

    config = _safe_getattr(strategy, "config")
    if config is _MISSING or config is None:
        return None
    return _identity_string(_safe_getattr(config, name))


def _identity_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


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


def _legacy_metadata(position: object) -> Mapping[str, Any]:
    open_legs = _safe_getattr(position, "open_legs")
    if not isinstance(open_legs, Mapping):
        return {}
    active_exchanges = tuple(
        normalized
        for exchange in open_legs
        if (normalized := _optional_string(exchange)) is not None
    )
    return {"active_exchanges": active_exchanges} if active_exchanges else {}


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


__all__ = [
    "StrategyPositionSnapshotIndex",
    "resolve_strategy_position_snapshot_index",
    "resolve_strategy_position_snapshots",
]
