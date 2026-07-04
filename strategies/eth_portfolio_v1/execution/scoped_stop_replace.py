from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping, Sequence

from src.order_management.stops import ScopedStopReplaceService, StopScope
from src.platform.exchanges.models import ExchangeName, PositionSide
from src.signals import TradeSignal


LF_SLEEVE_ID = "lf"


@dataclass(frozen=True)
class StopIdentifier:
    stop_order_id: str | None = None
    stop_client_order_id: str | None = None

    @property
    def is_available(self) -> bool:
        return _identifier(self.stop_order_id) is not None or _identifier(self.stop_client_order_id) is not None


def build_scoped_replace_signals(
    *,
    strategy_id: str,
    position_id: str | None,
    symbol: str,
    position_side: PositionSide | None,
    target_exchanges: Sequence[str],
    old_stop_identifiers: Mapping[str, Sequence[StopIdentifier]],
    new_stop_signal: TradeSignal,
    replace_reason: str,
) -> list[TradeSignal]:
    """Stage a new LF stop before exact cancellation of the old LF stops.

    The returned list is deliberately not an atomic batch. Runtime must submit
    and verify the first (new, reduce-only) stop before dispatching any later
    scoped cancel signal.
    """

    normalized_targets = tuple(dict.fromkeys(str(exchange).strip().lower() for exchange in target_exchanges))
    cancel_signals, missing_targets = build_scoped_cancel_signals(
        strategy_id=strategy_id,
        position_id=position_id,
        symbol=symbol,
        position_side=position_side,
        target_exchanges=normalized_targets,
        stop_identifiers=old_stop_identifiers,
        replace_reason=replace_reason,
    )
    missing_identifier = bool(missing_targets)
    metadata = {
        **dict(new_stop_signal.metadata),
        "strategy_id": strategy_id,
        "sleeve_id": LF_SLEEVE_ID,
        "position_id": position_id,
        "position_side": None if position_side is None else position_side.value,
        "target_exchanges": list(normalized_targets),
        "replace_mode": "staged_place_verify_scoped_cancel",
        "stop_replace_mode": "staged_place_verify_scoped_cancel",
        "stop_replace_atomic_supported": False,
        "stop_replace_non_atomic_reason": "verify_new_stop_before_scoped_cancel",
        "scoped_cancel_pending": bool(cancel_signals),
        "manual_stop_cleanup_required": missing_identifier,
    }
    if missing_identifier:
        metadata["scoped_cancel_skip_reason"] = "missing_old_stop_identifier"
        metadata["scoped_cancel_missing_target_exchanges"] = list(missing_targets)

    staged_new_stop = replace(new_stop_signal, metadata=metadata)
    return [staged_new_stop, *cancel_signals]


def build_scoped_cancel_signals(
    *,
    strategy_id: str,
    position_id: str | None,
    symbol: str,
    position_side: PositionSide | None,
    target_exchanges: Sequence[str],
    stop_identifiers: Mapping[str, Sequence[StopIdentifier]],
    replace_reason: str,
) -> tuple[list[TradeSignal], tuple[str, ...]]:
    """Build one exact cancel per old venue stop, never a global fallback."""

    normalized_position_id = str(position_id or "").strip()
    signals: list[TradeSignal] = []
    missing_targets: list[str] = []
    service = ScopedStopReplaceService()

    for exchange in target_exchanges:
        normalized_exchange = str(exchange).strip().lower()
        identifiers = _unique_available(stop_identifiers.get(normalized_exchange, ()))
        if not normalized_position_id or not identifiers:
            missing_targets.append(normalized_exchange)
            continue
        exchange_name = ExchangeName(normalized_exchange)
        for identifier in identifiers:
            scope = StopScope(
                strategy_id=strategy_id,
                sleeve_id=LF_SLEEVE_ID,
                position_id=normalized_position_id,
                symbol=symbol,
                position_side=position_side,
                target_exchanges=(exchange_name,),
                stop_client_order_id=_identifier(identifier.stop_client_order_id),
                stop_order_id=_identifier(identifier.stop_order_id),
            )
            cancel = service.build_cancel_signal(scope)
            signals.append(
                replace(
                    cancel,
                    reason=f"{replace_reason}_CANCEL_OLD_SCOPED",
                    metadata={
                        **dict(cancel.metadata),
                        "execution_purpose": "stop_sync",
                        "replace_reason": replace_reason,
                        "stop_replace_stage": "cancel_old_after_new_stop_verification",
                    },
                )
            )
    return signals, tuple(missing_targets)


def _unique_available(identifiers: Sequence[StopIdentifier]) -> tuple[StopIdentifier, ...]:
    unique: list[StopIdentifier] = []
    seen: set[tuple[str | None, str | None]] = set()
    for identifier in identifiers:
        key = (_identifier(identifier.stop_order_id), _identifier(identifier.stop_client_order_id))
        if key == (None, None) or key in seen:
            continue
        seen.add(key)
        unique.append(StopIdentifier(stop_order_id=key[0], stop_client_order_id=key[1]))
    return tuple(unique)


def _identifier(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text
