from __future__ import annotations

import asyncio
import inspect
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from src.order_management.quantity import NativeQuantityConverter
from src.platform.config import ProjectEnvConfig, get_project_env_config
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, InstrumentRule, Order, OrderStatus, Position, PositionMode, PositionSide
from src.platform.execution.ports import ExecutionClient
from src.runtime.strategy_positions import (
    StrategyPositionSnapshotIndex,
    resolve_strategy_position_snapshot_index,
)
from src.signals import TradeSignal
from src.signals.models import SignalAction
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)

from src.runtime.live_types import LiveRuntimeError, logger

async def _fetch_execution_instrument_rule(
    execution: ExecutionClient,
) -> InstrumentRule | None:
    """Read the bound rule through the public execution facade when exposed."""

    fetch_rule = getattr(execution, "fetch_instrument_rule", None)
    if not callable(fetch_rule):
        return None
    value = fetch_rule()
    if inspect.isawaitable(value):
        value = await value
    return value if isinstance(value, InstrumentRule) else None

def _event_time_ms(event: MarketEvent) -> int | None:
    if isinstance(event, MarketTrade):
        return event.trade_time_ms if event.trade_time_ms is not None else event.event_time_ms
    if isinstance(event, MarketOrderBook):
        return event.event_time_ms
    if isinstance(event, MarketKline):
        return event.close_time_ms
    if isinstance(event, MarketTicker):
        return event.time_ms
    return None

def _is_trade_at_or_before(event: MarketEvent, close_time_ms: int) -> bool:
    if not isinstance(event, MarketTrade) and event.event_type is not MarketEventType.TRADE:
        return False
    event_ms = _event_time_ms(event)
    return event_ms is not None and event_ms <= close_time_ms

def _stop_post_check_attempts_from_env(project_env: ProjectEnvConfig) -> int:
    """Parse ``AETHER_STOP_POST_CHECK_ATTEMPTS`` safely, clamping to >= 1."""
    raw = project_env.get("AETHER_STOP_POST_CHECK_ATTEMPTS", "").strip()
    if not raw:
        return 3
    try:
        value = int(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid stop post-check env value; using default | env=%s raw=%r default=3",
            "AETHER_STOP_POST_CHECK_ATTEMPTS",
            project_env.get("AETHER_STOP_POST_CHECK_ATTEMPTS", ""),
        )
        return 3
    return max(1, value)

def _account_snapshot_log_keepalive_seconds_from_env(project_env: ProjectEnvConfig) -> float:
    """Parse account snapshot INFO keepalive seconds, where zero disables it."""
    raw = project_env.get("AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS", "").strip()
    if not raw:
        return 3600
    try:
        value = float(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid account snapshot log keepalive env value; using default | env=%s raw=%r default=3600",
            "AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS",
            project_env.get("AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS", ""),
        )
        return 3600
    return max(0.0, value)

def _stop_post_check_delay_from_env(project_env: ProjectEnvConfig) -> float:
    """Parse ``AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS`` safely, clamping to >= 0.0."""
    raw = project_env.get("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "").strip()
    if not raw:
        return 0.5
    try:
        value = float(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid stop post-check env value; using default | env=%s raw=%r default=0.5",
            "AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS",
            project_env.get("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", ""),
        )
        return 0.5
    return max(0.0, value)

def _all_exchange_sandbox(exchanges: Sequence[ExchangeName], project_env: ProjectEnvConfig) -> bool:
    if not exchanges:
        return False
    return all(
        project_env.get_bool(f"{exchange.value.upper()}_SANDBOX", project_env.get_bool("SANDBOX", False))
        for exchange in exchanges
    )

def _single_active_exchange_position_or_none_for_legacy(
    positions: Sequence[Position],
) -> Position | None:
    active = tuple(position for position in positions if position.quantity != 0)
    return active[0] if len(active) == 1 else None


_first_active_position = _single_active_exchange_position_or_none_for_legacy

def _active_exchange_positions(
    positions: Sequence[Position],
) -> tuple[Position, ...]:
    return tuple(position for position in positions if position.quantity != 0)

def _exchange_positions_matching_strategy_position(
    positions: Sequence[Position],
    strategy_position: StrategyPositionSnapshot,
) -> tuple[Position, ...]:
    candidates = tuple(
        position
        for position in _active_exchange_positions(positions)
        if position.symbol == strategy_position.symbol
    )
    if strategy_position.side is StrategyPositionSide.LONG:
        return tuple(
            position
            for position in candidates
            if _exchange_position_matches_long(position)
        )
    if strategy_position.side is StrategyPositionSide.SHORT:
        return tuple(
            position
            for position in candidates
            if _exchange_position_matches_short(position)
        )
    if strategy_position.side in {
        StrategyPositionSide.BOTH,
        StrategyPositionSide.UNKNOWN,
    }:
        return candidates
    return ()

def _exchange_position_matches_long(position: Position) -> bool:
    if position.side is PositionSide.LONG:
        return True
    if position.side is PositionSide.SHORT:
        return False
    return position.quantity > 0

def _exchange_position_matches_short(position: Position) -> bool:
    if position.side is PositionSide.SHORT:
        return True
    if position.side is PositionSide.LONG:
        return False
    return position.quantity < 0

def _position_side_for_strategy_position(
    strategy_position: StrategyPositionSnapshot,
    exchange_position: Position,
) -> PositionSide | None:
    if strategy_position.side is StrategyPositionSide.LONG:
        return PositionSide.LONG
    if strategy_position.side is StrategyPositionSide.SHORT:
        return PositionSide.SHORT
    side = _position_side_from_quantity(exchange_position.quantity)
    if side is not None:
        return side
    if exchange_position.side in {PositionSide.LONG, PositionSide.SHORT}:
        return exchange_position.side
    return None

def _strategy_position_native_quantity(
    *,
    strategy_position: StrategyPositionSnapshot,
    active_pos: Position,
    exchange: ExchangeName,
    market_profile,
    converter: NativeQuantityConverter,
    logical_position_count: int,
    scoped_base_quantity: Decimal | None = None,
) -> Decimal:
    if scoped_base_quantity is not None and scoped_base_quantity > 0:
        try:
            return converter.convert_quantity(
                exchange=exchange,
                symbol=strategy_position.symbol,
                base_quantity=scoped_base_quantity,
                market_profile=market_profile,
            ).native_quantity
        except Exception as exc:
            raise LiveRuntimeError(
                "stop protection validation failed: strategy quantity conversion failed | "
                f"strategy_position_id={strategy_position.position_id} "
                f"symbol={strategy_position.symbol} exchange={exchange.value} error={exc}"
            ) from exc
    if logical_position_count <= 1:
        return abs(active_pos.quantity)
    if strategy_position.base_quantity > 0:
        try:
            return converter.convert_quantity(
                exchange=exchange,
                symbol=strategy_position.symbol,
                base_quantity=strategy_position.base_quantity,
                market_profile=market_profile,
            ).native_quantity
        except Exception as exc:
            raise LiveRuntimeError(
                "stop protection validation failed: strategy quantity conversion failed | "
                f"strategy_position_id={strategy_position.position_id} "
                f"symbol={strategy_position.symbol} exchange={exchange.value} error={exc}"
            ) from exc
    raise LiveRuntimeError(
        "stop protection validation failed: missing scoped strategy quantity | "
        f"strategy_position_id={strategy_position.position_id} "
        f"symbol={strategy_position.symbol} side={strategy_position.side.value} "
        f"exchange={exchange.value} active_strategy_positions={logical_position_count}"
    )

def _raise_ambiguous_exchange_positions(
    *,
    context: str,
    strategy_position: StrategyPositionSnapshot,
    exchange: str,
    ambiguous_count: int,
) -> None:
    raise LiveRuntimeError(
        f"{context}: ambiguous exchange positions | "
        f"strategy_position_id={strategy_position.position_id} "
        f"symbol={strategy_position.symbol} side={strategy_position.side.value} "
        f"exchange={exchange} ambiguous_count={ambiguous_count}"
    )

def _strategy_position_for_stop_signal(
    index: StrategyPositionSnapshotIndex,
    signal: TradeSignal,
) -> StrategyPositionSnapshot | None:
    position_id = _signal_position_id(signal)
    if position_id is not None:
        matches = tuple(
            snapshot
            for snapshot in index.by_position_id(position_id)
            if snapshot.status is StrategyPositionStatus.ACTIVE
        )
        return matches[0] if len(matches) == 1 else None

    side = {
        SignalAction.PLACE_STOP_LOSS_LONG: StrategyPositionSide.LONG,
        SignalAction.PLACE_STOP_LOSS_SHORT: StrategyPositionSide.SHORT,
    }.get(signal.action)
    if side is not None:
        matches = tuple(
            snapshot
            for snapshot in index.by_symbol_side(signal.symbol, side)
            if snapshot.status is StrategyPositionStatus.ACTIVE
        )
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return None

    # Legacy signals may not carry a position_id. This fallback is safe only
    # when the strategy exposes exactly one active logical position.
    return index.single_active_or_none_for_legacy()

def _signal_position_id(signal: TradeSignal) -> str | None:
    if not signal.metadata:
        return None
    value = signal.metadata.get("position_id")
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None

def _strategy_position_active_exchanges(
    snapshot: StrategyPositionSnapshot,
) -> frozenset[str]:
    value = snapshot.metadata.get("active_exchanges")
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        return frozenset()
    return frozenset(
        normalized
        for item in values
        if (normalized := str(item).strip().lower())
    )

def _strategy_position_requires_protective_stop(
    snapshot: StrategyPositionSnapshot,
) -> bool:
    value = snapshot.metadata.get("protective_stop_required", True)
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"false", "0", "no", "n", ""}:
        return False
    return True

def _strategy_position_stop_order_ids(
    snapshot: StrategyPositionSnapshot,
) -> tuple[str, ...]:
    raw = snapshot.metadata.get("stop_order_ids", ())
    if isinstance(raw, str):
        values = (raw,)
    elif isinstance(raw, (list, tuple, set, frozenset)):
        values = raw
    else:
        values = ()
    return tuple(
        normalized
        for value in values
        if (normalized := str(value or "").strip())
    )

def _place_stop_scope_covers(
    scopes: Mapping[str, set[str | None]],
    *,
    exchange: str,
    position_id: str | None,
    logical_position_count: int,
) -> bool:
    exchange_scopes = scopes.get(exchange, set())
    if position_id is not None and position_id in exchange_scopes:
        return True
    return None in exchange_scopes and logical_position_count <= 1

def _position_side_from_quantity(quantity: Decimal) -> PositionSide | None:
    if quantity > 0:
        return PositionSide.LONG
    if quantity < 0:
        return PositionSide.SHORT
    return None

def _exchange_position_metadata(
    *,
    active_pos: Position,
    exchange: ExchangeName,
    symbol: str,
    market_profile,
    converter: NativeQuantityConverter,
) -> dict[str, Any]:
    native_qty = abs(active_pos.quantity)
    side = _position_side_from_quantity(active_pos.quantity)
    if side is None and active_pos.side in {PositionSide.LONG, PositionSide.SHORT}:
        side = active_pos.side
    metadata: dict[str, Any] = {
        "exchange_position_source": "stop_post_check",
        "exchange_position_side": None if side is None else side.value,
        "exchange_position_native_quantity": native_qty,
        "exchange_position_entry_price": active_pos.entry_price,
    }
    try:
        metadata["exchange_position_base_quantity"] = converter.native_to_base_quantity(
            exchange=exchange,
            symbol=symbol,
            native_quantity=native_qty,
            market_profile=market_profile,
        )
    except Exception as exc:
        logger.warning(
            "Stop post-check exchange position quantity conversion failed | exchange=%s symbol=%s native_quantity=%s error=%s",
            exchange.value,
            symbol,
            native_qty,
            exc,
        )
        metadata["exchange_position_base_quantity_convert_error"] = str(exc)
    return metadata

def _position_side_label(position: Position) -> str:
    side = _position_side_from_quantity(position.quantity)
    if side is PositionSide.LONG:
        return "long"
    if side is PositionSide.SHORT:
        return "short"
    return "flat"

async def _jittered_sleep(stop_event: asyncio.Event, interval_seconds: float) -> None:
    import random
    jitter = random.uniform(0, min(5.0, interval_seconds * 0.1))
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds + jitter)
    except asyncio.TimeoutError:
        pass

__all__ = ['_account_snapshot_log_keepalive_seconds_from_env', '_active_exchange_positions', '_all_exchange_sandbox', '_event_time_ms', '_exchange_position_matches_long', '_exchange_position_matches_short', '_exchange_position_metadata', '_exchange_positions_matching_strategy_position', '_fetch_execution_instrument_rule', '_is_trade_at_or_before', '_jittered_sleep', '_place_stop_scope_covers', '_position_side_for_strategy_position', '_position_side_from_quantity', '_position_side_label', '_raise_ambiguous_exchange_positions', '_signal_position_id', '_single_active_exchange_position_or_none_for_legacy', '_stop_post_check_attempts_from_env', '_stop_post_check_delay_from_env', '_strategy_position_active_exchanges', '_strategy_position_for_stop_signal', '_strategy_position_native_quantity', '_strategy_position_requires_protective_stop', '_strategy_position_stop_order_ids']
