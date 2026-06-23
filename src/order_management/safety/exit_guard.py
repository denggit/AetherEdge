from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.order_management.quantity import NativeQuantityConverter
from src.platform.exchanges.models import (
    ExchangeName,
    OrderRequest,
    OrderSide,
    Position,
    PositionMode,
    PositionSide,
    StopMarketOrderRequest,
)
from src.platform.markets import MarketProfile
from src.signals.models import SignalAction


_LONG_EXIT_ACTIONS = {
    "close_long",
    "reduce_long",
    "place_stop_loss_long",
    "take_profit_long",
    "trailing_stop_long",
}
_SHORT_EXIT_ACTIONS = {
    "close_short",
    "reduce_short",
    "place_stop_loss_short",
    "take_profit_short",
    "trailing_stop_short",
}
_OTHER_EXIT_ACTIONS = {
    "stop_sync",
    "follower_close_after_master_close",
    "recovery_close",
    "recovery close",
    "manual_close_signal",
    "manual close signal",
}
_EXIT_ACTIONS = _LONG_EXIT_ACTIONS | _SHORT_EXIT_ACTIONS | _OTHER_EXIT_ACTIONS


class ExitSafetyError(ValueError):
    def __init__(self, reason: str, *, metadata: Mapping[str, Any] | None = None) -> None:
        self.reason = reason
        self.metadata = dict(metadata or {})
        super().__init__(reason)


@dataclass(frozen=True)
class ExitSafetyReport:
    exchange: ExchangeName
    symbol: str
    action: str
    side: OrderSide
    base_quantity: Decimal | None
    native_quantity: Decimal | None
    contract_value: Decimal | None
    current_position_base_quantity: Decimal
    current_position_native_quantity: Decimal
    reduce_only: bool
    close_position: bool
    position_side: PositionSide | None
    position_mode: PositionMode
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def as_log_fields(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange.value,
            "symbol": self.symbol,
            "action": self.action,
            "side": self.side.value,
            "base_quantity": _decimal_or_none(self.base_quantity),
            "native_quantity": _decimal_or_none(self.native_quantity),
            "contract_value": _decimal_or_none(self.contract_value),
            "current_position_base_quantity": _decimal_text(self.current_position_base_quantity),
            "current_position_native_quantity": _decimal_text(self.current_position_native_quantity),
            "reduce_only": self.reduce_only,
            "close_position": self.close_position,
            "position_side": None if self.position_side is None else self.position_side.value,
            "position_mode": self.position_mode.value,
            **dict(self.metadata),
        }


@dataclass(frozen=True)
class ExchangeExitNormalization:
    request: OrderRequest | StopMarketOrderRequest
    metadata: Mapping[str, Any] = field(default_factory=dict)


class ExitSafetyGuard:
    def __init__(
        self,
        *,
        quantity_converter: NativeQuantityConverter | None = None,
        tolerance: Decimal = Decimal("1.05"),
    ) -> None:
        if tolerance < Decimal("1"):
            raise ValueError("tolerance must be >= 1")
        self.quantity_converter = quantity_converter or NativeQuantityConverter()
        self.tolerance = tolerance

    def normalize_order(
        self,
        *,
        exchange: ExchangeName | str,
        action: SignalAction | str,
        request: OrderRequest,
        position_mode: PositionMode,
        positions: Sequence[Position],
        market_profile: MarketProfile,
    ) -> tuple[OrderRequest, ExitSafetyReport | None]:
        exchange_name = _exchange_name(exchange)
        action_value = _action_value(action)
        target_side = target_position_side_for_action(action_value)
        self._ensure_existing_position_side_safe(
            exchange=exchange_name,
            action=action_value,
            request=request,
            position_mode=position_mode,
            target_side=target_side,
        )
        normalized = replace(
            request,
            position_side=_request_position_side(
                exchange=exchange_name,
                position_mode=position_mode,
                target_side=target_side,
                existing=request.position_side,
            ),
        )
        if not is_exit_action(action_value):
            return normalized, None

        if not normalized.reduce_only:
            raise self._error(
                "exit_order_without_reduce_only_or_close_position",
                exchange=exchange_name,
                action=action_value,
                request=normalized,
                position_mode=position_mode,
            )
        report = self._validate_exit(
            exchange=exchange_name,
            action=action_value,
            request=normalized,
            position_mode=position_mode,
            positions=positions,
            market_profile=market_profile,
            close_position=False,
        )
        return normalized, report

    def normalize_stop_market(
        self,
        *,
        exchange: ExchangeName | str,
        action: SignalAction | str,
        request: StopMarketOrderRequest,
        position_mode: PositionMode,
        positions: Sequence[Position],
        market_profile: MarketProfile,
    ) -> tuple[StopMarketOrderRequest, ExitSafetyReport | None]:
        exchange_name = _exchange_name(exchange)
        action_value = _action_value(action)
        target_side = target_position_side_for_action(action_value)
        self._ensure_existing_position_side_safe(
            exchange=exchange_name,
            action=action_value,
            request=request,
            position_mode=position_mode,
            target_side=target_side,
        )
        normalized = replace(
            request,
            position_side=_request_position_side(
                exchange=exchange_name,
                position_mode=position_mode,
                target_side=target_side,
                existing=request.position_side,
            ),
        )
        if not is_exit_action(action_value):
            return normalized, None

        if not normalized.reduce_only and not normalized.close_position:
            raise self._error(
                "exit_order_without_reduce_only_or_close_position",
                exchange=exchange_name,
                action=action_value,
                request=normalized,
                position_mode=position_mode,
            )
        report = self._validate_exit(
            exchange=exchange_name,
            action=action_value,
            request=normalized,
            position_mode=position_mode,
            positions=positions,
            market_profile=market_profile,
            close_position=normalized.close_position,
        )
        return normalized, report

    def _validate_exit(
        self,
        *,
        exchange: ExchangeName,
        action: str,
        request: OrderRequest | StopMarketOrderRequest,
        position_mode: PositionMode,
        positions: Sequence[Position],
        market_profile: MarketProfile,
        close_position: bool,
    ) -> ExitSafetyReport:
        target_side = target_position_side_for_action(action)
        if target_side is None:
            target_side = _target_side_from_request_side(request.side)
        if target_side is None:
            raise self._error(
                "exit_order_position_side_unknown",
                exchange=exchange,
                action=action,
                request=request,
                position_mode=position_mode,
            )

        if position_mode is PositionMode.HEDGE and request.position_side is not target_side:
            raise self._error(
                "exit_order_wrong_position_side",
                exchange=exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                expected_position_side=target_side.value,
            )
        if request.position_side not in {None, PositionSide.BOTH, target_side}:
            raise self._error(
                "exit_order_wrong_position_side",
                exchange=exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                expected_position_side=target_side.value,
            )

        current_native = _current_position_native_quantity(positions, side=target_side, symbol=request.symbol)
        if current_native <= 0:
            raise self._error(
                "stop_order_without_existing_position" if isinstance(request, StopMarketOrderRequest) else "exit_order_without_existing_position",
                exchange=exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                target_position_side=target_side.value,
            )
        current_base = self.quantity_converter.native_to_base_quantity(
            exchange=exchange,
            symbol=request.symbol,
            native_quantity=current_native,
            market_profile=market_profile,
        )
        base_quantity = request.quantity
        native_quantity = None
        if base_quantity is not None:
            conversion = self.quantity_converter.convert_quantity(
                exchange=exchange,
                symbol=request.symbol,
                base_quantity=base_quantity,
                market_profile=market_profile,
            )
            native_quantity = conversion.native_quantity
            if base_quantity > current_base * self.tolerance or native_quantity > current_native * self.tolerance:
                raise self._error(
                    _exceeds_reason(action),
                    exchange=exchange,
                    action=action,
                    request=request,
                    position_mode=position_mode,
                    target_position_side=target_side.value,
                    base_quantity=str(base_quantity),
                    native_quantity=str(native_quantity),
                    current_position_base_quantity=str(current_base),
                    current_position_native_quantity=str(current_native),
                    tolerance=str(self.tolerance),
                )
        elif not close_position:
            raise self._error(
                "exit_order_quantity_missing",
                exchange=exchange,
                action=action,
                request=request,
                position_mode=position_mode,
            )

        return ExitSafetyReport(
            exchange=exchange,
            symbol=request.symbol,
            action=action,
            side=request.side,
            base_quantity=base_quantity,
            native_quantity=native_quantity,
            contract_value=market_profile.contract_value(exchange),
            current_position_base_quantity=current_base,
            current_position_native_quantity=current_native,
            reduce_only=getattr(request, "reduce_only", False),
            close_position=close_position,
            position_side=request.position_side,
            position_mode=position_mode,
        )

    def _ensure_existing_position_side_safe(
        self,
        *,
        exchange: ExchangeName,
        action: str,
        request: OrderRequest | StopMarketOrderRequest,
        position_mode: PositionMode,
        target_side: PositionSide | None,
    ) -> None:
        if target_side is None or request.position_side in {None, PositionSide.BOTH, target_side}:
            return
        raise self._error(
            "exit_order_wrong_position_side" if is_exit_action(action) else "order_wrong_position_side",
            exchange=exchange,
            action=action,
            request=request,
            position_mode=position_mode,
            expected_position_side=target_side.value,
        )

    def _error(
        self,
        reason: str,
        *,
        exchange: ExchangeName,
        action: str,
        request: OrderRequest | StopMarketOrderRequest,
        position_mode: PositionMode,
        **metadata: Any,
    ) -> ExitSafetyError:
        return ExitSafetyError(
            reason,
            metadata={
                "exchange": exchange.value,
                "symbol": request.symbol,
                "action": action,
                "side": request.side.value,
                "base_quantity": None if request.quantity is None else str(request.quantity),
                "reduce_only": getattr(request, "reduce_only", False),
                "close_position": getattr(request, "close_position", False),
                "position_side": None if request.position_side is None else request.position_side.value,
                "position_mode": position_mode.value,
                **metadata,
            },
        )


def is_exit_action(action: SignalAction | str) -> bool:
    return _action_value(action) in _EXIT_ACTIONS


def target_position_side_for_action(action: SignalAction | str) -> PositionSide | None:
    value = _action_value(action)
    if value in _LONG_EXIT_ACTIONS or value == "open_long":
        return PositionSide.LONG
    if value in _SHORT_EXIT_ACTIONS or value == "open_short":
        return PositionSide.SHORT
    return None


def normalize_exit_request_for_exchange(
    *,
    exchange: ExchangeName | str,
    action: SignalAction | str,
    request: OrderRequest | StopMarketOrderRequest,
    position_mode: PositionMode,
    safety_report: ExitSafetyReport | None,
) -> ExchangeExitNormalization:
    exchange_name = _exchange_name(exchange)
    action_value = _action_value(action)
    if exchange_name.value != "binance" or position_mode is not PositionMode.HEDGE or not is_exit_action(action_value):
        return ExchangeExitNormalization(request=request)
    if safety_report is None:
        raise ExitSafetyError(
            "exit_safety_report_required_for_binance_hedge_exit",
            metadata={
                "exchange": exchange_name.value,
                "symbol": request.symbol,
                "action": action_value,
                "position_mode": position_mode.value,
            },
        )
    if request.position_side not in {PositionSide.LONG, PositionSide.SHORT}:
        raise ExitSafetyError(
            "exit_order_position_side_unknown",
            metadata={
                "exchange": exchange_name.value,
                "symbol": request.symbol,
                "action": action_value,
                "side": request.side.value,
                "position_side": None if request.position_side is None else request.position_side.value,
                "position_mode": position_mode.value,
            },
        )
    if safety_report.base_quantity is not None and safety_report.base_quantity > safety_report.current_position_base_quantity:
        raise ExitSafetyError(
            _exceeds_reason(action_value),
            metadata={
                "exchange": exchange_name.value,
                "symbol": request.symbol,
                "action": action_value,
                "side": request.side.value,
                "base_quantity": str(safety_report.base_quantity),
                "current_position_base_quantity": str(safety_report.current_position_base_quantity),
                "position_side": request.position_side.value,
                "position_mode": position_mode.value,
            },
        )

    metadata = {
        "exchange": exchange_name.value,
        "position_mode": position_mode.value,
        "action": action_value,
        "position_side": request.position_side.value,
        "side": request.side.value,
        "base_quantity": _decimal_or_none(safety_report.base_quantity),
        "current_position_base_quantity": _decimal_text(safety_report.current_position_base_quantity),
        "reduce_only_requested": getattr(request, "reduce_only", False),
        "reduce_only_sent": False,
        "exit_safety_equivalent_reduce_only": True,
        "reduce_only_omitted_reason": "binance_hedge_mode_api_constraint",
        "safety_basis": "hedge_position_side_plus_local_quantity_guard",
    }
    if isinstance(request, StopMarketOrderRequest):
        use_close_position = (
            safety_report.base_quantity is not None
            and safety_report.base_quantity >= safety_report.current_position_base_quantity
        )
        if use_close_position:
            return ExchangeExitNormalization(
                request=replace(request, quantity=None, reduce_only=False, close_position=True),
                metadata={**metadata, "close_position_sent": True, "quantity_sent": False},
            )
        return ExchangeExitNormalization(
            request=replace(request, reduce_only=False, close_position=False),
            metadata={**metadata, "close_position_sent": False, "quantity_sent": request.quantity is not None},
        )
    return ExchangeExitNormalization(
        request=replace(request, reduce_only=False),
        metadata={**metadata, "close_position_sent": False, "quantity_sent": True},
    )


def _request_position_side(
    *,
    exchange: ExchangeName,
    position_mode: PositionMode,
    target_side: PositionSide | None,
    existing: PositionSide | None,
) -> PositionSide | None:
    if target_side is None:
        return existing
    if position_mode is PositionMode.HEDGE:
        return target_side
    if exchange.value in {"okx", "binance"}:
        return None
    return None if existing is PositionSide.BOTH else existing


def _current_position_native_quantity(
    positions: Sequence[Position],
    *,
    side: PositionSide,
    symbol: str,
) -> Decimal:
    total = Decimal("0")
    for position in positions:
        if position.symbol and position.symbol != symbol:
            continue
        position_side = _effective_position_side(position)
        if position_side is side:
            total += abs(position.quantity)
    return total


def _effective_position_side(position: Position) -> PositionSide | None:
    if position.side in {PositionSide.LONG, PositionSide.SHORT}:
        return position.side
    if position.quantity > 0:
        return PositionSide.LONG
    if position.quantity < 0:
        return PositionSide.SHORT
    return None


def _target_side_from_request_side(side: OrderSide) -> PositionSide | None:
    if side is OrderSide.SELL:
        return PositionSide.LONG
    if side is OrderSide.BUY:
        return PositionSide.SHORT
    return None


def _exceeds_reason(action: str) -> str:
    if "stop" in action:
        return "stop_quantity_exceeds_position"
    if "take_profit" in action:
        return "take_profit_quantity_exceeds_position"
    return "exit_order_quantity_exceeding_position"


def _exchange_name(exchange: ExchangeName | str) -> ExchangeName:
    return exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())


def _action_value(action: SignalAction | str) -> str:
    value = action.value if hasattr(action, "value") else str(action)
    return value.strip().lower()


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_text(value)
