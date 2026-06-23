from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.order_management.quantity import NativeQuantityConverter
from src.platform.exchanges.models import ExchangeName, Order, OrderSide, OrderStatus, PositionMode, PositionSide
from src.platform.markets import MarketProfile


_BOT_ID_RE = re.compile(r"^AE[A-Z0-9]{2}(SL|SS|CS|CL|RL|RS)[A-F0-9]{16}$")
_ACTIVE_STATUSES = {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED}


@dataclass(frozen=True)
class RecoveryExitOrderCheck:
    order: Order | None
    bot_owned: bool
    valid: bool
    invalid_reason: str | None
    should_cancel: bool
    should_replace: bool
    expected_side: OrderSide
    expected_position_side: PositionSide | None
    expected_base_quantity: Decimal
    expected_native_quantity: Decimal
    expected_trigger_price: Decimal
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def log_fields(self, *, action: str) -> dict[str, Any]:
        return {
            "exchange": self.metadata.get("exchange"),
            "symbol": self.metadata.get("symbol"),
            "position_side": None if self.expected_position_side is None else self.expected_position_side.value,
            "position_mode": self.metadata.get("position_mode"),
            "current_position_base_quantity": _decimal_text(self.expected_base_quantity),
            "current_position_native_quantity": _decimal_text(self.expected_native_quantity),
            "canonical_stop_price": _decimal_text(self.expected_trigger_price),
            "existing_order_id": None if self.order is None else self.order.order_id,
            "existing_client_order_id": None if self.order is None else self.order.client_order_id,
            "existing_side": None if self.order is None or self.order.side is None else self.order.side.value,
            "existing_quantity": None if self.order is None or self.order.quantity is None else _decimal_text(self.order.quantity),
            "existing_reduce_only": None if self.order is None else _raw_bool(self.order.raw, "reduceOnly", "reduce_only"),
            "existing_close_position": None if self.order is None else _raw_bool(self.order.raw, "closePosition", "close_position"),
            "existing_position_side": None if self.order is None else _raw_position_side(self.order),
            "valid": self.valid,
            "invalid_reason": self.invalid_reason,
            "action": action,
            "bot_owned": self.bot_owned,
        }


@dataclass(frozen=True)
class RecoveryExitValidationResult:
    exchange: ExchangeName
    symbol: str
    position_side: PositionSide
    position_mode: PositionMode
    current_position_base_quantity: Decimal
    current_position_native_quantity: Decimal
    canonical_stop_price: Decimal
    expected_side: OrderSide
    expected_position_side: PositionSide | None
    expected_native_quantity: Decimal
    checks: tuple[RecoveryExitOrderCheck, ...]
    unknown_exit_orders: tuple[Order, ...] = ()
    unsupported_bot_exit_orders: tuple[Order, ...] = ()

    @property
    def valid(self) -> bool:
        return self.has_valid_bot_owned_stop

    @property
    def should_replace(self) -> bool:
        return not self.should_keep_existing_stop

    @property
    def bot_owned_orders(self) -> tuple[Order, ...]:
        return tuple(check.order for check in self.checks if check.order is not None and check.bot_owned)

    @property
    def valid_bot_owned_orders(self) -> tuple[Order, ...]:
        return tuple(check.order for check in self.checks if check.order is not None and check.bot_owned and check.valid)

    @property
    def invalid_bot_owned_orders(self) -> tuple[Order, ...]:
        return tuple(check.order for check in self.checks if check.order is not None and check.bot_owned and not check.valid)

    @property
    def bot_owned_invalid_orders(self) -> tuple[Order, ...]:
        return self.invalid_bot_owned_orders

    @property
    def has_valid_bot_owned_stop(self) -> bool:
        return bool(self.valid_bot_owned_orders)

    @property
    def has_invalid_bot_owned_stop(self) -> bool:
        return bool(self.invalid_bot_owned_orders)

    @property
    def has_unknown_exit_orders(self) -> bool:
        return bool(self.unknown_exit_orders)

    @property
    def should_keep_existing_stop(self) -> bool:
        return self.has_valid_bot_owned_stop and not self.has_invalid_bot_owned_stop and len(self.bot_owned_orders) == 1

    @property
    def should_cancel_bot_owned_stops(self) -> bool:
        return self.should_cancel_and_replace_bot_stops

    @property
    def should_cancel_and_replace_bot_stops(self) -> bool:
        return self.has_invalid_bot_owned_stop or len(self.bot_owned_orders) > 1

    @property
    def should_place_new_stop(self) -> bool:
        return not self.has_valid_bot_owned_stop and not self.has_invalid_bot_owned_stop

    @property
    def primary_invalid_reason(self) -> str | None:
        if self.has_invalid_bot_owned_stop:
            return "invalid_bot_owned_stop_present"
        if len(self.valid_bot_owned_orders) > 1:
            return "duplicate_valid_bot_owned_stop"
        if self.should_keep_existing_stop:
            return None
        for check in self.checks:
            if check.invalid_reason:
                return check.invalid_reason
        return "missing_bot_owned_stop"


class RecoveryExitOrderValidator:
    """Validate open recovery exit orders against the active exchange position.

    Strategy state and plans use base-asset quantities. Exchange snapshots can
    report native quantities, so every comparison keeps both native and base
    values explicit.
    """

    def __init__(
        self,
        *,
        quantity_converter: NativeQuantityConverter | None = None,
        quantity_tolerance: Decimal = Decimal("0.05"),
        price_tolerance: Decimal = Decimal("0"),
    ) -> None:
        self.quantity_converter = quantity_converter or NativeQuantityConverter()
        self.quantity_tolerance = quantity_tolerance
        self.price_tolerance = price_tolerance

    def validate_stop_orders(
        self,
        *,
        exchange: ExchangeName | str,
        symbol: str,
        strategy_id: str,
        position_id: str | None,
        position_side: PositionSide,
        position_mode: PositionMode,
        current_position_native_quantity: Decimal,
        canonical_stop_price: Decimal,
        open_stop_orders: Sequence[Order],
        market_profile: MarketProfile,
        open_orders: Sequence[Order] = (),
    ) -> RecoveryExitValidationResult:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        current_position_native_quantity = abs(current_position_native_quantity)
        current_position_base_quantity = self.quantity_converter.native_to_base_quantity(
            exchange=exchange_name,
            symbol=symbol,
            native_quantity=current_position_native_quantity,
            market_profile=market_profile,
        )
        expected_side = _expected_close_side(position_side)
        expected_position_side = position_side if position_mode is PositionMode.HEDGE else None
        checks: list[RecoveryExitOrderCheck] = []
        for order in open_stop_orders:
            if order.exchange != exchange_name or order.symbol != symbol:
                checks.append(
                    self._check(
                        order=order,
                        bot_owned=False,
                        invalid_reason="exchange_or_symbol_mismatch",
                        valid=False,
                        should_cancel=False,
                        should_replace=False,
                        exchange=exchange_name,
                        symbol=symbol,
                        position_mode=position_mode,
                        expected_side=expected_side,
                        expected_position_side=expected_position_side,
                        expected_base_quantity=current_position_base_quantity,
                        expected_native_quantity=current_position_native_quantity,
                        expected_trigger_price=canonical_stop_price,
                    )
                )
                continue
            bot_owned = is_bot_owned_order(order=order, strategy_id=strategy_id, position_id=position_id)
            reason = self._invalid_reason(
                order=order,
                exchange=exchange_name,
                symbol=symbol,
                position_mode=position_mode,
                expected_side=expected_side,
                expected_position_side=expected_position_side,
                expected_native_quantity=current_position_native_quantity,
                canonical_stop_price=canonical_stop_price,
                bot_owned=bot_owned,
            )
            checks.append(
                self._check(
                    order=order,
                    bot_owned=bot_owned,
                    invalid_reason=reason,
                    valid=reason is None,
                    should_cancel=bot_owned and reason is not None,
                    should_replace=reason is not None,
                    exchange=exchange_name,
                    symbol=symbol,
                    position_mode=position_mode,
                    expected_side=expected_side,
                    expected_position_side=expected_position_side,
                    expected_base_quantity=current_position_base_quantity,
                    expected_native_quantity=current_position_native_quantity,
                    expected_trigger_price=canonical_stop_price,
                )
            )
        if not checks:
            checks.append(
                self._check(
                    order=None,
                    bot_owned=False,
                    invalid_reason="missing_bot_owned_stop",
                    valid=False,
                    should_cancel=False,
                    should_replace=True,
                    exchange=exchange_name,
                    symbol=symbol,
                    position_mode=position_mode,
                    expected_side=expected_side,
                    expected_position_side=expected_position_side,
                    expected_base_quantity=current_position_base_quantity,
                    expected_native_quantity=current_position_native_quantity,
                    expected_trigger_price=canonical_stop_price,
                )
            )
        unknown_exit_orders = tuple(
            order
            for order in open_stop_orders
            if order.exchange == exchange_name
            and order.symbol == symbol
            and not is_bot_owned_order(order=order, strategy_id=strategy_id, position_id=position_id)
        )
        unsupported_bot_exit_orders = tuple(
            order
            for order in open_orders
            if order.exchange == exchange_name
            and order.symbol == symbol
            and is_bot_owned_order(order=order, strategy_id=strategy_id, position_id=position_id)
            and _looks_like_unsupported_exit_order(order)
        )
        return RecoveryExitValidationResult(
            exchange=exchange_name,
            symbol=symbol,
            position_side=position_side,
            position_mode=position_mode,
            current_position_base_quantity=current_position_base_quantity,
            current_position_native_quantity=current_position_native_quantity,
            canonical_stop_price=canonical_stop_price,
            expected_side=expected_side,
            expected_position_side=expected_position_side,
            expected_native_quantity=current_position_native_quantity,
            checks=tuple(checks),
            unknown_exit_orders=unknown_exit_orders,
            unsupported_bot_exit_orders=unsupported_bot_exit_orders,
        )

    def _invalid_reason(
        self,
        *,
        order: Order,
        exchange: ExchangeName,
        symbol: str,
        position_mode: PositionMode,
        expected_side: OrderSide,
        expected_position_side: PositionSide | None,
        expected_native_quantity: Decimal,
        canonical_stop_price: Decimal,
        bot_owned: bool,
    ) -> str | None:
        if not bot_owned:
            return "unknown_manual_exit_order"
        if order.status not in _ACTIVE_STATUSES:
            return "order_not_active"
        if order.exchange != exchange or order.symbol != symbol:
            return "exchange_or_symbol_mismatch"
        if order.price is None or abs(order.price - canonical_stop_price) > self.price_tolerance:
            return "trigger_price_mismatch"
        if order.side is not expected_side:
            return "wrong_side"
        close_position = _raw_bool(order.raw, "closePosition", "close_position")
        quantity = order.quantity
        if not close_position:
            if quantity is None or quantity <= 0:
                return "quantity_missing"
            lower = expected_native_quantity * (Decimal("1") - self.quantity_tolerance)
            upper = expected_native_quantity * (Decimal("1") + self.quantity_tolerance)
            if quantity > upper:
                return "quantity_exceeds_position"
            if quantity < lower:
                return "quantity_below_position"
        raw_position_side = _raw_position_side(order)
        if position_mode is PositionMode.HEDGE:
            expected_raw = None if expected_position_side is None else expected_position_side.value
            if raw_position_side != expected_raw:
                return "wrong_position_side"
        reduce_only = _raw_bool(order.raw, "reduceOnly", "reduce_only")
        equivalent_reduce_only = _raw_bool(order.raw, "exit_safety_equivalent_reduce_only")
        if exchange.value == "binance" and position_mode is PositionMode.HEDGE:
            if not (reduce_only or close_position or equivalent_reduce_only):
                return "not_reduce_only"
            return None
        if not (reduce_only or close_position):
            return "not_reduce_only"
        return None

    def _check(
        self,
        *,
        order: Order | None,
        bot_owned: bool,
        invalid_reason: str | None,
        valid: bool,
        should_cancel: bool,
        should_replace: bool,
        exchange: ExchangeName,
        symbol: str,
        position_mode: PositionMode,
        expected_side: OrderSide,
        expected_position_side: PositionSide | None,
        expected_base_quantity: Decimal,
        expected_native_quantity: Decimal,
        expected_trigger_price: Decimal,
    ) -> RecoveryExitOrderCheck:
        return RecoveryExitOrderCheck(
            order=order,
            bot_owned=bot_owned,
            valid=valid,
            invalid_reason=invalid_reason,
            should_cancel=should_cancel,
            should_replace=should_replace,
            expected_side=expected_side,
            expected_position_side=expected_position_side,
            expected_base_quantity=expected_base_quantity,
            expected_native_quantity=expected_native_quantity,
            expected_trigger_price=expected_trigger_price,
            metadata={"exchange": exchange.value, "symbol": symbol, "position_mode": position_mode.value},
        )


def is_bot_owned_order(*, order: Order, strategy_id: str, position_id: str | None) -> bool:
    identifiers = [order.client_order_id, order.order_id]
    raw = dict(order.raw)
    for key in ("clientAlgoId", "algoClOrdId", "clOrdId", "clientOrderId", "newClientOrderId"):
        value = raw.get(key)
        if value:
            identifiers.append(str(value))
    strategy_id = str(strategy_id or "").lower()
    position_id = str(position_id or "").lower()
    for value in identifiers:
        text = str(value or "")
        lowered = text.lower()
        if strategy_id and strategy_id in lowered:
            return True
        if position_id and position_id in lowered:
            return True
        if _BOT_ID_RE.match(text.upper()):
            return True
    for key in ("strategy_id", "strategyId", "position_id", "positionId", "source"):
        value = raw.get(key)
        if value is None:
            continue
        lowered = str(value).lower()
        if strategy_id and strategy_id in lowered:
            return True
        if position_id and position_id in lowered:
            return True
        if lowered in {"aetheredge", "aether_edge"}:
            return True
    return False


def _expected_close_side(position_side: PositionSide) -> OrderSide:
    if position_side is PositionSide.LONG:
        return OrderSide.SELL
    if position_side is PositionSide.SHORT:
        return OrderSide.BUY
    raise ValueError("position_side must be LONG or SHORT")


def _raw_bool(raw: Mapping[str, Any], *keys: str) -> bool:
    for key in keys:
        if key not in raw:
            continue
        value = raw.get(key)
        if isinstance(value, bool):
            return value
        if str(value).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    return False


def _raw_position_side(order: Order) -> str | None:
    for key in ("positionSide", "posSide", "position_side"):
        value = order.raw.get(key)
        if value not in (None, ""):
            text = str(value).strip().lower()
            if text in {"long", "short"}:
                return text
            if text in {"both", "net"}:
                return "both"
    return None


def _looks_like_unsupported_exit_order(order: Order) -> bool:
    raw_text = " ".join(str(value).lower() for value in order.raw.values())
    order_type = "" if order.order_type is None else order.order_type.value.lower()
    return any(token in raw_text or token in order_type for token in ("take_profit", "take-profit", "trailing", "trail"))


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")
