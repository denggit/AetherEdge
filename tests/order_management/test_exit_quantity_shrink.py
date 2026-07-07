from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management.safety import (
    ExitSafetyError,
    ExitSafetyGuard,
    normalize_exit_request_for_exchange,
)
from src.platform import (
    ExchangeName,
    OrderSide,
    OrderType,
    Position,
    PositionMode,
    PositionSide,
    get_market_profile,
)
from src.platform.exchanges.models import OrderRequest, StopMarketOrderRequest
from src.signals import SignalAction


def _position(
    exchange: ExchangeName,
    side: PositionSide,
    quantity: Decimal,
) -> Position:
    return Position(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        side=side,
        quantity=quantity,
    )


def test_close_slightly_above_position_shrinks_to_position() -> None:
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.CLOSE_LONG,
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(
                ExchangeName.BINANCE,
                PositionSide.LONG,
                Decimal("9.9"),
            ),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.native_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["requested_base_quantity"] == "10"
    assert report.metadata["shrunk_base_quantity"] == "9.9"
    assert report.metadata["requested_native_quantity"] == "10"
    assert report.metadata["shrunk_native_quantity"] == "9.9"
    assert (
        report.metadata["shrink_reason"]
        == "requested_quantity_slightly_exceeds_position"
    )

    exchange_normalized = normalize_exit_request_for_exchange(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.CLOSE_LONG,
        request=request,
        position_mode=PositionMode.HEDGE,
        safety_report=report,
    )
    assert exchange_normalized.request.quantity == Decimal("9.9")
    assert exchange_normalized.request.reduce_only is False
    assert exchange_normalized.metadata[
        "reduce_only_omitted_reason"
    ] == "binance_hedge_mode_api_constraint"


def test_reduce_slightly_above_position_shrinks_to_position() -> None:
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.REDUCE_SHORT,
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(
                ExchangeName.BINANCE,
                PositionSide.SHORT,
                Decimal("-9.9"),
            ),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.metadata["quantity_shrunk_to_position"] is True


def test_close_far_above_position_is_still_rejected() -> None:
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action=SignalAction.CLOSE_LONG,
            request=OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=Decimal("10.5"),
                reduce_only=True,
            ),
            position_mode=PositionMode.HEDGE,
            positions=(
                _position(
                    ExchangeName.BINANCE,
                    PositionSide.LONG,
                    Decimal("9.9"),
                ),
            ),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_quantity_exceeding_position"


def test_take_profit_slightly_above_position_is_shrunk_to_position() -> None:
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="take_profit_long",
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10"),
            price=Decimal("3100"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(
                ExchangeName.BINANCE,
                PositionSide.LONG,
                Decimal("9.9"),
            ),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"
    assert report.metadata["requested_base_quantity"] == "10"
    assert report.metadata["shrunk_base_quantity"] == "9.9"


def test_stop_loss_stop_market_slightly_above_position_is_shrunk_to_position() -> None:
    guard = ExitSafetyGuard()

    request, report = guard.normalize_stop_market(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        request=StopMarketOrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            quantity=Decimal("10"),
            trigger_price=Decimal("2900"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(
                ExchangeName.BINANCE,
                PositionSide.LONG,
                Decimal("9.9"),
            ),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"
    assert report.metadata["requested_base_quantity"] == "10"
    assert report.metadata["shrunk_base_quantity"] == "9.9"
