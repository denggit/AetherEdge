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


# ---------------------------------------------------------------------------
# Protective exit shrink: within tolerance
# ---------------------------------------------------------------------------


def test_take_profit_long_request_10_actual_9_9_shrinks_to_position() -> None:
    """take_profit_long 请求 10，实际 long 9.9 → 裁剪到 9.9"""
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
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.native_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"
    assert report.metadata["requested_base_quantity"] == "10"
    assert report.metadata["shrunk_base_quantity"] == "9.9"
    assert report.metadata["requested_native_quantity"] == "10"
    assert report.metadata["shrunk_native_quantity"] == "9.9"


def test_place_stop_loss_long_stop_market_request_10_actual_9_9_shrinks_to_position() -> None:
    """place_stop_loss_long StopMarket 请求 10，实际 long 9.9 → 裁剪到 9.9"""
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
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.native_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"
    assert report.metadata["requested_base_quantity"] == "10"
    assert report.metadata["shrunk_base_quantity"] == "9.9"


def test_trailing_stop_long_request_10_actual_9_9_shrinks_to_position() -> None:
    """trailing_stop_long 请求 10，实际 long 9.9 → 裁剪到 9.9"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="trailing_stop_long",
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"


# ---------------------------------------------------------------------------
# Protective exit shrink: above tolerance (still shrinks, never rejects)
# ---------------------------------------------------------------------------


def test_take_profit_long_request_20_actual_9_9_shrinks_with_exceeded_tolerance_metadata() -> None:
    """take_profit_long 请求 20，实际 long 9.9，仍裁到 9.9，metadata 有 exceeded_tolerance"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="take_profit_long",
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("20"),
            price=Decimal("3100"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    # 20 > 9.9 * 1.05 = 10.395 → above tolerance, but still shrinks (never reject)
    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"
    assert report.metadata["requested_base_quantity"] == "20"
    assert report.metadata["shrunk_base_quantity"] == "9.9"
    assert report.metadata["quantity_shrink_exceeded_tolerance"] is True
    assert report.metadata["tolerance"] == "1.05"
    assert report.metadata["protective_exit_oversized"] is True


def test_place_stop_loss_long_stop_market_request_20_actual_9_9_shrinks_with_exceeded_tolerance() -> None:
    """place_stop_loss_long StopMarket 请求 20，实际 9.9，仍裁到 9.9 + exceeded_tolerance"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_stop_market(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        request=StopMarketOrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            quantity=Decimal("20"),
            trigger_price=Decimal("2900"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["quantity_shrink_exceeded_tolerance"] is True
    assert report.metadata["protective_exit_oversized"] is True


def test_trailing_stop_short_request_20_actual_9_9_shrinks_with_exceeded_tolerance() -> None:
    """trailing_stop_short 请求 20，实际 short 9.9，仍裁到 9.9 + exceeded_tolerance"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="trailing_stop_short",
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("20"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["quantity_shrink_exceeded_tolerance"] is True


# ---------------------------------------------------------------------------
# Short side protective exit shrink
# ---------------------------------------------------------------------------


def test_take_profit_short_request_10_actual_9_9_shrinks_to_position() -> None:
    """take_profit_short 请求 10，实际 short 9.9 → 裁剪到 9.9"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="take_profit_short",
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10"),
            price=Decimal("1900"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"


def test_place_stop_loss_short_stop_market_request_10_actual_9_9_shrinks_to_position() -> None:
    """place_stop_loss_short StopMarket 请求 10，实际 short 9.9 → 裁剪到 9.9"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_stop_market(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.PLACE_STOP_LOSS_SHORT,
        request=StopMarketOrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.BUY,
            quantity=Decimal("10"),
            trigger_price=Decimal("2100"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.base_quantity == Decimal("9.9")
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"


# ---------------------------------------------------------------------------
# Zero position: protective exits still rejected
# ---------------------------------------------------------------------------


def test_stop_loss_long_zero_position_still_rejected() -> None:
    """当前仓位为 0 时，stop-loss 仍报 stop_order_without_existing_position"""
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_stop_market(
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
            positions=(),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "stop_order_without_existing_position"


def test_take_profit_long_zero_position_still_rejected() -> None:
    """当前仓位为 0 时，take_profit 仍报 exit_order_without_existing_position"""
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
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
            positions=(),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_without_existing_position"


def test_trailing_stop_long_zero_position_still_rejected() -> None:
    """当前仓位为 0 时，trailing_stop 仍报 exit_order_without_existing_position"""
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action="trailing_stop_long",
            request=OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=Decimal("10"),
                reduce_only=True,
            ),
            position_mode=PositionMode.HEDGE,
            positions=(),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_without_existing_position"


# ---------------------------------------------------------------------------
# Active close/reduce: far-above-position still rejected (unchanged behavior)
# ---------------------------------------------------------------------------


def test_close_long_far_above_position_still_rejected() -> None:
    """主动 close_long 大幅超量仍然拒绝（不被本次改动放宽）"""
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
                _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
            ),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_quantity_exceeding_position"


def test_reduce_short_far_above_position_still_rejected() -> None:
    """主动 reduce_short 大幅超量仍然拒绝"""
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action=SignalAction.REDUCE_SHORT,
            request=OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("10.5"),
                reduce_only=True,
            ),
            position_mode=PositionMode.HEDGE,
            positions=(
                _position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-9.9")),
            ),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_quantity_exceeding_position"


# ---------------------------------------------------------------------------
# Binance hedge mode: protective shrink → normalize_exit_request_for_exchange
# ---------------------------------------------------------------------------


def test_binance_hedge_take_profit_shrink_then_normalize_no_error() -> None:
    """Binance hedge mode: protective shrink 后 normalize_exit_request_for_exchange 不报错"""
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
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert report is not None
    assert report.base_quantity == Decimal("9.9")

    # Should NOT raise because base_quantity == current_position_base_quantity after shrink
    exchange_normalized = normalize_exit_request_for_exchange(
        exchange=ExchangeName.BINANCE,
        action="take_profit_long",
        request=request,
        position_mode=PositionMode.HEDGE,
        safety_report=report,
    )
    assert exchange_normalized.request.quantity == Decimal("9.9")
    assert exchange_normalized.request.reduce_only is False


def test_binance_hedge_stop_loss_shrink_then_normalize_uses_close_position() -> None:
    """Binance hedge mode: stop-loss shrink 后 normalize 使用 close_position"""
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
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert report is not None
    assert report.base_quantity == Decimal("9.9")

    # After shrink, base_quantity == current_position_base_quantity → close_position=True
    exchange_normalized = normalize_exit_request_for_exchange(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        request=request,
        position_mode=PositionMode.HEDGE,
        safety_report=report,
    )
    assert exchange_normalized.request.close_position is True
    assert exchange_normalized.request.quantity is None
    assert exchange_normalized.request.reduce_only is False
    assert exchange_normalized.metadata["close_position_sent"] is True


# ---------------------------------------------------------------------------
# OKX hedge mode: protective exits still pass through unchanged
# ---------------------------------------------------------------------------


def test_okx_hedge_take_profit_shrink_still_passes() -> None:
    """OKX hedge mode: take_profit shrink 正常通过 (OKX uses contracts, ctVal=0.1)"""
    guard = ExitSafetyGuard()

    # OKX native position: 99 contracts = 9.9 ETH base (99 * 0.1)
    request, report = guard.normalize_order(
        exchange=ExchangeName.OKX,
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
            _position(ExchangeName.OKX, PositionSide.LONG, Decimal("99")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    # 10 ETH base > 9.9 ETH base (99 * 0.1) → shrunk to 9.9 base
    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert report.metadata["quantity_shrunk_to_position"] is True
    assert report.metadata["shrink_reason"] == "protective_exit_quantity_above_position"
    # OKX hedge: reduce_only stays True (not Binance)
    assert request.reduce_only is True


# ---------------------------------------------------------------------------
# quantity == position: no shrink needed
# ---------------------------------------------------------------------------


def test_take_profit_long_exact_position_no_shrink() -> None:
    """take_profit_long 请求数量 = 实际仓位，不触发 shrink"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="take_profit_long",
        request=OrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("9.9"),
            price=Decimal("3100"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert "quantity_shrunk_to_position" not in report.metadata


def test_stop_loss_long_exact_position_no_shrink() -> None:
    """stop_loss_long StopMarket 请求数量 = 实际仓位，不触发 shrink"""
    guard = ExitSafetyGuard()

    request, report = guard.normalize_stop_market(
        exchange=ExchangeName.BINANCE,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        request=StopMarketOrderRequest(
            symbol="ETH-USDT-PERP",
            side=OrderSide.SELL,
            quantity=Decimal("9.9"),
            trigger_price=Decimal("2900"),
            reduce_only=True,
        ),
        position_mode=PositionMode.HEDGE,
        positions=(
            _position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("9.9")),
        ),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.quantity == Decimal("9.9")
    assert report is not None
    assert "quantity_shrunk_to_position" not in report.metadata
