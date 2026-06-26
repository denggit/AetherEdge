from __future__ import annotations

import asyncio
from decimal import Decimal

from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderSide, OrderStatus, OrderType, Position, PositionMode, PositionSide
from src.platform.snapshot import PlatformSnapshot
from src.signals import SignalAction
from src.strategy import StrategyRecoveryContext
from strategies.eth_lf_portfolio_v8.strategy import Strategy


CANONICAL_STOP = Decimal("1686.42")
POSITION_ID = "follower-stop-repair"


def test_follower_stop_repair_when_stop_missing() -> None:
    signals, _strategy = _recover_with_follower(open_stop_orders=[])

    place = _single_place_stop(signals)
    assert place.action is SignalAction.PLACE_STOP_LOSS_SHORT
    assert place.metadata["target_exchanges"] == ["binance"]
    assert place.trigger_price == CANONICAL_STOP
    assert place.quantity == Decimal("0.598")
    assert place.metadata["execution_purpose"] == "follower_stop_repair"
    assert place.metadata["canonical_source_exchange"] == "okx"
    assert place.metadata["canonical_stop_price"] == str(CANONICAL_STOP)
    assert place.metadata["follower_position_base_quantity"] == "0.598"


def test_follower_stop_repair_when_stop_under_sized() -> None:
    signals, _strategy = _recover_with_follower(
        open_stop_orders=[_binance_stop(price=CANONICAL_STOP, quantity=Decimal("0.211"))]
    )

    place = _single_place_stop(signals)
    assert place.quantity == Decimal("0.598")
    assert "under_protected" in place.metadata["repair_reason"]
    assert "quantity_too_small" in place.metadata["repair_reason"]


def test_follower_stop_repair_when_stop_price_wrong() -> None:
    signals, _strategy = _recover_with_follower(
        open_stop_orders=[_binance_stop(price=Decimal("1700"), quantity=Decimal("0.598"))]
    )

    place = _single_place_stop(signals)
    assert place.trigger_price == CANONICAL_STOP
    assert "price_mismatch" in place.metadata["repair_reason"]


def test_follower_stop_repair_does_not_change_master_canonical_state() -> None:
    signals, strategy = _recover_with_follower(open_stop_orders=[], follower_entry_price=Decimal("9999"))

    assert _single_place_stop(signals).quantity == Decimal("0.598")
    assert strategy.position.avg_entry == Decimal("1620.30")
    assert strategy.position.stop_price == CANONICAL_STOP
    assert strategy.position.confirmed_stop_price == CANONICAL_STOP


def test_follower_side_mismatch_blocks_auto_repair() -> None:
    signals, strategy = _recover_with_follower(
        follower_quantity=Decimal("0.598"),
        follower_side=PositionSide.BOTH,
        open_stop_orders=[],
    )

    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT for signal in signals)
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert f"follower_position_side_mismatch_manual_required:binance" in strategy.recovery_alerts


def test_follower_stop_repair_uses_exchange_position_quantity_not_local_leg_qty() -> None:
    signals, _strategy = _recover_with_follower(open_stop_orders=[], local_follower_qty=Decimal("0.211"))

    place = _single_place_stop(signals)
    assert place.quantity == Decimal("0.598")
    assert place.metadata["follower_position_base_quantity"] == "0.598"


def test_follower_topup_uses_follower_leg_target_qty_not_master_qty() -> None:
    signals, strategy = _recover_with_follower(
        open_stop_orders=[],
        follower_quantity=Decimal("-0.211"),
        local_follower_qty=Decimal("0.598"),
    )

    topup = _single_follower_topup(signals)
    assert topup.action is SignalAction.OPEN_SHORT
    assert topup.metadata["target_exchanges"] == ["binance"]
    assert topup.metadata["execution_purpose"] == "follower_recovery_topup"
    assert topup.quantity == Decimal("0.387")
    assert topup.quantity != Decimal("0.255")
    assert topup.quantity != Decimal("0.255") - Decimal("0.211")

    assert strategy.position.qty == Decimal("0.255")
    assert _single_place_stop(signals).quantity == Decimal("0.211")


def _recover_with_follower(
    *,
    open_stop_orders: list[Order],
    follower_quantity: Decimal = Decimal("-0.598"),
    follower_side: PositionSide = PositionSide.BOTH,
    follower_entry_price: Decimal = Decimal("1620.30"),
    local_follower_qty: Decimal = Decimal("0.598"),
) -> tuple[tuple, Strategy]:
    strategy = Strategy()
    context = StrategyRecoveryContext(
        snapshots=(
            _snapshot(
                exchange=ExchangeName.OKX,
                position=_position(
                    exchange=ExchangeName.OKX,
                    raw_symbol="ETH-USDT-SWAP",
                    quantity=Decimal("-2.55"),
                    side=PositionSide.BOTH,
                    entry_price=Decimal("1620.30"),
                ),
                open_stop_orders=[_okx_stop()],
                position_mode=PositionMode.ONE_WAY,
            ),
            _snapshot(
                exchange=ExchangeName.BINANCE,
                position=_position(
                    exchange=ExchangeName.BINANCE,
                    raw_symbol="ETHUSDT",
                    quantity=follower_quantity,
                    side=follower_side,
                    entry_price=follower_entry_price,
                ),
                open_stop_orders=open_stop_orders,
                position_mode=PositionMode.ONE_WAY,
            ),
        ),
        reconcile_reports=(),
        metadata={"active_position_plans": [_active_plan(local_follower_qty=local_follower_qty)]},
    )
    signals = tuple(asyncio.run(strategy.recover(context)))
    return signals, strategy


def _single_place_stop(signals: tuple):
    return next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT)


def _single_follower_topup(signals: tuple):
    return next(
        signal
        for signal in signals
        if signal.action is SignalAction.OPEN_SHORT
        and signal.metadata.get("execution_purpose") == "follower_recovery_topup"
        and signal.metadata.get("target_exchanges") == ["binance"]
    )


def _active_plan(*, local_follower_qty: Decimal) -> dict:
    return {
        "position": {
            "status": "active",
            "side": "short",
            "canonical_stop_price": str(CANONICAL_STOP),
            "created_time_ms": 1,
            "entry_engine": "MOMENTUM_V3",
            "position_id": POSITION_ID,
        },
        "legs": [
            {"exchange": "okx", "target_qty_base": "0.255"},
            {"exchange": "binance", "target_qty_base": str(local_follower_qty)},
        ],
    }


def _snapshot(
    *,
    exchange: ExchangeName,
    position: Position,
    open_stop_orders: list[Order],
    position_mode: PositionMode,
) -> PlatformSnapshot:
    raw_symbol = "ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT"
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal("1000"), available=Decimal("900")),
        positions=[position],
        open_orders=[],
        open_stop_orders=open_stop_orders,
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol=raw_symbol, leverage=Decimal("3")),
        position_mode=position_mode,
    )


def _position(
    *,
    exchange: ExchangeName,
    raw_symbol: str,
    quantity: Decimal,
    side: PositionSide,
    entry_price: Decimal,
) -> Position:
    return Position(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol=raw_symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
    )


def _okx_stop() -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="okx-stop",
        client_order_id=f"{POSITION_ID}-okx-stop",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        price=CANONICAL_STOP,
        quantity=Decimal("2.55"),
        raw={"reduceOnly": "true", "position_id": POSITION_ID},
    )


def _binance_stop(*, price: Decimal, quantity: Decimal) -> Order:
    return Order(
        exchange=ExchangeName.BINANCE,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETHUSDT",
        order_id="binance-stop",
        client_order_id=f"{POSITION_ID}-binance-stop",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        price=price,
        quantity=quantity,
        raw={"reduceOnly": "true", "position_id": POSITION_ID},
    )
