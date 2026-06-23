from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management.quantity import NativeQuantityConverter
from src.platform import get_market_profile
from src.platform.exchanges.models import Balance, ExchangeName, LeverageInfo, Order, OrderSide, OrderStatus, OrderType, Position, PositionMode, PositionSide
from src.platform.snapshot import PlatformSnapshot
from src.signals import SignalAction
from src.strategy import StrategyRecoveryContext
from strategies.eth_lf_portfolio_v8.strategy import Strategy


def _snapshot(exchange: ExchangeName, positions: list[Position]) -> PlatformSnapshot:
    stop_orders = []
    if exchange is ExchangeName.OKX:
        qty = abs(positions[0].quantity) if positions else Decimal("0")
        side = OrderSide.SELL if positions and positions[0].side is PositionSide.LONG else OrderSide.BUY
        stop_orders = [
            Order(
                exchange=exchange,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                order_id="stop-1",
                client_order_id="pos-1-stop",
                status=OrderStatus.NEW,
                side=side,
                order_type=OrderType.MARKET,
                price=Decimal("1900"),
                quantity=qty,
                raw={"reduceOnly": "true"},
            )
        ]
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal("10000"), available=Decimal("10000")),
        positions=positions,
        open_orders=[],
        open_stop_orders=stop_orders,
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3")),
        position_mode=PositionMode.ONE_WAY,
    )


def _position(exchange: ExchangeName, side: PositionSide, qty: str) -> Position:
    return Position(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT",
        side=side,
        quantity=Decimal(qty),
        entry_price=Decimal("2000"),
    )


def _active_plan(target: str = "0.3", *, side: str = "long", stop_price: str = "1900", master_qty: str = "1.2") -> dict:
    return {
        "position": {
            "position_id": "pos-1",
            "strategy_id": "eth_lf_portfolio_v9c_reclaim_priority",
            "entry_engine": "BULL_RECLAIM_V2",
            "side": side,
            "status": "active",
            "canonical_stop_price": stop_price,
            "master_exchange": "okx",
            "master_target_qty_base": master_qty,
            "master_filled_qty_base": master_qty,
            "created_time_ms": 123,
        },
        "legs": [
            {"position_id": "pos-1", "exchange": "okx", "role": "master", "target_qty_base": master_qty, "filled_qty_base": master_qty},
            {"position_id": "pos-1", "exchange": "binance", "role": "follower", "target_qty_base": target, "filled_qty_base": "0"},
        ],
    }


def _custom_snapshot(
    exchange: ExchangeName,
    *,
    positions: list[Position],
    open_stop_orders: list[Order] | None = None,
    open_orders: list[Order] | None = None,
    position_mode: PositionMode = PositionMode.ONE_WAY,
) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal("10000"), available=Decimal("10000")),
        positions=positions,
        open_orders=open_orders or [],
        open_stop_orders=open_stop_orders or [],
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT", leverage=Decimal("3")),
        position_mode=position_mode,
    )


def _stop(
    exchange: ExchangeName,
    *,
    side: OrderSide,
    price: str,
    quantity: str,
    client_order_id: str = "pos-1-stop",
    order_id: str | None = None,
    reduce_only: bool = True,
    position_side: str | None = None,
) -> Order:
    raw = {"reduceOnly": "true" if reduce_only else "false"}
    if position_side is not None:
        raw["positionSide" if exchange is ExchangeName.BINANCE else "posSide"] = position_side
    return Order(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT",
        order_id=order_id or f"{exchange.value}-stop",
        client_order_id=client_order_id,
        status=OrderStatus.NEW,
        side=side,
        order_type=OrderType.MARKET,
        price=Decimal(price),
        quantity=Decimal(quantity),
        raw=raw,
    )


async def _recover_with_snapshots(strategy: Strategy, snapshots: tuple[PlatformSnapshot, ...], plans: list[dict]) -> list:
    return list(
        await strategy.recover(
            StrategyRecoveryContext(
                snapshots=snapshots,
                reconcile_reports=(),
                metadata={"active_position_plans": plans},
            )
        )
    )


async def _recover(strategy: Strategy, *, okx_positions: list[Position], binance_positions: list[Position], plans: list[dict]) -> list:
    return list(
        await strategy.recover(
            StrategyRecoveryContext(
                snapshots=(_snapshot(ExchangeName.OKX, okx_positions), _snapshot(ExchangeName.BINANCE, binance_positions)),
                reconcile_reports=(),
                metadata={"active_position_plans": plans},
            )
        )
    )


def test_recovery_hydrates_okx_master_and_topups_missing_follower_from_leg_plan():
    strategy = Strategy()

    signals = asyncio.run(_recover(strategy, okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "12")], binance_positions=[], plans=[_active_plan("0.3")]))

    assert strategy.position.in_pos is True
    assert strategy.position.position_id == "pos-1"
    assert strategy.position.legs["okx"].base_qty == Decimal("1.2")
    assert strategy.position.legs["binance"].sync_status == "missing"
    assert [(signal.action, signal.quantity, signal.metadata["target_exchanges"]) for signal in signals] == [(SignalAction.OPEN_LONG, Decimal("0.3"), ["binance"])]


def test_recovery_underfilled_topup_uses_follower_leg_delta_not_master_quantity():
    strategy = Strategy()

    signals = asyncio.run(
        _recover(
            strategy,
            okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "12")],
            binance_positions=[_position(ExchangeName.BINANCE, PositionSide.LONG, "0.1")],
            plans=[_active_plan("0.3")],
        )
    )

    assert strategy.position.legs["binance"].sync_status == "underfilled"
    topup = next(signal for signal in signals if signal.action is SignalAction.OPEN_LONG)
    assert topup.quantity == Decimal("0.2")


def test_recovery_active_master_without_plan_is_manual_required_not_flattened():
    strategy = Strategy()

    signals = asyncio.run(_recover(strategy, okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "12")], binance_positions=[], plans=[]))

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.position.legs["okx"].sync_status == "master_active_plan_unknown"
    assert strategy.position.stop_price is None
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "master_active_plan_unknown_manual_required" in strategy.recovery_alerts
    assert "active_master_without_position_plan_blocking" in strategy.recovery_alerts


def test_recovery_reverse_follower_is_manual_required_without_topup():
    strategy = Strategy()

    signals = asyncio.run(
        _recover(
            strategy,
            okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "12")],
            binance_positions=[_position(ExchangeName.BINANCE, PositionSide.SHORT, "-0.3")],
            plans=[_active_plan("0.3")],
        )
    )

    assert signals == []
    assert strategy.position.legs["binance"].sync_status == "reverse_position_manual_required"
    assert "follower_reverse_position:binance" in strategy.recovery_alerts


def test_recovery_existing_valid_master_stop_is_kept():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert signals == []
    assert strategy.position.qty == Decimal("0.282")
    assert strategy.position.legs["okx"].native_qty == Decimal("2.82")


def test_recovery_stop_same_price_but_oversized_is_replaced():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="28.2", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[0].metadata["target_exchanges"] == ["okx"]
    assert signals[1].quantity == Decimal("0.282")
    converted = NativeQuantityConverter().convert_quantity(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        base_quantity=signals[1].quantity,
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )
    assert converted.native_quantity == Decimal("2.82")


def test_recovery_active_position_without_exchange_stop_places_new_stop():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    place = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT)
    assert place.quantity == Decimal("0.282")
    assert place.trigger_price == Decimal("1719.40")
    converted = NativeQuantityConverter().convert_quantity(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        base_quantity=place.quantity,
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )
    assert converted.native_quantity == Decimal("2.82")
    assert converted.native_quantity != Decimal("28.2")


def test_recovery_valid_stop_plus_oversized_bot_stop_replaces_all_bot_stops():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="pos-1-stop-valid", order_id="okx-valid-stop", reduce_only=True),
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="28.2", client_order_id="pos-1-stop-oversized", order_id="okx-oversized-stop", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[0].metadata["target_exchanges"] == ["okx"]
    assert signals[1].metadata["target_exchanges"] == ["okx"]
    assert signals[1].quantity == Decimal("0.282")
    converted = NativeQuantityConverter().convert_quantity(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        base_quantity=signals[1].quantity,
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )
    assert converted.native_quantity == Decimal("2.82")


def test_recovery_multiple_valid_bot_stops_are_deduped():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="pos-1-stop-a", order_id="okx-valid-stop-a", reduce_only=True),
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="pos-1-stop-b", order_id="okx-valid-stop-b", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[0].metadata["target_exchanges"] == ["okx"]
    assert signals[1].quantity == Decimal("0.282")


def test_recovery_valid_stop_plus_wrong_side_bot_stop_is_not_considered_safe():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="pos-1-stop-valid", order_id="okx-valid-stop", reduce_only=True),
            _stop(ExchangeName.OKX, side=OrderSide.SELL, price="1719.40", quantity="2.82", client_order_id="pos-1-stop-wrong-side", order_id="okx-wrong-side-stop", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[0].metadata["target_exchanges"] == ["okx"]


def test_recovery_valid_bot_stop_plus_unknown_manual_stop_alerts_but_keeps_bot_stop():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="pos-1-stop-valid", order_id="okx-valid-stop", reduce_only=True),
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="manual-stop", order_id="okx-manual-stop", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert signals == []
    # ── Non-blocking: valid bot stop exists, unknown manual stop is just an alert ──
    assert strategy.recovery_manual_required is False
    assert strategy.recovery_blocking_manual_required is False
    assert any(alert.startswith("unknown_exit_order_manual_required:okx") for alert in strategy.recovery_alerts)


def test_recovery_invalid_bot_stop_plus_unknown_manual_stop_requires_manual_if_precise_cancel_unavailable():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="28.2", client_order_id="pos-1-stop-oversized", order_id="okx-oversized-stop", reduce_only=True),
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="manual-stop", order_id="okx-manual-stop", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert signals == []
    assert strategy.recovery_manual_required is True
    assert any(alert.startswith("unknown_exit_order_manual_required:okx") for alert in strategy.recovery_alerts)
    assert "critical_recovery_exit_order_manual_required:okx:unknown_stop_blocks_cancel_all" in strategy.recovery_alerts


def test_recovery_stop_same_price_but_not_reduce_only_is_replaced():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=False)
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[1].metadata["reduce_only"] is True


def test_recovery_stop_wrong_side_is_replaced():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.SELL, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]


def test_recovery_binance_hedge_stop_wrong_position_side_is_replaced():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(
        ExchangeName.BINANCE,
        positions=[_position(ExchangeName.BINANCE, PositionSide.SHORT, "-0.233")],
        open_stop_orders=[
            _stop(ExchangeName.BINANCE, side=OrderSide.BUY, price="1719.40", quantity="0.233", reduce_only=True, position_side="LONG")
        ],
        position_mode=PositionMode.HEDGE,
    )

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0.233", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    binance_signals = [signal for signal in signals if signal.metadata.get("target_exchanges") == ["binance"]]
    assert [signal.action for signal in binance_signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert binance_signals[1].quantity == Decimal("0.233")


def test_recovery_follower_missing_does_not_place_follower_stop():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(
        ExchangeName.BINANCE,
        positions=[],
        open_stop_orders=[
            _stop(ExchangeName.BINANCE, side=OrderSide.BUY, price="1719.40", quantity="0.233", reduce_only=True, position_side="SHORT")
        ],
        position_mode=PositionMode.HEDGE,
    )

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0.233", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT and signal.metadata.get("target_exchanges") == ["binance"] for signal in signals)
    assert any(signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS and signal.metadata.get("target_exchanges") == ["binance"] for signal in signals)
    assert strategy.position.in_pos is True
    assert strategy.position.legs["binance"].sync_status == "missing"
    assert "follower_missing_manual_required:binance" in strategy.recovery_alerts


def test_recovery_follower_missing_does_not_close_master_when_stop_missing():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0.233", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert not any(signal.action is SignalAction.CLOSE_SHORT and signal.metadata.get("target_exchanges") == ["okx"] for signal in signals)
    assert not any(signal.action is SignalAction.CLOSE_LONG and signal.metadata.get("target_exchanges") == ["okx"] for signal in signals)
    okx_stop = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT and signal.metadata.get("target_exchanges") == ["okx"])
    assert okx_stop.quantity == Decimal("0.282")
    assert strategy.position.in_pos is True
    assert strategy.position.legs["okx"].sync_status == "recovered_master"
    assert strategy.position.legs["binance"].sync_status == "missing"


def test_recovery_follower_missing_bot_stop_with_unknown_stop_is_manual_required_or_precisely_cancelled():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(
        ExchangeName.BINANCE,
        positions=[],
        open_stop_orders=[
            _stop(ExchangeName.BINANCE, side=OrderSide.BUY, price="1719.40", quantity="0.233", client_order_id="pos-1-binance-stop", order_id="binance-bot-stop", reduce_only=True, position_side="SHORT"),
            _stop(ExchangeName.BINANCE, side=OrderSide.BUY, price="1719.40", quantity="0.233", client_order_id="manual-stop", order_id="binance-manual-stop", reduce_only=True, position_side="SHORT"),
        ],
        position_mode=PositionMode.HEDGE,
    )

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0.233", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT and signal.metadata.get("target_exchanges") == ["binance"] for signal in signals)
    assert not any(signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS and signal.metadata.get("target_exchanges") == ["binance"] for signal in signals)
    assert not any(signal.action is SignalAction.CLOSE_SHORT and signal.metadata.get("target_exchanges") == ["okx"] for signal in signals)
    assert strategy.recovery_manual_required is True
    assert strategy.position.in_pos is True
    assert strategy.position.legs["binance"].sync_status == "missing"
    assert any(alert.startswith("unknown_exit_order_manual_required:binance") for alert in strategy.recovery_alerts)
    assert "critical_recovery_exit_order_manual_required:binance:unknown_stop_blocks_cancel_all" in strategy.recovery_alerts


def test_recovery_follower_missing_only_bot_stop_is_cancelled():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(
        ExchangeName.BINANCE,
        positions=[],
        open_stop_orders=[
            _stop(ExchangeName.BINANCE, side=OrderSide.BUY, price="1719.40", quantity="0.233", client_order_id="pos-1-binance-stop", order_id="binance-bot-stop", reduce_only=True, position_side="SHORT")
        ],
        position_mode=PositionMode.HEDGE,
    )

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0.233", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert any(signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS and signal.metadata.get("target_exchanges") == ["binance"] for signal in signals)
    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT and signal.metadata.get("target_exchanges") == ["binance"] for signal in signals)
    assert not any(signal.action is SignalAction.CLOSE_SHORT and signal.metadata.get("target_exchanges") == ["okx"] for signal in signals)
    assert strategy.position.in_pos is True
    assert strategy.position.legs["binance"].sync_status == "missing"


def test_recovery_follower_existing_invalid_stop_is_replaced():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(
        ExchangeName.BINANCE,
        positions=[_position(ExchangeName.BINANCE, PositionSide.SHORT, "-0.233")],
        open_stop_orders=[
            _stop(ExchangeName.BINANCE, side=OrderSide.BUY, price="1719.40", quantity="2.33", reduce_only=True, position_side="SHORT")
        ],
        position_mode=PositionMode.HEDGE,
    )

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0.233", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    binance_signals = [signal for signal in signals if signal.metadata.get("target_exchanges") == ["binance"]]
    assert [signal.action for signal in binance_signals] == [SignalAction.CANCEL_ALL_STOP_ORDERS, SignalAction.PLACE_STOP_LOSS_SHORT]
    assert binance_signals[1].quantity == Decimal("0.233")
    assert strategy.position.in_pos is True


def test_recovery_unknown_manual_stop_does_not_suppress_bot_stop_resync():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82", client_order_id="manual-stop", reduce_only=True)
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(
        _recover_with_snapshots(
            strategy,
            snapshots=(okx_snapshot, binance_snapshot),
            plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
        )
    )

    assert [signal.action for signal in signals] == [SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[0].metadata["target_exchanges"] == ["okx"]
    # ── Non-blocking: unknown manual stop does not prevent bot stop placement ──
    assert strategy.recovery_manual_required is False
    assert strategy.recovery_blocking_manual_required is False
    assert any(alert.startswith("unknown_exit_order_manual_required:okx") for alert in strategy.recovery_alerts)


def test_recovery_active_master_without_plan_remains_manual_required():
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(strategy, snapshots=(okx_snapshot, binance_snapshot), plans=[]))

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "master_active_plan_unknown_manual_required" in strategy.recovery_alerts
    assert "active_master_without_position_plan_blocking" in strategy.recovery_alerts


def test_recovery_active_master_without_plan_still_manual_required():
    strategy = Strategy()

    signals = asyncio.run(_recover(strategy, okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "12")], binance_positions=[], plans=[]))

    assert signals == []
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert not any(signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT} for signal in signals)
    assert not any(signal.action in {SignalAction.CLOSE_LONG, SignalAction.CLOSE_SHORT} for signal in signals)


# ══════════════════════════════════════════════════════════════════════════════
# New tests — Recovery protection postcondition (8.x series)
# ══════════════════════════════════════════════════════════════════════════════

def test_recovery_active_master_without_plan_sets_blocking_flag():
    """8.3: active position + no plan → blocking manual required, runtime must fatal."""
    strategy = Strategy()

    signals = asyncio.run(_recover(
        strategy,
        okx_positions=[_position(ExchangeName.OKX, PositionSide.SHORT, "-2.82")],
        binance_positions=[],
        plans=[],
    ))

    assert signals == []
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "active_master_without_position_plan_blocking" in strategy.recovery_alerts


def test_recovery_unknown_manual_stop_with_active_plan_places_bot_stop_non_blocking():
    """8.1: active position + manual stop + active plan → place bot stop, non-blocking."""
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.0",
                  client_order_id="user-manual-stop", order_id="manual-1", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(
        strategy,
        snapshots=(okx_snapshot, binance_snapshot),
        plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
    ))

    # Must generate PLACE_STOP_LOSS_SHORT signal
    assert [signal.action for signal in signals] == [SignalAction.PLACE_STOP_LOSS_SHORT]
    place = signals[0]
    assert place.quantity == Decimal("0.282")
    assert place.trigger_price == Decimal("1719.40")
    assert place.metadata["target_exchanges"] == ["okx"]

    # Non-blocking: manual stop is just an alert
    assert strategy.recovery_manual_required is False
    assert strategy.recovery_blocking_manual_required is False
    assert any("unknown_exit_order_manual_required:okx" in alert for alert in strategy.recovery_alerts)


def test_recovery_manual_stop_quantity_not_bot_owned_uses_base_quantity():
    """8.7: new stop quantity = 0.282 ETH, native = 2.82 contracts, not 28.2."""
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.0",
                  client_order_id="user-manual-stop", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(
        strategy,
        snapshots=(okx_snapshot, binance_snapshot),
        plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
    ))

    place = signals[0]
    assert place.quantity == Decimal("0.282")
    converted = NativeQuantityConverter().convert_quantity(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        base_quantity=place.quantity,
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )
    assert converted.native_quantity == Decimal("2.82")
    assert converted.native_quantity != Decimal("28.2")


def test_recovery_active_position_no_stop_places_bot_stop():
    """8.2: active position + no stop + active plan → PLACE_STOP_LOSS generated."""
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(
        strategy,
        snapshots=(okx_snapshot, binance_snapshot),
        plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
    ))

    assert [signal.action for signal in signals] == [SignalAction.PLACE_STOP_LOSS_SHORT]
    assert signals[0].quantity == Decimal("0.282")
    assert signals[0].trigger_price == Decimal("1719.40")
    assert strategy.recovery_manual_required is False


def test_recovery_valid_bot_stop_no_signals_needed():
    """8.5: active position + valid bot stop → strategy_signals can be 0."""
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[
            _stop(ExchangeName.OKX, side=OrderSide.BUY, price="1719.40", quantity="2.82",
                  client_order_id="pos-1-stop", reduce_only=True),
        ],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(
        strategy,
        snapshots=(okx_snapshot, binance_snapshot),
        plans=[_active_plan("0", side="short", stop_price="1719.40", master_qty="0.282")],
    ))

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.position.stop_price == Decimal("1719.40")


def test_recovery_side_mismatch_sets_blocking():
    """Side mismatch → falls through to _recover_active_master_without_plan → blocking."""
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(
        strategy,
        snapshots=(okx_snapshot, binance_snapshot),
        plans=[_active_plan("0", side="long", stop_price="1719.40", master_qty="0.282")],
    ))

    assert signals == []
    assert strategy.recovery_blocking_manual_required is True
    assert "master_active_plan_side_mismatch_manual_required" in strategy.recovery_alerts


def test_recovery_missing_canonical_stop_sets_blocking():
    """Missing canonical_stop_price → blocking."""
    strategy = Strategy()
    okx_snapshot = _custom_snapshot(
        ExchangeName.OKX,
        positions=[_position(ExchangeName.OKX, PositionSide.BOTH, "-2.82")],
        open_stop_orders=[],
    )
    binance_snapshot = _custom_snapshot(ExchangeName.BINANCE, positions=[])

    signals = asyncio.run(_recover_with_snapshots(
        strategy,
        snapshots=(okx_snapshot, binance_snapshot),
        plans=[_active_plan("0", side="short", stop_price="", master_qty="0.282")],
    ))

    assert signals == []
    assert strategy.recovery_blocking_manual_required is True
    assert "master_active_plan_missing_canonical_stop_manual_required" in strategy.recovery_alerts
