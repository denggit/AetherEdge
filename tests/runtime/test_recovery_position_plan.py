from __future__ import annotations

import asyncio
from decimal import Decimal

from src.platform.exchanges.models import Balance, ExchangeName, LeverageInfo, Order, OrderStatus, OrderType, Position, PositionMode, PositionSide
from src.platform.snapshot import PlatformSnapshot
from src.signals import SignalAction
from src.strategy import StrategyRecoveryContext
from strategies.eth_lf_portfolio_v8.strategy import Strategy


def _snapshot(exchange: ExchangeName, positions: list[Position]) -> PlatformSnapshot:
    stop_orders = []
    if exchange is ExchangeName.OKX:
        stop_orders = [
            Order(
                exchange=exchange,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                order_id="stop-1",
                client_order_id=None,
                status=OrderStatus.NEW,
                order_type=OrderType.MARKET,
                price=Decimal("1900"),
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


def _active_plan(target: str = "0.3") -> dict:
    return {
        "position": {
            "position_id": "pos-1",
            "strategy_id": "eth_lf_portfolio_v9c_reclaim_priority",
            "entry_engine": "BULL_RECLAIM_V2",
            "side": "long",
            "status": "active",
            "canonical_stop_price": "1900",
            "master_exchange": "okx",
            "master_target_qty_base": "1.2",
            "master_filled_qty_base": "1.2",
            "created_time_ms": 123,
        },
        "legs": [
            {"position_id": "pos-1", "exchange": "okx", "role": "master", "target_qty_base": "1.2", "filled_qty_base": "1.2"},
            {"position_id": "pos-1", "exchange": "binance", "role": "follower", "target_qty_base": target, "filled_qty_base": "0"},
        ],
    }


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

    signals = asyncio.run(_recover(strategy, okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "1.2")], binance_positions=[], plans=[_active_plan("0.3")]))

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
            okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "1.2")],
            binance_positions=[_position(ExchangeName.BINANCE, PositionSide.LONG, "0.1")],
            plans=[_active_plan("0.3")],
        )
    )

    assert strategy.position.legs["binance"].sync_status == "underfilled"
    assert signals[0].quantity == Decimal("0.2")


def test_recovery_active_master_without_plan_is_manual_required_not_flattened():
    strategy = Strategy()

    signals = asyncio.run(_recover(strategy, okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "1.2")], binance_positions=[], plans=[]))

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.position.legs["okx"].sync_status == "master_active_plan_unknown"
    assert strategy.position.stop_price is None
    assert strategy.recovery_manual_required is True
    assert "master_active_plan_unknown_manual_required" in strategy.recovery_alerts


def test_recovery_reverse_follower_is_manual_required_without_topup():
    strategy = Strategy()

    signals = asyncio.run(
        _recover(
            strategy,
            okx_positions=[_position(ExchangeName.OKX, PositionSide.LONG, "1.2")],
            binance_positions=[_position(ExchangeName.BINANCE, PositionSide.SHORT, "-0.3")],
            plans=[_active_plan("0.3")],
        )
    )

    assert signals == []
    assert strategy.position.legs["binance"].sync_status == "reverse_position_manual_required"
    assert "follower_reverse_position:binance" in strategy.recovery_alerts
