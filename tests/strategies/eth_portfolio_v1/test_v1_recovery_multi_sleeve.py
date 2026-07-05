from __future__ import annotations

import asyncio
import time
from decimal import Decimal

from src.platform import (
    Balance,
    ExchangeName,
    LeverageInfo,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionMode,
    PositionSide,
)
from src.platform.snapshot import PlatformSnapshot
from src.strategy import StrategyRecoveryContext
from strategies.eth_portfolio_v1.domain.mf_signal import MF_ENGINE_NAME
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.strategy import Strategy


SYMBOL = "ETH-USDT-PERP"


def _plan(
    sleeve: str,
    *,
    quantity: str,
    position_id: str | None = None,
    include_mf_time: bool = True,
) -> dict:
    is_mf = sleeve == "mf"
    position_id = position_id or (
        "mf-low-sweep-time48-1700000000000" if is_mf else "v9e-lf-recovery"
    )
    now_minute = (int(time.time() * 1000) // 60_000) * 60_000
    metadata = {
        "sleeve_id": sleeve,
        "position_id": position_id,
        "engine": MF_ENGINE_NAME if is_mf else "BULL_RECLAIM_V2",
    }
    if is_mf:
        metadata.update(
            {
                "signal_time_ms": now_minute - 49 * 60_000,
                "entry_tradebar_open_time_ms": now_minute - 48 * 60_000,
                "time48_holding_minutes": 48,
                "exit_variant": "time48",
                "quantity_scope": "mf_sleeve_quantity",
                "protective_stop_required": False,
                "average_entry_price": "2000",
            }
        )
        if include_mf_time:
            metadata["entry_execution_time_ms"] = now_minute - 48 * 60_000
    return {
        "position": {
            "position_id": position_id,
            "strategy_id": "eth_portfolio_v1",
            "entry_engine": metadata["engine"],
            "side": "long",
            "status": "active",
            "canonical_stop_price": None if is_mf else "1900",
            "master_exchange": "okx",
            "master_target_qty_base": quantity,
            "master_filled_qty_base": quantity,
            "created_time_ms": now_minute - 48 * 60_000,
            "metadata": metadata,
        },
        "legs": [
            {
                "position_id": position_id,
                "exchange": "okx",
                "role": "master",
                "target_qty_base": quantity,
                "filled_qty_base": quantity,
                "sync_status": "open",
                "stop_order_id": None if is_mf else f"{position_id}-okx-stop",
                "stop_client_order_id": None,
                "stop_price": None if is_mf else "1900",
            },
            {
                "position_id": position_id,
                "exchange": "binance",
                "role": "follower",
                "target_qty_base": quantity,
                "filled_qty_base": quantity,
                "sync_status": "open",
                "stop_order_id": None if is_mf else f"{position_id}-binance-stop",
                "stop_client_order_id": None,
                "stop_price": None if is_mf else "1900",
            },
        ],
    }


def _snapshot(
    exchange: ExchangeName,
    *,
    base_quantity: str,
    lf_plan: dict | None = None,
) -> PlatformSnapshot:
    base = Decimal(base_quantity)
    native = base * Decimal("10") if exchange is ExchangeName.OKX else base
    positions = (
        ()
        if base == 0
        else (
            Position(
                exchange=exchange,
                symbol=SYMBOL,
                raw_symbol=(
                    "ETH-USDT-SWAP"
                    if exchange is ExchangeName.OKX
                    else "ETHUSDT"
                ),
                side=PositionSide.LONG,
                quantity=native,
                entry_price=Decimal("2000"),
            ),
        )
    )
    stops: tuple[Order, ...] = ()
    if lf_plan is not None:
        lf_position = lf_plan["position"]
        lf_leg = next(
            leg
            for leg in lf_plan["legs"]
            if leg["exchange"] == exchange.value
        )
        lf_base = Decimal(lf_leg["filled_qty_base"])
        lf_native = (
            lf_base * Decimal("10")
            if exchange is ExchangeName.OKX
            else lf_base
        )
        stops = (
            Order(
                exchange=exchange,
                symbol=SYMBOL,
                raw_symbol=(
                    "ETH-USDT-SWAP"
                    if exchange is ExchangeName.OKX
                    else "ETHUSDT"
                ),
                order_id=lf_leg["stop_order_id"],
                client_order_id=None,
                status=OrderStatus.NEW,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                price=Decimal("1900"),
                quantity=lf_native,
                raw={
                    "reduceOnly": "true",
                    (
                        "posSide"
                        if exchange is ExchangeName.OKX
                        else "positionSide"
                    ): "long",
                    "position_id": lf_position["position_id"],
                },
            ),
        )
    return PlatformSnapshot(
        symbol=SYMBOL,
        balance=Balance(
            exchange=exchange,
            asset="USDT",
            total=Decimal("10000"),
            available=Decimal("9000"),
        ),
        positions=positions,
        open_orders=(),
        open_stop_orders=stops,
        leverage=LeverageInfo(
            exchange=exchange,
            symbol=SYMBOL,
            raw_symbol=(
                "ETH-USDT-SWAP"
                if exchange is ExchangeName.OKX
                else "ETHUSDT"
            ),
            leverage=Decimal("3"),
        ),
        position_mode=PositionMode.HEDGE,
    )


def _recover(strategy: Strategy, plans: list[dict], *, quantity: str) -> list:
    lf_plan = next(
        (plan for plan in plans if plan["position"]["metadata"]["sleeve_id"] == "lf"),
        None,
    )
    snapshots = (
        _snapshot(ExchangeName.OKX, base_quantity=quantity, lf_plan=lf_plan),
        _snapshot(
            ExchangeName.BINANCE,
            base_quantity=quantity,
            lf_plan=lf_plan,
        ),
    )
    return list(
        asyncio.run(
            strategy.recover(
                StrategyRecoveryContext(
                    snapshots=snapshots,
                    reconcile_reports=(),
                    metadata={"active_position_plans": plans},
                )
            )
        )
    )


def test_lf_only_active_plan_restores_lf_only() -> None:
    strategy = Strategy()
    lf = _plan("lf", quantity="0.6")

    _recover(strategy, [lf], quantity="0.6")

    assert strategy.position.in_pos is True
    assert strategy.position.position_id == "v9e-lf-recovery"
    assert strategy.mf_sleeve.active is False


def test_mf_only_active_plan_restores_mf_only() -> None:
    strategy = Strategy()
    mf = _plan("mf", quantity="0.4")

    _recover(strategy, [mf], quantity="0.4")

    assert strategy.position.in_pos is False
    assert strategy.mf_sleeve.active is True
    assert strategy.mf_sleeve.quantity == Decimal("0.4")
    assert strategy.mf_sleeve.average_entry_price == Decimal("2000")


def test_lf_and_mf_active_plans_restore_both_and_all_snapshots() -> None:
    strategy = Strategy()
    lf = _plan("lf", quantity="0.6")
    mf = _plan("mf", quantity="0.4")

    signals = _recover(strategy, [lf, mf], quantity="1.0")

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.mf_sleeve.active is True
    assert {item.sleeve_id for item in strategy.position_snapshots()} == {
        "lf",
        "mf",
    }
    assert strategy.last_recovery_audit["plans"]["active_count"] == 2
    assert Decimal(
        strategy.last_recovery_audit["exchange"]["okx"]["aggregate_qty"]
    ) == Decimal("1.0")


def test_mf_missing_entry_execution_time_requires_manual_and_blocks() -> None:
    strategy = Strategy()
    mf = _plan("mf", quantity="0.4", include_mf_time=False)

    _recover(strategy, [mf], quantity="0.4")

    assert strategy.mf_sleeve.active is False
    assert strategy.recovery_blocking_manual_required is True
    assert any(
        "mf_missing_metadata:entry_execution_time_ms" in issue
        for issue in strategy.last_recovery_audit["issues"]
    )


def test_duplicated_mf_active_plans_require_manual() -> None:
    strategy = Strategy()
    plans = [
        _plan("mf", quantity="0.2"),
        _plan(
            "mf",
            quantity="0.2",
            position_id="mf-low-sweep-time48-1700000001000",
        ),
    ]

    _recover(strategy, plans, quantity="0.4")

    assert strategy.recovery_blocking_manual_required is True
    assert "duplicated_active_plan:mf" in strategy.last_recovery_audit["issues"]


def test_duplicated_lf_active_plans_require_manual() -> None:
    strategy = Strategy()
    plans = [
        _plan("lf", quantity="0.3"),
        _plan("lf", quantity="0.3", position_id="v9e-lf-recovery-2"),
    ]

    _recover(strategy, plans, quantity="0.6")

    assert strategy.recovery_blocking_manual_required is True
    assert "duplicated_active_plan:lf" in strategy.last_recovery_audit["issues"]


def test_lf_recovery_does_not_overwrite_existing_mf_state() -> None:
    strategy = Strategy()
    strategy.mf_sleeve.reserve_open(
        position_id="mf-low-sweep-time48-existing",
        quantity=Decimal("0.2"),
        signal_time_ms=1,
        entry_execution_time_ms=2,
        tradebar_open_time_ms=2,
    )
    strategy.mf_sleeve.confirm_open(
        quantity=Decimal("0.2"),
        average_entry_price=Decimal("1990"),
        entry_time_ms=2,
    )

    _recover(strategy, [_plan("lf", quantity="0.6")], quantity="0.6")

    assert strategy.mf_sleeve.position_id == "mf-low-sweep-time48-existing"
    assert strategy.mf_sleeve.average_entry_price == Decimal("1990")


def test_mf_recovery_does_not_overwrite_existing_lf_state() -> None:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("1995"),
        qty=Decimal("0.1"),
        stop_price=Decimal("1900"),
        entry_engine="BULL_RECLAIM_V2",
        position_id="existing-lf",
    )

    _recover(strategy, [_plan("mf", quantity="0.4")], quantity="0.4")

    assert strategy.position.position_id == "existing-lf"
    assert strategy.position.avg_entry == Decimal("1995")


def test_mf_holding_state_is_computed_from_recovered_metadata() -> None:
    strategy = Strategy()

    _recover(strategy, [_plan("mf", quantity="0.4")], quantity="0.4")

    mf_audit = strategy.last_recovery_audit["mf"]
    assert mf_audit["holding_minutes_at_recovery"] >= 48
    assert mf_audit["time48_due_at_recovery"] is True


def test_mf_pending_leg_state_requires_manual_confirmation() -> None:
    strategy = Strategy()
    mf = _plan("mf", quantity="0.4")
    mf["legs"][0]["sync_status"] = "planned"

    _recover(strategy, [mf], quantity="0.4")

    assert strategy.recovery_blocking_manual_required is True
    assert any(
        "mf_leg_not_recoverable:okx:planned" in issue
        for issue in strategy.last_recovery_audit["issues"]
    )


def test_exchange_flat_with_local_mf_plan_is_hard_block() -> None:
    strategy = Strategy()

    _recover(strategy, [_plan("mf", quantity="0.4")], quantity="0")

    assert strategy.recovery_blocking_manual_required is True
    assert any(
        "exchange_aggregate_qty_mismatch:okx:long" in issue
        for issue in strategy.last_recovery_audit["issues"]
    )
