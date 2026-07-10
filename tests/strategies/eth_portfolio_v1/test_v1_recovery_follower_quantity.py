from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    PositionPlan,
    PositionPlanStatus,
    SqlitePositionPlanStore,
)
from src.platform import (
    Balance,
    ExchangeName,
    InstrumentRule,
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
from src.runtime.recovery.service import RuntimeRecoveryService
from src.signals import SignalAction
from src.strategy import StrategyRecoveryContext
from strategies.eth_portfolio_v1.strategy import Strategy


SYMBOL = "ETH-USDT-PERP"
POSITION_ID = "ae-recovery-follower-dust"
TARGET = Decimal("0.06195841427016089856221634149")
FILLED = Decimal("0.061")
STOP_PRICE = Decimal("1738.25")


def test_confirmed_fill_suppresses_theoretical_target_dust_and_keeps_stops() -> None:
    strategy = Strategy()

    signals = _recover(
        strategy,
        plan=_plan(sync_status="topup_failed"),
        actual_follower=FILLED,
    )

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.position.legs["binance"].sync_status == "synced"
    metadata = strategy.position.legs["binance"].metadata
    assert metadata["recovery_quantity_resolution"] == "confirmed_fill"
    assert metadata["raw_target_qty"] == str(TARGET)
    assert metadata["confirmed_filled_qty"] == "0.061"
    assert metadata["actual_exchange_qty"] == "0.061"
    assert metadata["raw_delta"] == str(TARGET - FILLED)
    assert metadata["normalized_delta"] == "0.000"
    assert metadata["quantity_step"] == "0.001"
    assert metadata["min_quantity"] == "0.001"
    assert metadata["reason"] == "non_executable_rounding_dust"
    assert not _actions(signals, SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT)
    assert not _actions(
        signals,
        SignalAction.PLACE_STOP_LOSS_LONG,
        SignalAction.PLACE_STOP_LOSS_SHORT,
        SignalAction.CANCEL_STOP_ORDER,
        SignalAction.CANCEL_ALL_STOP_ORDERS,
    )


def test_confirmed_fill_underfilled_by_one_step_emits_exact_topup() -> None:
    strategy = Strategy()

    signals = _recover(
        strategy,
        plan=_plan(),
        actual_follower=Decimal("0.060"),
    )

    topups = [
        signal
        for signal in signals
        if signal.metadata.get("execution_purpose") == "follower_recovery_topup"
    ]
    assert len(topups) == 1
    assert topups[0].action is SignalAction.OPEN_LONG
    assert topups[0].quantity == Decimal("0.001")
    assert topups[0].metadata["target_exchanges"] == ["binance"]
    assert topups[0].metadata["normalized_delta"] == "0.001"


def test_confirmed_fill_underfilled_by_less_than_step_is_synced_dust() -> None:
    strategy = Strategy()

    signals = _recover(
        strategy,
        plan=_plan(),
        actual_follower=Decimal("0.0605"),
    )

    assert not _actions(signals, SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT)
    assert strategy.position.legs["binance"].sync_status == "synced"
    metadata = strategy.position.legs["binance"].metadata
    assert metadata["raw_delta"] == "0.0005"
    assert metadata["normalized_delta"] == "0.000"
    assert metadata["reason"] == "non_executable_rounding_dust"


def test_never_filled_follower_uses_exchange_normalized_target() -> None:
    strategy = Strategy()

    signals = _recover(
        strategy,
        plan=_plan(filled=Decimal("0"), sync_status="planned"),
        actual_follower=Decimal("0"),
        include_follower_stop=False,
    )

    topups = [
        signal
        for signal in signals
        if signal.metadata.get("execution_purpose") == "follower_recovery_topup"
    ]
    assert len(topups) == 1
    assert topups[0].quantity == Decimal("0.061")
    assert topups[0].quantity != TARGET
    assert topups[0].metadata["recovery_quantity_resolution"] == (
        "normalized_planned_target"
    )


def test_topup_failed_is_persisted_as_synced_and_two_restarts_are_idempotent(
    tmp_path,
) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _seed_plan_store(store, sync_status=LegSyncStatus.TOPUP_FAILED)
    recovery_service = RuntimeRecoveryService(position_plan_store=store)

    for _ in range(2):
        strategy = Strategy()
        plans = list(store.serialize_active_positions())
        signals = _recover(
            strategy,
            plan=plans[0],
            actual_follower=FILLED,
        )
        recovery_service._apply_strategy_position_plan_updates(strategy)

        assert signals == []
        follower = {
            leg.exchange: leg for leg in store.get_legs(POSITION_ID)
        }[ExchangeName.BINANCE]
        assert follower.sync_status is LegSyncStatus.SYNCED
        assert follower.metadata["reason"] == "non_executable_rounding_dust"
        assert store.get_position(POSITION_ID).status is PositionPlanStatus.ACTIVE
        assert follower.stop_order_id == "3000002071794155"


def _recover(
    strategy: Strategy,
    *,
    plan: dict,
    actual_follower: Decimal,
    include_follower_stop: bool = True,
) -> list:
    snapshots = (
        _snapshot(
            exchange=ExchangeName.OKX,
            base_quantity=Decimal("0.07"),
            stop_order_id="3730662544135352320",
        ),
        _snapshot(
            exchange=ExchangeName.BINANCE,
            base_quantity=actual_follower,
            stop_order_id=("3000002071794155" if include_follower_stop else None),
        ),
    )
    return list(
        asyncio.run(
            strategy.recover(
                StrategyRecoveryContext(
                    snapshots=snapshots,
                    reconcile_reports=(),
                    metadata={"active_position_plans": [plan]},
                )
            )
        )
    )


def _plan(
    *,
    filled: Decimal = FILLED,
    sync_status: str = "synced",
) -> dict:
    return {
        "position": {
            "position_id": POSITION_ID,
            "strategy_id": "eth_portfolio_v1",
            "entry_engine": "BULL_RECLAIM_V2",
            "side": "long",
            "status": "active",
            "canonical_stop_price": str(STOP_PRICE),
            "master_exchange": "okx",
            "master_target_qty_base": "0.07",
            "master_filled_qty_base": "0.07",
            "created_time_ms": 1,
            "metadata": {"sleeve_id": "lf", "position_id": POSITION_ID},
        },
        "legs": [
            {
                "position_id": POSITION_ID,
                "exchange": "okx",
                "role": "master",
                "target_qty_base": "0.07",
                "filled_qty_base": "0.07",
                "sync_status": "synced",
                "stop_order_id": "3730662544135352320",
                "stop_price": str(STOP_PRICE),
            },
            {
                "position_id": POSITION_ID,
                "exchange": "binance",
                "role": "follower",
                "target_qty_base": str(TARGET),
                "filled_qty_base": str(filled),
                "sync_status": sync_status,
                "stop_order_id": "3000002071794155",
                "stop_price": str(STOP_PRICE),
            },
        ],
    }


def _snapshot(
    *,
    exchange: ExchangeName,
    base_quantity: Decimal,
    stop_order_id: str | None,
) -> PlatformSnapshot:
    native_quantity = (
        base_quantity / Decimal("0.1")
        if exchange is ExchangeName.OKX
        else base_quantity
    )
    raw_symbol = "ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT"
    positions = (
        ()
        if base_quantity <= 0
        else (
            Position(
                exchange=exchange,
                symbol=SYMBOL,
                raw_symbol=raw_symbol,
                side=PositionSide.LONG,
                quantity=native_quantity,
                entry_price=Decimal("1800"),
            ),
        )
    )
    stops = (
        ()
        if stop_order_id is None
        else (
            Order(
                exchange=exchange,
                symbol=SYMBOL,
                raw_symbol=raw_symbol,
                order_id=stop_order_id,
                client_order_id=f"{POSITION_ID}-{exchange.value}-stop",
                status=OrderStatus.NEW,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                price=STOP_PRICE,
                quantity=native_quantity,
                raw={
                    "reduceOnly": "true",
                    "positionSide": "LONG",
                    "position_id": POSITION_ID,
                    **(
                        {"closePosition": "true"}
                        if exchange is ExchangeName.BINANCE
                        else {}
                    ),
                },
            ),
        )
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
            raw_symbol=raw_symbol,
            leverage=Decimal("15"),
        ),
        position_mode=PositionMode.HEDGE,
        instrument_rule=InstrumentRule(
            exchange=exchange,
            symbol=SYMBOL,
            raw_symbol=raw_symbol,
            price_tick=Decimal("0.01"),
            quantity_step=(
                Decimal("1")
                if exchange is ExchangeName.OKX
                else Decimal("0.001")
            ),
            min_quantity=(
                Decimal("1")
                if exchange is ExchangeName.OKX
                else Decimal("0.001")
            ),
            min_notional=None,
        ),
    )


def _seed_plan_store(
    store: SqlitePositionPlanStore, *, sync_status: LegSyncStatus
) -> None:
    store.upsert_position(
        PositionPlan(
            position_id=POSITION_ID,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=STOP_PRICE,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.07"),
            master_filled_qty_base=Decimal("0.07"),
            created_time_ms=1,
            metadata={"sleeve_id": "lf", "position_id": POSITION_ID},
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id=POSITION_ID,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.07"),
            filled_qty_base=Decimal("0.07"),
            stop_order_id="3730662544135352320",
            stop_price=STOP_PRICE,
            sync_status=LegSyncStatus.SYNCED,
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id=POSITION_ID,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=TARGET,
            filled_qty_base=FILLED,
            stop_order_id="3000002071794155",
            stop_price=STOP_PRICE,
            sync_status=sync_status,
        )
    )


def _actions(signals: list, *actions: SignalAction) -> list:
    return [signal for signal in signals if signal.action in set(actions)]
