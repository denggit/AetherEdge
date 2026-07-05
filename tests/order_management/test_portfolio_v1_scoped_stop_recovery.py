from __future__ import annotations

from decimal import Decimal

from src.order_management.quantity import NativeQuantityConverter
from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    PositionPlan,
    PositionPlanStatus,
    SqlitePositionPlanStore,
)
from src.order_management.models import ExchangeOrderResult
from src.order_management.safety import (
    RecoveryExitOrderValidator,
    filter_orders_for_position_scope,
)
from src.platform import (
    ExchangeName,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionMode,
    PositionSide,
    get_market_profile,
)
from src.signals import SignalAction, TradeSignal


SYMBOL = "ETH-USDT-PERP"
LF_POSITION = "v9e-lf-scoped"
MF_POSITION = "mf-low-sweep-time48-scoped"


def _stop(
    position_id: str,
    *,
    quantity: str = "6",
    position_side: str = "long",
    reduce_only: bool = True,
) -> Order:
    raw = {
        "position_id": position_id,
        "posSide": position_side,
    }
    if reduce_only:
        raw["reduceOnly"] = "true"
    return Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id=f"{position_id}-stop",
        client_order_id=None,
        status=OrderStatus.NEW,
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        price=Decimal("1900"),
        quantity=Decimal(quantity),
        raw=raw,
    )


def _validate(position_id: str, orders: tuple[Order, ...]):
    scoped = filter_orders_for_position_scope(
        orders,
        position_id=position_id,
    )
    return RecoveryExitOrderValidator(
        quantity_converter=NativeQuantityConverter()
    ).validate_stop_orders(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=position_id,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=Decimal("1900"),
        open_stop_orders=scoped,
        market_profile=get_market_profile(SYMBOL),
    )


def test_lf_stop_does_not_satisfy_mf_scope() -> None:
    result = _validate(MF_POSITION, (_stop(LF_POSITION),))

    assert result.should_keep_existing_stop is False
    assert result.primary_invalid_reason == "missing_bot_owned_stop"


def test_mf_stop_does_not_satisfy_lf_scope() -> None:
    result = _validate(LF_POSITION, (_stop(MF_POSITION),))

    assert result.should_keep_existing_stop is False
    assert result.primary_invalid_reason == "missing_bot_owned_stop"


def test_both_scoped_stops_validate_independently() -> None:
    orders = (_stop(LF_POSITION), _stop(MF_POSITION))

    assert _validate(LF_POSITION, orders).should_keep_existing_stop is True
    assert _validate(MF_POSITION, orders).should_keep_existing_stop is True


def test_lf_missing_stop_reports_lf_only() -> None:
    orders = (_stop(MF_POSITION),)

    lf = _validate(LF_POSITION, orders)
    mf = _validate(MF_POSITION, orders)

    assert lf.primary_invalid_reason == "missing_bot_owned_stop"
    assert mf.should_keep_existing_stop is True


def test_mf_missing_stop_is_allowed_by_explicit_live_no_stop_policy() -> None:
    orders = (_stop(LF_POSITION),)

    mf_scoped = filter_orders_for_position_scope(
        orders,
        position_id=MF_POSITION,
    )

    assert mf_scoped == ()
    assert {"protective_stop_required": False}[
        "protective_stop_required"
    ] is False


def test_wrong_position_id_stop_is_rejected() -> None:
    result = _validate(LF_POSITION, (_stop("v9e-another-position"),))

    assert result.primary_invalid_reason == "missing_bot_owned_stop"


def test_wrong_position_side_stop_is_rejected() -> None:
    result = _validate(
        LF_POSITION,
        (_stop(LF_POSITION, position_side="short"),),
    )

    assert result.should_keep_existing_stop is False
    assert result.invalid_bot_owned_orders[0] is not None
    assert result.checks[0].invalid_reason == "wrong_position_side"


def test_reduce_only_missing_is_rejected() -> None:
    result = _validate(
        LF_POSITION,
        (_stop(LF_POSITION, reduce_only=False),),
    )

    assert result.should_keep_existing_stop is False
    assert result.checks[0].invalid_reason == "not_reduce_only"


def test_mf_signal_recovery_metadata_is_saved_to_position_plan(
    tmp_path,
) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    coordinator = MultiExchangeOrderCoordinator.__new__(
        MultiExchangeOrderCoordinator
    )
    coordinator.position_plan_store = store
    coordinator.master_follower_policy = None
    signal = TradeSignal(
        symbol=SYMBOL,
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.4"),
        metadata={
            "sleeve_id": "mf",
            "position_id": MF_POSITION,
            "engine": "MF_LOW_SWEEP_TIME48",
            "entry_execution_time_ms": 1_700_000_060_000,
            "entry_tradebar_open_time_ms": 1_700_000_060_000,
            "signal_time_ms": 1_700_000_000_000,
            "time48_holding_minutes": 48,
            "exit_variant": "time48",
            "quantity_scope": "mf_sleeve_quantity",
            "protective_stop_required": False,
            "audit": {"decimal_value": Decimal("0.60")},
        },
    )
    intent = OrderIntent(
        intent_id="mf-recovery-metadata",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="mf-entry-order",
        status=OrderStatus.FILLED,
        filled_quantity=Decimal("0.4"),
        avg_fill_price=Decimal("2000"),
    )

    coordinator._record_open_or_topup_plan(
        intent,
        (result,),
        purpose="normal_entry",
    )

    plan = store.get_position(MF_POSITION)
    assert plan is not None
    assert plan.metadata["sleeve_id"] == "mf"
    assert plan.metadata["entry_execution_time_ms"] == 1_700_000_060_000
    assert plan.metadata["entry_tradebar_open_time_ms"] == 1_700_000_060_000
    assert plan.metadata["time48_holding_minutes"] == 48
    assert plan.metadata["average_entry_price"] == "2000"
    assert plan.metadata["signal_metadata"]["audit"]["decimal_value"] == "0.60"


def test_unconfirmed_mf_close_is_persisted_as_manual_required(
    tmp_path,
) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    store.upsert_position(
        PositionPlan(
            position_id=MF_POSITION,
            strategy_id="eth_portfolio_v1",
            entry_engine="MF_LOW_SWEEP_TIME48",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.4"),
            master_filled_qty_base=Decimal("0.4"),
            metadata={"sleeve_id": "mf"},
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id=MF_POSITION,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.4"),
            filled_qty_base=Decimal("0.4"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    coordinator = MultiExchangeOrderCoordinator.__new__(
        MultiExchangeOrderCoordinator
    )
    coordinator.position_plan_store = store
    coordinator.master_follower_policy = None
    coordinator.repository = type(
        "_Repo",
        (),
        {"add_event": lambda self, event: None},
    )()
    signal = TradeSignal(
        symbol=SYMBOL,
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.4"),
        metadata={
            "sleeve_id": "mf",
            "position_id": MF_POSITION,
            "execution_purpose": "normal_close",
            "reduce_only": True,
        },
    )
    intent = OrderIntent(
        intent_id="mf-close-unconfirmed",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )

    coordinator._record_close_plan(
        intent,
        (
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=False,
                error="close not confirmed",
            ),
        ),
        purpose="normal_close",
    )

    plan = store.get_position(MF_POSITION)
    assert plan is not None
    assert plan.status is PositionPlanStatus.MANUAL_REQUIRED
    assert plan.metadata["pending_close_unconfirmed"] is True
