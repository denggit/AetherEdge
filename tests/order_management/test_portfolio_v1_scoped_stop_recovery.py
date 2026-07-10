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
    RecoveryStopScopeResolver,
    StopScopeResolutionStatus,
    filter_orders_for_position_scope,
)
from src.platform import (
    ExchangeName,
    InstrumentRule,
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
            "fixed_time_exit_holding_minutes": 48,
            "exit_variant": "time48",
            "quantity_scope": "mf_sleeve_quantity",
            "protective_stop_required": False,
            "unconfirmed_master_close_policy": "manual_required",
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
    assert plan.metadata["fixed_time_exit_holding_minutes"] == 48
    assert plan.metadata["signal_metadata"]["time48_holding_minutes"] == 48
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
            "unconfirmed_master_close_policy": "manual_required",
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


def test_verified_exchange_stop_price_is_persisted_with_theoretical_audit(
    tmp_path,
) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    store.upsert_position(
        PositionPlan(
            position_id=LF_POSITION,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1738.2542231936259150"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.6"),
            master_filled_qty_base=Decimal("0.6"),
            metadata={"sleeve_id": "lf"},
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id=LF_POSITION,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.6"),
            filled_qty_base=Decimal("0.6"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    coordinator = MultiExchangeOrderCoordinator.__new__(
        MultiExchangeOrderCoordinator
    )
    coordinator.position_plan_store = store
    signal = TradeSignal(
        symbol=SYMBOL,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        quantity=Decimal("0.6"),
        trigger_price=Decimal("1738.2542231936259150"),
        metadata={"position_id": LF_POSITION},
    )
    intent = OrderIntent(
        intent_id="normalized-stop-plan",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )

    coordinator._record_stop_plan(
        intent,
        (
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id="okx-normalized-stop",
                raw={
                    "confirmed_stop_price": "1738.25",
                    "actual_exchange_stop_price": "1738.25",
                },
            ),
        ),
    )

    plan = store.get_position(LF_POSITION)
    leg = store.get_legs(LF_POSITION)[0]
    assert plan is not None
    assert plan.canonical_stop_price == Decimal("1738.25")
    assert plan.metadata["strategy_theoretical_stop_price"] == (
        "1738.2542231936259150"
    )
    assert leg.stop_price == Decimal("1738.25")
    assert leg.stop_order_id == "okx-normalized-stop"


# ═══════════════════════════════════════════════════════════════════════
# Legacy Stop Scope Resolution Tests
# ═══════════════════════════════════════════════════════════════════════

LEGACY_POSITION_ID = "legacy-lf-position-001"
THEORETICAL_STOP = Decimal("1738.2542231936259150")
EXCHANGE_STOP_PRICE = Decimal("1738.25")
ETH_MARKET = get_market_profile(SYMBOL)
ETH_RULE = InstrumentRule(
    exchange=ExchangeName.OKX,
    symbol=SYMBOL,
    raw_symbol="ETH-USDT-SWAP",
    price_tick=Decimal("0.05"),
    quantity_step=Decimal("0.01"),
    min_quantity=Decimal("0.01"),
    contract_value=Decimal("1"),
)


def _legacy_okx_stop(
    *,
    order_id: str = "1234567890123456",
    client_order_id: str = "AEOKSL0123456789ABCDEF",
    price: Decimal = EXCHANGE_STOP_PRICE,
    quantity: str = "6",
    pos_side: str = "long",
    state: str = "live",
) -> Order:
    """Realistic OKX open algo stop order — no position_id in raw."""
    return Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id=order_id,
        client_order_id=client_order_id,
        price=price,
        quantity=Decimal(quantity),
        side=OrderSide.SELL,
        status=OrderStatus.NEW,
        raw={
            "algoId": order_id,
            "algoClOrdId": client_order_id,
            "instId": "ETH-USDT-SWAP",
            "posSide": pos_side,
            "side": "sell",
            "sz": quantity,
            "slTriggerPx": str(price),
            "reduceOnly": "true",
            "state": state,
        },
    )


def _legacy_binance_stop(
    *,
    order_id: str = "9876543210987654",
    client_order_id: str = "AEBISL0123456789ABCDEF",
    price: Decimal = EXCHANGE_STOP_PRICE,
    state: str = "NEW",
) -> Order:
    """Realistic Binance open algo stop order — no position_id in raw."""
    return Order(
        exchange=ExchangeName.BINANCE,
        symbol=SYMBOL,
        raw_symbol="ETHUSDT",
        order_id=order_id,
        client_order_id=client_order_id,
        price=price,
        quantity=None,
        side=OrderSide.SELL,
        status=OrderStatus.NEW,
        raw={
            "algoId": order_id,
            "clientAlgoId": client_order_id,
            "symbol": "ETHUSDT",
            "positionSide": "LONG",
            "side": "SELL",
            "triggerPrice": str(price),
            "closePosition": "true",
            "algoStatus": state,
        },
    )


def _resolver() -> RecoveryStopScopeResolver:
    return RecoveryStopScopeResolver(
        validator=RecoveryExitOrderValidator(
            quantity_converter=NativeQuantityConverter(),
        ),
    )


# ── Adoptable legacy tests ────────────────────────────────────────────


def test_legacy_stop_adoptable_okx_no_position_id_in_raw() -> None:
    """OKX legacy stop without position_id in raw → ADOPTABLE_LEGACY."""
    resolver = _resolver()
    stop = _legacy_okx_stop()

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.ADOPTABLE_LEGACY
    assert resolution.order_id == "1234567890123456"
    assert resolution.client_order_id == "AEOKSL0123456789ABCDEF"
    assert resolution.effective_stop_price == EXCHANGE_STOP_PRICE
    assert resolution.canonical_theoretical_stop_price == THEORETICAL_STOP
    assert resolution.is_adoptable is True
    assert resolution.is_blocking is False
    assert "legacy_stop_scope_will_be_adopted_during_runtime_recovery" in resolution.warnings


def test_legacy_stop_adoptable_binance_no_position_id_in_raw() -> None:
    """Binance legacy stop without position_id in raw → ADOPTABLE_LEGACY."""
    resolver = _resolver()
    stop = _legacy_binance_stop()

    resolution = resolver.resolve(
        exchange=ExchangeName.BINANCE,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.ADOPTABLE_LEGACY


def test_exact_match_has_priority_over_legacy() -> None:
    """Known stop_order_id match → EXACT, even if raw has no position_id."""
    resolver = _resolver()
    stop = _legacy_okx_stop(order_id="okx-known-stop-99")

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=("okx-known-stop-99", None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.EXACT
    assert resolution.detail["match_method"] == "exact_known_ids"


def test_second_restart_uses_exact_match_after_adoption() -> None:
    """After adoption writes stop IDs to PositionPlan, second restart → EXACT."""
    resolver = _resolver()
    stop = _legacy_okx_stop()

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=("1234567890123456", "AEOKSL0123456789ABCDEF"),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.EXACT
    assert resolution.detail["match_method"] == "exact_known_ids"


# ── Ambiguous tests ────────────────────────────────────────────────────


def test_legacy_stop_ambiguous_with_multiple_bot_stops() -> None:
    """Two valid bot-owned stops → AMBIGUOUS."""
    resolver = _resolver()
    stop1 = _legacy_okx_stop(order_id="111", client_order_id="AEOKSLAAAAAAAAAAAAAAAA")
    stop2 = _legacy_okx_stop(order_id="222", client_order_id="AEOKSLBBBBBBBBBBBBBBBB")

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop1, stop2),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.AMBIGUOUS
    assert "multiple_valid_bot_stops" in resolution.detail["reason"]


def test_legacy_stop_ambiguous_with_multiple_active_plans() -> None:
    """One valid stop but multiple active plans for same exchange/side → AMBIGUOUS."""
    resolver = _resolver()
    stop = _legacy_okx_stop()

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
        active_plan_count_same_exchange_side=2,
    )

    assert resolution.status == StopScopeResolutionStatus.AMBIGUOUS
    assert "multiple_active_plans" in resolution.detail["reason"]


# ── Invalid tests ──────────────────────────────────────────────────────


def test_legacy_stop_invalid_wrong_price() -> None:
    """Price mismatch → INVALID (not adoptable)."""
    resolver = _resolver()
    stop = _legacy_okx_stop(price=Decimal("1700.00"))

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.INVALID
    assert resolution.is_adoptable is False
    assert resolution.is_blocking is True


def test_legacy_stop_invalid_wrong_side() -> None:
    """Wrong side (BUY for LONG position) → INVALID."""
    resolver = _resolver()
    stop = Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id="999",
        client_order_id="AEOKSL0000000000000000",
        price=EXCHANGE_STOP_PRICE,
        quantity=Decimal("6"),
        side=OrderSide.BUY,
        status=OrderStatus.NEW,
        raw={
            "algoId": "999",
            "algoClOrdId": "AEOKSL0000000000000000",
            "instId": "ETH-USDT-SWAP",
            "posSide": "long",
            "side": "buy",
            "sz": "6",
            "slTriggerPx": str(EXCHANGE_STOP_PRICE),
            "reduceOnly": "true",
            "state": "live",
        },
    )

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.INVALID


def test_legacy_stop_invalid_wrong_position_side() -> None:
    """Wrong posSide (short for LONG position) → INVALID."""
    resolver = _resolver()
    stop = _legacy_okx_stop(pos_side="short")

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.INVALID


def test_legacy_stop_invalid_wrong_quantity() -> None:
    """Quantity mismatch → INVALID."""
    resolver = _resolver()
    stop = _legacy_okx_stop(quantity="100")

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.INVALID


def test_legacy_stop_invalid_missing_reduce_only() -> None:
    """Missing reduceOnly → INVALID for OKX."""
    resolver = _resolver()
    stop = Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id="555",
        client_order_id="AEOKSL0000000000000001",
        price=EXCHANGE_STOP_PRICE,
        quantity=Decimal("6"),
        side=OrderSide.SELL,
        status=OrderStatus.NEW,
        raw={
            "algoId": "555",
            "algoClOrdId": "AEOKSL0000000000000001",
            "instId": "ETH-USDT-SWAP",
            "posSide": "long",
            "side": "sell",
            "sz": "6",
            "slTriggerPx": str(EXCHANGE_STOP_PRICE),
            "reduceOnly": "false",
            "state": "live",
        },
    )

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.INVALID


def test_legacy_stop_invalid_inactive_status() -> None:
    """Inactive status → INVALID (not adoptable)."""
    resolver = _resolver()
    stop = _legacy_okx_stop(state="canceled")
    stop = Order(
        exchange=stop.exchange,
        symbol=stop.symbol,
        raw_symbol=stop.raw_symbol,
        order_id=stop.order_id,
        client_order_id=stop.client_order_id,
        price=stop.price,
        quantity=stop.quantity,
        side=stop.side,
        status=OrderStatus.CANCELED,
        raw={**stop.raw, "state": "canceled"},
    )

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.INVALID


def test_legacy_stop_never_adopts_manual_order() -> None:
    """Manual stop without bot ownership → MISSING, never adopted."""
    resolver = _resolver()
    stop = Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id="manual-1",
        client_order_id=None,
        price=EXCHANGE_STOP_PRICE,
        quantity=Decimal("6"),
        side=OrderSide.SELL,
        status=OrderStatus.NEW,
        raw={
            "algoId": "manual-1",
            "instId": "ETH-USDT-SWAP",
            "posSide": "long",
            "side": "sell",
            "sz": "6",
            "slTriggerPx": str(EXCHANGE_STOP_PRICE),
            "reduceOnly": "true",
            "state": "live",
        },
    )

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=(None, None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.MISSING
    assert "manual_orders_present_on_exchange" in resolution.detail["reason"]


def test_known_ids_exist_but_no_match_still_missing() -> None:
    """Known stop IDs exist but don't match any exchange order → MISSING."""
    resolver = _resolver()
    stop = _legacy_okx_stop()

    resolution = resolver.resolve(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        strategy_id="eth_portfolio_v1",
        position_id=LEGACY_POSITION_ID,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.HEDGE,
        current_position_native_quantity=Decimal("6"),
        canonical_stop_price=THEORETICAL_STOP,
        open_stop_orders=(stop,),
        known_stop_order_ids=("non-existent-stop-id", None),
        market_profile=ETH_MARKET,
        instrument_rule=ETH_RULE,
    )

    assert resolution.status == StopScopeResolutionStatus.MISSING
    assert "known_stop_ids_not_found_on_exchange" in resolution.detail["reason"]
