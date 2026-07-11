from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management.quantity import NativeQuantityConverter
from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    MasterFollowerExecutionPolicy,
    MultiExchangeOrderCoordinator,
    PositionPlan,
    PositionPlanStatus,
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
)
from src.order_management.safety import RecoveryExitOrderValidator
from src.order_management.stops import ScopedStopReplaceService, StopScope
from src.platform import ExchangeName
from src.platform.exchanges.models import (
    InstrumentRule,
    Order,
    OrderSide,
    OrderStatus,
    PositionMode,
    PositionSide,
)
from src.platform.markets import get_market_profile
from src.runtime.orders import LiveOrderIntentFactory
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy
from strategies.eth_portfolio_v1.domain.models import Side as PortfolioSide
from strategies.eth_portfolio_v1.strategy import Strategy as PortfolioStrategy


def test_invalid_stop_replace_does_not_cancel_confirmed_stop() -> None:
    strategy = _short_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("2.55"),
        stop_price=Decimal("1613.6875683869736345"),
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=8,
        reference_price=Decimal("1621.55"),
    )

    assert signals == []
    assert strategy.position.stop_price == Decimal("1686.4243161302636550")
    assert strategy.position.desired_stop_price is None
    assert strategy.position.pending_stop_replace is False
    assert strategy.last_stop_reject_reason == "invalid_stop:stop_not_exchange_valid"


def test_missing_exchange_stop_while_in_position_sets_manual_required() -> None:
    strategy = _short_strategy()
    validator = RecoveryExitOrderValidator(quantity_converter=NativeQuantityConverter())
    validation = validator.validate_stop_orders(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        strategy_id=strategy.config.strategy_id,
        position_id=strategy.position.position_id,
        position_side=PositionSide.SHORT,
        position_mode=PositionMode.ONE_WAY,
        current_position_native_quantity=Decimal("25.5"),
        canonical_stop_price=Decimal("1686.4243161302636550"),
        open_stop_orders=[],
        open_orders=[],
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    signals = strategy._signals_from_recovery_exit_validation(
        validation=validation,
        exchange="okx",
        quantity=Decimal("2.55"),
        stop_price=Decimal("1686.4243161302636550"),
        reason="RECOVERY_MASTER_STOP_SYNC",
    )

    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "critical_stop_missing_while_in_position_manual_required:okx" in strategy.recovery_alerts
    assert any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT for signal in signals)


def test_stop_replace_metadata_marks_non_atomic_when_no_targeted_cancel() -> None:
    strategy = _short_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("2.55"),
        stop_price=Decimal("1670"),
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=8,
    )

    place = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT)
    assert place.metadata["stop_replace_atomic_supported"] is False
    assert place.metadata["stop_replace_mode"] == "cancel_then_place_validated"
    assert place.metadata["stop_replace_non_atomic_reason"] == "no_targeted_stop_cancel_capability"
    assert place.metadata["replace_mode"] == "cancel_then_place_validated"


def test_stop_replace_never_places_new_then_cancel_all_without_targeted_cancel() -> None:
    strategy = _short_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("2.55"),
        stop_price=Decimal("1670"),
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=8,
    )

    seen_place = False
    for signal in signals:
        if signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            seen_place = True
            assert signal.metadata.get("stop_replace_mode") != "place_new_then_cancel_all"
        if seen_place:
            assert signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS


@pytest.mark.parametrize(
    ("exchange", "position_mode", "quantity", "raw"),
    [
        (
            ExchangeName.OKX,
            PositionMode.ONE_WAY,
            Decimal("10"),
            {"reduceOnly": "true", "source": "aetheredge"},
        ),
        (
            ExchangeName.BINANCE,
            PositionMode.HEDGE,
            None,
            {
                "closePosition": "true",
                "positionSide": "LONG",
                "source": "aetheredge",
            },
        ),
    ],
)
def test_tick_normalized_stop_price_is_valid_for_okx_and_binance(
    exchange,
    position_mode,
    quantity,
    raw,
) -> None:
    canonical = Decimal("1738.2542231936259150")
    order = Order(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol=(
            "ETH-USDT-SWAP"
            if exchange is ExchangeName.OKX
            else "ETHUSDT"
        ),
        order_id=f"{exchange.value}-stop",
        client_order_id=None,
        status=OrderStatus.NEW,
        side=OrderSide.SELL,
        price=Decimal("1738.25"),
        quantity=quantity,
        raw=raw,
    )
    result = RecoveryExitOrderValidator().validate_stop_orders(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        strategy_id="eth_portfolio_v1",
        position_id="position-tick-normalized",
        position_side=PositionSide.LONG,
        position_mode=position_mode,
        current_position_native_quantity=Decimal("10"),
        canonical_stop_price=canonical,
        open_stop_orders=(order,),
        market_profile=get_market_profile("ETH-USDT-PERP"),
        instrument_rule=InstrumentRule(
            exchange=exchange,
            symbol="ETH-USDT-PERP",
            raw_symbol=order.raw_symbol,
            price_tick=Decimal("0.01"),
        ),
    )

    assert result.valid is True
    assert result.should_keep_existing_stop is True
    assert result.effective_expected_stop_price == Decimal("1738.25")
    assert result.confirmed_stop_price == Decimal("1738.25")
    assert result.primary_invalid_reason is None


def test_tick_normalization_does_not_accept_a_genuinely_wrong_stop_price() -> None:
    order = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="wrong-price-stop",
        client_order_id=None,
        status=OrderStatus.NEW,
        side=OrderSide.SELL,
        price=Decimal("1738.20"),
        quantity=Decimal("10"),
        raw={"reduceOnly": "true", "source": "aetheredge"},
    )
    result = RecoveryExitOrderValidator().validate_stop_orders(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        strategy_id="eth_portfolio_v1",
        position_id="position-wrong-price",
        position_side=PositionSide.LONG,
        position_mode=PositionMode.ONE_WAY,
        current_position_native_quantity=Decimal("10"),
        canonical_stop_price=Decimal("1738.2542231936259150"),
        open_stop_orders=(order,),
        market_profile=get_market_profile("ETH-USDT-PERP"),
        instrument_rule=InstrumentRule(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            price_tick=Decimal("0.01"),
        ),
    )

    assert result.valid is False
    assert result.primary_invalid_reason == "invalid_bot_owned_stop_present"
    assert result.primary_invalid_detail_reason == "trigger_price_mismatch"
    fields = result.diagnostic_fields(action="reject")
    assert fields["invalid_category"] == "invalid_bot_owned_stop_present"
    assert fields["invalid_detail_reason"] == "trigger_price_mismatch"
    assert fields["canonical_stop_price"] == "1738.254223193625915"
    assert fields["effective_expected_stop_price"] == "1738.25"
    assert fields["actual_exchange_stop_price"] == "1738.2"
    assert fields["price_tick"] == "0.01"
    assert fields["price_difference"] == "0.05"


def test_scoped_stop_replace_builds_cancel_for_exact_scope() -> None:
    scope = _scoped_stop()

    signal = ScopedStopReplaceService().build_cancel_signal(scope)

    assert signal.action is SignalAction.CANCEL_STOP_ORDER
    assert signal.symbol == scope.symbol
    assert signal.client_order_id == "lf-old-stop-client"
    assert signal.metadata["stop_order_id"] == "lf-old-stop-order"
    assert signal.metadata["stop_client_order_id"] == "lf-old-stop-client"
    assert signal.metadata["strategy_id"] == "eth_portfolio_v1"
    assert signal.metadata["sleeve_id"] == "lf"
    assert signal.metadata["position_id"] == "lf-position-1"
    assert signal.metadata["position_side"] == "long"
    assert signal.metadata["target_exchanges"] == ["okx"]


def test_scoped_replace_stages_new_stop_before_scoped_old_stop_cancel() -> None:
    scope = _scoped_stop()
    new_stop = TradeSignal(
        symbol=scope.symbol,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        quantity=Decimal("0.25"),
        trigger_price=Decimal("2450"),
        client_order_id="lf-new-stop-client",
        metadata={
            "strategy_id": scope.strategy_id,
            "sleeve_id": scope.sleeve_id,
            "position_id": scope.position_id,
        },
    )

    signals = ScopedStopReplaceService().build_replace_signals(scope, new_stop)

    # R001 only stages the two boundaries. R002 must verify the first stop at
    # the venue before it dispatches the second signal.
    assert signals[0] is new_stop
    assert signals[0].action is SignalAction.PLACE_STOP_LOSS_LONG
    assert signals[0].metadata["sleeve_id"] == "lf"
    assert signals[1].action is SignalAction.CANCEL_STOP_ORDER
    assert signals[1].metadata["sleeve_id"] == "lf"
    assert all(signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)


class _SuccessfulStopClient:
    symbol = "ETH-USDT-PERP"

    def __init__(self, exchange: ExchangeName, *, fail: bool = False) -> None:
        self.exchange = exchange
        self.fail = fail
        self.place_stop_calls = 0

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY

    async def place_stop_market_order(self, request):
        self.place_stop_calls += 1
        if self.fail:
            raise RuntimeError("simulated exhausted stop repair failure")
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-new-stop",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
            side=request.side,
            price=request.trigger_price,
            quantity=request.quantity,
            raw={"confirmed_stop_price": str(request.trigger_price)},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", [ExchangeName.OKX, ExchangeName.BINANCE])
@pytest.mark.parametrize("repair_kind", ["missing", "invalid_replace"])
@pytest.mark.parametrize("terminal_failure", [False, True])
async def test_production_stop_repair_persists_generation_for_master_and_follower(
    tmp_path,
    exchange,
    repair_kind,
    terminal_failure,
) -> None:
    position_id = f"stop-generation-{exchange.value}-{repair_kind}"
    plan_path = tmp_path / f"{position_id}-plans.sqlite3"
    journal_path = tmp_path / f"{position_id}-journal.sqlite3"
    plans = SqlitePositionPlanStore(plan_path)
    plans.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1900"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
            metadata={"stop_generation": 0},
        )
    )
    for leg_exchange, role in (
        (ExchangeName.OKX, LegRole.MASTER),
        (ExchangeName.BINANCE, LegRole.FOLLOWER),
    ):
        plans.upsert_leg(
            LegPlan(
                position_id=position_id,
                exchange=leg_exchange,
                role=role,
                target_qty_base=Decimal("0.1"),
                filled_qty_base=Decimal("0.1"),
                sync_status=LegSyncStatus.OPEN,
            )
        )

    strategy = PortfolioStrategy()
    strategy.position.open_master(
        side=PortfolioSide.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.1"),
        stop_price=Decimal("1900"),
        entry_engine="BULL_RECLAIM_V2",
        position_id=position_id,
    )
    strategy.position.mark_leg_open(
        exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.1")
    )
    strategy.position.mark_leg_open(
        exchange="binance", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.1")
    )
    old_stop = ()
    if repair_kind == "invalid_replace":
        old_stop = (
            Order(
                exchange=exchange,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP" if exchange is ExchangeName.OKX else "ETHUSDT",
                order_id=f"{position_id}-old-stop",
                client_order_id=f"{position_id}-old-client",
                status=OrderStatus.NEW,
                side=OrderSide.SELL,
                price=Decimal("1850"),
                quantity=Decimal("0.1"),
                raw={"reduceOnly": "true", "source": "aetheredge"},
            ),
        )
    validation = RecoveryExitOrderValidator().validate_stop_orders(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        strategy_id=strategy.config.strategy_id,
        position_id=position_id,
        position_side=PositionSide.LONG,
        position_mode=PositionMode.ONE_WAY,
        current_position_native_quantity=Decimal("0.1"),
        canonical_stop_price=Decimal("1900"),
        open_stop_orders=old_stop,
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )
    signal_0 = strategy._signals_from_recovery_exit_validation(
        validation=validation,
        exchange=exchange.value,
        quantity=Decimal("0.1"),
        stop_price=Decimal("1900"),
        reason="RECOVERY_STOP_GENERATION_TEST",
        stop_generation=0,
    )[0]
    assert signal_0.metadata["stop_generation"] == 0

    factory = LiveOrderIntentFactory(
        strategy_id=strategy.config.strategy_id,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )
    intent_0 = factory.create(signal_0, source="recovery")
    client = _SuccessfulStopClient(exchange, fail=terminal_failure)
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(journal_path),
        position_plan_store=plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )
    results = await coordinator.execute(intent_0)
    assert client.place_stop_calls == 1
    assert all(result.ok == (not terminal_failure) for result in results)
    restarted_plans = SqlitePositionPlanStore(plan_path)
    persisted_generation = restarted_plans.get_position(position_id).metadata[
        "stop_generation"
    ]
    assert persisted_generation == 1

    if repair_kind == "invalid_replace" and not terminal_failure:
        cancels = await strategy.on_order_results(
            signal=signal_0,
            results=results,
            source="recovery",
            event_time_ms=None,
        )
        assert cancels
        assert all(cancel.metadata["stop_generation"] == 0 for cancel in cancels)
        assert all(
            cancel.metadata["stop_order_id"] == f"{position_id}-old-stop"
            for cancel in cancels
        )

    restarted_strategy = PortfolioStrategy()
    restarted_strategy.position.open_master(
        side=PortfolioSide.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.1"),
        stop_price=Decimal("1900"),
        entry_engine="BULL_RECLAIM_V2",
        position_id=position_id,
    )
    restarted_strategy.position.mark_leg_open(
        exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.1")
    )
    restarted_strategy.position.mark_leg_open(
        exchange="binance", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.1")
    )
    signal_1 = restarted_strategy._signals_from_recovery_exit_validation(
        validation=validation,
        exchange=exchange.value,
        quantity=Decimal("0.1"),
        stop_price=Decimal("1900"),
        reason="RECOVERY_STOP_GENERATION_TEST",
        stop_generation=persisted_generation,
    )[0]
    intent_1 = factory.create(signal_1, source="recovery")
    assert intent_1.intent_id != intent_0.intent_id


def _scoped_stop() -> StopScope:
    return StopScope(
        strategy_id="eth_portfolio_v1",
        sleeve_id="lf",
        position_id="lf-position-1",
        symbol="ETH-USDT-PERP",
        position_side=PositionSide.LONG,
        target_exchanges=(ExchangeName.OKX,),
        stop_client_order_id="lf-old-stop-client",
        stop_order_id="lf-old-stop-order",
    )


def _short_strategy() -> Strategy:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=1,
        avg_entry=Decimal("1620.30"),
        qty=Decimal("2.55"),
        stop_price=Decimal("1686.4243161302636550"),
        entry_engine="MOMENTUM_V3",
        position_id="stop-replace-safety",
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("1620.30"), base_qty=Decimal("2.55"))
    return strategy
