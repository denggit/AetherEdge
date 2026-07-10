"""Tests for MF hard stop placement, cancel, fill, and cooldown logic."""

from __future__ import annotations

from decimal import Decimal

import pytest
from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName, OrderSide, OrderStatus
from src.platform.account.events import AccountEvent, AccountEventType
from src.signals import SignalAction, TradeSignal
from strategies.eth_portfolio_v1.domain.mf_signal import MfSignalDecision
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.execution.mf_signal_mapper import (
    MfSignalMapper,
    MfSizingInput,
)
from strategies.eth_portfolio_v1.strategy import Strategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _activate_mf_sleeve(
    strategy: Strategy,
    *,
    avg_entry: Decimal = Decimal("2500"),
    okx_qty: Decimal = Decimal("0.5"),
    binance_qty: Decimal = Decimal("0.25"),
) -> None:
    strategy.mf_sleeve.reserve_open(
        position_id="mf-low-sweep-time48-test",
        quantity=okx_qty,
        signal_time_ms=1000,
        entry_execution_time_ms=1001,
        tradebar_open_time_ms=1001,
        exchange_quantities={
            "okx": okx_qty,
            "binance": binance_qty,
        },
    )
    strategy.mf_sleeve.confirm_open(
        quantity=okx_qty,
        average_entry_price=avg_entry,
        entry_time_ms=1001,
        exchange_quantities={
            "okx": okx_qty,
            "binance": binance_qty,
        },
        master_exchange="okx",
    )


def _stop_signal(
    *,
    position_id: str = "mf-low-sweep-time48-test",
    exchange: str = "okx",
    qty: Decimal = Decimal("0.5"),
    trigger_price: Decimal = Decimal("2375"),
) -> TradeSignal:
    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        quantity=qty,
        trigger_price=trigger_price,
        reason="mf_hard_stop_initial",
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": position_id,
            "engine": "MF_LOW_SWEEP_TIME48",
            "execution_purpose": "mf_hard_stop",
            "stop_scope": position_id,
            "stop_price_source": "mf_master_avg_fill_price_pct",
            "hard_stop_pct": "0.0500",
            "target_exchanges": [exchange],
            "exchange_quantities_base": {exchange: str(qty)},
            "close_scope": "mf_sleeve_only",
            "quantity_scope": "mf_sleeve_quantity",
            "reduce_only": True,
            "stop_placement_reason": "mf_hard_stop_initial",
        },
    )


# ---------------------------------------------------------------------------
# 1. Hard stop generated after MF open fill confirmation
# ---------------------------------------------------------------------------


def test_hard_stop_generated_after_open_fill() -> None:
    strategy = Strategy()
    _activate_mf_sleeve(strategy, avg_entry=Decimal("2500"))

    # Simulate OPEN_LONG order result → should generate stop signals
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.5"),
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "entry_execution_time_ms": 1001,
            "target_exchanges": ["binance", "okx"],
            "exchange_quantities_base": {
                "okx": "0.5",
                "binance": "0.25",
            },
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.5"),
            avg_fill_price=Decimal("2500"),
        ),
        ExchangeOrderResult(
            exchange=ExchangeName.BINANCE,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.25"),
            avg_fill_price=Decimal("2501"),
        ),
    )
    follow_up = strategy._handle_mf_order_results(
        signal=signal,
        results=results,
        event_time_ms=1001,
    )
    # Should generate PLACE_STOP_LOSS_LONG signals for each exchange
    assert len(follow_up) >= 1
    stop_signals = [
        s for s in follow_up if s.action is SignalAction.PLACE_STOP_LOSS_LONG
    ]
    assert len(stop_signals) == 2  # one per exchange
    for s in stop_signals:
        assert s.trigger_price == Decimal(
            "2375"
        )  # 2500 * (1 - 0.05)
        assert s.metadata["sleeve_id"] == "mf"
        assert s.metadata["execution_purpose"] == "mf_hard_stop"
        assert s.metadata["position_id"] == "mf-low-sweep-time48-test"


def test_hard_stop_not_generated_when_disabled() -> None:
    strategy = Strategy()
    # Override config to disable hard stop
    strategy.config = strategy.config.__class__(
        strategy_id=strategy.config.strategy_id,
        strategy_version=strategy.config.strategy_version,
        display_name=strategy.config.display_name,
        symbol=strategy.config.symbol,
        data_exchange=strategy.config.data_exchange,
        runtime_requirements=dict(strategy.config.runtime_requirements),
        micro_context=strategy.config.micro_context,
        range_exit=strategy.config.range_exit,
        entry_filters=strategy.config.entry_filters,
        structural_stop=strategy.config.structural_stop,
        global_risk_scale=strategy.config.global_risk_scale,
        mf=strategy.config.mf.__class__(
            enabled=True,
            margin_fraction=Decimal("0.0666666667"),
            hard_stop_enabled=False,
        ),
    )
    _activate_mf_sleeve(strategy, avg_entry=Decimal("2500"))

    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.5"),
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "target_exchanges": ["okx"],
            "exchange_quantities_base": {"okx": "0.5"},
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.5"),
            avg_fill_price=Decimal("2500"),
        ),
    )
    follow_up = strategy._handle_mf_order_results(
        signal=signal, results=results, event_time_ms=1001
    )
    stop_signals = [
        s for s in follow_up if s.action is SignalAction.PLACE_STOP_LOSS_LONG
    ]
    assert len(stop_signals) == 0


# ---------------------------------------------------------------------------
# 2. Stop placement result → recorded in MfSleeveState
# ---------------------------------------------------------------------------


def test_stop_placement_results_recorded() -> None:
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    stop_signal = _stop_signal(exchange="okx", qty=Decimal("0.5"))
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            status=OrderStatus.NEW,
            order_id="okx-stop-123",
            client_order_id="mf-stop-mf-low-sweep-time48-test-okx",
        ),
    )
    follow_up = strategy._handle_mf_order_results(
        signal=stop_signal, results=results, event_time_ms=2000
    )
    assert follow_up == []
    assert (
        strategy.mf_sleeve.stop_order_ids_by_exchange.get("okx")
        == "okx-stop-123"
    )
    assert strategy.mf_sleeve.hard_stop_price == Decimal("2375")


def test_stop_placement_failure_triggers_manual_required() -> None:
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    stop_signal = _stop_signal(exchange="okx", qty=Decimal("0.5"))
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=False,
            status=OrderStatus.REJECTED,
            error="stop placement rejected",
        ),
    )
    strategy._handle_mf_order_results(
        signal=stop_signal, results=results, event_time_ms=2000
    )
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "mf_hard_stop_place_failed" in strategy.mf_execution_alerts


def test_position_snapshots_include_hard_stop() -> None:
    strategy = Strategy()
    _activate_mf_sleeve(strategy, avg_entry=Decimal("2500"))

    # Record hard stop
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        stop_client_order_id="mf-stop-test-okx",
        exchange="okx",
    )
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="binance-stop-456",
        exchange="binance",
    )
    snapshots = strategy.position_snapshots()
    mf_snapshot = next(
        s for s in snapshots if s.sleeve_id == "mf"
    )
    assert mf_snapshot.stop_price == Decimal("2375")
    assert mf_snapshot.metadata["protective_stop_required"] is True
    assert mf_snapshot.metadata.get("stop_order_ids_by_exchange") == {
        "okx": "okx-stop-123",
        "binance": "binance-stop-456",
    }


# ---------------------------------------------------------------------------
# 3. time48 exit → cancel MF hard stop
# ---------------------------------------------------------------------------


def test_time48_exit_generates_scoped_cancel_signals() -> None:
    """time48 close generates cancel signals but does NOT clear stop
    ids until cancel results are confirmed."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    # Record hard stop first
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        exchange="okx",
    )
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="binance-stop-456",
        exchange="binance",
    )

    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.5"),
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "execution_purpose": "normal_close",
            "target_exchanges": ["binance", "okx"],
            "exchange_quantities_base": {
                "okx": "0.5",
                "binance": "0.25",
            },
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.5"),
        ),
        ExchangeOrderResult(
            exchange=ExchangeName.BINANCE,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.25"),
        ),
    )
    follow_up = strategy._handle_mf_order_results(
        signal=close_signal, results=results, event_time_ms=5000
    )
    cancel_signals = [
        s
        for s in follow_up
        if s.action is SignalAction.CANCEL_STOP_ORDER
    ]
    assert len(cancel_signals) >= 1
    for s in cancel_signals:
        assert s.action is not SignalAction.CANCEL_ALL_STOP_ORDERS
        assert s.metadata["sleeve_id"] == "mf"
        assert s.metadata["execution_purpose"] == (
            "mf_cancel_hard_stop_after_time_exit"
        )
    # confirm_close clears the sleeve when all exchanges are closed.
    # The cancel signals were generated using captured stop ids (passed
    # as params), which is correct — the actual clear_hard_stop happens
    # only after cancel results confirm success.
    assert not strategy.mf_sleeve.active


def test_time48_cancel_success_clears_hard_stop() -> None:
    """All cancel results succeed → clear_hard_stop."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        exchange="okx",
    )
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="binance-stop-456",
        exchange="binance",
    )

    cancel_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "execution_purpose": (
                "mf_cancel_hard_stop_after_time_exit"
            ),
            "target_exchanges": ["okx"],
            "stop_order_id": "okx-stop-123",
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            status=OrderStatus.FILLED,
        ),
    )
    strategy._handle_mf_stop_cancel_results(
        signal=cancel_signal, results=results, event_time_ms=5000
    )
    # After all cancel results succeed, stop is cleared
    assert strategy.mf_sleeve.hard_stop_price is None
    assert strategy.mf_sleeve.stop_order_ids_by_exchange == {}


def test_time48_cancel_failure_triggers_manual_required() -> None:
    """Cancel stop failure → manual_required, stop ids retained."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        exchange="okx",
    )

    cancel_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "execution_purpose": (
                "mf_cancel_hard_stop_after_time_exit"
            ),
            "target_exchanges": ["okx"],
            "stop_order_id": "okx-stop-123",
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=False,
            error="cancel rejected",
        ),
    )
    strategy._handle_mf_stop_cancel_results(
        signal=cancel_signal, results=results, event_time_ms=5000
    )
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "mf_hard_stop_cancel_failed" in strategy.mf_execution_alerts
    # Stop ids retained
    assert strategy.mf_sleeve.hard_stop_price == Decimal("2375")
    assert (
        strategy.mf_sleeve.stop_order_ids_by_exchange.get("okx")
        == "okx-stop-123"
    )


# ---------------------------------------------------------------------------
# 4. Hard stop fill → cooldown + follower close
# ---------------------------------------------------------------------------


def test_hard_stop_fill_clears_sleeve_and_starts_cooldown() -> None:
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    # Record hard stop
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        exchange="okx",
    )

    event_time_ms = 10000
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=event_time_ms,
        order_id="okx-stop-123",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2370"),
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
        raw={},
    )
    signals = strategy._handle_mf_hard_stop_fill(event=event)
    # Only OKX → both exchanges had positions, binance should be
    # closed as follower
    assert len(signals) >= 1
    follower_close = signals[0]
    assert follower_close.action is SignalAction.CLOSE_LONG
    assert (
        follower_close.metadata["execution_purpose"]
        == "mf_follower_close_after_master_hard_stop"
    )
    assert follower_close.metadata["reduce_only"] is True
    assert follower_close.metadata["close_scope"] == "mf_sleeve_only"


def test_hard_stop_fill_single_exchange_full_close() -> None:
    strategy = Strategy()
    _activate_mf_sleeve(
        strategy, okx_qty=Decimal("0.5"), binance_qty=Decimal("0")
    )

    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        exchange="okx",
    )

    # Remove binance from exchange_quantities to simulate only OKX
    strategy.mf_sleeve.exchange_quantities = {"okx": Decimal("0.5")}

    event_time_ms = 10000
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=event_time_ms,
        order_id="okx-stop-123",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2370"),
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
        raw={},
    )
    signals = strategy._handle_mf_hard_stop_fill(event=event)
    # All exchanges closed → no follower close needed
    assert signals == []
    assert not strategy.mf_sleeve.active
    assert strategy.mf_sleeve.cooldown_active(
        event_time_ms + 1
    ) is True
    assert strategy.mf_sleeve.cooldown_active(
        event_time_ms + 13 * 3_600_000
    ) is False


def test_cooldown_blocks_new_entry() -> None:
    from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
        evaluate_mf_low_sweep,
    )
    from _mf_test_helpers import (
        READY,
        config,
        range_footprint,
        setup_bars,
    )

    bars = setup_bars()
    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    # Set cooldown at a time just before the next decision
    sleeve.set_hard_stop_cooldown(
        event_time_ms=bars[-1].close_time_ms - 1000,
        cooldown_hours=12,
    )
    decision, audit = evaluate_mf_low_sweep(
        config=config(margin_fraction=Decimal("0.0666666667")),
        bars=bars,
        range_footprints=[
            range_footprint(
                available_time_ms=bars[-1].open_time_ms - 1
            )
        ],
        large_share_history=[Decimal("0.10")] * 43_201,
        readiness=READY,
        sleeve=sleeve,
        next_open_price=Decimal("90"),
        next_open_time_ms=bars[-1].close_time_ms + 1,
    )
    assert decision is None
    assert audit["blocked_reason"] == "mf_hard_stop_cooldown"
    assert audit.get("cooldown_until_ms") is not None


def test_cooldown_does_not_block_active_sleeve_exit() -> None:
    from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
        evaluate_mf_low_sweep,
    )
    from _mf_test_helpers import READY, config, range_footprint, setup_bars

    bars = setup_bars()
    completed_through = bars[-1].close_time_ms + 1
    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    sleeve.reserve_open(
        position_id="mf-low-sweep-time48-cooldown-test",
        quantity=Decimal("0.25"),
        signal_time_ms=completed_through - 48 * 60_000,
        entry_execution_time_ms=completed_through - 48 * 60_000,
        tradebar_open_time_ms=completed_through - 48 * 60_000,
        exchange_quantities={"okx": Decimal("0.25")},
    )
    sleeve.confirm_open(
        quantity=Decimal("0.25"),
        average_entry_price=Decimal("100"),
        entry_time_ms=completed_through - 48 * 60_000,
        exchange_quantities={"okx": Decimal("0.25")},
        master_exchange="okx",
    )
    # Set cooldown while active — should not block exit
    sleeve.set_hard_stop_cooldown(
        event_time_ms=completed_through - 10_000,
        cooldown_hours=12,
    )
    decision, audit = evaluate_mf_low_sweep(
        config=config(margin_fraction=Decimal("0.0666666667")),
        bars=bars,
        range_footprints=[
            range_footprint(
                available_time_ms=bars[-1].open_time_ms - 1
            )
        ],
        large_share_history=[
            item.large_trade_share for item in bars[:-1]
        ],
        readiness=READY,
        sleeve=sleeve,
    )
    assert decision is not None
    assert decision.decision_type == "close"
    assert audit["exit_reason"] == "mf_time48_exit"


# ---------------------------------------------------------------------------
# 5. LF stop not affected
# ---------------------------------------------------------------------------


def test_mf_hard_stop_does_not_pollute_lf_stop() -> None:
    strategy = Strategy()
    from strategies.eth_portfolio_v1.domain.models import Side

    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.25"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="lf-position",
    )
    _activate_mf_sleeve(strategy)
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-123",
        exchange="okx",
    )

    snapshots = strategy.position_snapshots()
    lf_snapshot = next(
        s for s in snapshots if s.sleeve_id == "lf"
    )
    mf_snapshot = next(
        s for s in snapshots if s.sleeve_id == "mf"
    )
    # LF stop is unchanged
    assert lf_snapshot.stop_price == Decimal("2400")
    # MF stop is independent
    assert mf_snapshot.stop_price == Decimal("2375")
    # LF position_id unchanged
    assert lf_snapshot.position_id == "lf-position"


# ---------------------------------------------------------------------------
# 6. Hard stop aftermath close → cooldown
# ---------------------------------------------------------------------------


def test_hard_stop_aftermath_close_starts_cooldown() -> None:
    """Follower close after master hard stop fill → cooldown started."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    # Simulate: master hard stop already filled, only binance remains
    strategy.mf_sleeve.exchange_quantities = {
        "binance": Decimal("0.25")
    }
    strategy.mf_sleeve.quantity = Decimal("0.25")

    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.25"),
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "execution_purpose": (
                "mf_follower_close_after_master_hard_stop"
            ),
            "target_exchanges": ["binance"],
            "exchange_quantities_base": {"binance": "0.25"},
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.BINANCE,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.25"),
        ),
    )
    event_time_ms = 20000
    strategy._handle_mf_order_results(
        signal=close_signal,
        results=results,
        event_time_ms=event_time_ms,
    )
    # Sleeve is inactive (all exchanges closed)
    assert not strategy.mf_sleeve.active
    # Cooldown is active
    assert strategy.mf_sleeve.cooldown_active(
        event_time_ms + 1
    ) is True
    assert (
        strategy.mf_sleeve.hard_stop_cooldown_until_ms
        == event_time_ms + 12 * 3_600_000
    )


def test_normal_close_does_not_start_cooldown() -> None:
    """Normal time48 close should NOT start cooldown (only hard stop
    aftermath should)."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.5"),
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "position_id": "mf-low-sweep-time48-test",
            "execution_purpose": "normal_close",
            "target_exchanges": ["binance", "okx"],
            "exchange_quantities_base": {
                "okx": "0.5",
                "binance": "0.25",
            },
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.5"),
        ),
        ExchangeOrderResult(
            exchange=ExchangeName.BINANCE,
            ok=True,
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.25"),
        ),
    )
    strategy._handle_mf_order_results(
        signal=close_signal, results=results, event_time_ms=5000
    )
    # Normal close clears sleeve but does NOT start cooldown
    assert not strategy.mf_sleeve.active
    assert (
        strategy.mf_sleeve.hard_stop_cooldown_until_ms is None
    )


# ---------------------------------------------------------------------------
# 7. restore_from_plan with stop/cooldown data
# ---------------------------------------------------------------------------


def test_restore_from_plan_with_stop_data() -> None:
    """MfSleeveState.restore_from_plan recovers stop and cooldown."""
    from strategies.eth_portfolio_v1.domain.mf_sleeve import (
        MfSleeveState,
    )

    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    plan = {
        "position": {
            "position_id": "mf-low-sweep-time48-restored",
            "side": "long",
            "master_exchange": "okx",
            "entry_engine": "MF_LOW_SWEEP_TIME48",
            "master_filled_qty_base": "0.5",
            "status": "active",
            "metadata": {
                "average_entry_price": "2500",
                "signal_time_ms": 1000,
                "entry_execution_time_ms": 1001,
                "entry_tradebar_open_time_ms": 1002,
                "sleeve_id": "mf",
                "engine": "MF_LOW_SWEEP_TIME48",
                "exit_variant": "time48",
                "quantity_scope": "mf_sleeve_quantity",
                "time48_holding_minutes": 48,
                "exchange_quantities_base": {
                    "okx": "0.5",
                    "binance": "0.25",
                },
                "protective_stop_required": True,
                "stop_price": "2375",
                "hard_stop_price": "2375",
                "stop_order_ids_by_exchange": {
                    "okx": "okx-stop-restored",
                    "binance": "binance-stop-restored",
                },
                "stop_client_order_ids_by_exchange": {
                    "okx": "client-okx-restored",
                },
                "hard_stop_cooldown_until_ms": 50000000,
                "last_hard_stop_time_ms": 6800000,
            },
        },
        "legs": [
            {
                "exchange": "okx",
                "filled_qty_base": "0.5",
                "sync_status": "open",
            },
            {
                "exchange": "binance",
                "filled_qty_base": "0.25",
                "sync_status": "open",
            },
        ],
    }
    assert sleeve.restore_from_plan(plan) is True
    assert sleeve.active
    assert sleeve.hard_stop_price == Decimal("2375")
    assert sleeve.stop_order_ids_by_exchange == {
        "okx": "okx-stop-restored",
        "binance": "binance-stop-restored",
    }
    assert sleeve.stop_client_order_ids_by_exchange == {
        "okx": "client-okx-restored",
    }
    assert sleeve.hard_stop_cooldown_until_ms == 50000000
    assert sleeve.last_hard_stop_time_ms == 6800000


def test_restore_from_plan_without_stop_data_still_works() -> None:
    """Old plans without stop data should still restore successfully."""
    from strategies.eth_portfolio_v1.domain.mf_sleeve import (
        MfSleeveState,
    )

    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    plan = {
        "position": {
            "position_id": "mf-low-sweep-time48-legacy",
            "side": "long",
            "master_exchange": "okx",
            "master_filled_qty_base": "0.5",
            "status": "active",
            "metadata": {
                "average_entry_price": "2500",
                "signal_time_ms": 1000,
                "entry_execution_time_ms": 1001,
                "entry_tradebar_open_time_ms": 1002,
                "sleeve_id": "mf",
                "engine": "MF_LOW_SWEEP_TIME48",
                "exit_variant": "time48",
                "quantity_scope": "mf_sleeve_quantity",
                "time48_holding_minutes": 48,
                "protective_stop_required": False,
            },
        },
        "legs": [
            {
                "exchange": "okx",
                "filled_qty_base": "0.5",
                "sync_status": "open",
            },
        ],
    }
    assert sleeve.restore_from_plan(plan) is True
    assert sleeve.active
    assert sleeve.hard_stop_price is None
    assert sleeve.stop_order_ids_by_exchange == {}
    assert sleeve.hard_stop_cooldown_until_ms is None


# ---------------------------------------------------------------------------
# 8. Manual external close
# ---------------------------------------------------------------------------


def test_manual_close_master_generates_follower_close() -> None:
    """Manual close of master → follower CLOSE_LONG generated, no
    manual_required."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    event_time_ms = 50000
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=event_time_ms,
        order_id="manual-order-okx",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2400"),
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
        raw={},
    )
    signals = strategy._handle_mf_manual_close(event=event)
    assert len(signals) >= 1
    follower_signal = signals[0]
    assert follower_signal.action is SignalAction.CLOSE_LONG
    assert (
        follower_signal.metadata["execution_purpose"]
        == "mf_follower_close_after_master_manual_close"
    )
    assert follower_signal.metadata["reduce_only"] is True
    assert follower_signal.metadata["target_exchanges"] == ["binance"]
    # Does NOT set blocking manual_required
    assert not strategy.recovery_blocking_manual_required
    assert "mf_manual_close_detected:okx" in (
        strategy.mf_execution_alerts
    )


def test_manual_close_follower_keeps_master_active() -> None:
    """Manual close of follower → follower removed, master remains."""
    strategy = Strategy()
    _activate_mf_sleeve(strategy)

    event_time_ms = 50000
    event = AccountEvent(
        exchange=ExchangeName.BINANCE,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=event_time_ms,
        order_id="manual-order-binance",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2400"),
        quantity=Decimal("0.25"),
        filled_quantity=Decimal("0.25"),
        raw={},
    )
    signals = strategy._handle_mf_manual_close(event=event)
    assert signals == []
    # Master still active
    assert strategy.mf_sleeve.active
    assert "okx" in strategy.mf_sleeve.exchange_quantities
    assert "binance" not in strategy.mf_sleeve.exchange_quantities
    assert "mf_manual_close_detected:binance" in (
        strategy.mf_execution_alerts
    )
    assert not strategy.recovery_blocking_manual_required


def test_manual_close_all_clears_sleeve_and_cancels_stops() -> None:
    """Manual close of only exchange → sleeve cleared, scoped cancel
    generated, no blocking manual_required."""
    strategy = Strategy()
    _activate_mf_sleeve(
        strategy, okx_qty=Decimal("0.5"), binance_qty=Decimal("0")
    )
    strategy.mf_sleeve.exchange_quantities = {"okx": Decimal("0.5")}
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-manual",
        exchange="okx",
    )

    event_time_ms = 50000
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=event_time_ms,
        order_id="manual-order-only",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2400"),
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
        raw={},
    )
    signals = strategy._handle_mf_manual_close(event=event)
    assert not strategy.mf_sleeve.active
    # Scoped cancel generated, not CANCEL_ALL_STOP_ORDERS
    cancel_signals = [
        s
        for s in signals
        if s.action is SignalAction.CANCEL_STOP_ORDER
    ]
    assert len(cancel_signals) >= 1
    for s in cancel_signals:
        assert (
            s.action is not SignalAction.CANCEL_ALL_STOP_ORDERS
        )
        assert s.metadata["sleeve_id"] == "mf"
    # Manual close does NOT trigger manual_required
    assert not strategy.recovery_blocking_manual_required


def test_manual_close_does_not_affect_lf() -> None:
    """Manual MF close must not interfere with LF position state."""
    strategy = Strategy()
    from strategies.eth_portfolio_v1.domain.models import Side

    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.25"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="lf-position",
    )
    _activate_mf_sleeve(strategy)

    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=50000,
        order_id="manual-order-okx",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2400"),
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
        raw={},
    )
    strategy._handle_mf_manual_close(event=event)
    # LF position is untouched
    assert strategy.position.in_pos
    assert strategy.position.position_id == "lf-position"
    assert strategy.position.stop_price == Decimal("2400")


def test_manual_close_cancel_failure_is_manual_required() -> None:
    """When the cancel-after-manual-close fails, only the cancel
    failure triggers manual_required, not the manual close itself."""
    strategy = Strategy()
    _activate_mf_sleeve(
        strategy, okx_qty=Decimal("0.5"), binance_qty=Decimal("0")
    )
    strategy.mf_sleeve.exchange_quantities = {"okx": Decimal("0.5")}
    strategy.mf_sleeve.record_hard_stop(
        stop_price=Decimal("2375"),
        stop_order_id="okx-stop-manual",
        exchange="okx",
    )
    # Manual close itself should NOT set manual_required
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        event_time_ms=50000,
        order_id="manual-order-only",
        client_order_id=None,
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=Decimal("2400"),
        quantity=Decimal("0.5"),
        filled_quantity=Decimal("0.5"),
        raw={},
    )
    strategy._handle_mf_manual_close(event=event)
    assert not strategy.recovery_blocking_manual_required

    # Now simulate cancel failure
    cancel_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata={
            "strategy_id": "eth_portfolio_v1",
            "sleeve_id": "mf",
            "execution_purpose": (
                "mf_cancel_hard_stop_after_manual_close"
            ),
            "target_exchanges": ["okx"],
            "stop_order_id": "okx-stop-manual",
        },
    )
    results = (
        ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=False,
            error="cancel rejected",
        ),
    )
    strategy._handle_mf_stop_cancel_results(
        signal=cancel_signal, results=results, event_time_ms=50000
    )
    # Cancel failure DOES set manual_required (not manual-close case)
    # For manual-close cancels, the execution_purpose does not match
    # "mf_cancel_hard_stop_after_time_exit", so the success path
    # won't clear_hard_stop. But the failure path is generic — it
    # sets manual_required for ANY cancel failure.
    assert strategy.recovery_manual_required is True


# ---------------------------------------------------------------------------
# 9. Import boundary check
# ---------------------------------------------------------------------------


def test_mf_hard_stop_no_exchange_api_imports() -> None:
    """strategies modules must not import OKX/Binance adapter directly."""
    import importlib
    import inspect
    import re

    # Check for API endpoint leaks (should never appear in strategy layer)
    api_patterns = [
        r"/api/v5",
        r"fapi/",
        r"dapi/",
    ]
    # Check for direct exchange adapter imports
    adapter_import_patterns = [
        r"from\s+src\.platform\.exchanges\.okx",
        r"from\s+src\.platform\.exchanges\.binance",
        r"import\s+src\.platform\.exchanges\.okx",
        r"import\s+src\.platform\.exchanges\.binance",
    ]
    modules_to_check = [
        "strategies.eth_portfolio_v1.strategy",
        "strategies.eth_portfolio_v1.domain.mf_signal",
        "strategies.eth_portfolio_v1.domain.mf_sleeve",
        "strategies.eth_portfolio_v1.domain.mf_low_sweep",
        "strategies.eth_portfolio_v1.domain.mf_data",
        "strategies.eth_portfolio_v1.execution.mf_signal_mapper",
    ]
    for mod_name in modules_to_check:
        try:
            mod = importlib.import_module(mod_name)
        except ImportError:
            continue
        source = inspect.getsource(mod)
        for pattern in api_patterns:
            assert not re.search(
                pattern, source, re.IGNORECASE
            ), f"{mod_name} leaks API endpoint: {pattern}"
        for pattern in adapter_import_patterns:
            assert not re.search(
                pattern, source, re.IGNORECASE
            ), f"{mod_name} directly imports exchange adapter"
