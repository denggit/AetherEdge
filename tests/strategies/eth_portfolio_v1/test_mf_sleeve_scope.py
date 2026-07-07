from __future__ import annotations

from decimal import Decimal

from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName, OrderStatus
from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.mf_signal import MfSignalDecision
from strategies.eth_portfolio_v1.domain.models import (
    Side,
    V8DecisionType,
    V8TradeDecision,
)
from strategies.eth_portfolio_v1.execution.mf_signal_mapper import (
    MfSignalMapper,
    MfSizingInput,
)
from strategies.eth_portfolio_v1.strategy import Strategy


def _activate_mf(strategy: Strategy) -> None:
    strategy.mf_sleeve.enabled = True
    strategy.mf_sleeve.reserve_open(
        position_id="mf-low-sweep-time48-recovered",
        quantity=Decimal("0.10"),
        signal_time_ms=10,
        entry_execution_time_ms=10,
        tradebar_open_time_ms=0,
        exchange_quantities={
            "okx": Decimal("0.10"),
            "binance": Decimal("0.05"),
        },
    )
    strategy.mf_sleeve.confirm_open(
        quantity=Decimal("0.10"),
        average_entry_price=Decimal("2500"),
        entry_time_ms=10,
        exchange_quantities={
            "okx": Decimal("0.10"),
            "binance": Decimal("0.05"),
        },
        master_exchange="okx",
    )


def _activate_lf(strategy: Strategy) -> None:
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.25"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="lf-position",
    )


def test_lf_and_mf_ledgers_remain_independent() -> None:
    strategy = Strategy()
    _activate_lf(strategy)
    _activate_mf(strategy)
    assert strategy.position.position_id == "lf-position"
    assert (
        strategy.mf_sleeve.position_id
        == "mf-low-sweep-time48-recovered"
    )


def test_mf_exit_scope_does_not_cancel_lf_stop() -> None:
    strategy = Strategy()
    _activate_lf(strategy)
    _activate_mf(strategy)
    mf_position_id = strategy.mf_sleeve.position_id
    lf_stop = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.25"),
        stop_price=Decimal("2450"),
        reason="LF_SCOPED_STOP",
        bar_close_time_ms=2,
    )[0]
    assert lf_stop.metadata["position_id"] == "lf-position"
    assert lf_stop.metadata["sleeve_id"] == "lf"
    assert strategy.mf_sleeve.position_id == mf_position_id
    assert lf_stop.action is not SignalAction.CANCEL_ALL_STOP_ORDERS


def test_mf_close_signal_leaves_lf_position_untouched() -> None:
    strategy = Strategy()
    _activate_lf(strategy)
    _activate_mf(strategy)
    decision = MfSignalDecision(
        decision_type="close",
        signal_time_ms=100,
        decision_time_ms=100,
        entry_execution_time_ms=10,
        position_id=strategy.mf_sleeve.position_id or "",
        reference_price=Decimal("2500"),
        reason="mf_time48_exit",
    )
    signal = MfSignalMapper(
        strategy_id=strategy.config.strategy_id,
        symbol=strategy.config.symbol,
        config=strategy.config.mf,
        master_exchange="okx",
    ).map_close(decision, sleeve=strategy.mf_sleeve)
    assert signal is not None
    assert signal.metadata["position_id"] == strategy.mf_sleeve.position_id
    assert signal.metadata["reduce_only"] is True
    assert signal.quantity == Decimal("0.10")
    assert signal.metadata["target_exchanges"] == ["binance", "okx"]
    assert signal.metadata["exchange_quantities_base"] == {
        "binance": "0.05",
        "okx": "0.10",
    }
    assert strategy.position.position_id == "lf-position"
    assert signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS


def test_lf_decision_mapping_leaves_mf_position_untouched() -> None:
    strategy = Strategy()
    _activate_mf(strategy)
    mf_position_id = strategy.mf_sleeve.position_id
    signal = strategy.signal_mapper.map_decision(
        V8TradeDecision(
            decision_type=V8DecisionType.OPEN,
            side=Side.LONG,
            symbol=strategy.config.symbol,
            quantity=Decimal("0.20"),
            reason="LF_ONLY",
            metadata={"position_id": "lf-new-position"},
        )
    )[0]
    assert signal.metadata["sleeve_id"] == "lf"
    assert strategy.mf_sleeve.position_id == mf_position_id


def test_recovery_snapshot_includes_active_mf_sleeve() -> None:
    strategy = Strategy()
    _activate_lf(strategy)
    _activate_mf(strategy)
    snapshots = strategy.position_snapshots()
    assert {snapshot.sleeve_id for snapshot in snapshots} == {"lf", "mf"}
    mf = next(
        snapshot for snapshot in snapshots if snapshot.sleeve_id == "mf"
    )
    assert mf.position_id == "mf-low-sweep-time48-recovered"
    assert mf.base_quantity == Decimal("0.10")
    assert mf.metadata["exchange_quantities_base"] == {
        "binance": "0.05",
        "okx": "0.10",
    }


def test_mf_open_confirmation_uses_master_filled_quantity() -> None:
    strategy = Strategy()
    signal = MfSignalMapper(
        strategy_id=strategy.config.strategy_id,
        symbol=strategy.config.symbol,
        config=strategy.config.mf,
        master_exchange="okx",
    ).map_open(
        MfSignalDecision(
            decision_type="open",
            signal_time_ms=100,
            decision_time_ms=100,
            entry_execution_time_ms=101,
            position_id="mf-low-sweep-time48-confirm",
            reference_price=Decimal("3000"),
            reason="mf_low_sweep_entry",
        ),
        sizing=MfSizingInput(
            equity=Decimal("1000"),
            available_equity=Decimal("1000"),
            equity_by_exchange={
                "okx": Decimal("1000"),
                "binance": Decimal("500"),
            },
            available_equity_by_exchange={
                "okx": Decimal("1000"),
                "binance": Decimal("500"),
            },
            leverage_by_exchange={
                "okx": Decimal("15"),
                "binance": Decimal("15"),
            },
            margin_mode_by_exchange={
                "okx": "isolated",
                "binance": "isolated",
            },
        ),
    )
    assert signal is not None
    strategy.mf_sleeve.reserve_open(
        position_id="mf-low-sweep-time48-confirm",
        quantity=signal.quantity or Decimal("0"),
        signal_time_ms=100,
        entry_execution_time_ms=101,
        tradebar_open_time_ms=101,
        exchange_quantities={
            "okx": Decimal("0.5"),
            "binance": Decimal("0.25"),
        },
    )

    strategy._handle_mf_order_results(
        signal=signal,
        results=(
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                status=OrderStatus.FILLED,
                filled_quantity=Decimal("0.49"),
                avg_fill_price=Decimal("3001"),
            ),
            ExchangeOrderResult(
                exchange=ExchangeName.BINANCE,
                ok=True,
                status=OrderStatus.FILLED,
                filled_quantity=Decimal("0.24"),
                avg_fill_price=Decimal("3002"),
            ),
        ),
        event_time_ms=101,
    )

    assert strategy.mf_sleeve.quantity == Decimal("0.49")
    assert strategy.mf_sleeve.exchange_quantities == {
        "binance": Decimal("0.24"),
        "okx": Decimal("0.49"),
    }


def test_mf_close_keeps_unclosed_follower_quantity() -> None:
    strategy = Strategy()
    _activate_mf(strategy)
    decision = MfSignalDecision(
        decision_type="close",
        signal_time_ms=200,
        decision_time_ms=200,
        entry_execution_time_ms=10,
        position_id=strategy.mf_sleeve.position_id or "",
        reference_price=Decimal("2500"),
        reason="mf_time48_exit",
    )
    signal = MfSignalMapper(
        strategy_id=strategy.config.strategy_id,
        symbol=strategy.config.symbol,
        config=strategy.config.mf,
        master_exchange="okx",
    ).map_close(decision, sleeve=strategy.mf_sleeve)
    assert signal is not None

    strategy._handle_mf_order_results(
        signal=signal,
        results=(
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                status=OrderStatus.FILLED,
                filled_quantity=Decimal("0.10"),
            ),
            ExchangeOrderResult(
                exchange=ExchangeName.BINANCE,
                ok=False,
                error="follower close failed",
            ),
        ),
        event_time_ms=200,
    )

    assert strategy.mf_sleeve.pending_close is True
    assert strategy.mf_sleeve.quantity == Decimal("0.05")
    assert strategy.mf_sleeve.exchange_quantities == {
        "binance": Decimal("0.05")
    }

    retry_signal = MfSignalMapper(
        strategy_id=strategy.config.strategy_id,
        symbol=strategy.config.symbol,
        config=strategy.config.mf,
        master_exchange="okx",
    ).map_close(decision, sleeve=strategy.mf_sleeve)

    assert retry_signal is not None
    assert retry_signal.quantity == Decimal("0.05")
    assert retry_signal.metadata["target_exchanges"] == ["binance"]
    assert retry_signal.metadata["exchange_quantities_base"] == {
        "binance": "0.05"
    }
