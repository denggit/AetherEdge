from __future__ import annotations

from decimal import Decimal

from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.execution.mf_signal_mapper import (
    MfSignalMapper,
)

from _mf_test_helpers import READY, config, range_footprint, setup_bars


def _active_sleeve(*, entry_time_ms: int) -> MfSleeveState:
    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    sleeve.reserve_open(
        position_id="mf-low-sweep-time48-entry",
        quantity=Decimal("0.25"),
        signal_time_ms=entry_time_ms,
        entry_execution_time_ms=entry_time_ms,
        tradebar_open_time_ms=entry_time_ms,
    )
    sleeve.confirm_open(
        quantity=Decimal("0.25"),
        average_entry_price=Decimal("100"),
        entry_time_ms=entry_time_ms,
    )
    return sleeve


def _evaluate_exit(*, holding_minutes: int):
    bars = setup_bars()
    completed_through = bars[-1].close_time_ms + 1
    sleeve = _active_sleeve(
        entry_time_ms=completed_through - holding_minutes * 60_000
    )
    decision, audit = evaluate_mf_low_sweep(
        config=config(),
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
    return decision, audit, sleeve


def test_holding_less_than_48_completed_minutes_has_no_exit() -> None:
    decision, audit, _ = _evaluate_exit(holding_minutes=47)
    assert decision is None
    assert audit["time48_due"] is False


def test_holding_48_completed_minutes_generates_close() -> None:
    decision, audit, sleeve = _evaluate_exit(holding_minutes=48)
    assert decision is not None
    assert decision.decision_type == "close"
    signal = MfSignalMapper(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        config=config(),
    ).map_close(decision, sleeve=sleeve)
    assert signal is not None
    assert signal.action is SignalAction.CLOSE_LONG
    assert signal.metadata["reduce_only"] is True
    assert signal.metadata["position_id"] == sleeve.position_id
    assert signal.quantity == sleeve.quantity
    assert audit["exit_reason"] == "mf_time48_exit"
