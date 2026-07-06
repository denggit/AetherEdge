from __future__ import annotations

from decimal import Decimal

from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataBuffer,
    MfFeatureObserver,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.execution.mf_signal_mapper import (
    MfSignalMapper,
    MfSizingInput,
)

from _mf_test_helpers import (
    READY,
    closed_tradebar_event,
    config,
    range_footprint,
    seed_large_share_history,
    setup_bars,
)


def _entry_result(
    tmp_path,
    *,
    bars=None,
    pressure="0.80",
    history_value="0.10",
    equity=Decimal("1000"),
    available_equity=Decimal("500"),
):
    cfg = config()
    bars = setup_bars() if bars is None else bars
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=100,
        decision_buffer_max_minutes=100,
        large_share_quantile_window_days=90,
    )
    seed_large_share_history(
        buffer,
        before_open_time_ms=bars[0].open_time_ms,
        value=history_value,
    )
    buffer.append_many(bars[:-1])
    buffer.append_range_footprint(
        range_footprint(
            available_time_ms=bars[-1].open_time_ms - 1,
            pressure=pressure,
        )
    )
    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    observer = MfFeatureObserver(
        buffer,
        config=cfg,
        sleeve=sleeve,
        signal_mapper=MfSignalMapper(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
            config=cfg,
        ),
        readiness=READY,
        sizing_provider=lambda: MfSizingInput(
            equity=equity,
            available_equity=available_equity,
        ),
    )
    signals = observer.on_market_feature(
        closed_tradebar_event(bars[-1])
    )
    return signals, observer, sleeve


def test_a0_single_swing_generates_next_open_mf_signal(tmp_path) -> None:
    signals, observer, _ = _entry_result(tmp_path)
    assert len(signals) == 1
    assert signals[0].action is SignalAction.OPEN_LONG
    assert observer.last_mf_signal_audit["entry_signal"] is True
    assert observer.last_mf_signal_audit["large_share_rq80_90d"] is True


def test_fp_abs_delta_below_threshold_produces_no_signal(tmp_path) -> None:
    signals, observer, _ = _entry_result(tmp_path, pressure="0.59")
    assert signals == ()
    assert observer.last_mf_signal_audit["entry_candidate"] is False


def test_large_share_below_historical_threshold_produces_no_signal(
    tmp_path,
) -> None:
    bars = setup_bars(latest_large_share="0.05")
    signals, observer, _ = _entry_result(tmp_path, bars=bars)
    assert signals == ()
    assert observer.last_mf_signal_audit["large_share_rq80_90d"] is False


def test_large_share_history_only_changes_quantile_feature(
    tmp_path,
) -> None:
    low_signals, low_observer, _ = _entry_result(
        tmp_path,
        history_value="0.10",
    )
    high_signals, high_observer, _ = _entry_result(
        tmp_path,
        history_value="0.95",
    )

    assert len(low_signals) == 1
    assert high_signals == ()
    assert low_observer.last_mf_signal_audit[
        "large_share_rq80_90d"
    ] is True
    assert high_observer.last_mf_signal_audit[
        "large_share_rq80_90d"
    ] is False
    for field in (
        "swing_low",
        "swing_low_age",
        "swing_low_prominence_pct",
        "low_sweep_event",
        "spike_pct",
        "close_pos",
    ):
        assert low_observer.last_mf_signal_audit[field] == (
            high_observer.last_mf_signal_audit[field]
        )


def test_no_primary_low_sweep_event_produces_no_signal(tmp_path) -> None:
    bars = setup_bars(latest_close="95")
    signals, observer, _ = _entry_result(tmp_path, bars=bars)
    assert signals == ()
    assert observer.last_mf_signal_audit["single_swing"] is True
    assert observer.last_mf_signal_audit["low_sweep_event"] is False


def test_entry_signal_has_independent_mf_scope(tmp_path) -> None:
    signals, observer, sleeve = _entry_result(tmp_path)
    signal = signals[0]
    assert signal.metadata["sleeve_id"] == "mf"
    assert signal.metadata["position_id"].startswith(
        "mf-low-sweep-time48-"
    )
    assert signal.metadata["position_id"] == sleeve.position_id
    assert signal.metadata["entry_mode"] == "next_open"
    causal = signal.metadata["audit"]
    assert signal.metadata["entry_tradebar_open_time_ms"] == causal[
        "entry_tradebar_open_time_ms"
    ]
    assert signal.metadata["time48_holding_minutes"] == 48
    assert signal.metadata["fixed_time_exit_holding_minutes"] == 48
    assert signal.metadata["unconfirmed_master_close_policy"] == "manual_required"
    assert signal.metadata["quantity_scope"] == "mf_sleeve_quantity"
    assert signal.metadata["protective_stop_required"] is False
    assert signal.metadata["sizing_input"]["position_fraction"] == "0.10"
    assert signal.metadata["sizing_input"]["equity"] == "1000"
    assert signal.quantity == Decimal("100") / Decimal("90")
    assert (
        signal.metadata["entry_execution_time_ms"]
        > causal["used_tradebar_close_time_ms"]
    )
    assert (
        causal["used_range_footprint_available_time_ms"]
        <= signal.metadata["signal_time_ms"]
    )
    assert (
        observer.last_mf_signal_audit[
            "used_range_footprint_available_time_ms"
        ]
        is not None
    )
    assert signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS


def test_available_equity_not_ready_blocks_open(tmp_path) -> None:
    signals, observer, _ = _entry_result(
        tmp_path, available_equity=None
    )
    assert signals == ()
    assert observer.last_mf_signal_audit["blocked_reason"] == (
        "sizing_not_ready"
    )


def test_available_equity_caps_mf_target_notional(tmp_path) -> None:
    signals, _, _ = _entry_result(
        tmp_path, available_equity=Decimal("50")
    )
    assert signals[0].quantity == Decimal("50") / Decimal("90")
    assert signals[0].metadata["sizing_input"]["target_notional"] == "50"
