from __future__ import annotations

from decimal import Decimal

from src.platform.exchanges.models import ExchangeName
from src.runtime.features import fixed_time_trade_bar_feature
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

from _mf_test_helpers import READY, config, range_footprint, setup_bars


def _entry_result(tmp_path, *, bars=None, pressure="0.80"):
    cfg = config()
    bars = setup_bars() if bars is None else bars
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=100,
        decision_buffer_max_minutes=100,
        large_share_quantile_window_days=1,
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
            equity=Decimal("1000"),
            available_equity=Decimal("500"),
        ),
    )
    signals = observer.on_market_feature(
        fixed_time_trade_bar_feature(
            bars[-1], exchange=ExchangeName.OKX
        )
    )
    return signals, observer, sleeve


def test_a0_single_swing_generates_next_open_mf_signal(tmp_path) -> None:
    signals, observer, _ = _entry_result(tmp_path)
    assert len(signals) == 1
    assert signals[0].action is SignalAction.OPEN_LONG
    assert observer.last_mf_signal_audit["entry_signal"] is True


def test_fp_abs_delta_below_threshold_produces_no_signal(tmp_path) -> None:
    signals, observer, _ = _entry_result(tmp_path, pressure="0.59")
    assert signals == ()
    assert observer.last_mf_signal_audit["entry_candidate"] is False


def test_large_share_below_historical_threshold_produces_no_signal(
    tmp_path,
) -> None:
    bars = setup_bars(latest_large_share="0.05")
    signals, _, _ = _entry_result(tmp_path, bars=bars)
    assert signals == ()


def test_no_single_swing_produces_no_signal(tmp_path) -> None:
    bars = setup_bars(latest_close="95")
    signals, observer, _ = _entry_result(tmp_path, bars=bars)
    assert signals == ()
    assert observer.last_mf_signal_audit["single_swing"] is False


def test_entry_signal_has_independent_mf_scope(tmp_path) -> None:
    signals, observer, sleeve = _entry_result(tmp_path)
    signal = signals[0]
    assert signal.metadata["sleeve_id"] == "mf"
    assert signal.metadata["position_id"].startswith(
        "mf-low-sweep-time48-"
    )
    assert signal.metadata["position_id"] == sleeve.position_id
    assert signal.metadata["entry_mode"] == "next_open"
    assert signal.metadata["sizing_input"]["position_fraction"] == "0.10"
    assert signal.metadata["sizing_input"]["equity"] == "1000"
    assert signal.quantity == Decimal("100") / Decimal("89.5")
    causal = signal.metadata["audit"]
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
