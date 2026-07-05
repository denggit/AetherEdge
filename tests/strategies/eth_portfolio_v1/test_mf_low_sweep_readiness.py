from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform.exchanges.models import ExchangeName
from src.runtime.features import (
    range_footprint_feature,
    trade_feature_readiness_feature,
)
from src.runtime.market_features import dispatch_market_feature_event
from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataBuffer,
    MfFeatureObserver,
)
from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
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
    historical_large_shares,
    range_footprint,
    seed_large_share_history,
    setup_bars,
)


def _evaluate(
    *,
    readiness=READY,
    cfg=None,
    bars=None,
    contexts=None,
    large_share_history=None,
):
    bars = setup_bars() if bars is None else bars
    contexts = (
        [range_footprint(available_time_ms=bars[-1].open_time_ms - 1)]
        if contexts is None
        else contexts
    )
    return evaluate_mf_low_sweep(
        config=cfg or config(),
        bars=bars,
        range_footprints=contexts,
        large_share_history=(
            historical_large_shares()
            if large_share_history is None
            else large_share_history
        ),
        readiness=readiness,
        sleeve=MfSleeveState(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
            enabled=True,
        ),
        next_open_price=bars[-1].close,
        next_open_time_ms=bars[-1].close_time_ms + 1,
    )


def test_data_not_ready_produces_no_mf_signal() -> None:
    decision, audit = _evaluate(
        readiness={
            "mf_signal_feature_ready": False,
            "range_footprint_ready": True,
            "tradebar_ready": True,
        }
    )
    assert decision is None
    assert audit["blocked_reason"] == "data_not_ready"


def test_range_footprint_not_ready_produces_no_mf_signal() -> None:
    decision, audit = _evaluate(
        readiness={
            "mf_signal_feature_ready": True,
            "range_footprint_ready": False,
            "tradebar_ready": True,
        }
    )
    assert decision is None
    assert audit["data_ready"] is False


def test_missing_large_share_threshold_produces_no_mf_signal() -> None:
    decision, audit = _evaluate(large_share_history=())
    assert decision is None
    assert audit["blocked_reason"] == "missing_feature"
    assert "large_share_rq80_90d" in audit["missing_features"]


def test_future_available_time_produces_no_signal_and_fails_causal_audit() -> None:
    bars = setup_bars()
    future = range_footprint(
        available_time_ms=bars[-1].close_time_ms + 2
    )
    decision, audit = _evaluate(bars=bars, contexts=[future])
    assert decision is None
    assert audit["causal_ok"] is False


def test_ready_but_no_setup_produces_no_signal() -> None:
    bars = setup_bars(
        latest_low="99",
        latest_close="99.5",
        latest_high="101",
    )
    decision, audit = _evaluate(bars=bars)
    assert decision is None
    assert audit["blocked_reason"] == "no_setup"


@pytest.mark.asyncio
async def test_runtime_readiness_event_enables_exact_setup_without_manual_set(
    tmp_path,
) -> None:
    cfg = config()
    bars = setup_bars()
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=2_000,
        decision_buffer_max_minutes=2_000,
    )
    seed_large_share_history(
        buffer, before_open_time_ms=bars[0].open_time_ms
    )
    buffer.append_many(bars[:-1])
    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
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
        sizing_provider=lambda: MfSizingInput(
            equity=Decimal("1000"),
            available_equity=Decimal("500"),
        ),
    )

    class ObserverStrategy:
        def market_feature_observers(self):
            return (observer,)

    strategy = ObserverStrategy()
    readiness_event = trade_feature_readiness_feature(
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        event_time_ms=bars[-1].open_time_ms - 2,
        readiness=READY,
        source="runtime_test_supervisor",
    )
    assert (
        await dispatch_market_feature_event(strategy, readiness_event)
        == ()
    )
    context = range_footprint(
        available_time_ms=bars[-1].open_time_ms - 1
    )
    assert (
        await dispatch_market_feature_event(
            strategy,
            range_footprint_feature(
                context, exchange=ExchangeName.OKX
            ),
        )
        == ()
    )
    signals = await dispatch_market_feature_event(
        strategy, closed_tradebar_event(bars[-1])
    )
    assert len(signals) == 1
    assert signals[0].action is SignalAction.OPEN_LONG
    assert observer.last_mf_signal_audit["readiness_source"] == (
        "runtime_test_supervisor"
    )
