from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path

import pytest

from src.market_data.models import (
    FixedTimeTradeBar,
    RangeFootprintFeature,
    TradeFootprintFeature,
)
from src.platform.exchanges.models import ExchangeName
from src.runtime.features import (
    fixed_time_trade_bar_feature,
    range_footprint_feature,
    trade_footprint_feature,
)
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataBuffer,
    MfFeatureObserver,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState


def _bar(open_time_ms: int) -> FixedTimeTradeBar:
    return FixedTimeTradeBar(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        available_time_ms=open_time_ms + 59_999,
        open=Decimal("1000"),
        high=Decimal("1001"),
        low=Decimal("999"),
        close=Decimal("1000.5"),
        volume=Decimal("3"),
        buy_volume=Decimal("2"),
        sell_volume=Decimal("1"),
        buy_notional=Decimal("2000"),
        sell_notional=Decimal("1000"),
        delta_volume=Decimal("1"),
        delta_notional=Decimal("1000"),
        abs_delta_notional=Decimal("1000"),
        trade_count=3,
    )


def _footprint(open_time_ms: int) -> TradeFootprintFeature:
    return TradeFootprintFeature(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        available_time_ms=open_time_ms + 59_999,
        delta_notional=Decimal("1000"),
        abs_delta_notional=Decimal("1000"),
        taker_buy_ratio=Decimal("0.6666666667"),
        close_pos=Decimal("0.75"),
        range_pct=Decimal("0.002"),
        return_pct=Decimal("0.0005"),
        fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
        context_available=True,
        quality="COMPLETE",
    )


def _range_footprint(available_time_ms: int) -> RangeFootprintFeature:
    return RangeFootprintFeature(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        range_bar_id=20260705000001,
        range_start_ms=available_time_ms - 1_000,
        range_end_ms=available_time_ms,
        available_time_ms=available_time_ms,
        fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
        fp_low_bucket_delta_pressure=Decimal("-0.2"),
        fp_high_bucket_delta_pressure=Decimal("0.4"),
        fp_delta_pressure=Decimal("0.1"),
        bucket_count=3,
        trade_count=5,
    )


def test_mf_observer_on_market_feature_returns_empty() -> None:
    observer = MfFeatureObserver()
    for event in (
        {"type": "fixed_time_trade_bar", "symbol": "ETH", "close_time_ms": 1},
        {"type": "trade_footprint_feature", "symbol": "ETH", "close_time_ms": 1},
        None,
        {},
    ):
        result = observer.on_market_feature(event)
        assert result == ()
        assert isinstance(result, tuple)
        assert len(result) == 0


def test_mf_observer_on_kline_returns_empty() -> None:
    observer = MfFeatureObserver()
    assert observer.on_kline() == ()
    assert observer.on_kline("fake_kline") == ()


def test_mf_observer_on_trade_returns_empty() -> None:
    observer = MfFeatureObserver()
    assert observer.on_trade() == ()
    assert observer.on_trade("fake_trade") == ()


def test_mf_observer_never_generates_trade_signals() -> None:
    """R007 guarantee: MF observer does not generate any TradeSignal."""
    from src.signals import TradeSignal

    observer = MfFeatureObserver()

    # Call all possible handlers
    results = [
        observer.on_market_feature({}),
        observer.on_kline({}),
        observer.on_trade({}),
    ]

    for result in results:
        assert result == ()
        for item in result:
            assert not isinstance(item, TradeSignal)


def test_tradebar_event_enters_bounded_buffer(tmp_path: Path) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=2,
        decision_buffer_max_minutes=2,
    )
    observer = MfFeatureObserver(buffer)
    base = 1_700_000_000_000

    for index in range(3):
        event = fixed_time_trade_bar_feature(
            _bar(base + index * 60_000),
            exchange=ExchangeName.OKX,
        )
        assert observer.on_market_feature(event) == ()

    assert buffer.bar_count == 2
    assert observer.audit()["tradebar_count"] == 3


def test_footprint_event_updates_audit_and_marks_minute_mismatch() -> None:
    observer = MfFeatureObserver()
    base = 1_700_000_000_000
    bar_event = fixed_time_trade_bar_feature(
        _bar(base), exchange=ExchangeName.OKX
    )
    footprint_event = trade_footprint_feature(
        _footprint(base + 60_000), exchange=ExchangeName.OKX
    )

    assert observer.on_market_feature(bar_event) == ()
    assert observer.on_market_feature(footprint_event) == ()

    audit = observer.audit()
    assert audit["minute_mismatch"] is True
    assert audit["latest_footprint"]["quality"] == "COMPLETE"
    assert (
        audit["latest_footprint"]["fp_max_bucket_abs_delta_pressure"]
        == "0.8"
    )


def test_range_footprint_event_reaches_observer_buffer(tmp_path: Path) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
    )
    observer = MfFeatureObserver(buffer)
    feature = _range_footprint(1_700_000_000_000)
    event = range_footprint_feature(feature, exchange=ExchangeName.OKX)

    assert observer.on_market_feature(event) == ()
    assert observer.audit()["range_footprint_count"] == 1
    assert buffer.range_footprints() == (feature,)


def test_stale_live_tradebar_is_blocked_before_signal_evaluation(
    tmp_path: Path,
) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
    )
    observer = MfFeatureObserver(
        buffer,
        sleeve=MfSleeveState(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
        ),
        readiness={
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "live_freshness_required": True,
            "live_freshness_max_age_ms": 300_000,
        },
    )
    stale = _bar(1_700_000_000_000)

    assert (
        observer.on_market_feature(
            fixed_time_trade_bar_feature(
                stale,
                exchange=ExchangeName.OKX,
                next_open_price=Decimal("1000"),
                next_open_time_ms=stale.close_time_ms + 1,
            )
        )
        == ()
    )
    assert observer.last_mf_signal_audit["blocked_reason"] == (
        "live_feature_stale"
    )
    assert observer.last_mf_signal_audit["live_fresh_ready"] is False


def _ready_state() -> dict[str, object]:
    return {
        "mf_signal_feature_ready": True,
        "range_footprint_ready": True,
        "tradebar_ready": True,
        "fixed_time_footprint_ready": True,
        "coverage_ready": True,
        "large_share_samples_ready": True,
        "live_freshness_required": True,
        "live_freshness_max_age_ms": 300_000,
        "source": "test",
    }


def _evaluate_fresh_tradebar(
    tmp_path: Path,
    readiness: dict[str, object],
) -> tuple[tuple[object, ...], dict[str, object]]:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
    )
    observer = MfFeatureObserver(
        buffer,
        sleeve=MfSleeveState(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
        ),
        readiness=readiness,
    )
    now_ms = int(time.time() * 1_000)
    open_time_ms = now_ms - (now_ms % 60_000) - 60_000
    bar = _bar(open_time_ms)
    observer.on_market_feature(
        range_footprint_feature(
            _range_footprint(open_time_ms - 1),
            exchange=ExchangeName.OKX,
        )
    )
    result = observer.on_market_feature(
        fixed_time_trade_bar_feature(
            bar,
            exchange=ExchangeName.OKX,
            next_open_price=bar.close,
            next_open_time_ms=bar.close_time_ms + 1,
        )
    )
    return result, observer.last_mf_signal_audit


@pytest.mark.parametrize(
    "missing_gate",
    (
        "mf_signal_feature_ready",
        "fixed_time_footprint_ready",
        "coverage_ready",
        "large_share_samples_ready",
    ),
)
def test_observer_real_evaluation_blocks_missing_readiness_gate(
    tmp_path: Path,
    missing_gate: str,
) -> None:
    readiness = _ready_state()
    readiness[missing_gate] = False

    result, audit = _evaluate_fresh_tradebar(tmp_path, readiness)

    assert result == ()
    assert audit["blocked_reason"] == "data_not_ready"
    assert audit["readiness_gates"][missing_gate] is False
    assert missing_gate in audit["missing_readiness_gates"]


def test_observer_real_evaluation_passes_all_readiness_gates(
    tmp_path: Path,
) -> None:
    result, audit = _evaluate_fresh_tradebar(tmp_path, _ready_state())

    assert result == ()
    assert audit["data_ready"] is True
    assert audit["missing_readiness_gates"] == []
    assert audit["blocked_reason"] != "data_not_ready"
    assert audit["live_fresh_ready"] is True
