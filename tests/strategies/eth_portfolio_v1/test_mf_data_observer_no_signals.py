from __future__ import annotations

from decimal import Decimal
from pathlib import Path

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


# ---------------------------------------------------------------------------
# R011: Tightened readiness gate tests
# ---------------------------------------------------------------------------

def test_observer_no_signal_when_fixed_time_footprint_not_ready(
    tmp_path: Path,
) -> None:
    """MF observer must not produce signals when fixed_time_footprint_ready is
    False, even if mf_signal_feature_ready is True."""
    observer = MfFeatureObserver()

    observer.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": False,
            "coverage_ready": False,
            "large_share_samples_ready": True,
        },
        source="test",
    )

    audit = observer.last_mf_signal_audit
    assert audit["data_ready"] is False
    gates = audit.get("readiness_gates", {})
    assert gates.get("fixed_time_footprint_ready") is False
    assert gates.get("coverage_ready") is False


def test_observer_no_signal_when_coverage_not_ready(
    tmp_path: Path,
) -> None:
    """MF observer must not produce signals when coverage_ready is False."""
    observer = MfFeatureObserver()

    observer.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": True,
            "coverage_ready": False,
            "large_share_samples_ready": True,
        },
        source="test",
    )

    audit = observer.last_mf_signal_audit
    assert audit["data_ready"] is False
    assert audit["readiness_gates"]["coverage_ready"] is False


def test_observer_no_signal_when_large_share_samples_not_ready(
    tmp_path: Path,
) -> None:
    """MF observer must not produce signals when large_share_samples_ready is
    False."""
    observer = MfFeatureObserver()

    observer.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": True,
            "coverage_ready": True,
            "large_share_samples_ready": False,
        },
        source="test",
    )

    audit = observer.last_mf_signal_audit
    assert audit["data_ready"] is False
    assert audit["readiness_gates"]["large_share_samples_ready"] is False


def test_observer_all_gates_true_data_is_ready(
    tmp_path: Path,
) -> None:
    """When all 6 readiness gates are True, data_ready is True."""
    observer = MfFeatureObserver()

    observer.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": True,
            "coverage_ready": True,
            "large_share_samples_ready": True,
        },
        source="test",
    )

    audit = observer.last_mf_signal_audit
    assert audit["data_ready"] is True
    gates = audit["readiness_gates"]
    assert all(gates.values())


def test_observer_readiness_transition_logs_missing_fields(
    tmp_path: Path,
) -> None:
    """When readiness changes, missing gates are listed in the log."""
    observer = MfFeatureObserver()

    observer.set_readiness(
        {
            "mf_signal_feature_ready": False,
            "range_footprint_ready": False,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": False,
            "coverage_ready": False,
            "large_share_samples_ready": False,
        },
        source="test",
    )

    # After setting partial readiness, data_ready must be False
    assert observer.last_mf_signal_audit["data_ready"] is False
