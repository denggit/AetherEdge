from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.market_data.models import FixedTimeTradeBar, TradeFootprintFeature
from src.platform.exchanges.models import ExchangeName
from src.runtime.features import (
    fixed_time_trade_bar_feature,
    trade_footprint_feature,
)
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataBuffer,
    MfFeatureObserver,
)


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
