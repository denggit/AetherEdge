from __future__ import annotations

from strategies.eth_portfolio_v1.domain.mf_data import MfFeatureObserver


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
