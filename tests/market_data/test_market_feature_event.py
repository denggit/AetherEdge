from __future__ import annotations

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.exchanges.models import ExchangeName


def _event(
    *,
    event_type: MarketFeatureEventType | str = MarketFeatureEventType.CLOSED_KLINE,
    event_time_ms: int = 100,
    available_time_ms: int | None = None,
) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=event_type,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="1m",
        event_time_ms=event_time_ms,
        available_time_ms=available_time_ms,
    )


def test_effective_available_time_defaults_to_event_time() -> None:
    event = _event(event_time_ms=123)

    assert event.available_time_ms is None
    assert event.effective_available_time_ms == 123


@pytest.mark.parametrize("available_time_ms", (100, 150))
def test_explicit_available_time_at_or_after_event_time_is_valid(
    available_time_ms: int,
) -> None:
    event = _event(event_time_ms=100, available_time_ms=available_time_ms)

    assert event.effective_available_time_ms == available_time_ms


def test_available_time_before_event_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="greater than or equal"):
        _event(event_time_ms=100, available_time_ms=99)


def test_negative_event_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="event_time_ms"):
        _event(event_time_ms=-1)


def test_negative_available_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="available_time_ms"):
        _event(event_time_ms=0, available_time_ms=-1)


@pytest.mark.parametrize(
    ("event_type", "expected"),
    (
        (MarketFeatureEventType.RANGE_AGGREGATE, "range_aggregate"),
        ("custom_context", "custom_context"),
    ),
)
def test_type_value_remains_compatible(
    event_type: MarketFeatureEventType | str,
    expected: str,
) -> None:
    assert _event(event_type=event_type).type_value == expected
