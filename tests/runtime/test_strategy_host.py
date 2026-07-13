from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import (
    MarketEventType,
    MarketKline,
    MarketOrderBook,
    MarketTicker,
    MarketTrade,
    TradeSide,
)
from src.runtime.strategy_host import StrategyHost
from src.signals import SignalAction, TradeSignal


def _signals() -> tuple[TradeSignal, TradeSignal]:
    return (
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.OPEN_LONG,
            quantity=Decimal("0.1"),
        ),
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CLOSE_LONG,
            quantity=Decimal("0.1"),
        ),
    )


def _kline() -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="1m",
        open_time_ms=0,
        close_time_ms=59_999,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1"),
    )


def _ticker() -> MarketTicker:
    return MarketTicker(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        time_ms=1,
    )


def _trade() -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_time_ms=2,
    )


def _order_book() -> MarketOrderBook:
    return MarketOrderBook(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        bids=(),
        asks=(),
        event_time_ms=3,
    )


def test_host_holds_only_the_strategy_instance() -> None:
    strategy = object()

    host = StrategyHost(strategy)

    assert vars(host) == {"_strategy": strategy}


@pytest.mark.asyncio
async def test_on_start_passes_same_snapshot_and_preserves_signals() -> None:
    snapshot = object()
    signals = _signals()
    received = []

    class Strategy:
        async def on_start(self, value):
            received.append(value)
            return signals

    result = await StrategyHost(Strategy()).on_start(snapshot)

    assert received == [snapshot]
    assert result is signals


@pytest.mark.asyncio
async def test_on_start_missing_or_none_returns_empty_sequence() -> None:
    class ReturnsNone:
        async def on_start(self, snapshot):
            return None

    assert await StrategyHost(object()).on_start(object()) == ()
    assert await StrategyHost(ReturnsNone()).on_start(object()) == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event", "callback_name"),
    [
        (_kline(), "on_kline"),
        (_ticker(), "on_ticker"),
        (_trade(), "on_trade"),
        (_order_book(), "on_order_book"),
    ],
)
async def test_market_event_dispatches_to_matching_callback(
    event, callback_name
) -> None:
    calls = []

    class Strategy:
        async def on_kline(self, value):
            calls.append(("on_kline", value))

        async def on_ticker(self, value):
            calls.append(("on_ticker", value))

        async def on_trade(self, value):
            calls.append(("on_trade", value))

        async def on_order_book(self, value):
            calls.append(("on_order_book", value))

    assert await StrategyHost(Strategy()).on_market_event(event) == ()
    assert calls == [(callback_name, event)]


@pytest.mark.asyncio
async def test_unknown_or_missing_market_callback_returns_empty_sequence() -> None:
    unknown = SimpleNamespace(event_type="unknown")

    assert await StrategyHost(object()).on_market_event(_trade()) == ()
    assert await StrategyHost(object()).on_market_event(unknown) == ()


@pytest.mark.asyncio
async def test_market_callback_preserves_signal_elements_and_order() -> None:
    signals = list(_signals())

    class Strategy:
        async def on_trade(self, event):
            return signals

    result = await StrategyHost(Strategy()).on_market_event(_trade())

    assert result is signals
    assert result[0] is signals[0]
    assert result[1] is signals[1]


@pytest.mark.asyncio
async def test_account_event_passes_same_event_and_missing_callback_is_empty() -> None:
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        event_time_ms=4,
    )
    received = []

    class Strategy:
        async def on_account_event(self, value):
            received.append(value)
            return None

    assert await StrategyHost(Strategy()).on_account_event(event) == ()
    assert received == [event]
    assert await StrategyHost(object()).on_account_event(event) == ()


@pytest.mark.asyncio
async def test_account_snapshot_passes_same_snapshot_and_missing_is_quiet() -> None:
    snapshot = object()
    received = []

    class Strategy:
        async def on_account_snapshot(self, value):
            received.append(value)

    assert await StrategyHost(Strategy()).on_account_snapshot(snapshot) is None
    assert received == [snapshot]
    assert await StrategyHost(object()).on_account_snapshot(snapshot) is None


@pytest.mark.asyncio
async def test_strategy_callback_exception_is_not_swallowed() -> None:
    class Strategy:
        async def on_start(self, snapshot):
            raise RuntimeError("strategy failed")

    with pytest.raises(RuntimeError, match="strategy failed"):
        await StrategyHost(Strategy()).on_start(object())


@pytest.mark.asyncio
async def test_host_does_not_invoke_signal_execution_or_order_behavior() -> None:
    signals = _signals()

    class Strategy:
        async def on_start(self, snapshot):
            return signals

        def _execute_signals(self, value):
            raise AssertionError("host must not execute signals")

        def place_order(self, value):
            raise AssertionError("host must not place orders")

    assert await StrategyHost(Strategy()).on_start(object()) is signals


@pytest.mark.asyncio
@pytest.mark.parametrize("container_type", [tuple, list])
async def test_order_results_preserves_arguments_and_follow_up_sequence(
    container_type,
) -> None:
    signal = _signals()[0]
    results = container_type(
        [ExchangeOrderResult(exchange=ExchangeName.OKX, ok=True)]
    )
    follow_up = container_type(_signals())
    received = []

    class Strategy:
        async def on_order_results(self, **kwargs):
            received.append(kwargs)
            return follow_up

    returned = await StrategyHost(Strategy()).on_order_results(
        signal=signal,
        results=results,
        source="root_source",
        event_time_ms=1234,
    )

    assert returned is follow_up
    assert list(returned) == list(follow_up)
    assert received[0]["signal"] is signal
    assert received[0]["results"] is results
    assert received[0]["source"] == "root_source"
    assert received[0]["event_time_ms"] == 1234


@pytest.mark.asyncio
async def test_order_results_missing_or_none_returns_empty_sequence() -> None:
    class ReturnsNone:
        async def on_order_results(self, **kwargs):
            return None

    kwargs = {
        "signal": _signals()[0],
        "results": (),
        "source": "test",
        "event_time_ms": None,
    }

    assert await StrategyHost(object()).on_order_results(**kwargs) == ()
    assert await StrategyHost(ReturnsNone()).on_order_results(**kwargs) == ()


@pytest.mark.asyncio
async def test_order_results_exception_is_not_swallowed() -> None:
    expected = RuntimeError("feedback failed")

    class Strategy:
        async def on_order_results(self, **kwargs):
            raise expected

    with pytest.raises(RuntimeError) as raised:
        await StrategyHost(Strategy()).on_order_results(
            signal=_signals()[0],
            results=(),
            source="test",
            event_time_ms=None,
        )

    assert raised.value is expected


@pytest.mark.asyncio
async def test_order_results_host_has_no_execution_side_effects() -> None:
    follow_up = _signals()

    class Strategy:
        async def on_order_results(self, **kwargs):
            return follow_up

        def coordinator(self):
            raise AssertionError("host must not call coordinator")

        def sync(self):
            raise AssertionError("host must not call sync")

        def save_order(self):
            raise AssertionError("host must not save orders")

        def emit(self):
            raise AssertionError("host must not emit alerts")

    returned = await StrategyHost(Strategy()).on_order_results(
        signal=_signals()[0],
        results=(),
        source="test",
        event_time_ms=None,
    )

    assert returned is follow_up
