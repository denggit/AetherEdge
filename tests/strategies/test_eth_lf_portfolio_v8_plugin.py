from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform import ExchangeName
from src.strategy.loader import load_strategy
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextConfig, MicroContextEngine
from strategies.eth_lf_portfolio_v8.strategy import Strategy


def _closed_kline(close_time_ms: int) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=close_time_ms,
        data={
            "open_time_ms": close_time_ms - 4 * 60 * 60 * 1000,
            "close_time_ms": close_time_ms,
            "open": "100",
            "high": "110",
            "low": "95",
            "close": "108",
            "volume": "1000",
            "quote_volume": "100000",
            "is_closed": True,
        },
    )


def _range_aggregate(close_time_ms: int, *, imbalance: str = "0.1", close_pos: str = "0.8") -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_AGGREGATE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=close_time_ms,
        data={
            "range_pct": "0.002",
            "bucket_start_ms": close_time_ms - 4 * 60 * 60 * 1000,
            "bucket_end_ms": close_time_ms,
            "bar_count": 8,
            "first_open": "100",
            "last_close": "108",
            "high": "110",
            "low": "95",
            "buy_notional_sum": "60000",
            "sell_notional_sum": "40000",
            "delta_notional_sum": "20000",
            "notional_sum": "100000",
            "micro_return_pct": "0.08",
            "imbalance": imbalance,
            "taker_buy_ratio": "0.6",
            "close_pos": close_pos,
        },
    )


def test_v8_strategy_loads_through_standard_loader() -> None:
    strategy = load_strategy("strategies.eth_lf_portfolio_v8:Strategy")

    assert isinstance(strategy, Strategy)


def test_v8_runtime_requirements_do_not_subscribe_order_book() -> None:
    strategy = Strategy()
    req = strategy.runtime_requirements()

    assert req["closed_kline"]["enabled"] is True
    assert req["closed_kline"]["interval"] == "4h"
    assert req["trades"]["stream_enabled"] is True
    assert req["range_bars"]["range_pct"] == "0.002"
    assert req["order_book"]["enabled"] is False
    assert req["private_account_stream"]["enabled"] is True


@pytest.mark.asyncio
async def test_v8_buffers_closed_kline_until_range_aggregate_arrives() -> None:
    strategy = Strategy()
    close_time_ms = 1_700_000_000_000

    signals = await strategy.on_market_feature(_closed_kline(close_time_ms))
    assert signals == []
    assert strategy.bar_ready_events == []

    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))
    assert signals == []
    assert len(strategy.bar_ready_events) == 1
    ready = strategy.bar_ready_events[0]
    assert ready.kline.close_time_ms == close_time_ms
    assert ready.range_aggregate is not None
    assert ready.final_entry_risk_scale == Decimal("1.3")


@pytest.mark.asyncio
async def test_v8_ignores_orderbook_by_runtime_requirement_but_keeps_interface() -> None:
    strategy = Strategy()
    assert await strategy.on_order_book(object()) == []  # type: ignore[arg-type]


def test_micro_context_soft_long_aligned_and_contra() -> None:
    engine = MicroContextEngine(MicroContextConfig(mode="soft"))
    close_time_ms = 1_700_000_000_000
    aligned = engine.evaluate(signal_side=Side.LONG, aggregate=_ctx(_range_aggregate(close_time_ms, imbalance="0.1", close_pos="0.8")))
    contra = engine.evaluate(signal_side=Side.LONG, aggregate=_ctx(_range_aggregate(close_time_ms, imbalance="-0.1", close_pos="0.2")))

    assert aligned.aligned is True
    assert aligned.entry_risk_scale == Decimal("1")
    assert contra.contra is True
    assert contra.entry_risk_scale == Decimal("0.50")


def test_micro_context_strict_blocks_not_aligned() -> None:
    engine = MicroContextEngine(MicroContextConfig(mode="strict"))
    close_time_ms = 1_700_000_000_000
    decision = engine.evaluate(signal_side=Side.SHORT, aggregate=_ctx(_range_aggregate(close_time_ms, imbalance="0", close_pos="0.5")))

    assert decision.context_available is True
    assert decision.entry_risk_scale == Decimal("0")
    assert decision.action == "NOT_ALIGNED_BLOCKED"


def _ctx(event: MarketFeatureEvent):
    from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_range_aggregate

    return parse_range_aggregate(event)
