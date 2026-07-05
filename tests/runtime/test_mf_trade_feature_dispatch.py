from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.runtime.runner import LiveRuntimeRunner


class _Env:
    def get(self, key: str, default: str) -> str:
        return default


class _Strategy:
    def trade_feature_runtime_config(self):
        return {
            "enabled": True,
            "range_pct": "0.002",
            "range_price_step": "1",
        }


def _trade(*, time_ms: int, price: str, side: TradeSide) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=side,
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
    )


@pytest.mark.asyncio
async def test_runtime_dispatches_all_trade_derived_feature_types() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=_Strategy())
    runner._project_env = _Env()
    runner._fixed_time_trade_bar_builder = None
    runner._trade_footprint_builder = None
    runner._range_footprint_builder = None
    events = []

    async def capture(event):
        events.append(event)

    runner.process_market_feature = capture
    base = 1_700_000_000_000
    await runner._dispatch_trade_derived_features(
        _trade(time_ms=base + 1_000, price="100", side=TradeSide.BUY)
    )
    await runner._dispatch_trade_derived_features(
        _trade(
            time_ms=base + 60_001,
            price="101",
            side=TradeSide.SELL,
        )
    )

    assert tuple(event.type_value for event in events) == (
        "range_footprint_feature",
        "fixed_time_trade_bar",
        "trade_footprint_feature",
    )
    assert events[0].data["source"] == "trade_derived_range_footprint"
    assert events[1].data["source"] == "trade_derived"
    assert events[2].data["source"] == "trade_derived"


@pytest.mark.asyncio
async def test_runtime_bridge_is_inert_when_strategy_disables_it() -> None:
    class Disabled:
        def trade_feature_runtime_config(self):
            return {"enabled": False}

    runner = object.__new__(LiveRuntimeRunner)
    runner.context = SimpleNamespace(strategy=Disabled())
    runner._project_env = _Env()
    runner._fixed_time_trade_bar_builder = None
    runner._trade_footprint_builder = None
    runner._range_footprint_builder = None
    events = []

    async def capture(event):
        events.append(event)

    runner.process_market_feature = capture
    await runner._dispatch_trade_derived_features(
        _trade(
            time_ms=1_700_000_000_000,
            price="100",
            side=TradeSide.BUY,
        )
    )
    assert events == []
