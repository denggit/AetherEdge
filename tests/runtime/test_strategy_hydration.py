from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from src.market_data.models import TimeRange
from src.platform.data.models import MarketKline
from src.platform.exchanges.models import ExchangeName
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.signal_execution_service import RuntimeSignalExecutionService
from strategies.eth_lf_portfolio_v8.strategy import Strategy


class FakeKlineRepository:
    def __init__(self, rows: list[MarketKline]) -> None:
        self.rows = rows

    def load(self, *, symbol: str, interval: str, time_range: TimeRange) -> list[MarketKline]:
        return [
            row
            for row in self.rows
            if row.symbol == symbol
            and row.interval == interval
            and time_range.start_time_ms <= row.open_time_ms <= time_range.end_time_ms
        ]


def _kline(open_time_ms: int) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4h",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 4 * 60 * 60_000 - 1,
        open=Decimal("2000"),
        high=Decimal("2100"),
        low=Decimal("1900"),
        close=Decimal("2050"),
        volume=Decimal("100"),
        is_closed=True,
    )


def test_closed_kline_warmup_replays_history_into_v9c_feature_buffer_before_first_live_bar():
    strategy = Strategy()
    runner = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    runner._signal_execution_service = RuntimeSignalExecutionService()
    runner.context = SimpleNamespace(strategy=strategy)
    runner.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")
    runner._closed_bar_interval = "4h"
    runner.stats = SimpleNamespace(feature_events_seen=0, signals_seen=0, dry_run_actions=0)
    runner.app_config.dry_run = True

    asyncio.run(
        runner._hydrate_strategy_closed_klines(
            FakeKlineRepository([_kline(0), _kline(4 * 60 * 60_000)]),
            time_range=TimeRange(0, 4 * 60 * 60_000),
        )
    )

    assert sorted(strategy.buffer.closed_klines) == [4 * 60 * 60_000 - 1, 8 * 60 * 60_000 - 1]
