from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from src.market_data.models import TimeRange
from src.platform.data.models import MarketKline
from src.platform.exchanges.models import ExchangeName
from src.runtime.components import StartupComponent
from src.runtime.context import RuntimeContext
from src.runtime.market_features import MarketFeaturePipeline
from src.runtime.ports import StartupPorts
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
    pipeline = MarketFeaturePipeline(strategy)
    component = StartupComponent(RuntimeContext())
    component.bind_dependencies(
        app_config=SimpleNamespace(
            symbol="ETH-USDT-PERP",
            dry_run=True,
        ),
        _closed_bar_interval="4h",
    )
    component.bind_ports(
        StartupPorts(
            get_account_clients=lambda: (),
            get_execution_clients=lambda: (),
            get_market_feature_pipeline=lambda: pipeline,
            process_market_feature=pipeline.dispatch,
            require_range_module=lambda: None,
            set_health=lambda *args, **kwargs: None,
            strategy_capabilities=lambda: None,
        )
    )

    asyncio.run(
        component._hydrate_strategy_closed_klines(
            FakeKlineRepository([_kline(0), _kline(4 * 60 * 60_000)]),
            time_range=TimeRange(0, 4 * 60 * 60_000),
        )
    )

    assert sorted(strategy.buffer.closed_klines) == [4 * 60 * 60_000 - 1, 8 * 60 * 60_000 - 1]
