from __future__ import annotations

from decimal import Decimal

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.platform import ExchangeName
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime.tasks import ClosedBarScheduler
from strategies.eth_lf_portfolio_v8.strategy import Strategy


H4 = 4 * 60 * 60_000


class _FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_klines(self, **kwargs):
        return []

    async def stream_trades(self):
        if False:
            yield None

    async def stream_order_book(self):
        if False:
            yield None


class _FakeStateStore:
    def list_open_orders(self, **kwargs):
        return []

    def save_snapshot(self, snapshot):
        pass


def test_v9c_strategy_config_min_range_bars_is_read_by_runner():
    """Real V9C Strategy.config.micro_context.min_range_bars (object path)
    must be read correctly by runner._get_min_range_bars() and return 5."""

    strategy = Strategy()  # uses default config.json → min_range_bars=5

    cfg = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.eth_lf_portfolio_v8.strategy:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=True,
        enable_email_alerts=False,
    )

    context = AppContext(
        data=_FakeData(),
        execution=object(),
        state_store=_FakeStateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )

    runtime_config = LiveRuntimeConfig(
        app=cfg,
        mode=RuntimeMode.LIVE_RUNTIME,
        closed_bar_buffer_ms=60_000,
    )

    runner = LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=runtime_config,
    )

    # Real V9C config is a V8Config dataclass with micro_context.min_range_bars=5.
    assert runner._get_min_range_bars() == 5, (
        f"Expected _get_min_range_bars() == 5 for real V9C Strategy, "
        f"got {runner._get_min_range_bars()}"
    )
