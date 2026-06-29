from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.platform import ExchangeName
from src.platform.markets import get_market_profile
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode, StrategyRuntimeRequirements


H4 = 4 * 60 * 60_000
BASE = 1_640_995_200_000 + 300 * H4


class EntryFilters:
    range_speed_rolling_window_bars = 100
    range_speed_min_periods = 100


class StrategyConfig:
    entry_filters = EntryFilters()


class RangeSpeedStrategy:
    config = StrategyConfig()

    def __init__(self) -> None:
        self.warmups: list[list[int]] = []

    def warmup_range_speed_history(self, values):
        rows = list(values)
        self.warmups.append(rows)
        return len(rows)


class FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")


def _app_config() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="unused",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def _runner(tmp_path: Path, strategy: RangeSpeedStrategy, *, status_json: Path | None = None) -> LiveRuntimeRunner:
    cfg = _app_config()
    context = AppContext(
        data=FakeData(),
        execution=object(),
        state_store=object(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    runtime_config = LiveRuntimeConfig(
        app=cfg,
        mode=RuntimeMode.LIVE_RUNTIME,
        range_checkpoint_db_path=str(tmp_path / "checkpoint.sqlite3"),
        realtime_trade_db_path=str(tmp_path / "market.sqlite3"),
        range_backfill_warning_interval_seconds=600,
    )
    services = {
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping(
            {"range_bars": {"enabled": True, "range_pct": "0.002", "aggregate_interval": "4h"}}
        )
    }
    if status_json is not None:
        services["range_backfill_supervisor"] = type("Supervisor", (), {"status_json": status_json})()
    return LiveRuntimeRunner(app_config=cfg, app_context=context, runtime_config=runtime_config, services=services)


def _insert_complete(store: SqliteRangeCheckpointStore, latest: int, offsets: list[int]) -> None:
    for offset in offsets:
        count = 100 - offset
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(latest - offset * H4, max(1, count)),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=latest + H4,
        )


def _aggregate(start: int, count: int) -> RangeBarAggregate:
    return RangeBarAggregate(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bucket_start_ms=start,
        bucket_end_ms=start + H4 - 1,
        bar_count=count,
        first_open=Decimal("100"),
        last_close=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("99"),
        buy_notional_sum=Decimal("60"),
        sell_notional_sum=Decimal("40"),
        delta_notional_sum=Decimal("20"),
        notional_sum=Decimal("100"),
    )


def test_non_continuous_100_complete_rows_do_not_warm_strategy(tmp_path: Path, monkeypatch) -> None:
    now_ms = BASE + 101 * H4 + 123
    latest = (now_ms // H4) * H4 - H4
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    _insert_complete(store, latest, [offset for offset in range(101) if offset != 3][:100])
    strategy = RangeSpeedStrategy()
    runner = _runner(tmp_path, strategy)
    monkeypatch.setattr("src.runtime.runner.time.time", lambda: now_ms / 1000)

    asyncio.run(runner._warmup_range_speed_history())

    assert strategy.warmups == []


def test_recent_continuous_100_warms_exactly_ascending_counts(tmp_path: Path, monkeypatch) -> None:
    now_ms = BASE + 101 * H4 + 123
    latest = (now_ms // H4) * H4 - H4
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    _insert_complete(store, latest, list(range(100)))
    strategy = RangeSpeedStrategy()
    runner = _runner(tmp_path, strategy)
    monkeypatch.setattr("src.runtime.runner.time.time", lambda: now_ms / 1000)

    asyncio.run(runner._warmup_range_speed_history())

    assert strategy.warmups == [list(range(1, 101))]


def test_continuity_warning_is_throttled_and_mentions_status_path(tmp_path: Path, monkeypatch, caplog) -> None:
    now_ms = BASE + 101 * H4 + 123
    latest = (now_ms // H4) * H4 - H4
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    _insert_complete(store, latest, [0, 1])
    strategy = RangeSpeedStrategy()
    status_json = tmp_path / "status.json"
    runner = _runner(tmp_path, strategy, status_json=status_json)
    monkeypatch.setattr("src.runtime.runner.time.time", lambda: now_ms / 1000)
    caplog.set_level("WARNING")

    asyncio.run(runner._warmup_range_speed_history())
    asyncio.run(runner._warmup_range_speed_history())

    messages = [record.getMessage() for record in caplog.records if "range-speed history warmup unavailable" in record.getMessage()]
    assert len(messages) == 1
    assert str(status_json) in messages[0]
