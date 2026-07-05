from __future__ import annotations

import asyncio
import inspect
from decimal import Decimal
from types import SimpleNamespace

from src.app import (
    AppConfig,
    AppContext,
    AsyncAlertDispatcher,
    NoopAlertSink,
)
from src.market_data.events import MarketFeatureEvent
from src.platform.exchanges.models import ExchangeName
from src.planner import ExecutionPlanner
from src.runtime.config import LiveRuntimeConfig
from src.runtime.models import RuntimeMode
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.startup_feature_backfill import (
    resolve_startup_feature_backfill_providers,
)
import src.runtime.runner as runner_module


class _Provider:
    name = "test_trade_features"
    poll_interval_seconds = 10.0

    def __init__(
        self,
        result,
        *,
        event: MarketFeatureEvent | None = None,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.event = event
        self.error = error
        self.calls = 0

    def check_and_launch(self):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result

    def poll_readiness(self):
        return self.result

    def market_feature_events(self, result):
        _ = result
        return () if self.event is None else (self.event,)


class _Strategy:
    def __init__(self, providers=()) -> None:
        self.providers = tuple(providers)

    def startup_feature_backfill_providers(self):
        return self.providers

    async def on_start(self, snapshot):
        _ = snapshot
        return ()


def _event() -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type="trade_feature_readiness",
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="1m",
        event_time_ms=1,
        available_time_ms=1,
        data={"ready": True},
    )


def _runner(strategy: object) -> LiveRuntimeRunner:
    app = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="test",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    context = AppContext(
        data=SimpleNamespace(
            exchange=ExchangeName.OKX,
            symbol=app.symbol,
        ),
        execution=SimpleNamespace(
            exchange=ExchangeName.OKX,
            symbol=app.symbol,
        ),
        state_store=SimpleNamespace(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    return LiveRuntimeRunner(
        app_config=app,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=app,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
    )


def test_no_provider_passes_without_health_entry() -> None:
    runner = _runner(_Strategy())

    asyncio.run(runner._check_startup_feature_backfills())

    assert "feature_backfill_results" not in (
        runner._health.metadata
    )


def test_runner_imports_feature_backfill_resolver() -> None:
    assert (
        runner_module.resolve_startup_feature_backfill_providers
        is resolve_startup_feature_backfill_providers
    )
    source = inspect.getsource(runner_module)
    assert (
        "from src.runtime.startup_feature_backfill import"
        in source
    )


def test_ready_result_updates_health_metadata() -> None:
    provider = _Provider(
        {"action": "none", "reason": "coverage_complete"}
    )
    runner = _runner(_Strategy((provider,)))

    asyncio.run(runner._check_startup_feature_backfills())

    result = runner._health.metadata[
        "feature_backfill_results"
    ][provider.name]
    assert result["reason"] == "coverage_complete"


def test_launched_not_ready_result_updates_health_metadata() -> None:
    provider = _Provider(
        {"action": "launched", "reason": "coverage_gap"}
    )
    runner = _runner(_Strategy((provider,)))

    asyncio.run(runner._check_startup_feature_backfills())

    result = runner._health.metadata[
        "feature_backfill_results"
    ][provider.name]
    assert result["action"] == "launched"


def test_provider_exception_is_audited_without_raising(caplog) -> None:
    provider = _Provider({}, error=RuntimeError("boom"))
    runner = _runner(_Strategy((provider,)))

    asyncio.run(runner._check_startup_feature_backfills())

    result = runner._health.metadata[
        "feature_backfill_results"
    ][provider.name]
    assert result["reason"] == "provider_failed"
    assert "boom" in result["error"]
    assert "provider failed" in caplog.text.lower()


def test_provider_can_emit_generic_market_feature_event(
    monkeypatch,
) -> None:
    provider = _Provider(
        {"action": "none"},
        event=_event(),
    )
    runner = _runner(_Strategy((provider,)))
    emitted = []

    async def capture(event):
        emitted.append(event)

    monkeypatch.setattr(
        runner,
        "process_market_feature",
        capture,
    )

    asyncio.run(runner._check_startup_feature_backfills())

    assert emitted == [provider.event]


def test_runtime_source_has_no_strategy_feature_naming() -> None:
    source = inspect.getsource(
        __import__(
            "src.runtime.runner",
            fromlist=["LiveRuntimeRunner"],
        )
    ).lower()
    assert ("mf_" + "feature") not in source
