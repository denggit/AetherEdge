from __future__ import annotations

import asyncio
import inspect
import logging
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from src.app import (
    AppConfig,
    AppContext,
    AsyncAlertDispatcher,
    NoopAlertSink,
)
from src.platform.config import ProjectEnvConfig
from src.platform.exchanges.models import ExchangeName, PositionMode
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime.position_mode_gate import PositionModeRequirement
from src.runtime.runner import (
    LiveRuntimeError,
    LiveRuntimeRunner,
    _is_fatal_startup_error,
)


class _Strategy:
    def __init__(
        self,
        *,
        require_hedge: bool,
        strategy_id: str,
    ) -> None:
        self.require_hedge = require_hedge
        self.config = SimpleNamespace(strategy_id=strategy_id)

    def runtime_startup_requirements(self):
        if not self.require_hedge:
            return ()
        return (
            PositionModeRequirement(
                required_mode=PositionMode.HEDGE,
                exchanges=(
                    ExchangeName.OKX,
                    ExchangeName.BINANCE,
                ),
                source=self.config.strategy_id,
            ),
        )

    async def on_start(self, snapshot):
        return ()


class _Account:
    def __init__(
        self,
        exchange: ExchangeName,
        mode: object,
        *,
        events: list[str] | None = None,
    ) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.mode = mode
        self.events = events

    async def fetch_position_mode(self):
        if self.events is not None:
            self.events.append(f"gate:{self.exchange.value}")
        if isinstance(self.mode, Exception):
            raise self.mode
        return self.mode


def _app(
    *,
    strategy: str = "eth_portfolio_v1",
    exchanges: tuple[ExchangeName, ...] = (
        ExchangeName.OKX,
        ExchangeName.BINANCE,
    ),
) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=exchanges,
        data_exchange=ExchangeName.OKX,
        strategy=strategy,
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def _runner(
    *,
    app: AppConfig,
    accounts: tuple[_Account, ...],
) -> LiveRuntimeRunner:
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
        strategy=_Strategy(
            require_hedge=app.strategy in {
                "eth_portfolio_v1",
                "strategies.eth_portfolio_v1:Strategy",
            },
            strategy_id=app.strategy,
        ),
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )
    project_env = ProjectEnvConfig(
        values=MappingProxyType({}),
        source_files=(),
        env_file=Path(".env"),
        example_file=None,
    )
    return LiveRuntimeRunner(
        app_config=app,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=app,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services={
            "account_clients": accounts,
            "project_env_config": project_env,
        },
    )


def _accounts(
    *,
    okx: object = PositionMode.HEDGE,
    binance: object = PositionMode.HEDGE,
    events: list[str] | None = None,
) -> tuple[_Account, ...]:
    return (
        _Account(ExchangeName.OKX, okx, events=events),
        _Account(ExchangeName.BINANCE, binance, events=events),
    )


def test_portfolio_v1_both_exchanges_hedge_passes() -> None:
    runner = _runner(
        app=_app(
            strategy="strategies.eth_portfolio_v1:Strategy"
        ),
        accounts=_accounts(),
    )
    asyncio.run(
        runner._check_strategy_position_mode_requirements()
    )
    audit = runner._health.metadata[
        "position_mode_requirements"
    ]
    assert audit["ok"] is True
    assert {
        item["exchange"]: item["actual_mode"]
        for item in audit["requirements"][0]["exchanges"]
    } == {"okx": "hedge", "binance": "hedge"}


@pytest.mark.parametrize(
    ("okx", "binance", "failed_exchange"),
    (
        (PositionMode.ONE_WAY, PositionMode.HEDGE, "okx"),
        (PositionMode.HEDGE, PositionMode.ONE_WAY, "binance"),
    ),
)
def test_portfolio_v1_one_way_is_hard_failure(
    okx: PositionMode,
    binance: PositionMode,
    failed_exchange: str,
) -> None:
    runner = _runner(
        app=_app(), accounts=_accounts(okx=okx, binance=binance)
    )
    with pytest.raises(
        LiveRuntimeError,
        match="strategy position mode requirement failed",
    ):
        asyncio.run(
            runner._check_strategy_position_mode_requirements()
        )
    audit = runner._health.metadata[
        "position_mode_requirements"
    ]
    failed = next(
        item
        for item in audit["requirements"][0]["exchanges"]
        if item["exchange"] == failed_exchange
    )
    assert failed["hedge_mode_ok"] is False
    assert failed["actual_mode"] == "one_way"


def test_portfolio_v1_unknown_mode_is_hard_failure() -> None:
    runner = _runner(
        app=_app(), accounts=_accounts(binance=object())
    )
    with pytest.raises(LiveRuntimeError, match="binance=unknown"):
        asyncio.run(
            runner._check_strategy_position_mode_requirements()
        )


def test_portfolio_v1_hedge_mode_failure_is_fatal_startup_error() -> None:
    error = LiveRuntimeError(
        "strategy position mode requirement failed"
    )

    assert _is_fatal_startup_error(error) is True


def test_v10b_is_not_subject_to_portfolio_gate() -> None:
    accounts = _accounts(
        okx=RuntimeError("must not fetch"),
        binance=RuntimeError("must not fetch"),
    )
    runner = _runner(
        app=_app(strategy="eth_lf_portfolio_v10b"),
        accounts=accounts,
    )
    asyncio.run(
        runner._check_strategy_position_mode_requirements()
    )
    assert (
        "position_mode_requirements"
        not in runner._health.metadata
    )


def test_gate_is_ordered_before_recovery_on_start_and_producers() -> None:
    startup = inspect.getsource(LiveRuntimeRunner._startup)
    run = inspect.getsource(LiveRuntimeRunner.run)
    assert startup.index("_bootstrap_account_config_if_enabled") < (
        startup.index("_check_strategy_position_mode_requirements")
    )
    assert startup.index("_check_strategy_position_mode_requirements") < (
        startup.index("_run_recovery")
    )
    assert startup.index("_check_strategy_position_mode_requirements") < (
        startup.index("_call_on_start")
    )
    assert run.index("await self._startup()") < run.index(
        "self._start_producers()"
    )


def test_startup_executes_gate_before_recovery_and_on_start(
    monkeypatch,
) -> None:
    events: list[str] = []
    runner = _runner(
        app=_app(), accounts=_accounts(events=events)
    )

    async def step(name: str, result=None):
        events.append(name)
        return result

    monkeypatch.setattr(
        runner, "_initialize_rangebar_trust_window", lambda: None
    )
    monkeypatch.setattr(
        runner,
        "_bootstrap_account_config_if_enabled",
        lambda: step("bootstrap"),
    )
    monkeypatch.setattr(
        runner, "_run_warmup", lambda: step("warmup")
    )
    monkeypatch.setattr(
        runner,
        "_warmup_range_speed_history",
        lambda: step("range_speed", 0),
    )
    monkeypatch.setattr(
        runner,
        "_check_startup_feature_backfills",
        lambda: step("feature_readiness"),
    )
    monkeypatch.setattr(
        runner,
        "_run_recovery",
        lambda: step("recovery", (object(),)),
    )
    monkeypatch.setattr(
        runner,
        "_run_reconciliation",
        lambda snapshots: step("reconciliation"),
    )
    monkeypatch.setattr(
        runner,
        "_call_on_start",
        lambda snapshot: step("on_start"),
    )
    monkeypatch.setattr(
        runner,
        "_evaluate_startup_catchup_once",
        lambda snapshot: step("catchup"),
    )
    monkeypatch.setattr(
        runner,
        "_finish_range_speed_warmup_after_catchup",
        lambda: step("finish_warmup"),
    )
    monkeypatch.setattr(
        runner, "_start_range_speed_background_services", lambda: None
    )
    runner._heartbeat_service = SimpleNamespace(
        start=lambda **kwargs: None
    )

    asyncio.run(runner._startup())

    assert events[:5] == [
        "bootstrap",
        "gate:okx",
        "gate:binance",
        "warmup",
        "range_speed",
    ]
    assert events.index("gate:binance") < events.index("recovery")
    assert events.index("gate:binance") < events.index("on_start")


def test_failure_log_contains_required_audit_fields(caplog) -> None:
    runner = _runner(
        app=_app(),
        accounts=_accounts(binance=PositionMode.ONE_WAY),
    )
    with caplog.at_level(logging.ERROR), pytest.raises(
        LiveRuntimeError
    ):
        asyncio.run(
            runner._check_strategy_position_mode_requirements()
        )
    message = "\n".join(record.message for record in caplog.records)
    assert "strategy=eth_portfolio_v1" in message
    assert "exchange=binance" in message
    assert "symbol=ETH-USDT-PERP" in message
    assert "required_mode=hedge" in message
    assert "actual_mode=one_way" in message
    assert "source=startup_hard_gate" in message
