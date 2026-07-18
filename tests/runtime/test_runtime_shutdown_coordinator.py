from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.app import AppConfig
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime import runner as runner_module
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner, LiveRuntimeStats
from src.runtime.shutdown_coordinator import RuntimeShutdownCoordinator


class _Alerts:
    def __init__(self, calls: list[str] | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.emitted = []

    def start(self) -> None:
        self.calls.append("alerts.start")

    async def stop(self) -> None:
        self.calls.append("alerts.stop")

    def emit(self, alert) -> None:
        self.emitted.append(alert)


def _runner(*, shutdown_coordinator=None, calls=None) -> LiveRuntimeRunner:
    config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    services = {
        "project_env_config": ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
        "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
    }
    if shutdown_coordinator is not None:
        services["shutdown_coordinator"] = shutdown_coordinator
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(
            strategy=object(),
            alerts=_Alerts(calls),
        ),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )


def _async_step(calls: list[str], name: str, *, error=None):
    async def step() -> None:
        calls.append(name)
        if error is not None:
            raise error

    return step


def test_coordinator_has_no_instance_state() -> None:
    assert vars(RuntimeShutdownCoordinator()) == {}


@pytest.mark.asyncio
async def test_empty_steps_return_none_without_task_helpers(monkeypatch) -> None:
    coordinator = RuntimeShutdownCoordinator()
    monkeypatch.setattr(
        asyncio,
        "create_task",
        Mock(side_effect=AssertionError("create_task must not be used")),
    )
    monkeypatch.setattr(
        asyncio,
        "gather",
        Mock(side_effect=AssertionError("gather must not be used")),
    )

    result = await coordinator.execute(())

    assert result is None


@pytest.mark.asyncio
async def test_steps_run_once_in_strict_sequence() -> None:
    coordinator = RuntimeShutdownCoordinator()
    calls: list[str] = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def first() -> None:
        calls.append("first.start")
        first_started.set()
        await release_first.wait()
        calls.append("first.end")

    async def second() -> None:
        calls.append("second")

    execution = asyncio.create_task(coordinator.execute((first, second)))
    await first_started.wait()
    assert calls == ["first.start"]

    release_first.set()
    result = await execution

    assert result is None
    assert calls == ["first.start", "first.end", "second"]


@pytest.mark.asyncio
async def test_original_exception_propagates_and_stops_sequence() -> None:
    coordinator = RuntimeShutdownCoordinator()
    calls: list[str] = []
    error = RuntimeError("shutdown failed")

    with pytest.raises(RuntimeError) as raised:
        await coordinator.execute(
            (
                _async_step(calls, "first"),
                _async_step(calls, "failed", error=error),
                _async_step(calls, "after"),
            )
        )

    assert raised.value is error
    assert calls == ["first", "failed"]


@pytest.mark.asyncio
async def test_cancelled_error_propagates_unchanged() -> None:
    coordinator = RuntimeShutdownCoordinator()
    cancelled = asyncio.CancelledError("cancelled")

    async def cancel() -> None:
        raise cancelled

    with pytest.raises(asyncio.CancelledError) as raised:
        await coordinator.execute((cancel,))

    assert raised.value is cancelled


def test_injected_coordinator_has_priority_and_is_not_executed(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    default_factory = Mock()
    monkeypatch.setattr(
        runner_module,
        "RuntimeShutdownCoordinator",
        default_factory,
    )

    runner = _runner(shutdown_coordinator=coordinator)

    default_factory.assert_not_called()
    coordinator.execute.assert_not_called()
    assert runner._shutdown_coordinator is coordinator
    assert runner.services["shutdown_coordinator"] is coordinator


def test_default_coordinator_is_created_once_written_back_and_not_executed(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    factory = Mock(return_value=coordinator)
    monkeypatch.setattr(
        "src.runtime.components.wiring.RuntimeShutdownCoordinator",
        factory,
    )

    runner = _runner()

    factory.assert_called_once_with()
    coordinator.execute.assert_not_called()
    assert runner._shutdown_coordinator is coordinator
    assert runner.services["shutdown_coordinator"] is coordinator


def _install_shutdown_steps(
    runner: LiveRuntimeRunner,
    calls: list[str],
    monkeypatch,
    *,
    errors: dict[str, BaseException] | None = None,
) -> None:
    errors = errors or {}
    for name in (
        "_stop_market_data_modules",
        "_stop_sync_tasks",
        "_stop_producers",
        "_stop_live_persistence_writer",
    ):
        label = name.removeprefix("_")
        monkeypatch.setattr(
            runner,
            name,
            _async_step(calls, label, error=errors.get(label)),
        )

    async def stop_alerts() -> None:
        calls.append("alerts.stop")
        error = errors.get("alerts.stop")
        if error is not None:
            raise error

    runner.context.alerts.stop = stop_alerts


@pytest.mark.asyncio
async def test_final_shutdown_passes_exact_five_bound_steps(monkeypatch) -> None:
    calls: list[str] = []
    captured = []

    class Coordinator:
        async def execute(self, steps) -> None:
            captured.append(tuple(steps))
            for step in steps:
                await step()

    runner = _runner(shutdown_coordinator=Coordinator())
    _install_shutdown_steps(runner, calls, monkeypatch)

    result = await runner._run_finally_shutdown()

    assert result is None
    assert len(captured) == 1
    assert calls == [
        "stop_market_data_modules",
        "stop_sync_tasks",
        "stop_producers",
        "stop_live_persistence_writer",
        "alerts.stop",
    ]
    assert len(captured[0]) == 5
    assert captured[0][0] is runner._stop_market_data_modules
    assert captured[0][1] is runner._stop_sync_tasks
    assert captured[0][2] is runner._stop_producers
    assert captured[0][3] is runner._stop_live_persistence_writer
    assert captured[0][4] is runner.context.alerts.stop


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer_error", (False, True))
async def test_run_executes_final_shutdown_once_on_both_paths(
    monkeypatch,
    consumer_error: bool,
) -> None:
    runner = _runner()
    final_shutdown = AsyncMock()
    monkeypatch.setattr(runner, "_run_finally_shutdown", final_shutdown)
    monkeypatch.setattr(runner, "_startup", AsyncMock())
    monkeypatch.setattr(runner, "_start_producers", Mock(return_value=[]))
    monkeypatch.setattr(runner, "_start_sync_tasks", Mock(return_value=[]))

    async def consume(*, max_market_events) -> None:
        if consumer_error:
            raise RuntimeError("runtime failed")

    monkeypatch.setattr(runner, "_consume_market_events", consume)

    if consumer_error:
        with pytest.raises(RuntimeError, match="runtime failed"):
            await runner.run(max_market_events=1)
    else:
        result = await runner.run(max_market_events=1)
        assert result is runner.stats

    final_shutdown.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_cleanup_error_propagates_and_skips_remaining_final_steps(
    monkeypatch,
) -> None:
    calls: list[str] = []
    error = RuntimeError("cleanup failed")
    runner = _runner()
    _install_shutdown_steps(
        runner,
        calls,
        monkeypatch,
        errors={"stop_sync_tasks": error},
    )

    with pytest.raises(RuntimeError) as raised:
        await runner._run_finally_shutdown()

    assert raised.value is error
    assert calls == [
        "stop_market_data_modules",
        "stop_sync_tasks",
    ]


@pytest.mark.asyncio
async def test_cleanup_error_can_override_runtime_error(monkeypatch) -> None:
    runtime_error = RuntimeError("runtime failed")
    cleanup_error = RuntimeError("cleanup failed")
    runner = _runner()
    monkeypatch.setattr(runner, "_startup", AsyncMock())
    monkeypatch.setattr(runner, "_start_producers", Mock(return_value=[]))
    monkeypatch.setattr(runner, "_start_sync_tasks", Mock(return_value=[]))
    monkeypatch.setattr(
        runner,
        "_consume_market_events",
        AsyncMock(side_effect=runtime_error),
    )
    monkeypatch.setattr(
        runner,
        "_run_finally_shutdown",
        AsyncMock(side_effect=cleanup_error),
    )

    with pytest.raises(RuntimeError) as raised:
        await runner.run(max_market_events=1)

    assert raised.value is cleanup_error


@pytest.mark.asyncio
async def test_explicit_stop_sets_event_then_runs_exact_three_steps_and_health(
    monkeypatch,
) -> None:
    calls: list[str] = []
    captured = []

    class Coordinator:
        runner = None

        async def execute(self, steps) -> None:
            assert self.runner._stop_event.is_set()
            calls.append("execute")
            captured.append(tuple(steps))
            for step in steps:
                await step()

    coordinator = Coordinator()
    runner = _runner(shutdown_coordinator=coordinator)
    coordinator.runner = runner
    _install_shutdown_steps(runner, calls, monkeypatch)
    stopped = RuntimeHealth(phase=RuntimePhase.STOPPED)

    def set_health(phase, **kwargs) -> None:
        calls.append("health")
        runner._health = stopped

    monkeypatch.setattr(runner, "_set_health", set_health)

    result = await runner.stop()

    assert result is stopped
    assert calls == [
        "execute",
        "stop_market_data_modules",
        "stop_producers",
        "stop_live_persistence_writer",
        "health",
    ]
    assert len(captured) == 1
    assert captured[0] == (
        runner._stop_market_data_modules,
        runner._stop_producers,
        runner._stop_live_persistence_writer,
    )
    excluded = {
        runner._stop_sync_tasks,
        runner.context.alerts.stop,
    }
    assert excluded.isdisjoint(captured[0])
    assert all(
        getattr(step, "__self__", None) is not runner._heartbeat_service
        for step in captured[0]
    )


@pytest.mark.asyncio
async def test_explicit_stop_error_prevents_health_update(monkeypatch) -> None:
    error = RuntimeError("stop cleanup failed")
    coordinator = SimpleNamespace(execute=AsyncMock(side_effect=error))
    runner = _runner(shutdown_coordinator=coordinator)
    set_health = Mock()
    monkeypatch.setattr(runner, "_set_health", set_health)

    with pytest.raises(RuntimeError) as raised:
        await runner.stop()

    assert raised.value is error
    assert runner._stop_event.is_set()
    set_health.assert_not_called()


@pytest.mark.asyncio
async def test_repeated_explicit_stop_keeps_current_non_idempotent_behavior(
    monkeypatch,
) -> None:
    coordinator = SimpleNamespace(execute=AsyncMock())
    runner = _runner(shutdown_coordinator=coordinator)
    set_health = Mock()
    monkeypatch.setattr(runner, "_set_health", set_health)

    await runner.stop()
    await runner.stop()

    assert coordinator.execute.await_count == 2
    assert set_health.call_count == 2
