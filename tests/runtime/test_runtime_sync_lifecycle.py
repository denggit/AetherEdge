from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.app import AppConfig
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime import runner as runner_module
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner
from src.runtime.sync_lifecycle import RuntimeSyncLifecycle


def _runner(*, sync_lifecycle=None) -> LiveRuntimeRunner:
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
    if sync_lifecycle is not None:
        services["sync_lifecycle"] = sync_lifecycle
    return LiveRuntimeRunner(
        app_config=config,
        app_context=SimpleNamespace(strategy=object()),
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=services,
    )


def test_lifecycle_initial_state_and_owned_fields() -> None:
    lifecycle = RuntimeSyncLifecycle()

    assert lifecycle.tasks == ()
    assert vars(lifecycle) == {"_tasks": []}


@pytest.mark.asyncio
async def test_start_calls_factories_in_order_once_and_tasks_start() -> None:
    lifecycle = RuntimeSyncLifecycle()
    factory_calls: list[str] = []
    started: list[str] = []
    all_started = asyncio.Event()
    release = asyncio.Event()

    def factory(label: str):
        def create():
            factory_calls.append(label)

            async def worker() -> None:
                started.append(label)
                if len(started) == 3:
                    all_started.set()
                await release.wait()

            return worker()

        return create

    tasks = lifecycle.start(
        [factory("account"), factory("order"), factory("heartbeat")]
    )

    assert factory_calls == ["account", "order", "heartbeat"]
    assert tasks is lifecycle._tasks
    assert lifecycle.tasks == tuple(tasks)
    await all_started.wait()
    assert started == ["account", "order", "heartbeat"]

    release.set()
    await asyncio.gather(*tasks)
    await lifecycle.stop()
    assert lifecycle.tasks == ()


def test_start_passes_each_awaitable_to_create_task_without_name(
    monkeypatch,
) -> None:
    lifecycle = RuntimeSyncLifecycle()
    awaitables = [object(), object()]
    created_tasks = [object(), object()]
    calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def create_task(awaitable, *args, **kwargs):
        calls.append((awaitable, args, kwargs))
        return created_tasks[len(calls) - 1]

    monkeypatch.setattr(asyncio, "create_task", create_task)

    tasks = lifecycle.start([lambda: awaitables[0], lambda: awaitables[1]])

    assert calls == [
        (awaitables[0], (), {}),
        (awaitables[1], (), {}),
    ]
    assert tasks is lifecycle._tasks
    assert tasks == created_tasks


@pytest.mark.asyncio
async def test_stop_cancels_in_order_and_gathers_with_return_exceptions(
    monkeypatch,
) -> None:
    lifecycle = RuntimeSyncLifecycle()
    cancelled: list[str] = []
    gathered: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class Task:
        def __init__(self, label: str) -> None:
            self.label = label

        def cancel(self) -> None:
            cancelled.append(self.label)

    tasks = [Task("account"), Task("order"), Task("heartbeat")]
    lifecycle._tasks = tasks  # type: ignore[assignment]

    async def gather(*items, **kwargs):
        gathered.append((items, kwargs))

    monkeypatch.setattr(asyncio, "gather", gather)

    await lifecycle.stop()

    assert cancelled == ["account", "order", "heartbeat"]
    assert gathered == [(tuple(tasks), {"return_exceptions": True})]
    assert lifecycle.tasks == ()


@pytest.mark.asyncio
async def test_task_failure_does_not_escape_stop() -> None:
    lifecycle = RuntimeSyncLifecycle()
    started = asyncio.Event()

    async def fail() -> None:
        started.set()
        raise RuntimeError("sync task failed")

    lifecycle.start([fail])
    await started.wait()

    await lifecycle.stop()

    assert lifecycle.tasks == ()


@pytest.mark.asyncio
async def test_empty_and_repeated_stop_are_safe() -> None:
    lifecycle = RuntimeSyncLifecycle()

    await lifecycle.stop()
    await lifecycle.stop()

    assert lifecycle.tasks == ()


def test_factory_error_is_not_swallowed_and_old_tasks_are_not_replaced() -> None:
    lifecycle = RuntimeSyncLifecycle()
    existing = [object()]
    lifecycle._tasks = existing  # type: ignore[assignment]
    error = RuntimeError("factory failed")

    def fail():
        raise error

    with pytest.raises(RuntimeError) as raised:
        lifecycle.start([fail])

    assert raised.value is error
    assert lifecycle._tasks is existing


def test_runner_uses_injected_lifecycle_and_writes_it_back_to_services() -> None:
    injected = object()

    runner = _runner(sync_lifecycle=injected)

    assert runner._sync_lifecycle is injected
    assert runner.services["sync_lifecycle"] is injected
    assert runner._sync_tasks == []


def test_runner_creates_one_default_lifecycle(monkeypatch) -> None:
    lifecycle = object()
    factory = Mock(return_value=lifecycle)
    monkeypatch.setattr(runner_module, "RuntimeSyncLifecycle", factory)

    runner = _runner()

    factory.assert_called_once_with()
    assert runner._sync_lifecycle is lifecycle
    assert runner.services["sync_lifecycle"] is lifecycle


class _RecordingLifecycle:
    def __init__(self) -> None:
        self.factories = []
        self.tasks: list[object] = []
        self.stop_calls = 0

    def start(self, factories):
        self.factories = list(factories)
        self.tasks = [factory() for factory in self.factories]
        return self.tasks

    async def stop(self) -> None:
        self.stop_calls += 1


def _runner_for_task_selection(
    *,
    account_enabled: bool,
    order_enabled: bool,
    feature_readiness_enabled: bool,
):
    runner = object.__new__(LiveRuntimeRunner)
    lifecycle = _RecordingLifecycle()
    stop_event = object()
    calls: list[tuple[str, object]] = []
    provider_resolution_calls = 0

    def task(label: str):
        def create(event):
            calls.append((label, event))
            return object()

        return create

    def providers():
        nonlocal provider_resolution_calls
        provider_resolution_calls += 1
        return (object(),) if feature_readiness_enabled else ()

    runner._sync_lifecycle = lifecycle
    runner._sync_tasks = []
    runner._stop_event = stop_event
    runner.requirements = SimpleNamespace(
        account_state=SimpleNamespace(poll_enabled=account_enabled),
        order_state=SimpleNamespace(
            poll_when_position_enabled=order_enabled
        ),
    )
    runner._get_account_sync_service = lambda: SimpleNamespace(
        run_periodic=task("account")
    )
    runner._get_order_sync_service = lambda: SimpleNamespace(
        run_periodic=task("order")
    )
    runner._periodic_follower_close_check = task("follower_close")
    runner._heartbeat_service = SimpleNamespace(
        run_periodic=task("heartbeat")
    )
    runner._get_startup_feature_backfill_providers = providers
    runner._periodic_feature_readiness_refresh = task("feature_readiness")
    return runner, lifecycle, stop_event, calls, lambda: provider_resolution_calls


@pytest.mark.parametrize(
    (
        "account_enabled",
        "order_enabled",
        "feature_readiness_enabled",
        "expected",
    ),
    (
        (False, False, False, ["heartbeat"]),
        (True, False, False, ["account", "heartbeat"]),
        (
            False,
            True,
            False,
            ["order", "follower_close", "heartbeat"],
        ),
        (
            True,
            True,
            True,
            [
                "account",
                "order",
                "follower_close",
                "heartbeat",
                "feature_readiness",
            ],
        ),
    ),
)
def test_runner_keeps_task_conditions_order_stop_event_and_list_identity(
    account_enabled: bool,
    order_enabled: bool,
    feature_readiness_enabled: bool,
    expected: list[str],
) -> None:
    runner, lifecycle, stop_event, calls, provider_resolution_count = (
        _runner_for_task_selection(
            account_enabled=account_enabled,
            order_enabled=order_enabled,
            feature_readiness_enabled=feature_readiness_enabled,
        )
    )

    tasks = runner._start_sync_tasks()

    assert [label for label, _ in calls] == expected
    assert all(event is stop_event for _, event in calls)
    assert tasks is lifecycle.tasks
    assert runner._sync_tasks is tasks
    assert provider_resolution_count() == 1


@pytest.mark.asyncio
async def test_runner_stop_delegates_once_and_clears_compatibility_list() -> None:
    runner = object.__new__(LiveRuntimeRunner)
    lifecycle = _RecordingLifecycle()
    runner._sync_lifecycle = lifecycle
    runner._sync_tasks = [object()]

    await runner._stop_sync_tasks()

    assert lifecycle.stop_calls == 1
    assert runner._sync_tasks == []
