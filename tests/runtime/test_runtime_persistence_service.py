from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.app import AppConfig, AppContext
from src.app.alerts import AppAlert
from src.planner import ExecutionPlanner
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime import persistence_service as persistence_service_module
from src.runtime.persistence import BackgroundWriteItem, BackgroundWriteQueue
from src.runtime.persistence_service import (
    RuntimePersistenceMetrics,
    RuntimePersistenceService,
)


class _Strategy:
    raw_trade_callbacks_enabled = True


class _Alerts:
    def __init__(self) -> None:
        self.items = []

    def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def emit(self, alert) -> None:
        self.items.append(alert)


def _runner(*, services: dict | None = None) -> LiveRuntimeRunner:
    config = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    context = AppContext(
        data=SimpleNamespace(exchange=ExchangeName.OKX, symbol=config.symbol),
        execution=SimpleNamespace(exchange=ExchangeName.OKX, symbol=config.symbol),
        state_store=object(),
        strategy=_Strategy(),
        planner=ExecutionPlanner(),
        alerts=_Alerts(),
    )
    injected = dict(services or {})
    injected.setdefault(
        "project_env_config",
        ProjectEnvConfig(
            values={}, source_files=(), env_file=Path(".env"), example_file=None
        ),
    )
    return LiveRuntimeRunner(
        app_config=config,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=config,
            mode=RuntimeMode.LIVE_RUNTIME,
            background_queue_maxsize=17,
        ),
        services=injected,
    )


def test_default_writer_is_lazy_named_configured_and_cached() -> None:
    service = RuntimePersistenceService(max_pending=7, writer_name="writer-name")

    assert vars(service) == {
        "_writer": None,
        "_max_pending": 7,
        "_writer_name": "writer-name",
    }

    writer = service.get_writer()

    assert isinstance(writer, BackgroundWriteQueue)
    assert writer.name == "writer-name"
    assert writer.max_pending == 7
    assert writer._thread is None
    assert service.get_writer() is writer
    writer.stop()


def test_injected_writer_is_reused_without_replacement() -> None:
    writer = object()
    service = RuntimePersistenceService(writer=writer, max_pending=7)

    assert service.get_writer() is writer
    assert service.get_writer() is writer


@pytest.mark.parametrize("accepted", [True, False])
def test_submit_preserves_item_fields_and_return_value(accepted: bool) -> None:
    captured: list[BackgroundWriteItem] = []

    class Writer:
        def submit(self, item):
            captured.append(item)
            return accepted

    service = RuntimePersistenceService(writer=Writer())
    write = lambda: None
    on_error = lambda exc: None

    result = service.submit(
        description="identity",
        write=write,
        on_error=on_error,
    )

    assert result is accepted
    assert len(captured) == 1
    assert captured[0].description == "identity"
    assert captured[0].write is write
    assert captured[0].on_error is on_error


def test_submit_exception_is_not_swallowed() -> None:
    failure = RuntimeError("submit failed")

    class Writer:
        def submit(self, item):
            raise failure

    with pytest.raises(RuntimeError, match="submit failed") as raised:
        RuntimePersistenceService(writer=Writer()).submit(
            description="failure",
            write=lambda: None,
        )

    assert raised.value is failure


@pytest.mark.asyncio
async def test_default_writer_stop_uses_to_thread(monkeypatch) -> None:
    calls = []

    async def fake_to_thread(function, *args, **kwargs):
        calls.append((function, args, kwargs))
        return function(*args, **kwargs)

    monkeypatch.setattr(persistence_service_module.asyncio, "to_thread", fake_to_thread)
    service = RuntimePersistenceService()
    writer = service.get_writer()

    await service.stop(flush=False)

    assert calls == [(writer.stop, (), {"flush": False})]


@pytest.mark.asyncio
async def test_injected_writer_stop_supports_sync_async_and_missing() -> None:
    sync_calls: list[bool] = []

    class SyncWriter:
        def stop(self, *, flush):
            sync_calls.append(flush)

    await RuntimePersistenceService(writer=SyncWriter()).stop(flush=False)
    assert sync_calls == [False]

    async_calls: list[bool] = []

    class AsyncWriter:
        async def stop(self, *, flush):
            async_calls.append(flush)

    await RuntimePersistenceService(writer=AsyncWriter()).stop(flush=True)
    assert async_calls == [True]

    await RuntimePersistenceService().stop(flush=False)
    await RuntimePersistenceService(writer=object()).stop(flush=False)


def test_metrics_are_exact_for_default_writer() -> None:
    service = RuntimePersistenceService(max_pending=3)
    writer = service.get_writer()
    writer.submitted = 5
    writer.written = 4
    writer.dropped = 3
    writer.failures = 2
    writer._queue.put_nowait(object())

    assert service.metrics() == RuntimePersistenceMetrics(
        pending_count=1,
        dropped=3,
        failures=2,
        written=4,
        submitted=5,
    )
    writer.stop(flush=False)


def test_metrics_do_not_guess_unknown_writer_attributes() -> None:
    writer = SimpleNamespace(
        pending_count=1,
        dropped=2,
        failures=3,
        written=4,
        submitted=5,
    )

    assert RuntimePersistenceService(writer=writer).metrics() == (
        RuntimePersistenceMetrics(
            pending_count=None,
            dropped=None,
            failures=None,
            written=None,
            submitted=None,
        )
    )


def test_runner_uses_injected_service_and_synchronizes_writer_references() -> None:
    writer = object()

    class Service:
        def get_writer(self):
            return writer

    service = Service()
    runner = _runner(services={"runtime_persistence_service": service})

    assert runner._runtime_persistence_service is service
    assert runner.services["runtime_persistence_service"] is service
    assert runner._get_live_persistence_writer() is writer
    assert runner._live_persistence_writer is writer
    assert runner.services["live_persistence_writer"] is writer


def test_runner_preserves_legacy_injected_writer() -> None:
    writer = object()
    runner = _runner(services={"live_persistence_writer": writer})

    assert runner._get_live_persistence_writer() is writer
    assert runner._runtime_persistence_service.get_writer() is writer
    assert runner._live_persistence_writer is writer
    assert runner.services["live_persistence_writer"] is writer


@pytest.mark.asyncio
async def test_runner_submit_captures_loop_and_keeps_drop_warning(
    caplog,
) -> None:
    writer = object()
    captured = []

    class Service:
        def get_writer(self):
            return writer

        def submit(self, **kwargs):
            captured.append(kwargs)
            return False

        def metrics(self):
            return RuntimePersistenceMetrics(3, 2, None, None, None)

    service = Service()
    runner = _runner(services={"runtime_persistence_service": service})
    write = lambda: None
    on_error = lambda exc: None

    with caplog.at_level(logging.WARNING):
        accepted = runner._submit_live_persistence_write(
            description="rejected",
            write=write,
            on_error=on_error,
        )

    assert accepted is False
    assert runner._persistence_alert_loop is asyncio.get_running_loop()
    assert captured == [
        {
            "description": "rejected",
            "write": write,
            "on_error": on_error,
        }
    ]
    assert (
        "Live persistence write dropped | description=rejected pending=3 dropped=2"
        in caplog.text
    )


@pytest.mark.asyncio
async def test_runner_stop_delegates_once_and_keeps_alert_emission() -> None:
    stop_calls: list[bool] = []

    class Service:
        async def stop(self, *, flush):
            stop_calls.append(flush)

    runner = _runner(services={"runtime_persistence_service": Service()})

    await runner._stop_live_persistence_writer(flush=False)

    assert stop_calls == [False]

    alert = AppAlert(subject="subject", content="content", severity="warning")
    runner._persistence_alert_loop = None
    runner._emit_alert_threadsafe(alert)
    assert runner.context.alerts.items == [alert]
