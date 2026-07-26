from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import pytest
from _pytest.monkeypatch import MonkeyPatch

from src.app.alerts import AppAlert, AsyncAlertDispatcher, EmailAlertSink
from src.platform.config import reset_project_env_config_for_tests
from tests.conftest import _external_email_failure, _forbid_external_email


@pytest.mark.asyncio
async def test_dispatcher_flush_waits_for_queued_alert_before_stop() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    received = []

    class BlockingSink:
        async def send(self, alert):
            entered.set()
            await release.wait()
            received.append(alert)

    dispatcher = AsyncAlertDispatcher(BlockingSink())
    alert = AppAlert("fatal", "runtime failed", "error")
    dispatcher.start()
    dispatcher.emit(alert)
    flushing = asyncio.create_task(
        dispatcher.flush(timeout_seconds=1)
    )
    await entered.wait()
    assert not flushing.done()

    release.set()
    assert await flushing is True
    await dispatcher.stop()

    assert received == [alert]
    assert dispatcher.sent == 1
    assert dispatcher.failed == 0


@pytest.mark.asyncio
async def test_dispatcher_flush_timeout_is_bounded() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingSink:
        async def send(self, _alert):
            entered.set()
            await release.wait()

    dispatcher = AsyncAlertDispatcher(BlockingSink())
    dispatcher.start()
    dispatcher.emit(AppAlert("fatal", "blocked", "error"))
    await entered.wait()

    assert await dispatcher.flush(timeout_seconds=0) is False
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_dispatcher_flush_propagates_cancellation(
    monkeypatch,
) -> None:
    dispatcher = AsyncAlertDispatcher()

    async def cancelled_join():
        raise asyncio.CancelledError("cancelled")

    monkeypatch.setattr(dispatcher._queue, "join", cancelled_join)

    with pytest.raises(asyncio.CancelledError, match="cancelled"):
        await dispatcher.flush(timeout_seconds=1)


@pytest.mark.asyncio
async def test_sink_failure_does_not_stop_alert_worker() -> None:
    received = []

    class FailOnceSink:
        async def send(self, alert):
            if not received:
                received.append(None)
                raise RuntimeError("injected sink failure")
            received.append(alert)

    dispatcher = AsyncAlertDispatcher(FailOnceSink())
    dispatcher.start()
    dispatcher.emit(AppAlert("first", "fails"))
    second = AppAlert("second", "succeeds")
    dispatcher.emit(second)

    assert await dispatcher.flush(timeout_seconds=1) is True
    await dispatcher.stop()

    assert received == [None, second]
    assert dispatcher.failed == 1
    assert dispatcher.sent == 1


@pytest.mark.asyncio
async def test_sink_false_is_counted_as_failed() -> None:
    class FalseSink:
        async def send(self, _alert):
            return False

    dispatcher = AsyncAlertDispatcher(FalseSink())
    dispatcher.start()
    dispatcher.emit(AppAlert("failed", "smtp returned false"))

    assert await dispatcher.flush(timeout_seconds=1) is True
    await dispatcher.stop()

    assert dispatcher.failed == 1
    assert dispatcher.sent == 0


def test_email_isolation_survives_project_env_reset() -> None:
    assert os.environ["AETHER_ENABLE_EMAIL_ALERT"] == "0"
    reset_project_env_config_for_tests()
    assert os.environ["AETHER_ENABLE_EMAIL_ALERT"] == "0"


def test_external_email_guard_message_identifies_test_and_alert() -> None:
    message = _external_email_failure(
        "tests/app/test_alerts.py::intentional",
        [{
            "subject": "AetherEdge live runtime error",
            "severity": "error",
            "content": "strategy capability validation failed",
        }],
        ["args=('smtp.qq.com', 587) kwargs={}"],
    )

    assert "tests/app/test_alerts.py::intentional" in message
    assert "AetherEdge live runtime error" in message
    assert "strategy capability validation failed" in message
    assert "smtp_connection_count=1" in message


@pytest.mark.asyncio
async def test_external_email_guard_rejects_sink_and_smtp_calls() -> None:
    patch = MonkeyPatch()
    guard = _forbid_external_email.__wrapped__(
        patch,
        SimpleNamespace(
            node=SimpleNamespace(
                nodeid="tests/app/test_alerts.py::guard_probe"
            )
        ),
    )
    next(guard)
    try:
        with pytest.raises(AssertionError, match="guard_probe"):
            await EmailAlertSink().send(
                AppAlert(
                    "AetherEdge live runtime error",
                    "strategy capability validation failed",
                    "error",
                )
            )
        import src.utils.email_sender as email_sender

        with pytest.raises(AssertionError, match="guard_probe"):
            email_sender.smtplib.SMTP("smtp.qq.com", 587)
        with pytest.raises(AssertionError, match="alert_count=1"):
            next(guard)
    finally:
        patch.undo()
