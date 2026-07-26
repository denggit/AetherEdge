from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import pytest
from _pytest.monkeypatch import MonkeyPatch

from src.app.alerts import AppAlert, AsyncAlertDispatcher, EmailAlertSink
from src.platform.config import reset_project_env_config_for_tests
from src.utils.email_sender import EmailSender
from tests.conftest import _external_email_failure, _forbid_external_email


_REAL_SEND_EMAIL_ASYNC = EmailSender.send_email_async


def test_dispatcher_emit_reports_success_and_queue_full_without_retry() -> None:
    dispatcher = AsyncAlertDispatcher(maxsize=1)
    first = AppAlert("first", "queued")

    assert dispatcher.emit(first) is True
    assert dispatcher.emit(AppAlert("second", "dropped")) is False

    assert dispatcher._queue.qsize() == 1
    assert dispatcher._queue.get_nowait() == first
    dispatcher._queue.task_done()
    assert dispatcher.dropped == 1


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
    dispatcher.emit(AppAlert("queued", "exercise join"))

    async def cancelled_join():
        raise asyncio.CancelledError("cancelled")

    monkeypatch.setattr(dispatcher._queue, "join", cancelled_join)

    with pytest.raises(asyncio.CancelledError, match="cancelled"):
        await dispatcher.flush(timeout_seconds=1)


@pytest.mark.asyncio
async def test_dispatcher_stop_is_idempotent_and_flush_without_worker_is_bounded() -> None:
    dispatcher = AsyncAlertDispatcher(maxsize=1)

    assert await dispatcher.flush(timeout_seconds=0) is True
    assert dispatcher.emit(AppAlert("queued", "without worker")) is True
    assert await dispatcher.flush(timeout_seconds=0) is False

    dispatcher._queue.get_nowait()
    dispatcher._queue.task_done()
    dispatcher.start()
    await dispatcher.stop()
    await dispatcher.stop()


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


class _RecordingSMTP:
    calls: list[tuple] = []

    def __init__(self, host, port, *, timeout):
        self.calls.append(("init", host, port, timeout))

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def starttls(self):
        self.calls.append(("starttls",))

    def login(self, sender, password):
        self.calls.append(("login", sender, password))

    def send_message(self, message):
        self.calls.append(("send_message", message["Subject"]))


@pytest.mark.asyncio
async def test_regular_email_passes_network_timeout(monkeypatch) -> None:
    import src.utils.email_sender as email_sender

    _RecordingSMTP.calls = []
    monkeypatch.setattr(
        email_sender.EmailSender,
        "send_email_async",
        _REAL_SEND_EMAIL_ASYNC,
    )
    monkeypatch.setattr(email_sender.smtplib, "SMTP", _RecordingSMTP)
    sender = email_sender.EmailSender(
        sender="sender@example.com",
        password="secret",
        receiver="receiver@example.com",
    )

    assert await sender.send_email_async("subject", "body") is True
    assert _RecordingSMTP.calls == [
        ("init", "smtp.qq.com", 587, email_sender.SMTP_TIMEOUT_SECONDS),
        ("starttls",),
        ("login", "sender@example.com", "secret"),
        ("send_message", "subject"),
    ]


@pytest.mark.asyncio
async def test_attachment_email_passes_network_timeout(
    monkeypatch,
    tmp_path,
) -> None:
    import src.utils.email_sender as email_sender

    attachment = tmp_path / "report.txt"
    attachment.write_text("report", encoding="utf-8")
    _RecordingSMTP.calls = []
    monkeypatch.setattr(email_sender.smtplib, "SMTP", _RecordingSMTP)
    sender = email_sender.EmailSender(
        sender="sender@example.com",
        password="secret",
        receiver="receiver@example.com",
    )

    assert await sender.send_file_async(
        str(attachment),
        subject="attachment",
    ) is True
    assert _RecordingSMTP.calls == [
        ("init", "smtp.qq.com", 587, email_sender.SMTP_TIMEOUT_SECONDS),
        ("starttls",),
        ("login", "sender@example.com", "secret"),
        ("send_message", "attachment"),
    ]


@pytest.mark.parametrize(
    "failure_stage",
    ("connect", "starttls", "login", "send_message"),
)
@pytest.mark.asyncio
async def test_smtp_timeout_is_failed_and_worker_continues(
    monkeypatch,
    failure_stage,
) -> None:
    import src.utils.email_sender as email_sender

    class TimeoutOnceSMTP(_RecordingSMTP):
        failed = False

        def __init__(self, host, port, *, timeout):
            if failure_stage == "connect" and not type(self).failed:
                type(self).failed = True
                raise TimeoutError("injected SMTP timeout")
            super().__init__(host, port, timeout=timeout)

        def _fail_once(self, stage):
            if failure_stage == stage and not type(self).failed:
                type(self).failed = True
                raise TimeoutError("injected SMTP timeout")

        def starttls(self):
            self._fail_once("starttls")
            super().starttls()

        def login(self, sender, password):
            self._fail_once("login")
            super().login(sender, password)

        def send_message(self, message):
            self._fail_once("send_message")
            super().send_message(message)

    TimeoutOnceSMTP.failed = False
    _RecordingSMTP.calls = []
    monkeypatch.setattr(
        email_sender.EmailSender,
        "send_email_async",
        _REAL_SEND_EMAIL_ASYNC,
    )
    monkeypatch.setattr(email_sender.smtplib, "SMTP", TimeoutOnceSMTP)
    sender = email_sender.EmailSender(
        sender="sender@example.com",
        password="secret",
        receiver="receiver@example.com",
    )

    class SenderSink:
        async def send(self, alert):
            return await sender.send_email_async(alert.subject, alert.content)

    dispatcher = AsyncAlertDispatcher(SenderSink())
    dispatcher.start()
    assert dispatcher.emit(AppAlert("timeout", "first")) is True
    assert dispatcher.emit(AppAlert("success", "second")) is True

    assert await dispatcher.flush(timeout_seconds=1) is True
    await dispatcher.stop()

    assert dispatcher.failed == 1
    assert dispatcher.sent == 1
    assert dispatcher._worker is not None and dispatcher._worker.done()
