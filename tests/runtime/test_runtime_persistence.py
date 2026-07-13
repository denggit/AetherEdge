from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from src.runtime.persistence import BackgroundWriteItem, BackgroundWriteQueue
from src.runtime.runner import LiveRuntimeRunner


TIMEOUT = 2.0


def _wait(event: threading.Event, message: str) -> None:
    assert event.wait(TIMEOUT), message


@pytest.mark.parametrize("max_pending", [0, -1])
def test_queue_rejects_non_positive_capacity(max_pending: int) -> None:
    with pytest.raises(ValueError, match="max_pending must be positive"):
        BackgroundWriteQueue(name="invalid", max_pending=max_pending)


def test_queue_initial_state_and_start_are_idempotent() -> None:
    writer = BackgroundWriteQueue(name="persistence-test", max_pending=3)

    assert writer.submitted == 0
    assert writer.written == 0
    assert writer.dropped == 0
    assert writer.failures == 0
    assert writer.pending_count == 0

    writer.start()
    thread = writer._thread
    writer.start()

    assert writer._thread is thread
    assert thread is not None
    assert thread.is_alive()
    assert thread.name == "persistence-test"
    assert thread.daemon is True

    writer.stop()
    assert writer._thread is None


def test_submit_executes_once_and_updates_counts() -> None:
    written = threading.Event()
    writer = BackgroundWriteQueue(name="normal-write")

    assert writer.submit(
        BackgroundWriteItem(description="normal", write=written.set)
    )
    _wait(written, "background write was not executed")
    writer.stop()

    assert writer.submitted == 1
    assert writer.written == 1
    assert writer.failures == 0
    assert writer.dropped == 0
    assert writer._thread is None


def test_writes_are_serial_and_preserve_queue_order() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    order: list[int] = []
    writer = BackgroundWriteQueue(name="ordered-write", max_pending=4)

    def write_first() -> None:
        order.append(1)
        first_started.set()
        _wait(release_first, "first write was not released")

    assert writer.submit(BackgroundWriteItem("first", write_first))
    _wait(first_started, "first write did not start")
    assert writer.submit(BackgroundWriteItem("second", lambda: order.append(2)))
    assert writer.submit(BackgroundWriteItem("third", lambda: order.append(3)))

    release_first.set()
    writer.stop()

    assert order == [1, 2, 3]
    assert writer.written == 3


def test_queue_full_evicts_oldest_pending_item() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    order: list[int] = []
    writer = BackgroundWriteQueue(name="drop-oldest", max_pending=2)

    def write_first() -> None:
        order.append(1)
        first_started.set()
        _wait(release_first, "blocking write was not released")

    assert writer.submit(BackgroundWriteItem("first", write_first))
    _wait(first_started, "blocking write did not start")
    assert writer.submit(BackgroundWriteItem("oldest-pending", lambda: order.append(2)))
    assert writer.submit(BackgroundWriteItem("newer-pending", lambda: order.append(3)))
    assert writer.submit(BackgroundWriteItem("newest", lambda: order.append(4)))

    assert writer.dropped == 1
    assert writer.submitted == 4
    assert writer.pending_count == 2

    release_first.set()
    writer.stop()

    assert order == [1, 3, 4]
    assert writer.written == 3


def test_write_failure_reports_same_exception_and_worker_continues() -> None:
    failure = RuntimeError("write failed")
    received: list[BaseException] = []
    later_write = threading.Event()
    writer = BackgroundWriteQueue(name="write-failure", max_pending=3)

    def fail() -> None:
        raise failure

    assert writer.submit(
        BackgroundWriteItem("failure", fail, on_error=received.append)
    )
    assert writer.submit(BackgroundWriteItem("later", later_write.set))
    _wait(later_write, "worker stopped after write failure")
    writer.stop()

    assert received == [failure]
    assert received[0] is failure
    assert writer.failures == 1
    assert writer.written == 1


def test_on_error_failure_is_swallowed_and_worker_continues() -> None:
    later_write = threading.Event()
    writer = BackgroundWriteQueue(name="error-callback-failure", max_pending=3)

    def fail_write() -> None:
        raise RuntimeError("write failed")

    def fail_on_error(exc: BaseException) -> None:
        raise RuntimeError("on_error failed")

    assert writer.submit(
        BackgroundWriteItem("failure", fail_write, on_error=fail_on_error)
    )
    assert writer.submit(BackgroundWriteItem("later", later_write.set))
    _wait(later_write, "worker stopped after on_error failure")
    writer.stop()

    assert writer.failures == 1
    assert writer.written == 1


def test_unknown_item_is_dropped_without_stopping_worker() -> None:
    later_write = threading.Event()
    writer = BackgroundWriteQueue(name="unknown-item", max_pending=3)
    writer.start()
    writer._queue.put_nowait(object())
    assert writer.submit(BackgroundWriteItem("later", later_write.set))

    _wait(later_write, "worker stopped after unknown item")
    writer.stop()

    assert writer.dropped == 1
    assert writer.written == 1


def test_stop_without_thread_and_repeated_stop_are_quiet() -> None:
    writer = BackgroundWriteQueue(name="never-started")

    writer.stop()
    writer.stop()

    assert writer._thread is None
    assert writer.pending_count == 0


def test_stop_without_flush_discards_pending_items() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    executed: list[str] = []
    writer = BackgroundWriteQueue(name="no-flush", max_pending=3)

    def blocking_write() -> None:
        executed.append("running")
        first_started.set()
        _wait(release_first, "blocking write was not released")

    assert writer.submit(BackgroundWriteItem("running", blocking_write))
    _wait(first_started, "blocking write did not start")
    assert writer.submit(
        BackgroundWriteItem("pending", lambda: executed.append("pending"))
    )

    writer.stop(flush=False, timeout=0.0)
    release_first.set()
    thread = writer._thread
    assert thread is not None
    thread.join(TIMEOUT)
    assert not thread.is_alive()
    writer.stop()

    assert executed == ["running"]
    assert writer._thread is None


def test_submit_is_rejected_while_writer_is_stopping() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    rejected_write = threading.Event()
    writer = BackgroundWriteQueue(name="stopping", max_pending=2)

    def blocking_write() -> None:
        first_started.set()
        _wait(release_first, "blocking write was not released")

    assert writer.submit(BackgroundWriteItem("running", blocking_write))
    _wait(first_started, "blocking write did not start")
    writer.stop(timeout=0.0)

    assert writer.submit(BackgroundWriteItem("rejected", rejected_write.set)) is False
    assert writer.dropped == 1

    release_first.set()
    thread = writer._thread
    assert thread is not None
    thread.join(TIMEOUT)
    assert not thread.is_alive()
    writer.stop()
    assert not rejected_write.is_set()


def _bare_runner(writer=None) -> LiveRuntimeRunner:
    runner = object.__new__(LiveRuntimeRunner)
    runner._live_persistence_writer = writer
    runner.services = {}
    runner.runtime_config = SimpleNamespace(background_queue_maxsize=7)
    return runner


def test_runner_default_writer_uses_config_and_is_cached() -> None:
    runner = _bare_runner()

    writer = runner._get_live_persistence_writer()

    assert isinstance(writer, BackgroundWriteQueue)
    assert writer.name == "live-persistence-writer"
    assert writer.max_pending == 7
    assert runner.services["live_persistence_writer"] is writer
    assert runner._get_live_persistence_writer() is writer
    writer.stop()


def test_runner_uses_injected_writer_without_creating_default() -> None:
    injected = object()
    runner = _bare_runner(injected)
    runner.services["live_persistence_writer"] = injected

    assert runner._get_live_persistence_writer() is injected


def test_runner_submit_passes_item_fields_by_identity() -> None:
    captured: list[BackgroundWriteItem] = []

    class Writer:
        def submit(self, item):
            captured.append(item)
            return True

    runner = _bare_runner(Writer())
    write = lambda: None
    on_error = lambda exc: None

    assert runner._submit_live_persistence_write(
        description="identity",
        write=write,
        on_error=on_error,
    )
    assert len(captured) == 1
    assert captured[0].description == "identity"
    assert captured[0].write is write
    assert captured[0].on_error is on_error


@pytest.mark.asyncio
async def test_runner_stops_background_queue_with_requested_flush() -> None:
    writer = BackgroundWriteQueue(name="runner-stop")
    runner = _bare_runner(writer)

    await runner._stop_live_persistence_writer(flush=False)

    assert writer._stopping is True
    assert writer._thread is None


@pytest.mark.asyncio
async def test_runner_stops_sync_and_async_injected_writers_once() -> None:
    sync_calls: list[bool] = []

    class SyncWriter:
        def stop(self, *, flush):
            sync_calls.append(flush)

    sync_runner = _bare_runner(SyncWriter())
    await sync_runner._stop_live_persistence_writer(flush=False)
    assert sync_calls == [False]

    async_calls: list[bool] = []

    class AsyncWriter:
        async def stop(self, *, flush):
            async_calls.append(flush)

    async_runner = _bare_runner(AsyncWriter())
    await async_runner._stop_live_persistence_writer(flush=True)
    assert async_calls == [True]
