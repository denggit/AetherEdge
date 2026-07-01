from __future__ import annotations

import pytest
from websockets.exceptions import ConnectionClosedError

from src.runtime.tasks import ProducerStatus, ProducerSupervisor


async def _failing_transient_stream():
    if False:
        yield object()
    raise ConnectionClosedError(None, None)


async def _failing_fatal_stream():
    if False:
        yield object()
    raise RuntimeError("fatal")


async def _single_item_stream(item):
    yield item


@pytest.mark.asyncio
async def test_producer_supervisor_restarts_transient_stream_failure():
    supervisor = ProducerSupervisor()
    calls = 0
    items = []
    transient_failures = []

    def stream_factory():
        nonlocal calls
        calls += 1
        if calls == 1:
            return _failing_transient_stream()
        return _single_item_stream("trade")

    async def on_item(item):
        items.append(item)

    await supervisor.run_resilient_stream(
        name="trades",
        stream_factory=stream_factory,
        on_item=on_item,
        restart_delay_seconds=0,
        max_restarts=1,
        on_transient_failure=(
            lambda name, exc: transient_failures.append((name, type(exc)))
        ),
    )

    assert calls == 2
    assert items == ["trade"]
    assert transient_failures == [("trades", ConnectionClosedError)]
    assert supervisor.monitor.snapshot()[0].status is ProducerStatus.STOPPED


@pytest.mark.asyncio
async def test_producer_supervisor_fails_after_restart_limit():
    supervisor = ProducerSupervisor()

    with pytest.raises(ConnectionClosedError):
        await supervisor.run_resilient_stream(
            name="trades",
            stream_factory=_failing_transient_stream,
            on_item=lambda item: None,
            restart_delay_seconds=0,
            max_restarts=1,
        )

    health = supervisor.monitor.snapshot()[0]
    assert health.status is ProducerStatus.FAILED
    assert "no close frame" in (health.error or "")


@pytest.mark.asyncio
async def test_producer_supervisor_does_not_restart_non_transient_failure():
    supervisor = ProducerSupervisor()

    with pytest.raises(RuntimeError):
        await supervisor.run_resilient_stream(
            name="trades",
            stream_factory=_failing_fatal_stream,
            on_item=lambda item: None,
            restart_delay_seconds=0,
        )

    health = supervisor.monitor.snapshot()[0]
    assert health.status is ProducerStatus.FAILED
    assert "fatal" in (health.error or "")
