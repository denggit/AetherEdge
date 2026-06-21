from __future__ import annotations

import pytest

from src.runtime.tasks import ProducerHealthMonitor, ProducerStatus, ProducerSupervisor


async def _failing_stream():
    if False:
        yield object()
    raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_producer_supervisor_records_failed_producer():
    supervisor = ProducerSupervisor()

    with pytest.raises(RuntimeError):
        await supervisor.run_stream(name="trades", stream=_failing_stream(), on_item=lambda item: None)

    health = supervisor.monitor.snapshot()[0]
    assert health.status is ProducerStatus.FAILED
    assert "boom" in (health.error or "")


def test_producer_health_monitor_marks_stale_producer():
    now = 1_000
    monitor = ProducerHealthMonitor(now_ms_fn=lambda: now)
    monitor.mark_running("trades")
    now = 70_000

    stale = monitor.mark_stale(stale_after_ms=60_000)

    assert stale[0].name == "trades"
    assert stale[0].status is ProducerStatus.STALE
