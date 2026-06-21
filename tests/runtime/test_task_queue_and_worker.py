from __future__ import annotations

import asyncio

from src.runtime.tasks import AsyncTaskQueue, BackgroundWorker


def test_async_task_queue_drops_oldest_when_full():
    async def scenario():
        queue = AsyncTaskQueue(name="audit", maxsize=1, drop_oldest=True)
        await queue.put("old")
        await queue.put("new")
        item = await queue.get()
        queue.task_done()
        await queue.drain()
        return item, queue.stats

    item, stats = asyncio.run(scenario())

    assert item == "new"
    assert stats.enqueued == 2
    assert stats.dropped == 1
    assert stats.processed == 1


def test_background_worker_processes_items_until_drained():
    processed = []

    async def scenario():
        queue = AsyncTaskQueue(name="db", maxsize=10)

        async def handler(item):
            processed.append(item)

        worker = BackgroundWorker(queue=queue, handler=handler)
        worker.start()
        await queue.put(1)
        await queue.put(2)
        await queue.drain()
        await worker.stop()

    asyncio.run(scenario())

    assert processed == [1, 2]
