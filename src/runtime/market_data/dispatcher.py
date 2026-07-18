from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Generic, Protocol, TypeVar


EventT = TypeVar("EventT")
EventHandler = Callable[[EventT], Awaitable[None] | None]
ErrorHandler = Callable[[str, BaseException], None]


class EventDispatcher(Protocol[EventT]):
    async def start(self) -> None: ...

    def publish(self, event: EventT) -> "DispatchResult": ...

    async def stop(self) -> None: ...


class BackpressurePolicy(str, Enum):
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"


@dataclass(frozen=True)
class DispatchResult:
    delivered: int
    dropped: int


@dataclass(frozen=True)
class SubscriptionHealth:
    subscriber_id: str
    queue_size: int
    queue_maxsize: int
    delivered: int
    dropped: int
    failed: bool
    error: str | None


class _Subscription(Generic[EventT]):
    def __init__(
        self,
        *,
        subscriber_id: str,
        handler: EventHandler[EventT],
        maxsize: int,
        policy: BackpressurePolicy,
        on_error: ErrorHandler | None,
    ) -> None:
        if not subscriber_id.strip():
            raise ValueError("subscriber id must be non-empty")
        if maxsize <= 0:
            raise ValueError("subscription queue maxsize must be positive")
        self.subscriber_id = subscriber_id.strip().lower()
        self.handler = handler
        self.policy = policy
        self.on_error = on_error
        self.queue: asyncio.Queue[EventT] = asyncio.Queue(maxsize=maxsize)
        self.task: asyncio.Task[None] | None = None
        self.delivered = 0
        self.dropped = 0
        self.error: BaseException | None = None

    def publish(self, event: EventT) -> bool:
        try:
            self.queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self.dropped += 1
            if self.policy is BackpressurePolicy.DROP_NEWEST:
                return False
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - same-loop guard
                return False
            self.queue.put_nowait(event)
            return True

    def start(self) -> None:
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(
                self._run(),
                name=f"event-consumer:{self.subscriber_id}",
            )

    async def drain(self) -> None:
        await self.queue.join()

    async def stop(self) -> None:
        task = self.task
        self.task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def health(self) -> SubscriptionHealth:
        return SubscriptionHealth(
            subscriber_id=self.subscriber_id,
            queue_size=self.queue.qsize(),
            queue_maxsize=self.queue.maxsize,
            delivered=self.delivered,
            dropped=self.dropped,
            failed=self.error is not None,
            error=(
                None
                if self.error is None
                else f"{type(self.error).__name__}: {self.error}"
            ),
        )

    async def _run(self) -> None:
        while True:
            event = await self.queue.get()
            try:
                result = self.handler(event)
                if inspect.isawaitable(result):
                    await result
                self.delivered += 1
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self.error = exc
                if self.on_error is not None:
                    self.on_error(self.subscriber_id, exc)
                raise
            finally:
                self.queue.task_done()


class BoundedEventDispatcher(Generic[EventT]):
    """Small fan-out dispatcher with one bounded queue per consumer."""

    def __init__(self, *, drain_timeout_seconds: float = 5.0) -> None:
        if drain_timeout_seconds < 0:
            raise ValueError("drain timeout must be non-negative")
        self._subscriptions: dict[str, _Subscription[EventT]] = {}
        self._started = False
        self._drain_timeout_seconds = float(drain_timeout_seconds)

    def subscribe(
        self,
        *,
        subscriber_id: str,
        handler: EventHandler[EventT],
        maxsize: int,
        policy: BackpressurePolicy = BackpressurePolicy.DROP_OLDEST,
        on_error: ErrorHandler | None = None,
    ) -> None:
        normalized = subscriber_id.strip().lower()
        if normalized in self._subscriptions:
            raise ValueError(f"duplicate subscriber id: {normalized}")
        subscription = _Subscription(
            subscriber_id=normalized,
            handler=handler,
            maxsize=maxsize,
            policy=policy,
            on_error=on_error,
        )
        self._subscriptions[normalized] = subscription
        if self._started:
            subscription.start()

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        for subscription in self._subscriptions.values():
            subscription.start()

    def publish(self, event: EventT) -> DispatchResult:
        delivered = 0
        dropped = 0
        for subscription in self._subscriptions.values():
            if subscription.publish(event):
                delivered += 1
            else:
                dropped += 1
        return DispatchResult(delivered=delivered, dropped=dropped)

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        drains = tuple(
            subscription.drain()
            for subscription in self._subscriptions.values()
        )
        if drains:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*drains),
                    timeout=self._drain_timeout_seconds,
                )
            except TimeoutError:
                pass
        await asyncio.gather(
            *(subscription.stop() for subscription in self._subscriptions.values())
        )

    def health(self) -> tuple[SubscriptionHealth, ...]:
        return tuple(
            subscription.health()
            for subscription in self._subscriptions.values()
        )

    @property
    def subscriber_ids(self) -> tuple[str, ...]:
        return tuple(self._subscriptions)

    @property
    def task_count(self) -> int:
        return sum(
            subscription.task is not None
            and not subscription.task.done()
            for subscription in self._subscriptions.values()
        )


@dataclass
class _OrderedSubscriber(Generic[EventT]):
    subscriber_id: str
    handler: EventHandler[EventT]
    on_error: ErrorHandler | None
    delivered: int = 0
    error: BaseException | None = None


class BoundedOrderedEventDispatcher(Generic[EventT]):
    """Bound WebSocket backpressure while preserving consumer call order."""

    def __init__(
        self,
        *,
        maxsize: int,
        policy: BackpressurePolicy = BackpressurePolicy.DROP_OLDEST,
        drain_timeout_seconds: float = 5.0,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("dispatcher queue maxsize must be positive")
        if drain_timeout_seconds < 0:
            raise ValueError("drain timeout must be non-negative")
        self._queue: asyncio.Queue[EventT] = asyncio.Queue(maxsize=maxsize)
        self._policy = policy
        self._drain_timeout_seconds = float(drain_timeout_seconds)
        self._subscribers: list[_OrderedSubscriber[EventT]] = []
        self._subscriber_ids: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._dropped = 0

    def subscribe(
        self,
        *,
        subscriber_id: str,
        handler: EventHandler[EventT],
        on_error: ErrorHandler | None = None,
    ) -> None:
        normalized = subscriber_id.strip().lower()
        if not normalized:
            raise ValueError("subscriber id must be non-empty")
        if normalized in self._subscriber_ids:
            raise ValueError(f"duplicate subscriber id: {normalized}")
        self._subscriber_ids.add(normalized)
        self._subscribers.append(
            _OrderedSubscriber(
                subscriber_id=normalized,
                handler=handler,
                on_error=on_error,
            )
        )

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(
                self._run(),
                name="ordered-event-dispatcher",
            )

    def publish(self, event: EventT) -> DispatchResult:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._policy is BackpressurePolicy.DROP_NEWEST:
                return DispatchResult(delivered=0, dropped=1)
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - same-loop guard
                return DispatchResult(delivered=0, dropped=1)
            self._queue.put_nowait(event)
        return DispatchResult(delivered=len(self._subscribers), dropped=0)

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        try:
            await asyncio.wait_for(
                self._queue.join(),
                timeout=self._drain_timeout_seconds,
            )
        except TimeoutError:
            pass
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def health(self) -> tuple[SubscriptionHealth, ...]:
        return tuple(
            SubscriptionHealth(
                subscriber_id=subscriber.subscriber_id,
                queue_size=self._queue.qsize(),
                queue_maxsize=self._queue.maxsize,
                delivered=subscriber.delivered,
                dropped=self._dropped,
                failed=subscriber.error is not None,
                error=(
                    None
                    if subscriber.error is None
                    else f"{type(subscriber.error).__name__}: "
                    f"{subscriber.error}"
                ),
            )
            for subscriber in self._subscribers
        )

    @property
    def subscriber_ids(self) -> tuple[str, ...]:
        return tuple(
            subscriber.subscriber_id for subscriber in self._subscribers
        )

    @property
    def task_count(self) -> int:
        return int(self._task is not None and not self._task.done())

    async def _run(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                for subscriber in self._subscribers:
                    try:
                        result = subscriber.handler(event)
                        if inspect.isawaitable(result):
                            await result
                        subscriber.delivered += 1
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        subscriber.error = exc
                        if subscriber.on_error is not None:
                            subscriber.on_error(subscriber.subscriber_id, exc)
                        raise
            finally:
                self._queue.task_done()


__all__ = [
    "BackpressurePolicy",
    "BoundedEventDispatcher",
    "BoundedOrderedEventDispatcher",
    "DispatchResult",
    "EventDispatcher",
    "SubscriptionHealth",
]
