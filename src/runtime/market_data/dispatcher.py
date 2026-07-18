from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, Protocol, TypeVar


EventT = TypeVar("EventT")
EventHandler = Callable[[EventT], Awaitable[None] | None]
ErrorHandler = Callable[[str, BaseException], None]
EventTimeReader = Callable[[EventT], int | None]


class EventDispatcher(Protocol[EventT]):
    async def start(self) -> None: ...

    def publish(self, event: EventT) -> "DispatchResult": ...

    async def stop(self) -> None: ...


class BackpressurePolicy(str, Enum):
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"


@dataclass(frozen=True)
class DispatchResult(Generic[EventT]):
    delivered: int
    dropped: int
    dropped_events: tuple[EventT, ...] = ()


@dataclass(frozen=True)
class DispatcherBarrierResult:
    cutoff_time_ms: int
    pending: int
    completed: bool


class DispatcherDrainTimeout(RuntimeError):
    pass


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

    def publish(self, event: EventT) -> tuple[bool, bool]:
        try:
            self.queue.put_nowait(event)
            return True, False
        except asyncio.QueueFull:
            self.dropped += 1
            if self.policy is BackpressurePolicy.DROP_NEWEST:
                return False, True
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - same-loop guard
                return False, True
            self.queue.put_nowait(event)
            return True, True

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
            accepted, discarded = subscription.publish(event)
            if accepted:
                delivered += 1
            if discarded:
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
    order: int
    delivered: int = 0
    error: BaseException | None = None


@dataclass
class _QueuedEvent(Generic[EventT]):
    sequence: int
    event: EventT
    event_time_ms: int | None
    done: asyncio.Event = field(default_factory=asyncio.Event)


class BoundedOrderedEventDispatcher(Generic[EventT]):
    """Bound WebSocket backpressure while preserving consumer call order."""

    def __init__(
        self,
        *,
        maxsize: int,
        policy: BackpressurePolicy = BackpressurePolicy.DROP_OLDEST,
        drain_timeout_seconds: float = 5.0,
        event_time_ms: EventTimeReader[EventT] | None = None,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("dispatcher queue maxsize must be positive")
        if drain_timeout_seconds < 0:
            raise ValueError("drain timeout must be non-negative")
        self._queue: asyncio.Queue[_QueuedEvent[EventT]] = asyncio.Queue(
            maxsize=maxsize
        )
        self._policy = policy
        self._drain_timeout_seconds = float(drain_timeout_seconds)
        self._subscribers: list[_OrderedSubscriber[EventT]] = []
        self._subscriber_ids: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._dropped = 0
        self._sequence = 0
        self._pending: dict[int, _QueuedEvent[EventT]] = {}
        self._event_time_ms = event_time_ms
        self._accepting = True
        self._error: BaseException | None = None

    def subscribe(
        self,
        *,
        subscriber_id: str,
        handler: EventHandler[EventT],
        on_error: ErrorHandler | None = None,
        order: int = 0,
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
                order=int(order),
            )
        )
        self._subscribers.sort(key=lambda subscriber: subscriber.order)

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._accepting = True
            self._error = None
            self._task = asyncio.create_task(
                self._run(),
                name="ordered-event-dispatcher",
            )

    def publish(self, event: EventT) -> DispatchResult:
        if not self._accepting:
            self._dropped += 1
            return DispatchResult(
                delivered=0,
                dropped=1,
                dropped_events=(event,),
            )
        self._sequence += 1
        queued = _QueuedEvent(
            sequence=self._sequence,
            event=event,
            event_time_ms=(
                None
                if self._event_time_ms is None
                else self._event_time_ms(event)
            ),
        )
        dropped_events: tuple[EventT, ...] = ()
        try:
            self._queue.put_nowait(queued)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._policy is BackpressurePolicy.DROP_NEWEST:
                queued.done.set()
                return DispatchResult(
                    delivered=0,
                    dropped=1,
                    dropped_events=(event,),
                )
            try:
                dropped = self._queue.get_nowait()
                self._queue.task_done()
                self._pending.pop(dropped.sequence, None)
                dropped.done.set()
                dropped_events = (dropped.event,)
            except asyncio.QueueEmpty:  # pragma: no cover - same-loop guard
                queued.done.set()
                return DispatchResult(
                    delivered=0,
                    dropped=1,
                    dropped_events=(event,),
                )
            self._queue.put_nowait(queued)
        self._pending[queued.sequence] = queued
        return DispatchResult(
            delivered=len(self._subscribers),
            dropped=len(dropped_events),
            dropped_events=dropped_events,
        )

    async def drain_through(
        self,
        cutoff_time_ms: int,
        *,
        timeout_seconds: float | None = None,
    ) -> DispatcherBarrierResult:
        pending = tuple(
            queued
            for queued in self._pending.values()
            if queued.event_time_ms is None
            or queued.event_time_ms <= cutoff_time_ms
        )
        if not pending:
            return DispatcherBarrierResult(
                cutoff_time_ms=cutoff_time_ms,
                pending=0,
                completed=True,
            )
        waiter = asyncio.gather(*(queued.done.wait() for queued in pending))
        timeout = (
            self._drain_timeout_seconds
            if timeout_seconds is None
            else max(0.0, float(timeout_seconds))
        )
        try:
            await asyncio.wait_for(waiter, timeout=timeout)
        except TimeoutError:
            return DispatcherBarrierResult(
                cutoff_time_ms=cutoff_time_ms,
                pending=sum(not queued.done.is_set() for queued in pending),
                completed=False,
            )
        self.raise_if_failed()
        return DispatcherBarrierResult(
            cutoff_time_ms=cutoff_time_ms,
            pending=0,
            completed=True,
        )

    async def stop(self) -> None:
        self._accepting = False
        task = self._task
        self._task = None
        if task is None:
            return
        drain_error: BaseException | None = None
        try:
            await asyncio.wait_for(
                self._queue.join(),
                timeout=self._drain_timeout_seconds,
            )
        except TimeoutError as exc:
            drain_error = DispatcherDrainTimeout(
                "ordered dispatcher drain timed out | "
                f"pending={self._queue.qsize()}"
            )
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if drain_error is not None:
            raise drain_error

    def health(self) -> tuple[SubscriptionHealth, ...]:
        return tuple(
            SubscriptionHealth(
                subscriber_id=subscriber.subscriber_id,
                queue_size=self._queue.qsize(),
                queue_maxsize=self._queue.maxsize,
                delivered=subscriber.delivered,
                dropped=self._dropped,
                failed=(
                    subscriber.error is not None or self._error is not None
                ),
                error=(
                    f"{type(subscriber.error or self._error).__name__}: "
                    f"{subscriber.error or self._error}"
                    if subscriber.error is not None or self._error is not None
                    else None
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

    @property
    def dropped_count(self) -> int:
        return self._dropped

    @property
    def error(self) -> BaseException | None:
        if self._error is not None:
            return self._error
        task = self._task
        if task is not None and task.done() and not task.cancelled():
            try:
                return task.exception()
            except asyncio.CancelledError:
                return None
        return None

    def raise_if_failed(self) -> None:
        error = self.error
        if error is not None:
            raise RuntimeError(
                "ordered event dispatcher failed | "
                f"error={type(error).__name__}: {error}"
            ) from error

    async def _run(self) -> None:
        while True:
            queued = await self._queue.get()
            try:
                for subscriber in self._subscribers:
                    try:
                        result = subscriber.handler(queued.event)
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
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self._error = exc
                self._accepting = False
                self._discard_queued_after_failure()
                raise
            finally:
                self._pending.pop(queued.sequence, None)
                queued.done.set()
                self._queue.task_done()

    def _discard_queued_after_failure(self) -> None:
        while True:
            try:
                queued = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self._dropped += 1
            self._pending.pop(queued.sequence, None)
            queued.done.set()
            self._queue.task_done()


__all__ = [
    "BackpressurePolicy",
    "BoundedEventDispatcher",
    "BoundedOrderedEventDispatcher",
    "DispatchResult",
    "DispatcherBarrierResult",
    "DispatcherDrainTimeout",
    "EventDispatcher",
    "SubscriptionHealth",
]
