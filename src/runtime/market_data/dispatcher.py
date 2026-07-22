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
class DispatchResult(Generic[EventT]):
    delivered: int
    dropped: int
    dropped_events: tuple[EventT, ...] = ()


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

    def publish(self, event: EventT) -> tuple[bool, EventT | None]:
        try:
            self.queue.put_nowait(event)
            return True, None
        except asyncio.QueueFull:
            self.dropped += 1
            if self.policy is BackpressurePolicy.DROP_NEWEST:
                return False, event
            try:
                dropped = self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - same-loop guard
                return False, event
            self.queue.put_nowait(event)
            return True, dropped

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

    def discard_pending(self) -> None:
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self.queue.task_done()

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
        self._accepting = True
        self._error: BaseException | None = None
        self._dropped = 0
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
        def record_error(failed_id: str, exc: BaseException) -> None:
            self._record_failure(failed_id, exc)
            if on_error is not None:
                on_error(failed_id, exc)

        subscription = _Subscription(
            subscriber_id=normalized,
            handler=handler,
            maxsize=maxsize,
            policy=policy,
            on_error=record_error,
        )
        self._subscriptions[normalized] = subscription
        if self._started:
            subscription.start()

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._accepting = True
        self._error = None
        for subscription in self._subscriptions.values():
            subscription.start()

    def publish(self, event: EventT) -> DispatchResult:
        if not self._accepting:
            self._dropped += 1
            return DispatchResult(
                delivered=0,
                dropped=1,
                dropped_events=(event,),
            )
        delivered = 0
        dropped = 0
        dropped_events: list[EventT] = []
        for subscription in self._subscriptions.values():
            accepted, discarded = subscription.publish(event)
            if accepted:
                delivered += 1
            if discarded is not None:
                dropped += 1
                self._dropped += 1
                dropped_events.append(discarded)
        return DispatchResult(
            delivered=delivered,
            dropped=dropped,
            dropped_events=tuple(dropped_events),
        )

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        self._accepting = False
        drains = tuple(
            subscription.drain()
            for subscription in self._subscriptions.values()
        )
        drain_error: BaseException | None = None
        if drains:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*drains),
                    timeout=self._drain_timeout_seconds,
                )
            except TimeoutError:
                unfinished = sum(
                    1
                    for item in self._subscriptions.values()
                    if item.task is not None and not item.task.done()
                )
                if unfinished > 0:
                    drain_error = DispatcherDrainTimeout(
                        "event dispatcher drain timed out | "
                        f"pending={sum(item.queue.qsize() for item in self._subscriptions.values())}"
                    )
                    for subscription in self._subscriptions.values():
                        subscription.discard_pending()
        await asyncio.gather(
            *(subscription.stop() for subscription in self._subscriptions.values())
        )
        if drain_error is not None:
            raise drain_error

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

    @property
    def dropped_count(self) -> int:
        return self._dropped

    @property
    def error(self) -> BaseException | None:
        if self._error is not None:
            return self._error
        for subscription in self._subscriptions.values():
            task = subscription.task
            if subscription.error is not None:
                return subscription.error
            if task is not None and task.done() and not task.cancelled():
                try:
                    error = task.exception()
                except asyncio.CancelledError:
                    error = None
                if error is not None:
                    return error
        return None

    def raise_if_failed(self) -> None:
        error = self.error
        if error is not None:
            raise RuntimeError(
                "event dispatcher failed | "
                f"error={type(error).__name__}: {error}"
            ) from error

    def _record_failure(self, subscriber_id: str, exc: BaseException) -> None:
        if self._error is None:
            self._error = RuntimeError(
                "event subscriber failed | "
                f"subscriber_id={subscriber_id} "
                f"error={type(exc).__name__}: {exc}"
            )
            self._error.__cause__ = exc
        self._accepting = False
        current = asyncio.current_task()
        for subscription in self._subscriptions.values():
            subscription.discard_pending()
            if subscription.task is not None and subscription.task is not current:
                subscription.task.cancel()


__all__ = [
    "BackpressurePolicy",
    "BoundedEventDispatcher",
    "DispatchResult",
    "DispatcherDrainTimeout",
    "EventDispatcher",
    "SubscriptionHealth",
]
