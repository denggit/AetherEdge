from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


SignalBatch = Sequence[Any]
SignalResults = Sequence[Any]


@dataclass(frozen=True)
class RuntimeSignalExecutionRequest:
    signals: SignalBatch
    source: str
    event_time_ms: int | None
    metadata: Mapping[str, Any] | None = None
    feedback_depth: int = 0


PrepareSignal = Callable[
    [Any, RuntimeSignalExecutionRequest],
    bool,
]
CreateIntent = Callable[
    [Any, RuntimeSignalExecutionRequest],
    Any,
]
ExecuteIntent = Callable[[Any], Awaitable[SignalResults]]
PostSubmitSync = Callable[
    [Any, RuntimeSignalExecutionRequest],
    Awaitable[None],
]
HandleResults = Callable[[Any, SignalResults], None]
PostOrderSync = Callable[
    [Any, RuntimeSignalExecutionRequest],
    Awaitable[None],
]
ProcessFeedback = Callable[
    [Any, SignalResults, RuntimeSignalExecutionRequest],
    Awaitable[SignalBatch],
]
BuildFeedbackRequest = Callable[
    [Any, SignalBatch, RuntimeSignalExecutionRequest],
    RuntimeSignalExecutionRequest | None,
]


@dataclass(frozen=True)
class RuntimeSignalExecutionPlan:
    prepare_signal: PrepareSignal
    create_intent: CreateIntent
    execute_intent: ExecuteIntent
    post_submit_sync: PostSubmitSync
    handle_results: HandleResults
    post_order_sync: PostOrderSync
    process_feedback: ProcessFeedback
    build_feedback_request: BuildFeedbackRequest


class RuntimeSignalExecutionService:
    async def execute(
        self,
        request: RuntimeSignalExecutionRequest,
        plan: RuntimeSignalExecutionPlan,
    ) -> None:
        for signal in request.signals:
            if not plan.prepare_signal(signal, request):
                continue

            intent = plan.create_intent(signal, request)
            results = await plan.execute_intent(intent)

            await plan.post_submit_sync(signal, request)
            plan.handle_results(signal, results)
            await plan.post_order_sync(signal, request)

            follow_up = await plan.process_feedback(
                signal,
                results,
                request,
            )
            if follow_up:
                feedback_request = plan.build_feedback_request(
                    signal,
                    follow_up,
                    request,
                )
                if feedback_request is not None:
                    await self.execute(feedback_request, plan)


__all__ = [
    "RuntimeSignalExecutionPlan",
    "RuntimeSignalExecutionRequest",
    "RuntimeSignalExecutionService",
    "SignalBatch",
    "SignalResults",
]
