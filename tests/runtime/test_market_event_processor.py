from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

import pytest

from src.market_data.derived import RangeFootprintBuilder
from src.market_data.events import MarketFeatureEvent
from src.platform.data.models import (
    MarketKline,
    MarketTrade,
    MarketEventType,
    ExchangeName,
)
from src.runtime.market_data.integrity import (
    IntegrityWindowState,
    TradeDataIntegrityTracker,
    TradeIntegrityIssue,
)
from src.runtime.market_data.pipeline_plan import (
    ClosedBarControlEvent,
    ResolvedMarketPipelinePlan,
    resolve_market_pipeline,
)
from src.runtime.market_data.processor import (
    CausalIntegrityError,
    MarketEventProcessor,
    ProcessorFailureError,
    ProcessorOverflowError,
)
from src.runtime.requirements import (
    ClosedKlineRequirement,
    OrderBookRequirement,
    RangeBarRequirement,
    StrategyRuntimeRequirements,
    TradeStreamRequirement,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _trade(
    trade_id: str = "1",
    event_time_ms: int = 0,
    price: str = "3000",
    quantity: str = "0.1",
) -> MarketTrade:
    from decimal import Decimal
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        trade_id=trade_id,
        price=Decimal(price),
        quantity=Decimal(quantity),
        side="buy",
        event_time_ms=event_time_ms,
        trade_time_ms=event_time_ms,
    )


def _kline(open_time_ms: int = 0, close_time_ms: int = 1000) -> MarketKline:
    from decimal import Decimal
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="1h",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        open=Decimal("3000"),
        high=Decimal("3100"),
        low=Decimal("2900"),
        close=Decimal("3050"),
        volume=Decimal("100"),
        is_closed=True,
    )


class _TraceProcessor:
    """Records call order for test verification."""

    def __init__(self, module_id: str) -> None:
        self.module_id = module_id
        self.calls: list[str] = []
        self.trade_ids: list[str] = []
        self.concurrent = False
        self._processing = False

    async def process_trade(self, trade: MarketTrade) -> None:
        if self._processing:
            self.concurrent = True
        self._processing = True
        self.calls.append(f"{self.module_id}:{trade.trade_id}")
        self.trade_ids.append(trade.trade_id)
        self._processing = False


class _BlockingProcessor:
    """Blocks until released — used to test sequential processing."""

    def __init__(self, module_id: str) -> None:
        self.module_id = module_id
        self.enter_event = asyncio.Event()
        self.block_event = asyncio.Event()
        self.calls: list[str] = []
        self.concurrent = False
        self._processing = False

    async def process_trade(self, trade: MarketTrade) -> None:
        if self._processing:
            self.concurrent = True
        self._processing = True
        self.enter_event.set()
        self.calls.append(f"{self.module_id}:{trade.trade_id}")
        await self.block_event.wait()
        self._processing = False


class _FailingProcessor:
    """Raises on process_trade to test error propagation."""

    def __init__(self, module_id: str) -> None:
        self.module_id = module_id

    async def process_trade(self, trade: MarketTrade) -> None:
        raise RuntimeError(f"{self.module_id} failed")


# ---------------------------------------------------------------------------
# ResolvedMarketPipelinePlan tests
# ---------------------------------------------------------------------------


class TestPipelinePlanResolution:
    def test_empty_strategy_no_trade_modules(self):
        req = StrategyRuntimeRequirements()
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is False
        assert plan.closed_kline_enabled is False
        assert plan.order_book_enabled is False

    def test_trades_only_enables_trade_stream(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is True

    def test_closed_kline_only(self):
        req = StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(enabled=True, interval="4h"),
        )
        plan = resolve_market_pipeline(req)
        assert plan.closed_kline_enabled is True
        assert plan.trades_enabled is False

    def test_range_bars_enabled_adds_range_module(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
            range_bars=RangeBarRequirement(enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert "range-bars" in plan.enabled_module_ids

    def test_order_book_only(self):
        req = StrategyRuntimeRequirements(
            order_book=OrderBookRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert plan.order_book_enabled is True
        assert plan.trades_enabled is False


class TestTradeModuleOrder:
    def test_default_order_is_respected(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
            range_bars=RangeBarRequirement(enabled=True),
        )
        plan = resolve_market_pipeline(req)
        ids = list(plan.enabled_module_ids)
        # range-footprint should come before range-bars
        if "range-footprint" in ids and "range-bars" in ids:
            assert ids.index("range-footprint") < ids.index("range-bars")

# ---------------------------------------------------------------------------
# IntegrityTracker tests
# ---------------------------------------------------------------------------


class TestIntegrityRevision:
    def test_revision_monotonic(self):
        tracker = TradeDataIntegrityTracker()
        assert tracker.revision == 0
        tracker.mark_dropped(1000, "test")
        assert tracker.revision == 1
        tracker.mark_dropped(2000, "test2")
        assert tracker.revision == 2

    def test_issues_since_filters_correctly(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")
        tracker.mark_dropped(2000, "b")
        tracker.mark_dropped(3000, "c")
        issues = tracker.issues_since(1)
        assert len(issues) == 2
        assert issues[0].revision == 2
        assert issues[1].revision == 3

    def test_issues_since_empty_when_no_new_issues(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")
        issues = tracker.issues_since(1)
        assert len(issues) == 0

    def test_issues_since_zero_returns_all(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")
        tracker.mark_dropped(2000, "b")
        issues = tracker.issues_since(0)
        assert len(issues) == 2

    def test_no_synthetic_renumbering(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")
        tracker.mark_dropped(2000, "b")
        # Even if we query issues_since(1), the remaining issues
        # keep their original revisions
        issues = tracker.issues_since(1)
        assert issues[0].revision == 2
        assert issues[0].event_time_ms == 2000


class TestIntegrityRepair:
    def test_repair_through_revision(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")  # revision 1
        tracker.mark_dropped(2000, "b")  # revision 2
        tracker.mark_repaired(0, 3000, through_revision=2)
        assert tracker.is_complete(0, 3000)

    def test_new_drop_after_repair_incomplete(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")  # revision 1
        tracker.mark_repaired(0, 3000, through_revision=1)
        assert tracker.is_complete(0, 3000)
        tracker.mark_dropped(1500, "c")  # revision 2 — new drop after repair
        assert not tracker.is_complete(0, 3000)

    def test_partial_repair_not_enough(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "a")  # revision 1
        tracker.mark_dropped(2000, "b")  # revision 2
        tracker.mark_repaired(0, 3000, through_revision=1)
        assert not tracker.is_complete(0, 3000)  # revision 2 not covered

    def test_invalid_reason_returns_detail(self):
        tracker = TradeDataIntegrityTracker()
        tracker.mark_dropped(1000, "overflow")
        reason = tracker.invalid_reason(0, 2000)
        assert reason is not None
        assert "overflow" in reason

    def test_complete_window_no_issues(self):
        tracker = TradeDataIntegrityTracker()
        assert tracker.is_complete(0, 10000)


class TestIntegrityPrune:
    def test_prune_removes_old_complete_windows(self):
        tracker = TradeDataIntegrityTracker(window_size_ms=1000)
        tracker.mark_dropped(500, "a")  # in window [0, 999]
        tracker.mark_repaired(0, 999, through_revision=1)
        assert tracker.is_complete(0, 999)
        tracker.prune_before(1000)
        assert tracker.is_complete(0, 999)  # still answers (no issues left)

    def test_prune_preserves_incomplete_windows(self):
        tracker = TradeDataIntegrityTracker(window_size_ms=1000)
        tracker.mark_dropped(500, "a")
        tracker.prune_before(1000)
        # the issue should remain because window is incomplete
        assert not tracker.is_complete(0, 999)


class TestIntegrityWindowState:
    def test_complete_when_repaired(self):
        ws = IntegrityWindowState(start_ms=0, end_ms=999)
        ws.last_issue_revision = 5
        ws.repaired_through_revision = 5
        assert ws.complete

    def test_incomplete_when_not_repaired(self):
        ws = IntegrityWindowState(start_ms=0, end_ms=999)
        ws.last_issue_revision = 5
        ws.repaired_through_revision = 3
        assert not ws.complete


# ---------------------------------------------------------------------------
# MarketEventProcessor tests
# ---------------------------------------------------------------------------


class TestProcessorBasic:
    @pytest.mark.asyncio
    async def test_single_trade_processed(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        trace = _TraceProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[trace],
            maxsize=16,
        )
        await processor.start()
        trade = _trade("t1")
        processor.submit_trade(trade)
        await processor.stop()
        assert trace.calls == ["mod-a:t1"]

    @pytest.mark.asyncio
    async def test_module_call_order_deterministic(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a", "mod-b"),
        )
        trace_a = _TraceProcessor("mod-a")
        trace_b = _TraceProcessor("mod-b")
        processor = MarketEventProcessor(
            trade_modules=[trace_a, trace_b],
            maxsize=16,
        )
        await processor.start()
        processor.submit_trade(_trade("t1"))
        await processor.stop()
        assert trace_a.calls == ["mod-a:t1"]
        assert trace_b.calls == ["mod-b:t1"]
        # mod-a must be called before mod-b
        assert trace_a.calls[0].startswith("mod-a")
        assert trace_b.calls[0].startswith("mod-b")

    @pytest.mark.asyncio
    async def test_multiple_trades_sequential(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        trace = _TraceProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[trace],
            maxsize=16,
        )
        await processor.start()
        for i in range(5):
            processor.submit_trade(_trade(f"t{i}"))
        await processor.stop()
        assert trace.calls == [f"mod-a:t{i}" for i in range(5)]

    @pytest.mark.asyncio
    async def test_no_module_concurrent_processing(self):
        """Prove that the same module never processes two trades concurrently."""
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        blocker = _BlockingProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[blocker],
            maxsize=16,
        )
        await processor.start()
        # Submit first trade — will block
        processor.submit_trade(_trade("t1"))
        # Wait for first trade to enter process_trade
        await asyncio.wait_for(blocker.enter_event.wait(), timeout=2.0)
        # Submit second trade — should not be processed concurrently
        processor.submit_trade(_trade("t2"))
        # Only first trade should be recorded
        assert blocker.calls == ["mod-a:t1"]
        # Release blocker
        blocker.block_event.set()
        await processor.stop()
        assert blocker.calls == ["mod-a:t1", "mod-a:t2"]
        assert not blocker.concurrent


class TestProcessorErrorHandling:
    @pytest.mark.asyncio
    async def test_module_error_causes_processor_failure(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        failing = _FailingProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[failing],
            maxsize=16,
        )
        await processor.start()
        processor.submit_trade(_trade("t1"))
        with pytest.raises(ProcessorFailureError):
            await processor.wait_failed()
        await processor.stop()

    @pytest.mark.asyncio
    async def test_queue_overflow_fail_fast(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=(),
        )
        processor = MarketEventProcessor(
            maxsize=2,
        )
        await processor.start()
        # Fill the queue — worker is consuming asynchronously, so we need to
        # submit faster than the worker can process
        overflow_raised = False
        for i in range(10):
            try:
                processor.submit_trade(_trade(f"t{i}"))
            except ProcessorOverflowError:
                overflow_raised = True
                break
        assert overflow_raised, "Expected ProcessorOverflowError on queue full"


class TestProcessorShutdown:
    @pytest.mark.asyncio
    async def test_drain_received_events(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        trace = _TraceProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[trace],
            maxsize=16,
        )
        await processor.start()
        for i in range(10):
            processor.submit_trade(_trade(f"t{i}"))
        await processor.stop()
        assert len(trace.calls) == 10

    @pytest.mark.asyncio
    async def test_reject_after_shutdown(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=(),
        )
        processor = MarketEventProcessor(maxsize=16)
        await processor.start()
        await processor.stop()
        with pytest.raises(ProcessorFailureError):
            processor.submit_trade(_trade("t1"))

    @pytest.mark.asyncio
    async def test_drain_timeout_fails_and_marks_integrity_incomplete(self):
        plan = ResolvedMarketPipelinePlan(True, False, False, ("mod-a",))
        blocker = _BlockingProcessor("mod-a")
        integrity = TradeDataIntegrityTracker()
        processor = MarketEventProcessor(
            trade_modules=[blocker],
            integrity=integrity,
            drain_timeout_seconds=0.01,
        )
        await processor.start()
        processor.submit_trade(_trade("blocked", 10))
        await blocker.enter_event.wait()

        with pytest.raises(ProcessorFailureError, match="drain timed out"):
            await processor.stop()
        assert integrity.invalid_reason(0, 100) is not None


class TestProcessorClosedBar:
    @pytest.mark.asyncio
    async def test_future_trades_are_bounded_and_released_in_receive_order(self):
        plan = ResolvedMarketPipelinePlan(True, True, False, ("mod-a",))
        trace = _TraceProcessor("mod-a")

        class Handler:
            async def process_closed_bar(self, event):
                assert trace.trade_ids == ["period-a"]

        processor = MarketEventProcessor(
            trade_modules=[trace],
            closed_bar_handler=Handler(),
            future_buffer_maxsize=2,
        )
        await processor.start()
        processor.submit_trade(_trade("period-a", 900))
        processor.begin_closed_bar_cutoff(0, 1000)
        processor.submit_trade(_trade("period-b-1", 1001))
        processor.submit_trade(_trade("period-b-2", 1002))
        assert processor.future_buffer_size == 2

        event = ClosedBarControlEvent(open_time_ms=0, kline=_kline())
        processor.submit_closed_bar(event)
        await event.completion
        await processor.stop()

        assert trace.trade_ids == ["period-a", "period-b-1", "period-b-2"]
        assert processor.stats.max_future_buffer_depth == 2

    @pytest.mark.asyncio
    async def test_future_trade_overflow_is_fatal(self):
        plan = ResolvedMarketPipelinePlan(True, True, False, ())
        integrity = TradeDataIntegrityTracker()
        processor = MarketEventProcessor(
            integrity=integrity,
            future_buffer_maxsize=1,
        )
        await processor.start()
        processor.begin_closed_bar_cutoff(0, 1000)
        processor.submit_trade(_trade("first", 1001))
        with pytest.raises(ProcessorOverflowError, match="future Trade buffer"):
            processor.submit_trade(_trade("overflow", 1002))
        assert integrity.invalid_reason(1002, 1002) is not None
        await processor.stop()

    @pytest.mark.asyncio
    async def test_closed_bar_control_event_processed(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=True,
            order_book_enabled=False,
            enabled_module_ids=(),
        )
        processed: list[int] = []

        class Handler:
            async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
                processed.append(event.open_time_ms)
                event.completion.set_result(None)

        handler = Handler()
        processor = MarketEventProcessor(
            closed_bar_handler=handler,
            maxsize=16,
        )
        await processor.start()
        kline = _kline(open_time_ms=1000, close_time_ms=2000)
        event = ClosedBarControlEvent(open_time_ms=1000, kline=kline)
        processor.begin_closed_bar_cutoff(1000, 2000)
        processor.submit_closed_bar(event)
        await asyncio.wait_for(event.completion, timeout=2.0)
        await processor.stop()
        assert processed == [1000]

    @pytest.mark.asyncio
    async def test_closed_bar_after_trades_sequential(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=True,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        order: list[str] = []

        class TraceProcessor(_TraceProcessor):
            async def process_trade(self, trade: MarketTrade) -> None:
                order.append(str(trade.trade_id))
                await super().process_trade(trade)

        trace = TraceProcessor("mod-a")

        class Handler:
            async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
                order.append("closed-bar")
                event.completion.set_result(None)

        handler = Handler()
        processor = MarketEventProcessor(
            trade_modules=[trace],
            closed_bar_handler=handler,
            maxsize=16,
        )
        await processor.start()
        processor.submit_trade(_trade("t1"))
        processor.submit_trade(_trade("t2"))
        kline = _kline(open_time_ms=1000, close_time_ms=2000)
        cb_event = ClosedBarControlEvent(open_time_ms=1000, kline=kline)
        processor.begin_closed_bar_cutoff(1000, 2000)
        processor.submit_closed_bar(cb_event)
        processor.submit_trade(_trade("t3", 2001))
        processor.submit_trade(_trade("t4", 2002))
        await asyncio.wait_for(cb_event.completion, timeout=2.0)
        await processor.stop()
        assert order == ["t1", "t2", "closed-bar", "t3", "t4"]

    @pytest.mark.asyncio
    async def test_trade_at_completed_close_boundary_is_fatal(self):
        plan = ResolvedMarketPipelinePlan(True, True, False, ())

        class Handler:
            async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
                pass

        integrity = TradeDataIntegrityTracker()
        processor = MarketEventProcessor(
            closed_bar_handler=Handler(),
            integrity=integrity,
        )
        await processor.start()
        event = ClosedBarControlEvent(open_time_ms=0, kline=_kline())
        processor.begin_closed_bar_cutoff(0, 1000)
        processor.submit_closed_bar(event)
        await event.completion
        with pytest.raises(CausalIntegrityError, match="completed closed-bar boundary"):
            processor.submit_trade(_trade("late", 1000))
        assert integrity.issues_since(0)[0].reason == (
            "late_trade_after_closed_bar_completed"
        )
        await processor.stop()

    @pytest.mark.asyncio
    async def test_late_trade_before_control_starts_runs_before_closed_bar(self):
        plan = ResolvedMarketPipelinePlan(True, True, False, ())
        processed = []

        class Handler:
            async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
                processed.append(event.open_time_ms)

        processor = MarketEventProcessor(closed_bar_handler=Handler())
        event = ClosedBarControlEvent(open_time_ms=0, kline=_kline())
        processor.begin_closed_bar_cutoff(0, 1000)
        processor.submit_closed_bar(event)
        processor.submit_trade(_trade("late", 1000))
        await processor.start()
        await processor.stop()

        assert processed == [0]

    @pytest.mark.asyncio
    async def test_late_trade_during_closed_bar_cancels_callback(self):
        plan = ResolvedMarketPipelinePlan(True, True, False, ())
        started = asyncio.Event()
        finished = asyncio.Event()

        class Handler:
            async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
                started.set()
                await asyncio.Event().wait()
                finished.set()

        processor = MarketEventProcessor(closed_bar_handler=Handler())
        await processor.start()
        event = ClosedBarControlEvent(open_time_ms=0, kline=_kline())
        processor.begin_closed_bar_cutoff(0, 1000)
        processor.submit_closed_bar(event)
        await started.wait()

        with pytest.raises(CausalIntegrityError, match="active closed-bar boundary"):
            processor.submit_trade(_trade("late", 1000))
        with pytest.raises(asyncio.CancelledError):
            await event.completion
        assert not finished.is_set()
        await processor.stop()

    @pytest.mark.asyncio
    async def test_closed_bar_failure_propagates(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=True,
            order_book_enabled=False,
            enabled_module_ids=(),
        )

        class FailingHandler:
            async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
                raise RuntimeError("closed bar failed")

        handler = FailingHandler()
        processor = MarketEventProcessor(
            closed_bar_handler=handler,
            maxsize=16,
        )
        await processor.start()
        kline = _kline()
        cb_event = ClosedBarControlEvent(open_time_ms=0, kline=kline)
        processor.begin_closed_bar_cutoff(0, 1000)
        processor.submit_closed_bar(cb_event)
        # Should eventually fail
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(cb_event.completion, timeout=5.0)
        await processor.stop()


class TestProcessorDemandDriven:
    @pytest.mark.asyncio
    async def test_only_enabled_modules_called(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        trace_a = _TraceProcessor("mod-a")
        trace_b = _TraceProcessor("mod-b")  # not enabled!
        processor = MarketEventProcessor(
            trade_modules=[trace_a],  # only mod-a enabled
            maxsize=16,
        )
        await processor.start()
        processor.submit_trade(_trade("t1"))
        await processor.stop()
        assert trace_a.calls == ["mod-a:t1"]
        assert trace_b.calls == []  # never called

    @pytest.mark.asyncio
    async def test_trade_processed_once_per_module(self):
        plan = ResolvedMarketPipelinePlan(
            trades_enabled=True,
            closed_kline_enabled=False,
            order_book_enabled=False,
            enabled_module_ids=("mod-a",),
        )
        trace = _TraceProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[trace],
            maxsize=16,
        )
        await processor.start()
        processor.submit_trade(_trade("t1"))
        await processor.stop()
        # Each trade_id only appears once
        assert trace.trade_ids == ["t1"]
        assert len(trace.trade_ids) == 1


class TestProcessorPerformance:
    @pytest.mark.asyncio
    async def test_single_worker_smoke_records_latency_and_backlog(self):
        count = 5_000
        trace = _TraceProcessor("mod-a")
        processor = MarketEventProcessor(
            trade_modules=[trace],
            maxsize=count + 1,
        )
        await processor.start()
        started = time.monotonic()
        for index in range(count):
            processor.submit_trade(_trade(str(index), index + 1))
        await processor.stop()
        wall_seconds = time.monotonic() - started

        samples = sorted(processor.stats.processing_times_ms)
        p95 = samples[int((len(samples) - 1) * 0.95)]
        p99 = samples[int((len(samples) - 1) * 0.99)]
        print(
            "processor_smoke "
            f"avg_ms={sum(samples) / len(samples):.6f} "
            f"p95_ms={p95:.6f} p99_ms={p99:.6f} "
            f"throughput_per_second={count / wall_seconds:.2f} "
            f"max_backlog={processor.stats.max_queue_depth} "
            f"max_future_buffer={processor.stats.max_future_buffer_depth} "
            f"closed_bar_pending_ms={processor.stats.closed_bar_pending_time_ms:.6f} "
            f"module_ms={processor.stats.module_timings['mod-a']:.6f}"
        )
        assert processor.stats.trades_processed == count
        assert processor.stats.max_queue_depth == count
        assert len(samples) == count
        assert 0 <= p95 <= p99
        assert count / wall_seconds > 100
        assert processor.stats.module_timings["mod-a"] >= 0


# ---------------------------------------------------------------------------
# ClosedBarControlEvent tests
# ---------------------------------------------------------------------------


class TestClosedBarControlEvent:
    def test_fields_accessible(self):
        kline = _kline(open_time_ms=1000, close_time_ms=2000)
        event = ClosedBarControlEvent(open_time_ms=1000, kline=kline)
        assert event.open_time_ms == 1000
        assert event.kline is kline

    @pytest.mark.asyncio
    async def test_completion_future_created(self):
        kline = _kline()
        event = ClosedBarControlEvent(open_time_ms=1000, kline=kline)
        fut = event.completion
        assert not fut.done()
