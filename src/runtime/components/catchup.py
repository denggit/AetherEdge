from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange, WarmupRequest
from src.market_data.storage import SqliteKlineStore
from src.platform.data.models import MarketKline
from src.platform.snapshot import PlatformSnapshot
from src.runtime.features import closed_kline_feature, range_aggregate_unavailable_feature
from src.runtime.startup_catchup import (
    StartupCatchupConfig,
    StartupCatchupDecision,
    _check_price_guard,
    _deviation_pct,
)
from src.signals import TradeSignal
from src.signals.models import SignalAction

from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


@dataclass(frozen=True)
class _CatchupWindow:
    config: StartupCatchupConfig
    current_open_ms: int
    candidate_open_ms: int
    candidate_close_ms: int
    fresh_age_ms: int


class CatchupComponent(RuntimeComponent):
    async def _call_on_start(self, snapshot: PlatformSnapshot) -> None:
        signals = await self._strategy_host.on_start(snapshot)
        if signals is None:
            return
        self.stats.on_start_called = True
        logger.info("Strategy on_start completed | signals=%s", len(signals or ()))
        await self._execute_signals(signals or (), source="on_start", event_time_ms=None)

    async def _fetch_current_market_price(self) -> Decimal | None:
        """Fetch current market price for price guard validation.

        Uses the data feed ticker endpoint.  Returns ``None`` when the
        price cannot be obtained so the caller can skip catch-up with
        ``reason=current_price_unavailable``.
        """
        try:
            ticker = await self.context.data.fetch_ticker()
            return ticker.price
        except Exception:
            logger.warning("Startup catchup cannot fetch current market price")
            return None

    async def _fetch_current_4h_open_price(self, current_4h_open_ms: int) -> Decimal | None:
        """Fetch the open price of the current (still-forming) 4H bar.

        Returns ``None`` when unavailable; the caller should fall back to
        the candidate closed bar close price.
        """
        try:
            rows = await self.context.data.fetch_klines(
                interval=self._closed_bar_interval,
                limit=1,
                start_time_ms=current_4h_open_ms,
                end_time_ms=current_4h_open_ms,
                use_cache=True,
                oldest_first=True,
            )
            if rows:
                return rows[0].open
        except Exception:
            pass
        return None

    def _has_any_active_position_for_catchup(
        self, snapshots: tuple[PlatformSnapshot, ...]
    ) -> bool:
        """Return True when ANY active-position or pending-state source is true.

        Checks (any single true → skip catch-up):
        1. Exchange snapshot positions with non-zero quantity
        2. Strategy exposes one or more active logical position snapshots
        3. Strategy reports pending transient work
        4. PositionPlanStore ``list_active_positions()`` non-empty
        5. StateStore has open orders (including stop orders)
        """
        # 1. Exchange snapshots — any non-zero position quantity
        for snap in snapshots:
            for pos in getattr(snap, "positions", ()) or ():
                qty = getattr(pos, "quantity", None)
                if qty is not None and qty != 0:
                    return True

        # 2 & 3. Strategy-internal logical positions / pending entry
        if self._strategy_position_index().active:
            return True
        provider = self._strategy_pending_work_provider()
        if provider is not None and provider.has_pending_strategy_work():
            return True

        # 4. PositionPlanStore active plans
        store = self._position_plan_store or self._get_position_plan_store()
        try:
            if store.list_active_positions():
                return True
        except Exception:
            pass

        # 5. StateStore open orders
        if self._has_open_orders():
            return True

        # 6. Unresolved follower close (master closed, follower still open)
        if self._has_unresolved_follower_close():
            return True

        return False

    async def _preview_strategy_market_features(
        self, events: Sequence[MarketFeatureEvent]
    ) -> list[TradeSignal]:
        """Feed market-feature events to the strategy **without** executing.

        Returns the raw ``TradeSignal`` objects the strategy produced.
        The caller is responsible for filtering, price-guard checks, and
        eventual execution.
        """
        signals: list[TradeSignal] = []
        for event in events:
            signals.extend(await self._get_market_feature_pipeline().dispatch(event))
        return signals

    def _capture_startup_preview_state(self) -> StartupPreviewState:
        provider = self._strategy_startup_preview_provider()
        return StartupPreviewState(
            provider=provider,
            state=(
                provider.capture_startup_preview_state()
                if provider is not None
                else None
            ),
        )

    def _restore_startup_preview_state(self, state: StartupPreviewState) -> None:
        if state.provider is not None:
            state.provider.restore_startup_preview_state(state.state)

    async def _build_range_aggregate_events_for_bucket(
        self, bucket_start_ms: int
    ) -> list[MarketFeatureEvent]:
        return self._require_range_module().build_aggregate_events(
            bucket_start_ms
        )

    def _get_min_range_bars(self) -> int:
        return self._require_range_module().config.min_bars

    async def _evaluate_startup_catchup_once(
        self,
        snapshot: PlatformSnapshot,
    ) -> None:
        """Evaluate one guarded startup catch-up entry without retrying."""

        window = self._startup_catchup_window(snapshot)
        if window is None:
            return
        kline = self._load_startup_catchup_kline(window)
        if kline is None:
            return
        range_ready, range_events = await self._startup_catchup_range_events(
            window.candidate_open_ms
        )
        if not range_ready:
            return
        prices = await self._startup_catchup_prices(window, kline)
        if prices is None:
            return
        current_price, theoretical_open = prices
        preview_events = [closed_kline_feature(kline), *range_events]
        preview_state = self._capture_startup_preview_state()
        signals = await self._preview_strategy_market_features(preview_events)
        self._startup_catchup_range_observed = bool(range_events)
        logger.info(
            "Startup catchup strategy preview | total_signals=%s",
            len(signals),
        )
        signals_to_execute, range_bar_count = (
            self._filter_startup_catchup_signals(
                signals,
                window=window,
                current_price=current_price,
                theoretical_open=theoretical_open,
                range_events=range_events,
            )
        )
        if not signals_to_execute:
            self._restore_startup_preview_state(preview_state)
            logger.info(
                "Startup catchup skipped | reason=no_open_signal_after_price_guard "
                "total_signals=%s candidate_open_ms=%s",
                len(signals),
                window.candidate_open_ms,
            )
            self._closed_bar_scheduler.mark_emitted(
                window.candidate_open_ms
            )
            return
        await self._complete_startup_catchup(
            signals_to_execute,
            window=window,
            current_price=current_price,
            theoretical_open=theoretical_open,
            range_bar_count=range_bar_count,
        )

    def _startup_catchup_window(
        self,
        snapshot: PlatformSnapshot,
    ) -> _CatchupWindow | None:
        if self._startup_catchup_evaluated:
            return None
        self._startup_catchup_evaluated = True
        if not self.requirements.closed_kline.enabled:
            logger.info(
                "Startup catchup skipped | reason=closed_kline_disabled"
            )
            return None
        config: StartupCatchupConfig = self.runtime_config.startup_catchup
        if not config.enabled:
            logger.info(
                "Startup catchup skipped | reason=startup_catchup_disabled"
            )
            return None
        now_ms = int(time.time() * 1000)
        interval_ms = self._closed_bar_interval_ms
        current_open = (now_ms // interval_ms) * interval_ms
        candidate_open = current_open - interval_ms
        fresh_age_ms = now_ms - current_open
        if fresh_age_ms > config.fresh_open_window_seconds * 1000:
            logger.info(
                "Startup catchup skipped | reason=outside_fresh_4h_open_window "
                "age_seconds=%s window_seconds=%s",
                fresh_age_ms // 1000,
                config.fresh_open_window_seconds,
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return None
        self._heartbeat_service.read_previous()
        snapshots = self._last_snapshots or (snapshot,)
        if self._has_any_active_position_for_catchup(snapshots):
            logger.info(
                "Startup catchup skipped | "
                "reason=active_position_or_pending_state_exists"
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return None
        return _CatchupWindow(
            config=config,
            current_open_ms=current_open,
            candidate_open_ms=candidate_open,
            candidate_close_ms=current_open - 1,
            fresh_age_ms=fresh_age_ms,
        )

    def _load_startup_catchup_kline(
        self,
        window: _CatchupWindow,
    ) -> MarketKline | None:
        repository = (
            self.service_dependencies().kline_store or SqliteKlineStore()
        )
        rows = repository.load(
            symbol=self.app_config.symbol,
            interval=self._closed_bar_interval,
            time_range=TimeRange(
                window.candidate_open_ms,
                window.candidate_close_ms,
            ),
        )
        closed_rows = [
            row
            for row in rows
            if row.is_closed
            and row.open_time_ms == window.candidate_open_ms
        ]
        if not closed_rows:
            logger.info(
                "Startup catchup skipped | reason=no_closed_bar_found "
                "candidate_open_ms=%s",
                window.candidate_open_ms,
            )
            return None
        kline = closed_rows[-1]
        if (
            kline.close_time_ms != window.candidate_close_ms
            or kline.open_time_ms != window.candidate_open_ms
        ):
            logger.info(
                "Startup catchup skipped | reason=candidate_bar_not_previous_4h "
                "expected_open_ms=%s expected_close_ms=%s "
                "actual_open_ms=%s actual_close_ms=%s",
                window.candidate_open_ms,
                window.candidate_close_ms,
                kline.open_time_ms,
                kline.close_time_ms,
            )
            self._closed_bar_scheduler.mark_emitted(
                window.candidate_open_ms
            )
            return None
        if (
            self._closed_bar_scheduler.last_emitted_open_time_ms
            == window.candidate_open_ms
        ):
            logger.info(
                "Startup catchup skipped | reason=already_executed "
                "candidate_open_ms=%s",
                window.candidate_open_ms,
            )
            return None
        return kline

    async def _startup_catchup_range_events(
        self,
        candidate_open_ms: int,
    ) -> tuple[bool, list[MarketFeatureEvent]]:
        if not self.requirements.range_bars.enabled:
            return True, []
        events = await self._build_range_aggregate_events_for_bucket(
            candidate_open_ms
        )
        if not events:
            logger.info(
                "Startup catchup skipped | reason=range_aggregate_unavailable "
                "bucket_start_ms=%s",
                candidate_open_ms,
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open_ms)
            return False, []
        logger.info(
            "Startup catchup range aggregate ready | bucket_start_ms=%s "
            "events=%s",
            candidate_open_ms,
            len(events),
        )
        return True, events

    async def _startup_catchup_prices(
        self,
        window: _CatchupWindow,
        kline: MarketKline,
    ) -> tuple[Decimal, Decimal] | None:
        current_price = await self._fetch_current_market_price()
        if current_price is None:
            logger.info(
                "Startup catchup skipped | reason=current_price_unavailable "
                "candidate_open_ms=%s",
                window.candidate_open_ms,
            )
            self._closed_bar_scheduler.mark_emitted(
                window.candidate_open_ms
            )
            return None
        theoretical_open = await self._fetch_current_4h_open_price(
            window.current_open_ms
        )
        if theoretical_open is None:
            theoretical_open = kline.close
            logger.info(
                "Startup catchup using bar close as theoretical open | "
                "current_4h_open_ms=%s fallback=%s",
                window.current_open_ms,
                theoretical_open,
            )
        else:
            logger.info(
                "Startup catchup using live 4H open | open_price=%s",
                theoretical_open,
            )
        return current_price, theoretical_open

    def _filter_startup_catchup_signals(
        self,
        signals: Sequence[TradeSignal],
        *,
        window: _CatchupWindow,
        current_price: Decimal,
        theoretical_open: Decimal,
        range_events: Sequence[MarketFeatureEvent],
    ) -> tuple[list[TradeSignal], int]:
        selected: list[TradeSignal] = []
        range_bar_count = (
            range_events[0].data.get("bar_count", 0)
            if range_events
            else 0
        )
        for signal in signals:
            if signal.action not in {
                SignalAction.OPEN_LONG,
                SignalAction.OPEN_SHORT,
            }:
                continue
            side = (
                "long"
                if signal.action == SignalAction.OPEN_LONG
                else "short"
            )
            price_ok = _check_price_guard(
                current_price=current_price,
                theoretical_open_price=theoretical_open,
                side=side,
                max_adverse_pct=window.config.max_adverse_price_pct,
                max_favorable_pct=window.config.max_favorable_price_pct,
            )
            deviation_pct = _deviation_pct(
                current_price, theoretical_open
            )
            if not price_ok:
                logger.info(
                    "Startup catchup signal discarded | "
                    "reason=price_guard_failed action=%s side=%s "
                    "current_price=%s theoretical_open=%s deviation_pct=%s",
                    signal.action.value,
                    side,
                    current_price,
                    theoretical_open,
                    deviation_pct,
                )
                continue
            position_id = (
                signal.metadata.get("position_id")
                if signal.metadata
                else None
            )
            if position_id and self._startup_intent_exists(position_id):
                logger.info(
                    "Startup catchup signal discarded | "
                    "reason=order_journal_duplicate position_id=%s "
                    "action=%s candidate_open_ms=%s",
                    position_id,
                    signal.action.value,
                    window.candidate_open_ms,
                )
                continue
            selected.append(
                TradeSignal(
                    symbol=signal.symbol,
                    action=signal.action,
                    quantity=signal.quantity,
                    order_type=signal.order_type,
                    price=signal.price,
                    trigger_price=signal.trigger_price,
                    client_order_id=signal.client_order_id,
                    reason=signal.reason or "startup_catchup",
                    metadata={
                        **dict(signal.metadata or {}),
                        "startup_catchup": True,
                        "fresh_window_age_seconds": (
                            window.fresh_age_ms // 1000
                        ),
                        "price_guard": "passed",
                        "current_price": str(current_price),
                        "theoretical_open_price": str(theoretical_open),
                        "price_deviation_pct": str(deviation_pct),
                        "range_bar_count": range_bar_count,
                        "side": side,
                        "candidate_open_ms": window.candidate_open_ms,
                    },
                    created_time_ms=signal.created_time_ms,
                )
            )
        return selected, int(range_bar_count)

    def _startup_intent_exists(self, position_id: object) -> bool:
        journal = self._order_journal or self._get_order_journal()
        has_intent = getattr(
            journal, "has_intent_with_position_id", None
        )
        return bool(has_intent(str(position_id))) if callable(has_intent) else False

    async def _complete_startup_catchup(
        self,
        signals: Sequence[TradeSignal],
        *,
        window: _CatchupWindow,
        current_price: Decimal,
        theoretical_open: Decimal,
        range_bar_count: int,
    ) -> None:
        logger.info(
            "Startup catchup executing signals | count=%s "
            "candidate_open_ms=%s",
            len(signals),
            window.candidate_open_ms,
        )
        metadata = {
            "startup_catchup": True,
            "fresh_window_age_seconds": window.fresh_age_ms // 1000,
            "current_price": str(current_price),
            "theoretical_open_price": str(theoretical_open),
            "range_bar_count": range_bar_count,
            "candidate_open_ms": window.candidate_open_ms,
        }
        await self._execute_signals(
            signals,
            source="startup_catchup",
            event_time_ms=window.candidate_open_ms,
            metadata=metadata,
        )
        self._closed_bar_scheduler.mark_emitted(
            window.candidate_open_ms
        )
        self.stats.closed_klines_seen += 1
        self._startup_catchup_decision = StartupCatchupDecision(
            eligible=True,
            reason="all_guards_passed",
            metadata={**metadata, "signals_executed": len(signals)},
        )

    def _has_open_orders(self) -> bool:
        """Check for open orders across all configured exchanges."""
        list_open = getattr(self.context.state_store, "list_open_orders", None)
        if not callable(list_open):
            return False
        for exchange in self.app_config.exchanges:
            if list_open(exchange=exchange, symbol=self.app_config.symbol, include_stop_orders=True):
                return True
        return False

