from __future__ import annotations

import logging
import time
from collections import deque
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from src.market_data.events import MarketFeatureEventType
from src.market_data.models import (
    FixedTimeTradeBar,
    RangeFootprintFeature,
    TradeFeatureQuality,
    TradeFootprintFeature,
)
from src.market_data.storage.trade_feature_store import (
    LargeTradeShareSample,
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (
    latest_range_footprint_context_audit,
    resolve_trade_feature_readiness,
)
from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
    mf_readiness_gates,
)
from strategies.eth_portfolio_v1.domain.mf_signal import (
    MF_RANGE_FOOTPRINT_EVENT_TYPE,
    MF_READINESS_EVENT_TYPE,
    MfLowSweepConfig,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.execution.mf_signal_mapper import (
    MfSignalMapper,
    MfSizingInput,
)


logger = logging.getLogger(__name__)


class MfDataBuffer:
    """Bounded decision bars plus lightweight long-history MF scalars."""

    def __init__(
        self,
        *,
        symbol: str,
        exchange: str = "okx",
        store_path: str = "data/market_data/aether_market_data.sqlite3",
        decision_buffer_minutes: int = 4_320,
        decision_buffer_max_minutes: int = 10_080,
        large_share_quantile_window_days: int = 90,
        range_footprint_max_features: int = 10_080,
        range_pct: Decimal | str = Decimal("0.002"),
        range_price_step: Decimal | str = Decimal("1"),
    ) -> None:
        if decision_buffer_minutes > decision_buffer_max_minutes:
            raise ValueError(
                "decision_buffer_minutes must be <= decision_buffer_max_minutes"
            )
        self.symbol = symbol
        self.exchange = exchange
        self._store = SqliteTradeFeatureStore(path=store_path)
        self._decision_maxlen = max(1, int(decision_buffer_max_minutes))
        self._decision_default_minutes = max(
            1, int(decision_buffer_minutes)
        )
        self._large_share_window_days = max(
            1, int(large_share_quantile_window_days)
        )
        self._large_share_max_samples = (
            self._large_share_window_days * 1_440
        )
        self._range_pct = Decimal(str(range_pct))
        self._range_price_step = Decimal(str(range_price_step))
        self._bars: deque[FixedTimeTradeBar] = deque(
            maxlen=self._decision_maxlen
        )
        self._large_trade_shares: deque[tuple[int, Decimal]] = deque(
            maxlen=self._large_share_max_samples + 1
        )
        self._range_footprints: deque[RangeFootprintFeature] = deque(
            maxlen=max(1, int(range_footprint_max_features))
        )
        self._latest_history_open_time_ms: int | None = None
        self._loaded_initial = False
        self._last_audit_ms = 0

    def load_initial(self) -> int:
        large_share_samples = (
            self._store.load_recent_large_trade_shares(
                symbol=self.symbol,
                exchange=self.exchange,
                limit=self._large_share_max_samples,
            )
        )
        for sample in large_share_samples:
            self._append_large_share_sample(sample)

        recent_bars = self._store.load_recent_tradebars(
            symbol=self.symbol,
            exchange=self.exchange,
            limit=self._decision_default_minutes,
        )
        for bar in recent_bars:
            self._bars.append(bar)

        latest_bar = recent_bars[-1] if recent_bars else None
        if latest_bar is not None:
            context = self._store.load_latest_range_footprint_context(
                symbol=self.symbol,
                exchange=self.exchange,
                cutoff_ms=int(latest_bar.open_time_ms),
                range_pct=self._range_pct,
                price_step=self._range_price_step,
            )
            if context is not None:
                self.append_range_footprint(context)
        self._loaded_initial = True
        return len(self._bars)

    def append_tradebar(self, bar: FixedTimeTradeBar) -> None:
        if self._bars and bar.open_time_ms == self._bars[-1].open_time_ms:
            self._bars[-1] = bar
        elif not self._bars or bar.open_time_ms > self._bars[-1].open_time_ms:
            self._bars.append(bar)
        self._append_large_share(bar)

    def append_many(self, bars: Sequence[FixedTimeTradeBar]) -> None:
        for bar in bars:
            self.append_tradebar(bar)

    def append_range_footprint(
        self, feature: RangeFootprintFeature
    ) -> None:
        if (
            self._range_footprints
            and feature.range_bar_id
            == self._range_footprints[-1].range_bar_id
        ):
            self._range_footprints[-1] = feature
        elif (
            not self._range_footprints
            or feature.available_time_ms
            >= self._range_footprints[-1].available_time_ms
        ):
            self._range_footprints.append(feature)

    def recent_bars(
        self, n_minutes: int | None = None
    ) -> tuple[FixedTimeTradeBar, ...]:
        if n_minutes is None:
            n_minutes = self._decision_default_minutes
        n = min(max(1, int(n_minutes)), len(self._bars))
        return tuple(list(self._bars)[-n:])

    def range_footprints(self) -> tuple[RangeFootprintFeature, ...]:
        return tuple(self._range_footprints)

    def large_trade_share_history(
        self, *, before_open_time_ms: int | None = None
    ) -> tuple[Decimal, ...]:
        values = [
            value
            for open_time_ms, value in self._large_trade_shares
            if (
                before_open_time_ms is None
                or open_time_ms < before_open_time_ms
            )
        ]
        return tuple(values[-self._large_share_max_samples :])

    @property
    def bar_count(self) -> int:
        return len(self._bars)

    @property
    def loaded(self) -> bool:
        return self._loaded_initial

    def large_trade_share_median(self) -> float:
        values = self.large_trade_share_history()
        if not values:
            return 0.0
        return float(_quantile(values, Decimal("0.5")))

    def large_trade_share_quantile(self, q: float = 0.75) -> float:
        values = self.large_trade_share_history()
        if not values:
            return 0.0
        return float(_quantile(values, Decimal(str(q))))

    def audit(self) -> Mapping[str, Any]:
        self._last_audit_ms = int(time.time() * 1000)
        latest = None
        if self._bars:
            last_bar = self._bars[-1]
            latest = {
                "open_time_ms": last_bar.open_time_ms,
                "close_time_ms": last_bar.close_time_ms,
                "available_time_ms": last_bar.available_time_ms,
            }
        latest_range = None
        if self._range_footprints:
            feature = self._range_footprints[-1]
            latest_range = {
                "range_bar_id": feature.range_bar_id,
                "available_time_ms": feature.available_time_ms,
                "context_available": feature.context_available,
                "quality": feature.quality,
            }
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "bar_count": len(self._bars),
            "decision_buffer_maxlen": self._decision_maxlen,
            "large_share_samples": min(
                len(self._large_trade_shares),
                self._large_share_max_samples,
            ),
            "range_footprint_count": len(self._range_footprints),
            "loaded_initial": self._loaded_initial,
            "latest_bar": latest,
            "latest_range_footprint": latest_range,
            "last_audit_ms": self._last_audit_ms,
        }

    def last_audit(self) -> Mapping[str, Any]:
        return self.audit()

    def _append_large_share(self, bar: FixedTimeTradeBar) -> None:
        self._append_large_share_sample(
            LargeTradeShareSample(
                open_time_ms=bar.open_time_ms,
                large_trade_share=bar.large_trade_share,
                quality=bar.quality,
            )
        )

    def _append_large_share_sample(
        self,
        sample: LargeTradeShareSample,
    ) -> None:
        open_time_ms = int(sample.open_time_ms)
        if (
            self._latest_history_open_time_ms is not None
            and open_time_ms < self._latest_history_open_time_ms
        ):
            return
        if (
            open_time_ms == self._latest_history_open_time_ms
            and self._large_trade_shares
            and self._large_trade_shares[-1][0] == open_time_ms
        ):
            self._large_trade_shares.pop()
        self._latest_history_open_time_ms = open_time_ms
        value = sample.large_trade_share
        if (
            sample.quality == TradeFeatureQuality.COMPLETE.value
            and value is not None
            and value.is_finite()
        ):
            self._large_trade_shares.append((open_time_ms, value))

        cutoff_ms = open_time_ms - (
            self._large_share_window_days * 1_440 * 60_000
        )
        while (
            self._large_trade_shares
            and self._large_trade_shares[0][0] < cutoff_ms
        ):
            self._large_trade_shares.popleft()


class MfDataReadiness:
    """Startup/transition readiness snapshot; never queried by entry logic."""

    def __init__(
        self,
        *,
        symbol: str,
        exchange: str = "okx",
        store_path: str = "data/market_data/aether_market_data.sqlite3",
        required_minutes: int = 4_320,
        worker_status_path: str | None = None,
        global_lock_path: str | None = None,
        range_pct: str = "0.002",
        price_step: str = "1",
        large_share_min_samples: int = 43_200,
        large_share_window_days: int = 90,
        decision_buffer_minutes: int | None = None,
        archive_publish_lag_hours: float = 8.0,
    ) -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._store = SqliteTradeFeatureStore(path=store_path)
        self._required_minutes = required_minutes
        self._worker_status_path = worker_status_path
        self._global_lock_path = global_lock_path
        self._range_pct = range_pct
        self._price_step = price_step
        self._large_share_min_samples = max(
            1, int(large_share_min_samples)
        )
        self._large_share_window_days = max(
            1, int(large_share_window_days)
        )
        self._decision_required_minutes = max(
            1,
            int(
                decision_buffer_minutes
                if decision_buffer_minutes is not None
                else min(int(required_minutes), 4_320)
            ),
        )
        self._archive_publish_lag_hours = max(
            0.0,
            float(archive_publish_lag_hours),
        )
        self._last: dict[str, Any] | None = None

    def readiness(self) -> Mapping[str, Any]:
        result = resolve_trade_feature_readiness(
            symbol=self.symbol,
            exchange=self.exchange,
            store=self._store,
            required_minutes=self._required_minutes,
            worker_status_path=self._worker_status_path,
            global_lock_path=self._global_lock_path,
            range_pct=self._range_pct,
            price_step=self._price_step,
            archive_publish_lag_hours=(
                self._archive_publish_lag_hours
            ),
        )
        audit = dict(result.audit())
        audit["historical_tradebar_ready"] = bool(
            audit.get("tradebar_ready", False)
        )
        audit["historical_fixed_time_footprint_ready"] = bool(
            audit.get("fixed_time_footprint_ready", False)
        )
        audit["historical_range_footprint_ready"] = bool(
            audit.get("range_footprint_ready", False)
        )
        audit["historical_coverage_ready"] = bool(
            audit.get("coverage_ready", False)
        )
        recent_bars = self._store.load_recent_tradebars(
            symbol=self.symbol,
            exchange=self.exchange,
            limit=self._decision_required_minutes,
        )
        latest_bar = recent_bars[-1] if recent_bars else None
        decision_complete_minutes = _complete_contiguous_tradebars(
            recent_bars
        )
        now_ms = int(time.time() * 1000)
        latest_tradebar_causal = bool(
            latest_bar is not None
            and latest_bar.close_time_ms <= now_ms
            and latest_bar.available_time_ms <= now_ms
        )
        latest_tradebar_complete = bool(
            latest_bar is not None
            and latest_bar.quality == TradeFeatureQuality.COMPLETE.value
            and latest_tradebar_causal
        )
        tradebar_ready = bool(
            latest_tradebar_complete
            and decision_complete_minutes
            >= self._decision_required_minutes
        )

        large_share_samples = self._store.load_recent_large_trade_shares(
            symbol=self.symbol,
            exchange=self.exchange,
            limit=self._large_share_window_days * 1_440 + 1,
        )
        large_share_reference_ms = (
            latest_bar.open_time_ms
            if latest_bar is not None
            else (
                large_share_samples[-1].open_time_ms
                if large_share_samples
                else None
            )
        )
        window_start_ms = (
            None
            if large_share_reference_ms is None
            else large_share_reference_ms
            - self._large_share_window_days * 1_440 * 60_000
        )
        large_share_sample_count = sum(
            1
            for sample in large_share_samples
            if large_share_reference_ms is not None
            and window_start_ms is not None
            and window_start_ms <= sample.open_time_ms
            and sample.open_time_ms < large_share_reference_ms
            and sample.quality == TradeFeatureQuality.COMPLETE.value
            and sample.large_trade_share is not None
            and sample.large_trade_share.is_finite()
        )
        large_share_samples_ready = (
            large_share_sample_count >= self._large_share_min_samples
        )

        context_audit = latest_range_footprint_context_audit(
            symbol=self.symbol,
            exchange=self.exchange,
            store=self._store,
            cutoff_ms=(
                latest_bar.open_time_ms if latest_bar is not None else 0
            ),
            range_pct=self._range_pct,
            price_step=self._price_step,
        )
        range_context_ready = bool(
            context_audit.get("range_footprint_context_ready", False)
        )
        mf_signal_feature_ready = bool(
            tradebar_ready
            and large_share_samples_ready
            and range_context_ready
        )

        audit.update(context_audit)
        audit["decision_buffer_required_minutes"] = (
            self._decision_required_minutes
        )
        audit["decision_tradebar_complete_minutes"] = (
            decision_complete_minutes
        )
        audit["latest_tradebar_open_time_ms"] = (
            None if latest_bar is None else latest_bar.open_time_ms
        )
        audit["latest_tradebar_close_time_ms"] = (
            None if latest_bar is None else latest_bar.close_time_ms
        )
        audit["latest_tradebar_available_time_ms"] = (
            None if latest_bar is None else latest_bar.available_time_ms
        )
        audit["latest_tradebar_causal_ok"] = latest_tradebar_causal
        audit["latest_tradebar_complete"] = latest_tradebar_complete
        audit["tradebar_ready"] = tradebar_ready
        audit["range_footprint_context_ready"] = range_context_ready
        # Backward-compatible MF field: in strategy readiness events this
        # now reflects the actual context gate, while historical coverage is
        # retained under historical_range_footprint_ready.
        audit["range_footprint_ready"] = range_context_ready
        audit["coverage_ready"] = mf_signal_feature_ready
        audit["large_share_sample_count"] = large_share_sample_count
        audit["large_share_min_samples"] = (
            self._large_share_min_samples
        )
        audit["large_share_window_days"] = (
            self._large_share_window_days
        )
        audit["large_share_samples_ready"] = (
            large_share_samples_ready
        )
        audit["mf_signal_feature_ready"] = mf_signal_feature_ready
        signal_ready = bool(
            mf_signal_feature_ready
            and tradebar_ready
            and large_share_samples_ready
            and range_context_ready
        )
        self._last = {
            **audit,
            "mf_signal_ready": signal_ready,
            "exit_variant": "time48",
            "source": "strategy_startup_store_scan",
            "live_freshness_required": True,
            "live_freshness_max_age_ms": 300_000,
            "mf_freshness_mode": "live_freshness_at_event",
        }
        return dict(self._last)

    @property
    def mf_signal_ready(self) -> bool:
        return bool(
            self._last is not None
            and self._last.get("mf_signal_ready", False)
        )


class MfFeatureObserver:
    """V1 MF feature observer and signal boundary."""

    observer_id = "eth_portfolio_v1_mf"
    enabled = True

    def __init__(
        self,
        buffer: MfDataBuffer | None = None,
        *,
        config: MfLowSweepConfig | None = None,
        sleeve: MfSleeveState | None = None,
        signal_mapper: MfSignalMapper | None = None,
        readiness: Mapping[str, Any] | None = None,
        sizing_provider: Callable[[], MfSizingInput] | None = None,
    ) -> None:
        self.config = config or MfLowSweepConfig()
        self._buffer = buffer
        self._sleeve = sleeve
        self._signal_mapper = signal_mapper
        self._sizing_provider = sizing_provider
        self._readiness = dict(readiness or {})
        self._last_readiness_state: tuple[bool, ...] | None = None
        self._last_tradebar_ms = 0
        self._last_footprint_ms = 0
        self._last_range_footprint_ms = 0
        self._tradebar_count = 0
        self._footprint_count = 0
        self._range_footprint_count = 0
        self._latest_tradebar_open_time_ms: int | None = None
        self._latest_footprint_open_time_ms: int | None = None
        self._latest_footprint_audit: dict[str, Any] | None = None
        self._next_open_price: Decimal | None = None
        self._next_open_time_ms: int | None = None
        self._last_evaluated_open_time_ms: int | None = None
        self._last_causal_warning_ms = 0
        self.last_mf_signal_audit: dict[str, Any] = {
            "enabled": self.config.enabled,
            "data_ready": False,
            "blocked_reason": "data_not_ready",
            "reason": "data_not_ready",
            "readiness_source": self._readiness.get(
                "source", "unavailable"
            ),
        }

    def set_readiness(
        self,
        readiness: Mapping[str, Any],
        *,
        source: str | None = None,
    ) -> None:
        self._readiness = dict(readiness)
        if source is not None:
            self._readiness["source"] = str(source)
        self._log_readiness_transition()
        gates = mf_readiness_gates(self._readiness)
        self.last_mf_signal_audit.update(
            {
                "enabled": self.config.enabled,
                "data_ready": all(gates.values()),
                "signal_feature_ready": gates[
                    "mf_signal_feature_ready"
                ],
                "readiness_source": self._readiness.get(
                    "source", "unavailable"
                ),
                "readiness_reason": self._readiness.get("reason"),
                "readiness_gates": gates,
                "missing_readiness_gates": [
                    field
                    for field, ready in gates.items()
                    if not ready
                ],
            }
        )

    def on_market_feature(self, event: Any) -> tuple[Any, ...]:
        feature = self._handle_event(event)
        if not isinstance(feature, FixedTimeTradeBar):
            return ()
        if (
            self._last_evaluated_open_time_ms is not None
            and feature.open_time_ms <= self._last_evaluated_open_time_ms
        ):
            return ()
        self._last_evaluated_open_time_ms = feature.open_time_ms
        return self._evaluate(feature)

    def on_kline(self, *args: Any, **kwargs: Any) -> tuple:
        return ()

    def on_trade(self, *args: Any, **kwargs: Any) -> tuple:
        return ()

    def _evaluate(self, bar: FixedTimeTradeBar) -> tuple[Any, ...]:
        self._log_readiness_transition()
        if self._buffer is None or self._sleeve is None:
            self.last_mf_signal_audit = {
                "enabled": self.config.enabled,
                "data_ready": False,
                "blocked_reason": "data_not_ready",
                "reason": "data_not_ready",
                "missing_features": ["mf_buffer", "mf_sleeve"],
            }
            return ()
        freshness_required = bool(
            self._readiness.get("live_freshness_required", False)
        )
        freshness_max_age_ms = max(
            0,
            int(
                self._readiness.get(
                    "live_freshness_max_age_ms",
                    300_000,
                )
            ),
        )
        event_available_ms = max(
            int(bar.available_time_ms),
            int(self._next_open_time_ms or 0),
        )
        event_age_ms = max(
            0,
            int(time.time() * 1000) - event_available_ms,
        )
        if freshness_required and event_age_ms > freshness_max_age_ms:
            self.last_mf_signal_audit = {
                "enabled": self.config.enabled,
                "data_ready": False,
                "signal_feature_ready": bool(
                    self._readiness.get(
                        "mf_signal_feature_ready", False
                    )
                ),
                "blocked_reason": "live_feature_stale",
                "reason": "live_feature_stale",
                "live_fresh_ready": False,
                "live_feature_age_ms": event_age_ms,
                "live_freshness_max_age_ms": freshness_max_age_ms,
                "signal_time_ms": event_available_ms,
                "readiness_source": self._readiness.get(
                    "source", "unavailable"
                ),
            }
            return ()
        decision, audit = evaluate_mf_low_sweep(
            config=self.config,
            bars=self._buffer.recent_bars(),
            range_footprints=self._buffer.range_footprints(),
            large_share_history=self._buffer.large_trade_share_history(
                before_open_time_ms=bar.open_time_ms
            ),
            readiness=self._readiness,
            sleeve=self._sleeve,
            next_open_price=self._next_open_price,
            next_open_time_ms=self._next_open_time_ms,
        )
        audit["live_fresh_ready"] = True
        audit["live_feature_age_ms"] = event_age_ms
        audit["live_freshness_max_age_ms"] = freshness_max_age_ms
        self.last_mf_signal_audit = _json_safe(audit)
        if audit.get("blocked_reason") == "invalid_causal_feature":
            signal_time_ms = int(audit.get("signal_time_ms") or 0)
            if signal_time_ms - self._last_causal_warning_ms >= 300_000:
                self._last_causal_warning_ms = signal_time_ms
                logger.info(
                    "MF blocked by invalid causal feature | signal_time_ms=%s "
                    "tradebar_available_time_ms=%s "
                    "range_available_time_ms=%s",
                    signal_time_ms,
                    audit.get("used_tradebar_available_time_ms"),
                    audit.get("used_range_footprint_available_time_ms"),
                )
        if decision is None or self._signal_mapper is None:
            return ()
        if decision.decision_type == "open":
            sizing = (
                self._sizing_provider()
                if self._sizing_provider is not None
                else MfSizingInput(None, None)
            )
            self.last_mf_signal_audit["sizing_input"] = {
                "equity": (
                    None if sizing.equity is None else str(sizing.equity)
                ),
                "available_equity": (
                    None
                    if sizing.available_equity is None
                    else str(sizing.available_equity)
                ),
                "sizing_equity_by_exchange": _string_decimal_mapping(
                    sizing.equity_by_exchange
                ),
                "available_equity_by_exchange": _string_decimal_mapping(
                    sizing.available_equity_by_exchange
                ),
                "leverage_by_exchange": _string_decimal_mapping(
                    sizing.leverage_by_exchange
                ),
                "margin_mode_by_exchange": dict(
                    sizing.margin_mode_by_exchange
                ),
                "margin_fraction": str(self.config.margin_fraction),
                "available_margin_buffer": str(
                    self.config.available_margin_buffer
                ),
            }
            signal = self._signal_mapper.map_open(
                decision, sizing=sizing
            )
            if signal is None:
                self.last_mf_signal_audit["entry_signal"] = False
                self.last_mf_signal_audit["blocked_reason"] = (
                    "sizing_not_ready"
                )
                self.last_mf_signal_audit["reason"] = "sizing_not_ready"
                return ()
            exchange_quantities = _exchange_quantities_from_signal(
                signal.metadata
            )
            self.last_mf_signal_audit["sizing_input"] = dict(
                signal.metadata.get("sizing_input", {})
            )
            self.last_mf_signal_audit["target_exchanges"] = list(
                signal.metadata.get("target_exchanges", ())
            )
            self.last_mf_signal_audit["exchange_quantities_base"] = dict(
                signal.metadata.get("exchange_quantities_base", {})
            )
            self._sleeve.reserve_open(
                position_id=decision.position_id,
                quantity=signal.quantity or Decimal("0"),
                signal_time_ms=decision.signal_time_ms,
                entry_execution_time_ms=decision.entry_execution_time_ms,
                tradebar_open_time_ms=int(
                    decision.audit.get("entry_tradebar_open_time_ms")
                    or bar.open_time_ms + 60_000
                ),
                exchange_quantities=exchange_quantities,
            )
            self.last_mf_signal_audit["sleeve_state"] = (
                self._sleeve.state_label
            )
            logger.info(
                "MF entry signal generated | position_id=%s "
                "signal_time_ms=%s quantity=%s",
                decision.position_id,
                decision.signal_time_ms,
                signal.quantity,
            )
            return (signal,)
        signal = self._signal_mapper.map_close(
            decision, sleeve=self._sleeve
        )
        if signal is None:
            return ()
        self._sleeve.reserve_close()
        self.last_mf_signal_audit["sleeve_state"] = (
            self._sleeve.state_label
        )
        logger.info(
            "MF exit signal generated | position_id=%s "
            "signal_time_ms=%s reason=mf_time48_exit",
            decision.position_id,
            decision.signal_time_ms,
        )
        return (signal,)

    def _handle_event(
        self, event: Any
    ) -> FixedTimeTradeBar | TradeFootprintFeature | RangeFootprintFeature | None:
        event_type = getattr(event, "event_type", None)
        if event_type is None:
            return None
        type_value = (
            event_type.value
            if hasattr(event_type, "value")
            else str(event_type)
        )
        if type_value == MarketFeatureEventType.FIXED_TIME_TRADE_BAR.value:
            bar = self._event_to_tradebar(event)
            if bar is None:
                return None
            data = getattr(event, "data", {})
            if isinstance(data, Mapping):
                raw_price = data.get("next_open_price")
                raw_time = data.get("next_open_time_ms")
                try:
                    self._next_open_price = (
                        None
                        if raw_price is None
                        else Decimal(str(raw_price))
                    )
                    self._next_open_time_ms = (
                        None
                        if raw_time is None
                        else int(raw_time)
                    )
                except (ArithmeticError, TypeError, ValueError):
                    self._next_open_price = None
                    self._next_open_time_ms = None
            self._tradebar_count += 1
            self._last_tradebar_ms = int(
                getattr(event, "event_time_ms", 0)
            )
            self._latest_tradebar_open_time_ms = bar.open_time_ms
            if self._buffer is not None:
                self._buffer.append_tradebar(bar)
            return bar
        if type_value == MarketFeatureEventType.TRADE_FOOTPRINT_FEATURE.value:
            feature = self._event_to_footprint(event)
            if feature is None:
                return None
            self._footprint_count += 1
            self._last_footprint_ms = int(
                getattr(event, "event_time_ms", 0)
            )
            self._latest_footprint_open_time_ms = feature.open_time_ms
            self._latest_footprint_audit = {
                "open_time_ms": feature.open_time_ms,
                "close_time_ms": feature.close_time_ms,
                "available_time_ms": feature.available_time_ms,
                "fp_max_bucket_abs_delta_pressure": str(
                    feature.fp_max_bucket_abs_delta_pressure
                ),
                "context_available": feature.context_available,
                "quality": feature.quality,
            }
            return feature
        if type_value == MF_RANGE_FOOTPRINT_EVENT_TYPE:
            feature = self._event_to_range_footprint(event)
            if feature is None:
                return None
            self._range_footprint_count += 1
            self._last_range_footprint_ms = int(
                getattr(event, "event_time_ms", 0)
            )
            if self._buffer is not None:
                self._buffer.append_range_footprint(feature)
            return feature
        if type_value == MF_READINESS_EVENT_TYPE:
            data = getattr(event, "data", {})
            if not isinstance(data, Mapping):
                return None
            self.set_readiness(
                data,
                source=str(
                    data.get("source", "runtime_readiness_event")
                ),
            )
            return None
        return None

    def _log_readiness_transition(self) -> None:
        gates = mf_readiness_gates(self._readiness)
        state = tuple(gates.values())
        if state == self._last_readiness_state:
            return
        self._last_readiness_state = state
        missing = [
            name
            for name, ok in gates.items()
            if not ok
        ]
        logger.info(
            "MF data readiness changed | signal_feature_ready=%s "
            "tradebar_ready=%s large_share_samples_ready=%s "
            "range_footprint_context_ready=%s source=%s "
            "missing_gates=%s",
            *state,
            self._readiness.get("source", "unavailable"),
            missing,
        )

    @staticmethod
    def _event_to_tradebar(event: Any) -> FixedTimeTradeBar | None:
        data = getattr(event, "data", {})
        if not isinstance(data, Mapping) or not data:
            return None
        try:
            exchange = _exchange_value(getattr(event, "exchange", ""))
            return FixedTimeTradeBar(
                exchange=exchange,
                symbol=str(getattr(event, "symbol", "")),
                timeframe=str(getattr(event, "timeframe", "1m")),
                open_time_ms=int(data.get("open_time_ms", 0)),
                close_time_ms=int(data.get("close_time_ms", 0)),
                available_time_ms=int(
                    data.get(
                        "available_time_ms",
                        getattr(event, "effective_available_time_ms", 0),
                    )
                ),
                open=Decimal(str(data.get("open", "0"))),
                high=Decimal(str(data.get("high", "0"))),
                low=Decimal(str(data.get("low", "0"))),
                close=Decimal(str(data.get("close", "0"))),
                volume=Decimal(str(data.get("volume", "0"))),
                buy_volume=Decimal(str(data.get("buy_volume", "0"))),
                sell_volume=Decimal(str(data.get("sell_volume", "0"))),
                buy_notional=Decimal(str(data.get("buy_notional", "0"))),
                sell_notional=Decimal(str(data.get("sell_notional", "0"))),
                delta_volume=Decimal(str(data.get("delta_volume", "0"))),
                delta_notional=Decimal(
                    str(data.get("delta_notional", "0"))
                ),
                abs_delta_notional=Decimal(
                    str(data.get("abs_delta_notional", "0"))
                ),
                trade_count=int(data.get("trade_count", 0)),
                large_buy_notional=Decimal(
                    str(data.get("large_buy_notional", "0"))
                ),
                large_sell_notional=Decimal(
                    str(data.get("large_sell_notional", "0"))
                ),
                large_trade_count=int(
                    data.get("large_trade_count", 0)
                ),
                large_trade_share=Decimal(
                    str(data.get("large_trade_share", "0"))
                ),
                quality=str(data.get("quality", "COMPLETE")),
                source=str(data.get("source", "trade_derived")),
            )
        except (ArithmeticError, TypeError, ValueError):
            return None

    @staticmethod
    def _event_to_footprint(
        event: Any,
    ) -> TradeFootprintFeature | None:
        data = getattr(event, "data", {})
        if not isinstance(data, Mapping) or not data:
            return None
        try:
            return TradeFootprintFeature(
                exchange=_exchange_value(getattr(event, "exchange", "")),
                symbol=str(getattr(event, "symbol", "")),
                timeframe=str(getattr(event, "timeframe", "1m")),
                open_time_ms=int(data.get("open_time_ms", 0)),
                close_time_ms=int(data.get("close_time_ms", 0)),
                available_time_ms=int(
                    data.get(
                        "available_time_ms",
                        getattr(event, "effective_available_time_ms", 0),
                    )
                ),
                delta_notional=Decimal(
                    str(data.get("delta_notional", "0"))
                ),
                abs_delta_notional=Decimal(
                    str(data.get("abs_delta_notional", "0"))
                ),
                taker_buy_ratio=Decimal(
                    str(data.get("taker_buy_ratio", "0"))
                ),
                close_pos=Decimal(str(data.get("close_pos", "0"))),
                range_pct=Decimal(str(data.get("range_pct", "0"))),
                return_pct=Decimal(str(data.get("return_pct", "0"))),
                fp_max_bucket_abs_delta_pressure=Decimal(
                    str(
                        data.get(
                            "fp_max_bucket_abs_delta_pressure", "0"
                        )
                    )
                ),
                context_available=bool(
                    data.get("context_available", False)
                ),
                quality=str(data.get("quality", "INCOMPLETE")),
                source=str(data.get("source", "trade_derived")),
            )
        except (ArithmeticError, TypeError, ValueError):
            return None

    @staticmethod
    def _event_to_range_footprint(
        event: Any,
    ) -> RangeFootprintFeature | None:
        data = getattr(event, "data", {})
        if not isinstance(data, Mapping) or not data:
            return None
        try:
            return RangeFootprintFeature(
                exchange=_exchange_value(getattr(event, "exchange", "")),
                symbol=str(getattr(event, "symbol", "")),
                range_pct=Decimal(str(data.get("range_pct", "0"))),
                price_step=Decimal(str(data.get("price_step", "0"))),
                range_bar_id=int(data.get("range_bar_id", 0)),
                range_start_ms=int(data.get("range_start_ms", 0)),
                range_end_ms=int(data.get("range_end_ms", 0)),
                available_time_ms=int(
                    data.get(
                        "available_time_ms",
                        getattr(event, "effective_available_time_ms", 0),
                    )
                ),
                fp_max_bucket_abs_delta_pressure=Decimal(
                    str(
                        data.get(
                            "fp_max_bucket_abs_delta_pressure", "0"
                        )
                    )
                ),
                fp_low_bucket_delta_pressure=Decimal(
                    str(data.get("fp_low_bucket_delta_pressure", "0"))
                ),
                fp_high_bucket_delta_pressure=Decimal(
                    str(data.get("fp_high_bucket_delta_pressure", "0"))
                ),
                fp_delta_pressure=Decimal(
                    str(data.get("fp_delta_pressure", "0"))
                ),
                bucket_count=int(data.get("bucket_count", 0)),
                trade_count=int(data.get("trade_count", 0)),
                context_available=bool(
                    data.get("context_available", False)
                ),
                quality=str(data.get("quality", "INCOMPLETE")),
                source=str(
                    data.get(
                        "source",
                        "trade_derived_range_footprint",
                    )
                ),
            )
        except (ArithmeticError, TypeError, ValueError):
            return None

    def audit(self) -> Mapping[str, Any]:
        minute_mismatch = bool(
            self._latest_tradebar_open_time_ms is not None
            and self._latest_footprint_open_time_ms is not None
            and self._latest_tradebar_open_time_ms
            != self._latest_footprint_open_time_ms
        )
        return {
            "tradebar_count": self._tradebar_count,
            "footprint_count": self._footprint_count,
            "range_footprint_count": self._range_footprint_count,
            "last_tradebar_ms": self._last_tradebar_ms,
            "last_footprint_ms": self._last_footprint_ms,
            "last_range_footprint_ms": self._last_range_footprint_ms,
            "latest_tradebar_open_time_ms": (
                self._latest_tradebar_open_time_ms
            ),
            "latest_footprint_open_time_ms": (
                self._latest_footprint_open_time_ms
            ),
            "latest_footprint": self._latest_footprint_audit,
            "minute_mismatch": minute_mismatch,
            "readiness": dict(self._readiness),
            "last_mf_signal_audit": dict(self.last_mf_signal_audit),
            "buffer": self._buffer.last_audit() if self._buffer else None,
        }


def _exchange_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _quantile(values: Sequence[Decimal], q: Decimal) -> Decimal:
    ordered = sorted(values)
    position = Decimal(len(ordered) - 1) * q
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    weight = position - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _complete_contiguous_tradebars(
    bars: Sequence[FixedTimeTradeBar],
) -> int:
    count = 0
    previous_open: int | None = None
    for bar in reversed(bars):
        if previous_open is not None and (
            previous_open - int(bar.open_time_ms) != 60_000
        ):
            break
        if str(bar.quality).upper() != TradeFeatureQuality.COMPLETE.value:
            break
        if any(
            value is None
            for value in (
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.large_trade_share,
                bar.available_time_ms,
            )
        ):
            break
        previous_open = int(bar.open_time_ms)
        count += 1
    return count


def _json_safe(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, Decimal):
            result[str(key)] = str(item)
        elif isinstance(item, Mapping):
            result[str(key)] = _json_safe(item)
        elif isinstance(item, (list, tuple)):
            result[str(key)] = [
                str(entry) if isinstance(entry, Decimal) else entry
                for entry in item
            ]
        else:
            result[str(key)] = item
    return result


def _string_decimal_mapping(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {
        str(exchange): str(value)
        for exchange, value in values.items()
        if value is not None
    }


def _exchange_quantities_from_signal(
    metadata: Mapping[str, Any],
) -> dict[str, Decimal]:
    raw = metadata.get("exchange_quantities_base")
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, Decimal] = {}
    for key, value in raw.items():
        exchange = str(key).strip().lower()
        if not exchange:
            continue
        try:
            quantity = Decimal(str(value))
        except Exception:
            continue
        if quantity > 0:
            out[exchange] = quantity
    return out


__all__ = [
    "MfDataBuffer",
    "MfDataReadiness",
    "MfFeatureObserver",
]
