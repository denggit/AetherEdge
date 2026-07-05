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
    TimeRange,
    TradeFootprintFeature,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import resolve_mf_readiness
from strategies.eth_portfolio_v1.domain.mf_low_sweep import (
    evaluate_mf_low_sweep,
)
from strategies.eth_portfolio_v1.domain.mf_signal import (
    MF_RANGE_FOOTPRINT_EVENT_TYPE,
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
        self._range_pct = Decimal(str(range_pct))
        self._range_price_step = Decimal(str(range_price_step))
        self._bars: deque[FixedTimeTradeBar] = deque(
            maxlen=self._decision_maxlen
        )
        self._large_trade_shares: deque[tuple[int, Decimal]] = deque(
            maxlen=self._large_share_window_days * 1_440
        )
        self._range_footprints: deque[RangeFootprintFeature] = deque(
            maxlen=max(1, int(range_footprint_max_features))
        )
        self._latest_history_open_time_ms: int | None = None
        self._loaded_initial = False
        self._last_audit_ms = 0

    def load_initial(self) -> int:
        history_limit = max(
            self._decision_default_minutes,
            self._large_share_window_days * 1_440,
        )
        history = self._store.load_recent_tradebars(
            symbol=self.symbol,
            exchange=self.exchange,
            limit=history_limit,
        )
        for bar in history:
            self._append_large_share(bar)
        for bar in history[-self._decision_default_minutes :]:
            self._bars.append(bar)

        latest_range_ms = (
            self._store.latest_any_range_footprint_available_time_ms(
                symbol=self.symbol,
                exchange=self.exchange,
                range_pct=self._range_pct,
                price_step=self._range_price_step,
            )
        )
        if latest_range_ms is not None:
            for feature in self._store.load_range_footprint_features(
                symbol=self.symbol,
                exchange=self.exchange,
                range_pct=self._range_pct,
                price_step=self._range_price_step,
                time_range=TimeRange(latest_range_ms, latest_range_ms),
            ):
                self.append_range_footprint(feature)
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
        return tuple(
            value
            for open_time_ms, value in self._large_trade_shares
            if (
                before_open_time_ms is None
                or open_time_ms < before_open_time_ms
            )
        )

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
            "large_share_samples": len(self._large_trade_shares),
            "range_footprint_count": len(self._range_footprints),
            "loaded_initial": self._loaded_initial,
            "latest_bar": latest,
            "latest_range_footprint": latest_range,
            "last_audit_ms": self._last_audit_ms,
        }

    def last_audit(self) -> Mapping[str, Any]:
        return self.audit()

    def _append_large_share(self, bar: FixedTimeTradeBar) -> None:
        if (
            self._latest_history_open_time_ms is not None
            and bar.open_time_ms <= self._latest_history_open_time_ms
        ):
            return
        self._large_trade_shares.append(
            (bar.open_time_ms, bar.large_trade_share)
        )
        self._latest_history_open_time_ms = bar.open_time_ms


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
    ) -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._store = SqliteTradeFeatureStore(path=store_path)
        self._required_minutes = required_minutes
        self._worker_status_path = worker_status_path
        self._global_lock_path = global_lock_path
        self._range_pct = range_pct
        self._price_step = price_step
        self._last: dict[str, Any] | None = None

    def readiness(self) -> Mapping[str, Any]:
        result = resolve_mf_readiness(
            symbol=self.symbol,
            exchange=self.exchange,
            store=self._store,
            required_minutes=self._required_minutes,
            worker_status_path=self._worker_status_path,
            global_lock_path=self._global_lock_path,
            range_pct=self._range_pct,
            price_step=self._price_step,
        )
        audit = dict(result.audit())
        signal_ready = all(
            bool(audit.get(field, False))
            for field in (
                "mf_signal_feature_ready",
                "range_footprint_ready",
                "tradebar_ready",
            )
        )
        self._last = {
            **audit,
            "mf_signal_ready": signal_ready,
            "exit_variant": "time48",
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
        self._last_readiness_state: tuple[bool, bool, bool] | None = None
        self._last_tradebar_ms = 0
        self._last_footprint_ms = 0
        self._last_range_footprint_ms = 0
        self._tradebar_count = 0
        self._footprint_count = 0
        self._range_footprint_count = 0
        self._latest_tradebar_open_time_ms: int | None = None
        self._latest_footprint_open_time_ms: int | None = None
        self._latest_footprint_audit: dict[str, Any] | None = None
        self._last_evaluated_open_time_ms: int | None = None
        self._last_causal_warning_ms = 0
        self.last_mf_signal_audit: dict[str, Any] = {
            "enabled": self.config.enabled,
            "data_ready": False,
            "blocked_reason": "data_not_ready",
            "reason": "data_not_ready",
        }

    def set_readiness(self, readiness: Mapping[str, Any]) -> None:
        self._readiness = dict(readiness)

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
        decision, audit = evaluate_mf_low_sweep(
            config=self.config,
            bars=self._buffer.recent_bars(),
            range_footprints=self._buffer.range_footprints(),
            large_share_history=self._buffer.large_trade_share_history(
                before_open_time_ms=bar.open_time_ms
            ),
            readiness=self._readiness,
            sleeve=self._sleeve,
        )
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
            self._sleeve.reserve_open(
                position_id=decision.position_id,
                quantity=signal.quantity or Decimal("0"),
                signal_time_ms=decision.signal_time_ms,
                entry_execution_time_ms=decision.entry_execution_time_ms,
                tradebar_open_time_ms=bar.open_time_ms,
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
        return None

    def _log_readiness_transition(self) -> None:
        state = (
            bool(self._readiness.get("mf_signal_feature_ready", False)),
            bool(self._readiness.get("range_footprint_ready", False)),
            bool(self._readiness.get("tradebar_ready", False)),
        )
        if state == self._last_readiness_state:
            return
        self._last_readiness_state = state
        logger.info(
            "MF data readiness changed | signal_feature_ready=%s "
            "range_footprint_ready=%s tradebar_ready=%s",
            *state,
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


__all__ = [
    "MfDataBuffer",
    "MfDataReadiness",
    "MfFeatureObserver",
]
