from __future__ import annotations

import time
from collections import deque
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.market_data.models import FixedTimeTradeBar, TradeFootprintFeature
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import resolve_mf_readiness


# ---------------------------------------------------------------------------
# Bounded buffer
# ---------------------------------------------------------------------------

class MfDataBuffer:
    """Bounded in-memory buffer for MF trade-derived 1m features.

    Decision buffer (default 3 days = 4320 minutes) holds complete
    FixedTimeTradeBar objects. Long-history rolling scalars (e.g.
    large_trade_share) are tracked separately as lightweight values.

    Constraints:
    - Never loads raw trades
    - Never loads full 90-day bars into memory
    - Signal evaluation reads buffer/scalars only, no DB scans
    - Auto-evicts oldest entries when full
    """

    def __init__(
        self,
        *,
        symbol: str,
        exchange: str = "okx",
        store_path: str = "data/market_data/aether_market_data.sqlite3",
        decision_buffer_minutes: int = 4320,
        decision_buffer_max_minutes: int = 10080,
        large_share_quantile_window_days: int = 90,
    ) -> None:
        if decision_buffer_minutes > decision_buffer_max_minutes:
            raise ValueError("decision_buffer_minutes must be <= decision_buffer_max_minutes")

        self.symbol = symbol
        self.exchange = exchange
        self._store = SqliteTradeFeatureStore(path=store_path)
        self._decision_maxlen = max(1, int(decision_buffer_max_minutes))
        self._decision_default_minutes = max(1, int(decision_buffer_minutes))
        self._large_share_window_days = max(1, int(large_share_quantile_window_days))

        self._bars: deque[FixedTimeTradeBar] = deque(maxlen=self._decision_maxlen)
        self._large_trade_shares: deque[float] = deque(
            maxlen=self._large_share_window_days * 1440
        )

        self._loaded_initial = False
        self._last_audit_ms: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def load_initial(self) -> int:
        bars = self._store.load_recent_tradebars(
            symbol=self.symbol,
            exchange=self.exchange,
            limit=self._decision_default_minutes,
        )
        for bar in bars:
            self._bars.append(bar)
            share_val = float(bar.large_trade_share)
            if share_val > 0:
                self._large_trade_shares.append(share_val)
        self._loaded_initial = True
        return len(bars)

    def append_tradebar(self, bar: FixedTimeTradeBar) -> None:
        self._bars.append(bar)
        share_val = float(bar.large_trade_share)
        if share_val > 0:
            self._large_trade_shares.append(share_val)

    def append_many(self, bars: Sequence[FixedTimeTradeBar]) -> None:
        for bar in bars:
            self.append_tradebar(bar)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def recent_bars(self, n_minutes: int | None = None) -> tuple[FixedTimeTradeBar, ...]:
        if n_minutes is None:
            n_minutes = self._decision_default_minutes
        n = min(max(1, int(n_minutes)), len(self._bars))
        items = list(self._bars)
        return tuple(items[-n:])

    @property
    def bar_count(self) -> int:
        return len(self._bars)

    @property
    def loaded(self) -> bool:
        return self._loaded_initial

    # ------------------------------------------------------------------
    # Rolling scalars
    # ------------------------------------------------------------------

    def large_trade_share_median(self) -> float:
        if not self._large_trade_shares:
            return 0.0
        sorted_shares = sorted(self._large_trade_shares)
        n = len(sorted_shares)
        if n % 2 == 0:
            return (sorted_shares[n // 2 - 1] + sorted_shares[n // 2]) / 2.0
        return sorted_shares[n // 2]

    def large_trade_share_quantile(self, q: float = 0.75) -> float:
        if not self._large_trade_shares:
            return 0.0
        sorted_shares = sorted(self._large_trade_shares)
        idx = max(0, min(len(sorted_shares) - 1, int(len(sorted_shares) * q)))
        return sorted_shares[idx]

    # ------------------------------------------------------------------
    # Audit
    # ------------------------------------------------------------------

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
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "bar_count": len(self._bars),
            "decision_buffer_maxlen": self._decision_maxlen,
            "large_share_samples": len(self._large_trade_shares),
            "loaded_initial": self._loaded_initial,
            "latest_bar": latest,
            "last_audit_ms": self._last_audit_ms,
        }

    def last_audit(self) -> Mapping[str, Any]:
        return self.audit()


# ---------------------------------------------------------------------------
# Readiness gate
# ---------------------------------------------------------------------------

class MfDataReadiness:
    """Readiness gate for MF trade-derived feature pipeline.

    In R007, mf_signal_ready is ALWAYS False.
    """

    def __init__(
        self,
        *,
        symbol: str,
        exchange: str = "okx",
        store_path: str = "data/market_data/aether_market_data.sqlite3",
        required_minutes: int = 4320,
        worker_status_path: str | None = None,
        global_lock_path: str | None = None,
    ) -> None:
        self.symbol = symbol
        self.exchange = exchange
        self._store = SqliteTradeFeatureStore(path=store_path)
        self._required_minutes = required_minutes
        self._worker_status_path = worker_status_path
        self._global_lock_path = global_lock_path

    def readiness(self) -> Mapping[str, Any]:
        result = resolve_mf_readiness(
            symbol=self.symbol,
            exchange=self.exchange,
            store=self._store,
            required_minutes=self._required_minutes,
            worker_status_path=self._worker_status_path,
            global_lock_path=self._global_lock_path,
        )
        return dict(result.audit())

    @property
    def mf_signal_ready(self) -> bool:
        """R007 guarantee: MF signal readiness is ALWAYS False."""
        return False


# ---------------------------------------------------------------------------
# Feature observer — receives features, buffers them, returns 0 signals
# ---------------------------------------------------------------------------

class MfFeatureObserver:
    """MF observer that receives trade-derived features but always returns 0 signals.

    In R007 this observer:
    - Receives FIXED_TIME_TRADE_BAR events and buffers them
    - Receives TRADE_FOOTPRINT_FEATURE events and tracks them via audit
    - Always returns empty tuple — never generates TradeSignal
    """

    def __init__(self, buffer: MfDataBuffer | None = None) -> None:
        self._buffer = buffer
        self._last_tradebar_ms: int = 0
        self._last_footprint_ms: int = 0
        self._tradebar_count: int = 0
        self._footprint_count: int = 0
        self._latest_tradebar_open_time_ms: int | None = None
        self._latest_footprint_open_time_ms: int | None = None
        self._latest_footprint_audit: dict[str, Any] | None = None

    def on_market_feature(self, event: Any) -> tuple[Any, ...]:
        """Process market feature events, buffer data, return empty signals."""
        self._handle_event(event)
        return ()

    def on_kline(self, *args: Any, **kwargs: Any) -> tuple:
        return ()

    def on_trade(self, *args: Any, **kwargs: Any) -> tuple:
        return ()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _handle_event(self, event: Any) -> None:
        """Internal dispatch: try to buffer trade-derived features."""
        event_type = getattr(event, "event_type", None)
        if event_type is None:
            return

        type_val = event_type.value if hasattr(event_type, "value") else str(event_type)
        if type_val == MarketFeatureEventType.FIXED_TIME_TRADE_BAR.value:
            self._tradebar_count += 1
            self._last_tradebar_ms = getattr(event, "event_time_ms", 0)
            if self._buffer is not None:
                try:
                    bar = self._event_to_tradebar(event)
                    if bar is not None:
                        self._latest_tradebar_open_time_ms = bar.open_time_ms
                        self._buffer.append_tradebar(bar)
                except Exception:
                    pass
            else:
                bar = self._event_to_tradebar(event)
                if bar is not None:
                    self._latest_tradebar_open_time_ms = bar.open_time_ms
        elif type_val == MarketFeatureEventType.TRADE_FOOTPRINT_FEATURE.value:
            self._footprint_count += 1
            self._last_footprint_ms = getattr(event, "event_time_ms", 0)
            data = getattr(event, "data", {})
            if isinstance(data, Mapping):
                raw_open = data.get("open_time_ms")
                if raw_open is not None:
                    try:
                        self._latest_footprint_open_time_ms = int(raw_open)
                    except (TypeError, ValueError):
                        self._latest_footprint_open_time_ms = None
                self._latest_footprint_audit = {
                    "open_time_ms": self._latest_footprint_open_time_ms,
                    "close_time_ms": data.get("close_time_ms"),
                    "fp_max_bucket_abs_delta_pressure": data.get(
                        "fp_max_bucket_abs_delta_pressure"
                    ),
                    "context_available": data.get("context_available"),
                    "quality": data.get("quality"),
                }

    @staticmethod
    def _event_to_tradebar(event: Any) -> FixedTimeTradeBar | None:
        """Convert a FIXED_TIME_TRADE_BAR event to FixedTimeTradeBar."""
        data = getattr(event, "data", {})
        if not data:
            return None
        try:
            raw_exchange = getattr(event, "exchange", "")
            exchange = (
                raw_exchange.value
                if hasattr(raw_exchange, "value")
                else str(raw_exchange)
            )
            return FixedTimeTradeBar(
                exchange=exchange,
                symbol=str(getattr(event, "symbol", "")),
                timeframe=str(getattr(event, "timeframe", "1m")),
                open_time_ms=int(data.get("open_time_ms", 0)),
                close_time_ms=int(data.get("close_time_ms", 0)),
                available_time_ms=data.get("available_time_ms", data.get("close_time_ms", 0)),
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
                delta_notional=Decimal(str(data.get("delta_notional", "0"))),
                abs_delta_notional=Decimal(str(data.get("abs_delta_notional", "0"))),
                trade_count=int(data.get("trade_count", 0)),
                large_buy_notional=Decimal(str(data.get("large_buy_notional", "0"))),
                large_sell_notional=Decimal(str(data.get("large_sell_notional", "0"))),
                large_trade_count=int(data.get("large_trade_count", 0)),
                large_trade_share=Decimal(str(data.get("large_trade_share", "0"))),
                quality=str(data.get("quality", "COMPLETE")),
                source=str(data.get("source", "trade_derived")),
            )
        except Exception:
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
            "last_tradebar_ms": self._last_tradebar_ms,
            "last_footprint_ms": self._last_footprint_ms,
            "latest_tradebar_open_time_ms": self._latest_tradebar_open_time_ms,
            "latest_footprint_open_time_ms": self._latest_footprint_open_time_ms,
            "latest_footprint": self._latest_footprint_audit,
            "minute_mismatch": minute_mismatch,
            "buffer": self._buffer.last_audit() if self._buffer else None,
        }
