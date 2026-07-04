from __future__ import annotations

import time
from collections import deque
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.market_data.models import FixedTimeTradeBar
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import resolve_mf_readiness


class MfDataBuffer:
    """Bounded in-memory buffer for MF trade-derived 1m features.

    Decision buffer (default 3 days = 4320 minutes) holds complete
    FixedTimeTradeBar objects. Long-history rolling scalars (e.g.
    large_trade_share) are tracked separately as lightweight values
    to avoid keeping full bar objects for 90 days.

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
        decision_buffer_minutes: int = 4320,  # 3 days
        decision_buffer_max_minutes: int = 10080,  # 7 days
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

        # Bounded deque for decision buffer
        self._bars: deque[FixedTimeTradeBar] = deque(maxlen=self._decision_maxlen)

        # Rolling scalars: only keep lightweight values
        self._large_trade_shares: deque[float] = deque(
            maxlen=self._large_share_window_days * 1440
        )

        self._loaded_initial = False
        self._last_audit_ms: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def load_initial(self) -> int:
        """Load recent bars from SQLite into the bounded buffer."""
        bars = self._store.load_recent(
            symbol=self.symbol,
            exchange=self.exchange,
            limit=self._decision_default_minutes,
        )
        for bar in bars:
            self._bars.append(bar)
            if float(bar.large_trade_share) > 0:
                self._large_trade_shares.append(float(bar.large_trade_share))

        self._loaded_initial = True
        return len(bars)

    def append(self, bar: FixedTimeTradeBar) -> None:
        """Append a new closed 1m bar to the buffer."""
        self._bars.append(bar)
        share_val = float(bar.large_trade_share)
        if share_val > 0:
            self._large_trade_shares.append(share_val)

    def append_many(self, bars: Sequence[FixedTimeTradeBar]) -> None:
        for bar in bars:
            self.append(bar)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def recent_bars(self, n_minutes: int | None = None) -> tuple[FixedTimeTradeBar, ...]:
        """Return the most recent N bars from the buffer."""
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
        """Median large_trade_share over rolling window."""
        if not self._large_trade_shares:
            return 0.0
        sorted_shares = sorted(self._large_trade_shares)
        n = len(sorted_shares)
        if n % 2 == 0:
            return (sorted_shares[n // 2 - 1] + sorted_shares[n // 2]) / 2.0
        return sorted_shares[n // 2]

    def large_trade_share_quantile(self, q: float = 0.75) -> float:
        """Quantile of large_trade_share over rolling window."""
        if not self._large_trade_shares:
            return 0.0
        sorted_shares = sorted(self._large_trade_shares)
        idx = max(0, min(len(sorted_shares) - 1, int(len(sorted_shares) * q)))
        return sorted_shares[idx]

    # ------------------------------------------------------------------
    # Audit
    # ------------------------------------------------------------------

    def audit(self) -> Mapping[str, Any]:
        """JSON-safe audit of buffer state."""
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
        """Return the most recent audit result."""
        return self.audit()


class MfDataReadiness:
    """Readiness gate for MF trade-derived feature pipeline.

    In R007, mf_signal_ready is ALWAYS False. This gate provides
    detailed readiness info for monitoring and audit purposes.
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
        """Return full MF readiness including detailed coverage info."""
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


class MfFeatureObserver:
    """MF observer placeholder that always returns empty signals.

    In R007, this observer exists only to establish the interface boundary.
    It never generates TradeSignals.
    """

    def on_market_feature(self, event: Any) -> tuple[Any, ...]:
        """Returns empty tuple — no MF signals in R007."""
        return ()

    def on_kline(self, *args: Any, **kwargs: Any) -> tuple:
        return ()

    def on_trade(self, *args: Any, **kwargs: Any) -> tuple:
        return ()
