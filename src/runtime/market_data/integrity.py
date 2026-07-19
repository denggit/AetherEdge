from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class TradeIntegrityIssue:
    revision: int
    event_time_ms: int
    reason: str


class TradeDataIntegrityTracker:
    """Bounded, window-aware record of gaps in the normalized Trade stream."""

    def __init__(self, *, max_issues: int = 4_096) -> None:
        if max_issues <= 0:
            raise ValueError("max_issues must be positive")
        self._issues: deque[TradeIntegrityIssue] = deque(maxlen=max_issues)
        self._revision = 0
        self._dropped_count = 0

    def mark_dropped(self, event_time_ms: int, reason: str) -> None:
        normalized_reason = str(reason).strip() or "trade_data_incomplete"
        self._revision += 1
        self._dropped_count += 1
        self._issues.append(
            TradeIntegrityIssue(
                revision=self._revision,
                event_time_ms=int(event_time_ms),
                reason=normalized_reason,
            )
        )

    def is_complete(self, window_start_ms: int, window_end_ms: int) -> bool:
        return self.invalid_reason(window_start_ms, window_end_ms) is None

    def invalid_reason(
        self,
        window_start_ms: int,
        window_end_ms: int,
    ) -> str | None:
        start = int(window_start_ms)
        end = int(window_end_ms)
        if end < start:
            raise ValueError("window end must not precede window start")
        matches = tuple(
            issue
            for issue in self._issues
            if start <= issue.event_time_ms <= end
        )
        if not matches:
            return None
        reasons = ",".join(dict.fromkeys(issue.reason for issue in matches))
        return f"{reasons};dropped_count={len(matches)}"

    def issues_since(self, revision: int) -> tuple[TradeIntegrityIssue, ...]:
        return tuple(issue for issue in self._issues if issue.revision > revision)

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def dropped_count(self) -> int:
        return self._dropped_count


@dataclass(frozen=True)
class OrderBookIntegritySnapshot:
    dropped_count: int
    resync_required: bool
    reason: str | None


class OrderBookDataIntegrityTracker:
    """Expose whether a snapshot/delta book must be rebuilt before reuse."""

    def __init__(self) -> None:
        self._dropped_count = 0
        self._resync_required = False
        self._reason: str | None = None

    def mark_dropped(self, reason: str) -> None:
        self._dropped_count += 1
        self._resync_required = True
        self._reason = str(reason).strip() or "order_book_data_incomplete"

    def mark_resynced(self) -> None:
        self._resync_required = False
        self._reason = None

    def snapshot(self) -> OrderBookIntegritySnapshot:
        return OrderBookIntegritySnapshot(
            dropped_count=self._dropped_count,
            resync_required=self._resync_required,
            reason=self._reason,
        )

    @property
    def resync_required(self) -> bool:
        return self._resync_required


__all__ = [
    "OrderBookDataIntegrityTracker",
    "OrderBookIntegritySnapshot",
    "TradeDataIntegrityTracker",
    "TradeIntegrityIssue",
]
