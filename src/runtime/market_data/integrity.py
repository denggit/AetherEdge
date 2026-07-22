from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


_DEFAULT_MAX_RETAINED = 131_072  # far larger than 4096 — hours of continuous drops


@dataclass(frozen=True)
class TradeIntegrityIssue:
    revision: int
    event_time_ms: int
    reason: str


@dataclass
class IntegrityWindowState:
    start_ms: int
    end_ms: int
    last_issue_revision: int = 0
    repaired_through_revision: int = 0
    dropped_count: int = 0
    reasons: set[str] = field(default_factory=set)

    @property
    def complete(self) -> bool:
        return self.last_issue_revision <= self.repaired_through_revision


class TradeDataIntegrityTracker:
    """Window-aware record of gaps in the normalized Trade stream.

    Every dropped event is stored with its exact *event_time_ms* and a
    monotonically-increasing *revision*.  ``issues_since(revision)`` returns
    only issues whose true revision exceeds the given value — no synthetic
    renumbering.
    """

    def __init__(
        self,
        *,
        max_retained: int = _DEFAULT_MAX_RETAINED,
        window_size_ms: int = 60_000,
    ) -> None:
        if max_retained <= 0:
            raise ValueError("max_retained must be positive")
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        self._max_retained = max_retained
        self._window_size_ms = window_size_ms
        self._issues: list[TradeIntegrityIssue] = []
        self._windows: dict[int, IntegrityWindowState] = {}
        self._revision: int = 0
        self._dropped_count: int = 0
        self._repaired_ranges: dict[
            tuple[int, int], int
        ] = {}  # (start_ms,end_ms) → through_revision

    def mark_dropped(self, event_time_ms: int, reason: str) -> None:
        normalized_reason = str(reason).strip() or "trade_data_incomplete"
        time_ms = int(event_time_ms)
        self._revision += 1
        self._dropped_count += 1
        issue = TradeIntegrityIssue(
            revision=self._revision,
            event_time_ms=time_ms,
            reason=normalized_reason,
        )
        self._issues.append(issue)
        wkey = self._window_key(time_ms)
        if wkey not in self._windows:
            self._windows[wkey] = IntegrityWindowState(
                start_ms=wkey,
                end_ms=wkey + self._window_size_ms - 1,
            )
        wstate = self._windows[wkey]
        wstate.dropped_count += 1
        wstate.reasons.add(normalized_reason)
        wstate.last_issue_revision = self._revision

    def mark_repaired(
        self,
        window_start_ms: int,
        window_end_ms: int,
        *,
        through_revision: int,
    ) -> None:
        start = int(window_start_ms)
        end = int(window_end_ms)
        key = (start, end)
        existing = self._repaired_ranges.get(key, 0)
        self._repaired_ranges[key] = max(existing, int(through_revision))
        for wkey, wstate in list(self._windows.items()):
            if start <= wstate.start_ms and wstate.end_ms <= end:
                wstate.repaired_through_revision = max(
                    wstate.repaired_through_revision,
                    int(through_revision),
                )
        self._compact_repaired_details()

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

        # Check repaired ranges first — repair through_revision must cover
        # every issue in the window.
        for (r_start, r_end), through_rev in self._repaired_ranges.items():
            if r_start <= start and end <= r_end:
                # Still need to check for issues AFTER the repair
                unmatched: list[str] = []
                for issue in self._issues:
                    if start <= issue.event_time_ms <= end:
                        if issue.revision > through_rev:
                            unmatched.append(issue.reason)
                if not unmatched:
                    return None
                return (
                    "trade_data_incomplete;repaired_through="
                    f"{through_rev};"
                    "new_drop_after_repair=" + ",".join(unmatched)
                )

        # No repair covers this window — check issues
        matched_reasons: dict[str, None] = {}
        matched_count = 0
        for issue in self._issues:
            if start <= issue.event_time_ms <= end:
                matched_count += 1
                matched_reasons[issue.reason] = None
        if matched_count == 0:
            return None
        reason_str = ",".join(matched_reasons)
        return f"{reason_str};dropped_count={matched_count}"

    def issues_since(self, revision: int) -> tuple[TradeIntegrityIssue, ...]:
        return tuple(
            issue for issue in self._issues if issue.revision > revision
        )

    def prune_before(self, watermark_ms: int) -> None:
        """Remove entries whose *event_time_ms* is before *watermark_ms*.

        Only prunes issues belonging to **complete** windows.  Incomplete
        windows are preserved so recovery and closed-bar checks remain accurate.
        """
        wm = int(watermark_ms)
        incomplete_windows: set[int] = set()
        for wkey, wstate in self._windows.items():
            if not wstate.complete:
                incomplete_windows.add(wkey)

        self._issues = [
            issue
            for issue in self._issues
            if issue.event_time_ms >= wm
            or self._window_key(issue.event_time_ms) in incomplete_windows
        ]

        stale_windows = [
            wkey
            for wkey, wstate in self._windows.items()
            if wstate.end_ms < wm and wstate.complete
        ]
        for wkey in stale_windows:
            self._windows.pop(wkey, None)

        self._repaired_ranges = {
            k: v
            for k, v in self._repaired_ranges.items()
            if k[1] >= wm
        }

    @property
    def revision(self) -> int:
        """Monotonic counter — increments on every mark_dropped call."""
        return self._revision

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    def _window_key(self, event_time_ms: int) -> int:
        return (event_time_ms // self._window_size_ms) * self._window_size_ms

    def _compact_repaired_details(self) -> None:
        excess = len(self._issues) - self._max_retained
        if excess <= 0:
            return
        retained: list[TradeIntegrityIssue] = []
        for issue in self._issues:
            window = self._windows.get(self._window_key(issue.event_time_ms))
            if excess > 0 and window is not None and window.complete:
                excess -= 1
                continue
            retained.append(issue)
        self._issues = retained


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
    "IntegrityWindowState",
    "OrderBookDataIntegrityTracker",
    "OrderBookIntegritySnapshot",
    "TradeDataIntegrityTracker",
    "TradeIntegrityIssue",
]
