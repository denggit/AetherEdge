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
    forced_incomplete: bool = False

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
        self._restored_windows: dict[tuple[int, int], IntegrityWindowState] = {}
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
        through = int(through_revision)
        if through < 0 or through > self._revision:
            raise ValueError(
                "through_revision must be between zero and current revision"
            )
        key = (start, end)
        existing = self._repaired_ranges.get(key, 0)
        self._repaired_ranges[key] = max(existing, through)
        for wkey, wstate in list(self._windows.items()):
            if start <= wstate.start_ms and wstate.end_ms <= end:
                wstate.repaired_through_revision = max(
                    wstate.repaired_through_revision,
                    through,
                )
        for (w_start, w_end), wstate in self._restored_windows.items():
            if start <= w_start and w_end <= end:
                wstate.repaired_through_revision = max(
                    wstate.repaired_through_revision, through
                )
                wstate.forced_incomplete = False
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

        through_rev = max(
            (
                revision
                for (r_start, r_end), revision
                in self._repaired_ranges.items()
                if r_start <= start and end <= r_end
            ),
            default=None,
        )
        if through_rev is not None:
            unmatched = [
                issue.reason
                for issue in self._issues
                if start <= issue.event_time_ms <= end
                and issue.revision > through_rev
            ]
            if not unmatched:
                return self._restored_invalid_reason(start, end, through_rev)
            return (
                "trade_data_incomplete;repaired_through="
                f"{through_rev};new_drop_after_repair="
                + ",".join(unmatched)
            )

        # No repair covers this window — check issues
        matched_reasons: dict[str, None] = {}
        matched_count = 0
        for issue in self._issues:
            if start <= issue.event_time_ms <= end:
                matched_count += 1
                matched_reasons[issue.reason] = None
        if matched_count == 0:
            return self._restored_invalid_reason(start, end)
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

    def restore_revision(self, revision: int) -> None:
        """Resume the durable monotonic revision without fabricating issues."""
        value = int(revision)
        if value < 0:
            raise ValueError("revision must be non-negative")
        self._revision = max(self._revision, value)

    def restore_window(
        self,
        start_ms: int,
        end_ms: int,
        *,
        last_issue_revision: int,
        repaired_through_revision: int,
        reason: str | None,
        complete: bool | None = None,
    ) -> None:
        """Restore durable aggregate integrity without inventing live drops."""
        start, end = int(start_ms), int(end_ms)
        issue, repaired = int(last_issue_revision), int(repaired_through_revision)
        if end < start or issue < 0 or repaired < 0:
            raise ValueError("invalid durable integrity window")
        self.restore_revision(max(issue, repaired))
        state = self._restored_windows.setdefault(
            (start, end), IntegrityWindowState(start_ms=start, end_ms=end)
        )
        state.last_issue_revision = max(state.last_issue_revision, issue)
        state.repaired_through_revision = max(
            state.repaired_through_revision, repaired
        )
        if complete is not None:
            state.forced_incomplete = not complete
        if reason:
            state.reasons.add(str(reason))
        if repaired:
            key = (start, end)
            self._repaired_ranges[key] = max(
                self._repaired_ranges.get(key, 0), repaired
            )

    def _restored_invalid_reason(
        self, start_ms: int, end_ms: int, through_revision: int = 0
    ) -> str | None:
        for state in self._restored_windows.values():
            if (
                state.start_ms <= end_ms
                and start_ms <= state.end_ms
                and (
                    state.forced_incomplete
                    or state.last_issue_revision
                    > max(state.repaired_through_revision, through_revision)
                )
            ):
                reasons = ",".join(sorted(state.reasons)) or "trade_data_incomplete"
                return f"{reasons};durable_window_incomplete"
        return None

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
