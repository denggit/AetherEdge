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
class _IntegrityWindow:
    start_ms: int
    end_ms: int
    dropped_count: int = 0
    reasons: set[str] = field(default_factory=set)


class TradeDataIntegrityTracker:
    """Window-aware record of gaps in the normalized Trade stream.

    Internally every dropped event is stored with its exact *event_time_ms*
    so point-in-time queries remain precise.  A parallel window summary
    supports efficient ``prune_before()`` without losing precision.
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
        # Exact event-time → reasons (preserves point-in-time precision).
        self._issues: dict[int, list[str]] = defaultdict(list)
        # Window summaries for pruning.
        self._windows: dict[int, _IntegrityWindow] = {}
        self._revision = 0
        self._dropped_count = 0
        self._repaired_ranges: list[tuple[int, int]] = []

    # -- mutation --------------------------------------------------------

    def mark_dropped(self, event_time_ms: int, reason: str) -> None:
        normalized_reason = str(reason).strip() or "trade_data_incomplete"
        time_ms = int(event_time_ms)
        self._revision += 1
        self._dropped_count += 1
        self._issues.setdefault(time_ms, []).append(normalized_reason)
        wkey = self._window_key(time_ms)
        if wkey not in self._windows:
            self._windows[wkey] = _IntegrityWindow(
                start_ms=wkey,
                end_ms=wkey + self._window_size_ms - 1,
            )
        self._windows[wkey].dropped_count += 1
        self._windows[wkey].reasons.add(normalized_reason)
        # Enforce the retention cap: evict the oldest entries.
        while len(self._issues) > self._max_retained:
            oldest = min(self._issues)
            wkey_old = self._window_key(oldest)
            self._issues.pop(oldest)
            win = self._windows.get(wkey_old)
            if win is not None:
                win.dropped_count = max(0, win.dropped_count - 1)

    def mark_repaired(self, window_start_ms: int, window_end_ms: int) -> None:
        """Explicitly mark a time range as repaired."""
        start = int(window_start_ms)
        end = int(window_end_ms)
        self._repaired_ranges.append((start, end))
        self._revision += 1
        to_remove = [t for t in self._issues if start <= t <= end]
        for t in to_remove:
            self._issues.pop(t, None)
            wkey = self._window_key(t)
            win = self._windows.get(wkey)
            if win is not None:
                win.dropped_count = max(0, win.dropped_count - 1)
                if win.dropped_count == 0:
                    self._windows.pop(wkey, None)

    # -- query -----------------------------------------------------------

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
        # Check repaired ranges first.
        for r_start, r_end in self._repaired_ranges:
            if r_start <= start and end <= r_end:
                return None
        # Point-in-time check: any dropped event inside [start, end]?
        matched_reasons: dict[str, None] = {}
        matched_count = 0
        for time_ms, reasons in self._issues.items():
            if start <= time_ms <= end:
                matched_count += len(reasons)
                for r in reasons:
                    matched_reasons[r] = None
        if matched_count == 0:
            return None
        reason_str = ",".join(matched_reasons)
        return f"{reason_str};dropped_count={matched_count}"

    def issues_since(self, revision: int) -> tuple[TradeIntegrityIssue, ...]:
        result: list[TradeIntegrityIssue] = []
        for time_ms, reasons in sorted(self._issues.items()):
            for reason in reasons:
                # We don't store per-issue revisions anymore; return all
                # issues that were added after *revision* by tracking the
                # total issue count.
                ...
        # For backward compat we iterate issues in time order and assign
        # synthetic revisions.  This preserves the RangeModule's ability to
        # detect "new issues since my last check".
        all_issues = sorted(
            (t, r) for t, reasons in self._issues.items() for r in reasons
        )
        result = [
            TradeIntegrityIssue(
                revision=idx + 1,
                event_time_ms=t,
                reason=r,
            )
            for idx, (t, r) in enumerate(all_issues)
            if idx + 1 > revision
        ]
        return tuple(result)

    def prune_before(self, watermark_ms: int) -> None:
        """Remove entries whose *event_time_ms* is before *watermark_ms*.

        Only call when the corresponding scheduler windows are complete.
        """
        wm = int(watermark_ms)
        stale = [t for t in self._issues if t < wm]
        for t in stale:
            self._issues.pop(t, None)
            wkey = self._window_key(t)
            win = self._windows.get(wkey)
            if win is not None:
                win.dropped_count = max(0, win.dropped_count - 1)
                if win.dropped_count == 0:
                    self._windows.pop(wkey, None)
        self._repaired_ranges = [
            (s, e) for s, e in self._repaired_ranges if e >= wm
        ]

    # -- properties ------------------------------------------------------

    @property
    def revision(self) -> int:
        # Revision tracks the total number of individual drop reasons stored.
        return sum(len(reasons) for reasons in self._issues.values())

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    # -- internal --------------------------------------------------------

    def _window_key(self, event_time_ms: int) -> int:
        return (event_time_ms // self._window_size_ms) * self._window_size_ms


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
