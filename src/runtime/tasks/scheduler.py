from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ClosedBarOutcome(str, Enum):
    COMPLETED = "completed"
    SKIPPED_INCOMPLETE = "skipped_incomplete"
    RETRYABLE_MISSING = "retryable_missing"
    FATAL_PIPELINE_FAILURE = "fatal_pipeline_failure"


def closed_bar_open_time_ms(now_ms: int, *, interval_ms: int, close_buffer_ms: int = 0) -> int:
    """Return the latest fully closed bar open time for ``now_ms``.

    At exactly a 4H boundary, the bar that just closed started one interval
    earlier. This prevents using the newly opened, unclosed bar.
    """

    if interval_ms <= 0:
        raise ValueError("interval_ms must be positive")
    if close_buffer_ms < 0:
        raise ValueError("close_buffer_ms must be non-negative")
    effective_now = max(0, now_ms - close_buffer_ms)
    boundary = effective_now - (effective_now % interval_ms)
    return boundary - interval_ms


def next_bar_close_time_ms(now_ms: int, *, interval_ms: int) -> int:
    if interval_ms <= 0:
        raise ValueError("interval_ms must be positive")
    boundary = now_ms - (now_ms % interval_ms)
    return boundary + interval_ms


@dataclass
class ClosedBarScheduler:
    interval_ms: int
    close_buffer_ms: int = 0
    retry_interval_ms: int = 0
    missing_alert_after_ms: int = 120_000
    last_emitted_open_time_ms: int | None = None
    last_attempt_open_time_ms: int | None = None
    last_attempt_time_ms: int | None = None
    missing_alerted_open_time_ms: set[int] = field(default_factory=set)
    skipped_open_time_ms: set[int] = field(default_factory=set)
    skipped_reasons: dict[int, str] = field(default_factory=dict)
    skipped_alerted: set[int] = field(default_factory=set)

    def due_closed_bar(self, now_ms: int) -> int | None:
        open_time_ms = closed_bar_open_time_ms(
            now_ms,
            interval_ms=self.interval_ms,
            close_buffer_ms=self.close_buffer_ms,
        )
        if open_time_ms < 0:
            return None
        if self.last_emitted_open_time_ms == open_time_ms:
            return None
        # Never retry a permanently skipped window.
        if open_time_ms in self.skipped_open_time_ms:
            return None
        if (
            self.retry_interval_ms > 0
            and self.last_attempt_open_time_ms == open_time_ms
            and self.last_attempt_time_ms is not None
            and now_ms - self.last_attempt_time_ms < self.retry_interval_ms
        ):
            return None
        self.last_attempt_open_time_ms = open_time_ms
        self.last_attempt_time_ms = now_ms
        return open_time_ms

    def mark_emitted(self, open_time_ms: int) -> None:
        if open_time_ms < 0:
            raise ValueError("open_time_ms must be non-negative")
        self.last_emitted_open_time_ms = open_time_ms
        self.missing_alerted_open_time_ms.discard(open_time_ms)
        self.skipped_open_time_ms.discard(open_time_ms)

    def mark_skipped(self, open_time_ms: int, reason: str) -> None:
        """Permanently skip this window — never retry."""
        if open_time_ms < 0:
            raise ValueError("open_time_ms must be non-negative")
        self.skipped_open_time_ms.add(open_time_ms)
        self.skipped_reasons[open_time_ms] = str(reason)
        self.last_emitted_open_time_ms = open_time_ms

    def is_skipped(self, open_time_ms: int) -> bool:
        return open_time_ms in self.skipped_open_time_ms

    def skipped_reason(self, open_time_ms: int) -> str | None:
        return self.skipped_reasons.get(open_time_ms)

    def should_alert_skipped(self, open_time_ms: int) -> bool:
        """Return True only once for a given skipped window."""
        if open_time_ms not in self.skipped_open_time_ms:
            return False
        if open_time_ms in self.skipped_alerted:
            return False
        self.skipped_alerted.add(open_time_ms)
        return True

    def should_alert_missing(self, open_time_ms: int, now_ms: int) -> bool:
        if open_time_ms in self.missing_alerted_open_time_ms:
            return False
        close_time_ms = open_time_ms + self.interval_ms
        if now_ms - close_time_ms < self.missing_alert_after_ms:
            return False
        self.missing_alerted_open_time_ms.add(open_time_ms)
        return True
