from __future__ import annotations

from dataclasses import dataclass


def closed_bar_open_time_ms(now_ms: int, *, interval_ms: int) -> int:
    """Return the latest fully closed bar open time for ``now_ms``.

    At exactly a 4H boundary, the bar that just closed started one interval
    earlier. This prevents using the newly opened, unclosed bar.
    """

    if interval_ms <= 0:
        raise ValueError("interval_ms must be positive")
    boundary = now_ms - (now_ms % interval_ms)
    return boundary - interval_ms if now_ms == boundary else boundary - interval_ms


def next_bar_close_time_ms(now_ms: int, *, interval_ms: int) -> int:
    if interval_ms <= 0:
        raise ValueError("interval_ms must be positive")
    boundary = now_ms - (now_ms % interval_ms)
    return boundary + interval_ms


@dataclass
class ClosedBarScheduler:
    interval_ms: int
    last_emitted_open_time_ms: int | None = None

    def due_closed_bar(self, now_ms: int) -> int | None:
        open_time_ms = closed_bar_open_time_ms(now_ms, interval_ms=self.interval_ms)
        if open_time_ms < 0:
            return None
        if self.last_emitted_open_time_ms == open_time_ms:
            return None
        self.last_emitted_open_time_ms = open_time_ms
        return open_time_ms
