from __future__ import annotations

from src.runtime.tasks import ClosedBarScheduler, closed_bar_open_time_ms, next_bar_close_time_ms

H4 = 4 * 60 * 60_000


def test_closed_bar_open_time_never_returns_current_unclosed_bar():
    assert closed_bar_open_time_ms(H4, interval_ms=H4) == 0
    assert closed_bar_open_time_ms(H4 + 1, interval_ms=H4) == 0
    assert closed_bar_open_time_ms(2 * H4, interval_ms=H4) == H4
    assert next_bar_close_time_ms(H4 + 1, interval_ms=H4) == 2 * H4


def test_closed_bar_scheduler_emits_each_closed_bar_once():
    scheduler = ClosedBarScheduler(interval_ms=H4)

    assert scheduler.due_closed_bar(H4) == 0
    scheduler.mark_emitted(0)
    assert scheduler.due_closed_bar(H4 + 1) is None
    assert scheduler.due_closed_bar(2 * H4) == H4


def test_closed_bar_not_marked_emitted_until_dispatch_succeeds():
    scheduler = ClosedBarScheduler(interval_ms=H4)

    assert scheduler.due_closed_bar(H4) == 0
    assert scheduler.due_closed_bar(H4 + 1) == 0

    scheduler.mark_emitted(0)

    assert scheduler.due_closed_bar(H4 + 2) is None
