#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Lightweight counters and state-change trackers for log noise reduction.

These are generic, thread-safe helpers that do NOT depend on any business
module. Business modules should instantiate these classes with their own
keys and message formatting.
"""

import collections
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple


class SuppressedLogCounter:
    def __init__(self):
        self.counts = collections.Counter()
        self._lock = threading.Lock()

    def inc(self, key: str, amount: int = 1) -> None:
        if not key or amount <= 0:
            return
        with self._lock:
            self.counts[str(key)] += int(amount)

    def snapshot_and_reset(self) -> Dict[str, int]:
        with self._lock:
            snapshot = dict(self.counts)
            self.counts.clear()
        return snapshot


suppressed_log_counter = SuppressedLogCounter()


# ────────────────────────────────────────────────────────────────────────────
# Time-window log limiter
# ────────────────────────────────────────────────────────────────────────────


class TimeWindowLogLimiter:
    """Rate-limit log messages by key: allow at most one message per interval.

    Thread-safe. Intended for use in async loops — the caller is responsible
    for passing consistent monotonic timestamps.
    """

    def __init__(self, *, now_fn: Callable[[], float] | None = None) -> None:
        self._now_fn = now_fn or time.monotonic
        self._last_by_key: dict[str, float] = {}
        self._lock = threading.Lock()

    def should_log(self, key: str, *, now: float | None = None, interval_seconds: float = 0) -> bool:
        """Return True if *interval_seconds* have passed since the last emit for *key*."""
        now = now if now is not None else self._now_fn()
        with self._lock:
            last = self._last_by_key.get(key)
            if last is None or (now - last) >= interval_seconds:
                self._last_by_key[key] = now
                return True
            return False

    def reset(self, key: str) -> None:
        with self._lock:
            self._last_by_key.pop(key, None)


# ────────────────────────────────────────────────────────────────────────────
# State-change log tracker
# ────────────────────────────────────────────────────────────────────────────


class StateChangeLogTracker:
    """Track a named piece of state and report whether it has changed.

    Designed for async-loop use in a single coroutine — no locking needed
    unless shared across threads.
    """

    def __init__(self) -> None:
        self._states: dict[str, object] = {}

    def changed(self, key: str, value: object) -> bool:
        """Return True the first time *key* is seen or when its value differs
        from the last recorded value. Always updates the stored value.
        """
        if key in self._states:
            prev = self._states[key]
            self._states[key] = value
            return prev != value
        self._states[key] = value
        return True

    def get(self, key: str) -> object:
        return self._states.get(key)

    def clear(self) -> None:
        self._states.clear()


# ────────────────────────────────────────────────────────────────────────────
# Periodic skip summary logger
# ────────────────────────────────────────────────────────────────────────────


class PeriodicSkipSummary:
    """Accumulate skip counts and emit a summary at most every *interval_seconds*.

    Thread-safe. Typical usage in a loop that skips work:

        if not active:
            summary.record_skip("inactive")
            if summary.should_emit_summary("inactive", interval_s=600.0):
                logger.info("Order sync still inactive | skipped_ticks=%d", summary.count("inactive"))
            continue
        summary.reset("inactive")
    """

    def __init__(self, *, now_fn: Callable[[], float] | None = None) -> None:
        self._now_fn = now_fn or time.monotonic
        self._counts: dict[str, int] = {}
        self._last_emit: dict[str, float] = {}
        self._lock = threading.Lock()

    def record_skip(self, key: str) -> None:
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1

    def count(self, key: str) -> int:
        with self._lock:
            return self._counts.get(key, 0)

    def should_emit_summary(self, key: str, *, interval_seconds: float, now: float | None = None) -> bool:
        """Return True if at least *interval_seconds* have passed since the last
        summary emit for *key*. Updates the last-emit timestamp on True.
        """
        now = now if now is not None else self._now_fn()
        with self._lock:
            last = self._last_emit.get(key)
            if last is None or (now - last) >= interval_seconds:
                self._last_emit[key] = now
                return True
            return False

    def reset(self, key: str) -> None:
        with self._lock:
            self._counts[key] = 0
            self._last_emit.pop(key, None)


# ────────────────────────────────────────────────────────────────────────────
# Composite account fingerprint helper (generic — no business logic)
# ────────────────────────────────────────────────────────────────────────────


def build_fingerprint(
    *,
    balance_total: object = None,
    balance_available: object = None,
    nonzero_position_count: int = 0,
    position_quantities: tuple = (),
    leverage: object = None,
    position_mode: object = None,
) -> Tuple:
    """Build a hashable fingerprint tuple for change detection.

    Callers (e.g. AccountStateSyncService) decide what goes into the
    fingerprint; this function is a pure-data helper.
    """
    return (
        balance_total,
        balance_available,
        nonzero_position_count,
        position_quantities,
        leverage,
        position_mode,
    )
