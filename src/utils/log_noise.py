#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Lightweight counters for intentionally suppressed research logs."""

import collections
import threading
from typing import Dict


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
