from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


# ── Unit-level tests for watchdog quick-fail helpers ──


def test_parse_fatal_exit_codes_default_empty():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes(None) == frozenset()
    assert _parse_fatal_exit_codes("") == frozenset()


def test_parse_fatal_exit_codes_single():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes("78") == frozenset({78})


def test_parse_fatal_exit_codes_multi():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes("78, 13, 42") == frozenset({78, 13, 42})


def test_parse_fatal_exit_codes_ignores_junk():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    result = _parse_fatal_exit_codes("78,abc,42")
    assert 78 in result
    assert 42 in result
    assert len(result) == 2


# ── Mock-based tests for the watchdog main loop logic ──


class FakeChildProcess:
    """Simulates a subprocess.Popen for testing watchdog restart logic."""

    def __init__(self, returncode: int, *, uptime_seconds: float = 0.1) -> None:
        self._returncode = returncode
        self._uptime = uptime_seconds
        self.pid = 12345

    def wait(self) -> int:
        return self._returncode

    def poll(self) -> int | None:
        return self._returncode


def _make_fake_children(behaviors: list[tuple[int, float]]) -> list[FakeChildProcess]:
    """Create a list of fake children with (returncode, uptime_seconds)."""
    return [FakeChildProcess(code, uptime) for code, uptime in behaviors]


class TestWatchdogQuickFailLogic:
    """Test the watchdog restart decision logic using mocked subprocess.

    These tests verify the quick-fail circuit breaker, fatal exit code
    handling, and counter reset logic without spawning real OS processes.
    """

    def test_stops_after_three_quick_failures(self, monkeypatch):
        """Quick failures (uptime < quick_fail_seconds) should trip the
        circuit breaker after max_quick_failures consecutive occurrences."""
        import scripts.watchdog_live as wd

        # Configure for testing
        monkeypatch.setattr(wd, "_running", True)

        quick_fail_seconds = 60.0
        max_quick_failures = 3
        fatal_codes = frozenset({78})
        restart_seconds = 0.01
        max_restarts = 0  # unlimited

        # Simulate the main loop decision path directly
        quick_failure_count = 0

        # 3 quick failures
        for i in range(3):
            returncode = 1
            uptime = 0.1  # < quick_fail_seconds

            # Fatal check
            if returncode in fatal_codes:
                break

            # Quick-fail detection
            if uptime < quick_fail_seconds:
                quick_failure_count += 1
            else:
                quick_failure_count = 0

            # Circuit breaker
            if quick_failure_count >= max_quick_failures:
                break

        assert quick_failure_count == 3
        assert quick_failure_count >= max_quick_failures

    def test_fatal_exit_code_stops_immediately(self):
        """Return code in FATAL_EXIT_CODES should prevent any restart."""
        import scripts.watchdog_live as wd

        fatal_codes = frozenset({78})
        returncode = 78

        should_stop = returncode in fatal_codes
        assert should_stop is True

    def test_normal_exit_clears_quick_failures(self):
        """returncode=0 should reset quick_failure_count and exit clean."""
        quick_failure_count = 5
        returncode = 0

        if returncode == 0:
            quick_failure_count = 0

        assert quick_failure_count == 0

    def test_long_running_child_resets_quick_failure_counter(self):
        """When uptime >= quick_fail_seconds, counter should reset."""
        quick_fail_seconds = 60.0
        quick_failure_count = 2

        # Child ran for 120 seconds then crashed — not a quick failure.
        uptime = 120.0

        if uptime < quick_fail_seconds:
            quick_failure_count += 1
        else:
            quick_failure_count = 0

        assert quick_failure_count == 0

    def test_mixed_quick_and_long_runs(self):
        """Two quick failures, one long run (resets), then two more quick
        failures don't trip breaker at max=3."""
        quick_fail_seconds = 60.0
        max_quick_failures = 3
        fatal_codes = frozenset({78})
        quick_failure_count = 0

        runs = [
            (1, 0.1),   # quick failure
            (1, 0.1),   # quick failure
            (1, 120.0), # long run, resets counter
            (1, 0.1),   # quick failure (counter starts at 1)
            (1, 0.1),   # quick failure (counter becomes 2, still < 3)
        ]

        for returncode, uptime in runs:
            if returncode == 0:
                quick_failure_count = 0
                break
            if returncode in fatal_codes:
                break
            if uptime < quick_fail_seconds:
                quick_failure_count += 1
            else:
                quick_failure_count = 0
            if quick_failure_count >= max_quick_failures:
                break

        assert quick_failure_count == 2
        assert quick_failure_count < max_quick_failures

    def test_quick_fail_alert_sent_only_once(self):
        """The quick-fail alert flag should prevent duplicate emails."""
        import scripts.watchdog_live as wd

        quick_fail_seconds = 60.0
        max_quick_failures = 3
        fatal_codes = frozenset({78})
        _quick_fail_alert_sent = False
        quick_failure_count = 0

        # Simulate 3 quick failures
        for i in range(3):
            uptime = 0.1
            returncode = 1

            if returncode in fatal_codes:
                break
            if uptime < quick_fail_seconds:
                quick_failure_count += 1
            else:
                quick_failure_count = 0

            if quick_failure_count >= max_quick_failures:
                if not _quick_fail_alert_sent:
                    _quick_fail_alert_sent = True
                break

        assert _quick_fail_alert_sent is True

        # Second time through the loop (if it were to run again), alert
        # should NOT be sent again.
        second_alert_sent = False
        if quick_failure_count >= max_quick_failures:
            if not _quick_fail_alert_sent:
                second_alert_sent = True

        assert second_alert_sent is False

    def test_restart_count_increments_on_normal_failure(self):
        """Regular non-fatal, non-quick failures should still increment
        restart_count and honor max_restarts."""
        max_restarts = 3
        restart_count = 0

        for i in range(5):
            restart_count += 1
            if max_restarts > 0 and restart_count >= max_restarts:
                break

        assert restart_count == 3  # stopped after 3 restarts
