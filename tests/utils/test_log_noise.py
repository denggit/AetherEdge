from __future__ import annotations

import time

import pytest

from src.utils.log_noise import (
    PeriodicSkipSummary,
    StateChangeLogTracker,
    TimeWindowLogLimiter,
    build_fingerprint,
    SuppressedLogCounter,
)


# ────────────────────────────────────────────────────────────────────────────
# SuppressedLogCounter
# ────────────────────────────────────────────────────────────────────────────


def test_suppressed_log_counter_inc_and_snapshot():
    counter = SuppressedLogCounter()
    counter.inc("a", 1)
    counter.inc("b", 3)
    counter.inc("a", 2)

    snapshot = counter.snapshot_and_reset()
    assert snapshot == {"a": 3, "b": 3}

    # After reset, counts are cleared
    snapshot2 = counter.snapshot_and_reset()
    assert snapshot2 == {}


def test_suppressed_log_counter_ignores_invalid_inputs():
    counter = SuppressedLogCounter()
    counter.inc("", 1)
    counter.inc("key", 0)
    counter.inc("key", -1)
    assert counter.snapshot_and_reset() == {}


# ────────────────────────────────────────────────────────────────────────────
# TimeWindowLogLimiter
# ────────────────────────────────────────────────────────────────────────────


def test_time_window_limiter_first_call_always_true():
    limiter = TimeWindowLogLimiter()
    assert limiter.should_log("key1", now=100.0, interval_seconds=10.0) is True


def test_time_window_limiter_second_call_within_interval_is_false():
    limiter = TimeWindowLogLimiter()
    assert limiter.should_log("key1", now=100.0, interval_seconds=10.0) is True
    assert limiter.should_log("key1", now=105.0, interval_seconds=10.0) is False


def test_time_window_limiter_after_interval_is_true():
    limiter = TimeWindowLogLimiter()
    assert limiter.should_log("key1", now=100.0, interval_seconds=10.0) is True
    assert limiter.should_log("key1", now=105.0, interval_seconds=10.0) is False
    assert limiter.should_log("key1", now=110.0, interval_seconds=10.0) is True


def test_time_window_limiter_different_keys_independent():
    limiter = TimeWindowLogLimiter()
    assert limiter.should_log("a", now=100.0, interval_seconds=5.0) is True
    assert limiter.should_log("b", now=100.0, interval_seconds=5.0) is True
    assert limiter.should_log("a", now=102.0, interval_seconds=5.0) is False
    assert limiter.should_log("b", now=102.0, interval_seconds=5.0) is False


def test_time_window_limiter_reset():
    limiter = TimeWindowLogLimiter()
    limiter.should_log("key1", now=100.0, interval_seconds=60.0)
    limiter.reset("key1")
    assert limiter.should_log("key1", now=105.0, interval_seconds=60.0) is True


# ────────────────────────────────────────────────────────────────────────────
# StateChangeLogTracker
# ────────────────────────────────────────────────────────────────────────────


def test_state_change_tracker_first_set_is_change():
    tracker = StateChangeLogTracker()
    assert tracker.changed("k", "v1") is True


def test_state_change_tracker_same_value_not_changed():
    tracker = StateChangeLogTracker()
    tracker.changed("k", "v1")
    assert tracker.changed("k", "v1") is False


def test_state_change_tracker_different_value_is_change():
    tracker = StateChangeLogTracker()
    tracker.changed("k", "v1")
    assert tracker.changed("k", "v2") is True


def test_state_change_tracker_handles_none():
    tracker = StateChangeLogTracker()
    assert tracker.changed("k", None) is True
    assert tracker.changed("k", None) is False
    assert tracker.changed("k", "something") is True


def test_state_change_tracker_multiple_keys_independent():
    tracker = StateChangeLogTracker()
    assert tracker.changed("a", 1) is True
    assert tracker.changed("b", 10) is True
    assert tracker.changed("a", 1) is False
    assert tracker.changed("b", 20) is True


def test_state_change_tracker_clear():
    tracker = StateChangeLogTracker()
    tracker.changed("k", "v1")
    tracker.clear()
    assert tracker.changed("k", "v1") is True


def test_state_change_tracker_get():
    tracker = StateChangeLogTracker()
    tracker.changed("k", "v1")
    assert tracker.get("k") == "v1"
    assert tracker.get("nonexistent") is None


# ────────────────────────────────────────────────────────────────────────────
# PeriodicSkipSummary
# ────────────────────────────────────────────────────────────────────────────


def test_periodic_skip_summary_counts():
    summary = PeriodicSkipSummary()
    summary.record_skip("inactive")
    summary.record_skip("inactive")
    summary.record_skip("inactive")
    assert summary.count("inactive") == 3


def test_periodic_skip_summary_first_emit_true():
    summary = PeriodicSkipSummary()
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=100.0) is True


def test_periodic_skip_summary_within_interval_false():
    summary = PeriodicSkipSummary()
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=100.0) is True
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=300.0) is False


def test_periodic_skip_summary_after_interval_true():
    summary = PeriodicSkipSummary()
    summary.should_emit_summary("inactive", interval_seconds=600.0, now=100.0)
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=700.0) is True


def test_periodic_skip_summary_reset():
    summary = PeriodicSkipSummary()
    summary.record_skip("inactive")
    summary.record_skip("inactive")
    assert summary.count("inactive") == 2
    summary.reset("inactive")
    assert summary.count("inactive") == 0
    # After reset, should_emit_summary is true again (first call)
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=100.0) is True


def test_periodic_skip_summary_different_keys():
    summary = PeriodicSkipSummary()
    summary.record_skip("a")
    summary.record_skip("b")
    assert summary.count("a") == 1
    assert summary.count("b") == 1
    assert summary.should_emit_summary("a", interval_seconds=10.0, now=100.0) is True
    assert summary.should_emit_summary("b", interval_seconds=10.0, now=100.0) is True


def test_periodic_skip_summary_mark_emitted_blocks_immediate_summary():
    """mark_emitted seeds the timer so should_emit_summary won't fire
    until the full interval has elapsed."""
    summary = PeriodicSkipSummary()
    summary.record_skip("inactive")
    # First inactive: state-change log fired; seed the timer.
    summary.mark_emitted("inactive", now=100.0)

    # 20 seconds later (within the 600s window): should NOT emit.
    summary.record_skip("inactive")
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=120.0) is False

    # 10 minutes + 1 second later: SHOULD emit.
    summary.record_skip("inactive")
    assert summary.should_emit_summary("inactive", interval_seconds=600.0, now=701.0) is True


def test_periodic_skip_summary_mark_emitted_does_not_reset_count():
    """mark_emitted only updates the timer; skip counts are preserved."""
    summary = PeriodicSkipSummary()
    summary.record_skip("inactive")
    summary.record_skip("inactive")
    summary.record_skip("inactive")
    summary.mark_emitted("inactive", now=100.0)
    assert summary.count("inactive") == 3


def test_periodic_skip_summary_without_mark_emitted_first_call_still_true():
    """Without mark_emitted, the very first should_emit_summary call
    returns True (backward-compatible behaviour for callers that don't
    use an external state-change log)."""
    summary = PeriodicSkipSummary()
    summary.record_skip("x")
    assert summary.should_emit_summary("x", interval_seconds=600.0, now=100.0) is True


# ────────────────────────────────────────────────────────────────────────────
# build_fingerprint
# ────────────────────────────────────────────────────────────────────────────


def test_build_fingerprint_same_inputs_produce_same_output():
    fp1 = build_fingerprint(
        balance_total="100",
        balance_available="90",
        nonzero_position_count=1,
        position_quantities=(("LONG", "1.0"),),
        leverage="10",
        position_mode="ONE_WAY",
    )
    fp2 = build_fingerprint(
        balance_total="100",
        balance_available="90",
        nonzero_position_count=1,
        position_quantities=(("LONG", "1.0"),),
        leverage="10",
        position_mode="ONE_WAY",
    )
    assert fp1 == fp2
    assert hash(fp1) == hash(fp2)


def test_build_fingerprint_different_balance_produces_different_output():
    fp1 = build_fingerprint(
        balance_total="100", balance_available="90",
        nonzero_position_count=0, position_quantities=(),
        leverage="10", position_mode="ONE_WAY",
    )
    fp2 = build_fingerprint(
        balance_total="90", balance_available="80",
        nonzero_position_count=0, position_quantities=(),
        leverage="10", position_mode="ONE_WAY",
    )
    assert fp1 != fp2


def test_build_fingerprint_position_change_detected():
    fp1 = build_fingerprint(
        balance_total="100", balance_available="90",
        nonzero_position_count=0, position_quantities=(),
        leverage="10", position_mode="ONE_WAY",
    )
    fp2 = build_fingerprint(
        balance_total="100", balance_available="90",
        nonzero_position_count=1, position_quantities=(("LONG", "1.0"),),
        leverage="10", position_mode="ONE_WAY",
    )
    assert fp1 != fp2


def test_build_fingerprint_leverage_change_detected():
    fp1 = build_fingerprint(
        balance_total="100", balance_available="90",
        nonzero_position_count=0, position_quantities=(),
        leverage="10", position_mode="ONE_WAY",
    )
    fp2 = build_fingerprint(
        balance_total="100", balance_available="90",
        nonzero_position_count=0, position_quantities=(),
        leverage="20", position_mode="ONE_WAY",
    )
    assert fp1 != fp2
