from __future__ import annotations


def test_range_bars_by_bucket_prune_prevents_unbounded_growth() -> None:
    """Simulate the prune logic to verify old buckets are removed.

    The prune keeps the current bucket + the N most recent earlier buckets.
    This test verifies the algorithm without requiring a full LiveRuntimeRunner.
    """
    closed_bar_interval_ms = 4 * 60 * 60_000  # 4h
    prune_count = 3

    # Simulate many buckets being populated
    buckets: dict[int, list[int]] = {}
    base_ms = 1_700_000_000_000

    # Add 100 "buckets" worth of bars
    for i in range(100):
        bucket_key = base_ms + i * closed_bar_interval_ms
        buckets[bucket_key] = [1, 2, 3]  # fake range bars

    assert len(buckets) == 100

    # Run prune algorithm
    current_bucket = base_ms + 99 * closed_bar_interval_ms
    keep = prune_count

    bucket_keys = sorted(buckets.keys(), reverse=True)
    latest_key = bucket_keys[0] if bucket_keys else current_bucket
    threshold = latest_key - (keep * closed_bar_interval_ms)

    stale = [k for k in bucket_keys if k < threshold]
    stale = [k for k in stale if k < current_bucket]
    for k in stale:
        del buckets[k]

    # Should have at most keep + 1 (current) buckets
    assert len(buckets) <= keep + 2  # latest + keep older + maybe current
    # At most 5 buckets should remain
    assert len(buckets) <= 5

    # The current and latest buckets should still be present
    assert current_bucket in buckets
    assert latest_key in buckets


def test_prune_does_not_remove_current_bucket() -> None:
    closed_bar_interval_ms = 4 * 60 * 60_000
    prune_count = 3
    buckets: dict[int, list[int]] = {}
    base_ms = 1_700_000_000_000

    for i in range(10):
        buckets[base_ms + i * closed_bar_interval_ms] = [1]

    current_bucket = base_ms + 5 * closed_bar_interval_ms
    keep = prune_count

    bucket_keys = sorted(buckets.keys(), reverse=True)
    latest_key = bucket_keys[0]
    threshold = latest_key - (keep * closed_bar_interval_ms)

    stale = [k for k in bucket_keys if k < threshold]
    stale = [k for k in stale if k < current_bucket]
    for k in stale:
        del buckets[k]

    # Current bucket must always survive
    assert current_bucket in buckets


def test_prune_checkpoint_generation_still_works_with_remaining_buckets() -> None:
    """After pruning, the most recent buckets should still be available
    for range aggregate generation."""
    closed_bar_interval_ms = 4 * 60 * 60_000
    prune_count = 3
    buckets: dict[int, list[int]] = {}
    base_ms = 1_700_000_000_000

    for i in range(50):
        buckets[base_ms + i * closed_bar_interval_ms] = [i]

    current_bucket = base_ms + 49 * closed_bar_interval_ms
    keep = prune_count

    bucket_keys = sorted(buckets.keys(), reverse=True)
    latest_key = bucket_keys[0]
    threshold = latest_key - (keep * closed_bar_interval_ms)

    stale = [k for k in bucket_keys if k < threshold]
    stale = [k for k in stale if k < current_bucket]
    for k in stale:
        del buckets[k]

    # The most recent buckets should still have data for checkpoint generation
    remaining_keys = sorted(buckets.keys())
    assert len(remaining_keys) >= 1
    assert remaining_keys[-1] >= current_bucket
    # The second most recent (for checkpoint context) should be available
    if len(remaining_keys) >= 2:
        assert remaining_keys[-2] >= current_bucket - closed_bar_interval_ms
