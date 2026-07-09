from __future__ import annotations

from dataclasses import dataclass

from tools.range_backfill_worker import _partial_without_progress


@dataclass(frozen=True)
class _FakeSummary:
    status: str = "partial"
    aggregates_written: int = 0
    range_bars_written: int = 0
    missing_after: int = 1
    filtered_reason_if_zero: str | None = None


def test_resource_limit_before_target_is_not_treated_as_no_progress() -> None:
    """A cycle that stops before the target because of a resource limit
    must NOT be classified as archive_gap_partial_no_progress.  The worker
    should retry on its normal cadence instead of entering a long cooldown."""
    summary = _FakeSummary(
        status="partial",
        aggregates_written=0,
        range_bars_written=0,
        missing_after=1,
        filtered_reason_if_zero="resource_limit_before_target_complete",
    )
    assert _partial_without_progress(summary) is False, (
        "resource_limit_before_target_complete must NOT trigger "
        "archive_gap_partial_no_progress cooldown"
    )


def test_partial_with_no_other_reason_is_still_no_progress() -> None:
    """When aggregates_written=0 and status=partial but there is no
    overriding filtered_reason, the worker should still treat it as
    no-progress (e.g. degraded bucket, genuinely missing data)."""
    summary = _FakeSummary(
        status="partial",
        aggregates_written=0,
        range_bars_written=0,
        missing_after=1,
        filtered_reason_if_zero=None,
    )
    assert _partial_without_progress(summary) is True


def test_partial_with_writes_is_never_no_progress() -> None:
    """Any progress (aggregates_written > 0 or range_bars_written > 0)
    means the worker should NOT exit with cooldown."""
    # Aggregates written
    assert _partial_without_progress(_FakeSummary(
        status="partial", aggregates_written=1, range_bars_written=0,
        missing_after=1,
    )) is False
    # Range bars written
    assert _partial_without_progress(_FakeSummary(
        status="partial", aggregates_written=0, range_bars_written=1,
        missing_after=1,
    )) is False
