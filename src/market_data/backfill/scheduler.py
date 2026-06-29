from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from src.market_data.backfill.models import BackfillPlan
from src.platform.exchanges.okx.historical_archive import is_completed_utc_day


@dataclass
class TailCooldownTracker:
    """Tracks which tail buckets are temporarily unavailable.

    When a REST tail fetch fails for a current-UTC-day bucket, that bucket
    enters cooldown so the worker does not waste every cycle retrying it
    while older historical buckets remain unprocessed.
    """

    cooldown_buckets: dict[int, int] = field(default_factory=dict)
    cooldown_seconds: int = 600

    def is_in_cooldown(self, bucket_start_ms: int, now_ms: int) -> bool:
        until = self.cooldown_buckets.get(bucket_start_ms)
        if until is None:
            return False
        return now_ms < until

    def add(self, bucket_start_ms: int, now_ms: int) -> None:
        self.cooldown_buckets[bucket_start_ms] = now_ms + self.cooldown_seconds * 1000

    def clean_expired(self, now_ms: int) -> int:
        """Remove expired cooldown entries. Returns count of entries removed."""
        expired = [k for k, v in self.cooldown_buckets.items() if now_ms >= v]
        for k in expired:
            del self.cooldown_buckets[k]
        return len(expired)

    def cooldown_bucket_starts(self) -> list[int]:
        """Return sorted list of bucket starts currently in cooldown."""
        return sorted(self.cooldown_buckets.keys())

    def to_dict(self) -> dict[str, object]:
        return {
            "cooldown_buckets": {str(k): v for k, v in self.cooldown_buckets.items()},
            "cooldown_seconds": self.cooldown_seconds,
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> TailCooldownTracker:
        raw = d.get("cooldown_buckets", {})
        if isinstance(raw, dict):
            buckets = {int(k): int(v) for k, v in raw.items()}  # type: ignore[arg-type]
        else:
            buckets = {}
        seconds = int(d.get("cooldown_seconds", 600))
        return cls(cooldown_buckets=buckets, cooldown_seconds=seconds)

    @classmethod
    def from_status(cls, status: dict[str, object]) -> TailCooldownTracker:
        cd = status.get("tail_cooldown")
        if isinstance(cd, dict):
            return cls.from_dict(cd)  # type: ignore[arg-type]
        return cls()


def select_candidates(
    plan: BackfillPlan,
    max_buckets: int,
    cooldown_tracker: TailCooldownTracker | None,
    now: datetime,
) -> tuple[list[int], dict[str, object]]:
    """Build a prioritized, cooldown-filtered list of candidate bucket starts.

    Priority order (highest first):

    1. Dirty buckets — always first; must be rebuilt regardless of day.
    2. Non-dirty missing/incomplete buckets in **recency order** (most recent
       first, as emitted by the scanner) — but current-UTC-day tail buckets
       that are in cooldown are **deferred to the end**.
    3. Deferred (cooldown) tail buckets — lowest priority; only attempted
       when no other eligible candidates remain.

    The recency ordering means the most recent (tail) bucket is attempted
    *first*.  If it fails the fallthrough loop in ``process_plan`` continues
    to the next candidate (historical).  On subsequent cycles the tail enters
    cooldown and is deferred, letting historical buckets proceed directly.

    Returns ``(candidates, meta)`` where *candidates* is the ordered list
    and *meta* carries diagnostic counts.
    """
    max_buckets = max(0, int(max_buckets))
    tracker = cooldown_tracker or TailCooldownTracker()
    now_ms = int(now.timestamp() * 1000)

    # 1. Dirty buckets (recency order).
    dirty = list(_recent_unique(plan.dirty_bucket_starts))

    # 2. Non-dirty missing/incomplete — iterate in recency order, classifying
    #    each bucket.  Tail buckets in cooldown are moved to a deferred list
    #    so they do not consume early slots.
    missing_incomplete = [
        start
        for start in _recent_unique(
            [*plan.missing_bucket_starts, *plan.incomplete_coverage_bucket_starts]
        )
        if start not in set(dirty)
    ]

    eligible: list[int] = []
    tail_deferred: list[int] = []
    historical_count = 0
    tail_eligible_count = 0

    for start in missing_incomplete:
        bucket_day = datetime.fromtimestamp(start / 1000, tz=UTC).date()
        if is_completed_utc_day(bucket_day, now=now):
            eligible.append(start)
            historical_count += 1
        elif tracker.is_in_cooldown(start, now_ms):
            tail_deferred.append(start)
        else:
            eligible.append(start)
            tail_eligible_count += 1

    # 3. Assemble: dirty → eligible (recency order) → deferred tail (last).
    candidates: list[int] = []
    candidates.extend(dirty)
    candidates.extend(eligible)
    candidates.extend(tail_deferred)

    # Do NOT cap here.  The caller (process_plan) iterates candidates in
    # order and stops once max_buckets have been successfully processed.
    # Capping would prevent fallthrough when early candidates fail.

    cooldown_starts = tracker.cooldown_bucket_starts()

    meta: dict[str, object] = {
        "total": len(candidates),
        "historical": historical_count,
        "tail": tail_eligible_count,
        "dirty": len(dirty),
        "cooldown": cooldown_starts,
        "deferred": tail_deferred,
    }
    return candidates, meta


def _recent_unique(values: tuple[int, ...] | list[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out
