from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RangeBucketIntegrityStatus(str, Enum):
    CLEAN = "clean"
    DEGRADED = "degraded"
    REPAIRING = "repairing"
    REPAIRED = "repaired"


@dataclass
class RangeBucketIntegrityState:
    status: RangeBucketIntegrityStatus = RangeBucketIntegrityStatus.CLEAN
    last_issue_revision: int = 0
    repaired_through_revision: int = 0
    repair_started_revision: int | None = None
    reason: str | None = None

    @property
    def complete(self) -> bool:
        return (self.status is RangeBucketIntegrityStatus.CLEAN and self.last_issue_revision == 0) or (
            self.status is RangeBucketIntegrityStatus.REPAIRED
            and self.last_issue_revision <= self.repaired_through_revision
        )
