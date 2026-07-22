from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RangeBucketIntegrityState:
    last_issue_revision: int = 0
    repaired_through_revision: int = 0
    repair_started_revision: int | None = None
    reason: str | None = None

    @property
    def complete(self) -> bool:
        return self.last_issue_revision <= self.repaired_through_revision
