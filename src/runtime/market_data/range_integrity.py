from __future__ import annotations

from collections.abc import Iterator, MutableMapping, MutableSet
from dataclasses import dataclass
from typing import Any


@dataclass
class RangeBucketIntegrityState:
    last_issue_revision: int = 0
    repaired_through_revision: int = 0
    reason: str | None = None

    @property
    def complete(self) -> bool:
        return self.last_issue_revision <= self.repaired_through_revision


class DegradedBucketView(MutableMapping[int, str]):
    def __init__(self, owner: Any) -> None:
        self._owner = owner

    def __getitem__(self, key: int) -> str:
        reason = self._owner.degraded_reason(int(key))
        if reason is None:
            raise KeyError(key)
        return reason

    def __setitem__(self, key: int, value: str) -> None:
        self._owner.mark_degraded(bucket_start_ms=int(key), reason=str(value))

    def __delitem__(self, key: int) -> None:
        state = self._owner._bucket_states.get(int(key))
        if state is None or state.complete:
            raise KeyError(key)
        state.repaired_through_revision = state.last_issue_revision
        state.reason = None

    def __iter__(self) -> Iterator[int]:
        return iter(
            key for key, state in self._owner._bucket_states.items()
            if not state.complete
        )

    def __len__(self) -> int:
        return sum(not state.complete for state in self._owner._bucket_states.values())


class RepairedBucketView(MutableSet[int]):
    def __init__(self, owner: Any) -> None:
        self._owner = owner

    def __contains__(self, value: object) -> bool:
        state = self._owner._bucket_states.get(value) if isinstance(value, int) else None
        return state is not None and state.complete and state.repaired_through_revision > 0

    def __iter__(self) -> Iterator[int]:
        return iter(
            key for key, state in self._owner._bucket_states.items()
            if state.complete and state.repaired_through_revision > 0
        )

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def add(self, value: int) -> None:
        state = self._owner._bucket_state(int(value))
        state.last_issue_revision = max(state.last_issue_revision, 1)
        state.repaired_through_revision = max(
            state.repaired_through_revision,
            state.last_issue_revision,
        )
        state.reason = None

    def discard(self, value: int) -> None:
        state = self._owner._bucket_states.get(int(value))
        if state is not None and state.repaired_through_revision > 0:
            state.repaired_through_revision = 0
