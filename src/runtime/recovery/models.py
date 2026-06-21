from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class RecoveryReport:
    """Generic runtime recovery report.

    Strategy-specific recovery remains inside strategy plugins. This report is
    intentionally generic so runtime does not learn V8 internals.
    """

    ok: bool
    issues: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
