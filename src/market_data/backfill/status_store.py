from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Mapping


def now_ms() -> int:
    return int(time.time() * 1000)


class RangeBackfillStatusStore:
    def __init__(self, path: str | Path = "data/state/range_backfill_status.json") -> None:
        self.path = Path(path)

    def read(self) -> dict[str, Any] | None:
        try:
            if not self.path.exists():
                return None
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def write(self, values: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": 1, **dict(values), "updated_at_ms": now_ms()}
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def patch(self, **updates: Any) -> None:
        current = self.read() or {}
        current.update(updates)
        self.write(current)
