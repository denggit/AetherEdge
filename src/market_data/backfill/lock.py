from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from src.market_data.backfill.status_store import (
    RangeBackfillStatusStore,
    now_ms,
    process_id_exists,
    worker_heartbeat_ms,
)


class RangeBackfillLock:
    def __init__(
        self,
        path: str | Path = "data/state/range_backfill.lock",
        *,
        status_path: str | Path | None = "data/state/range_backfill_status.json",
        stale_after_seconds: int = 180,
    ) -> None:
        self.path = Path(path)
        self.status_path = None if status_path is None else Path(status_path)
        self.stale_after_ms = max(0, int(stale_after_seconds)) * 1000
        self.acquired = False

    def acquire(self, *, mode: str, force: bool = False) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"pid": os.getpid(), "started_at_ms": now_ms(), "mode": mode})
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(self.path), flags)
        except FileExistsError:
            if not force and not self._existing_is_stale():
                return False
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            fd = os.open(str(self.path), flags)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        self.acquired = True
        return True

    def release(self) -> None:
        if not self.acquired:
            return
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        finally:
            self.acquired = False

    def __enter__(self) -> "RangeBackfillLock":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()

    def _existing_is_stale(self) -> bool:
        if self.status_path is not None:
            status = RangeBackfillStatusStore(self.status_path).read()
            if status is not None:
                if status.get("running") and process_id_exists(status.get("pid")) is False:
                    return True
                heartbeat = worker_heartbeat_ms(status)
                running = bool(status.get("running"))
                if heartbeat is not None:
                    return (not running) or (now_ms() - int(heartbeat) > self.stale_after_ms)
        try:
            age_ms = now_ms() - int(self.path.stat().st_mtime * 1000)
        except OSError:
            return True
        return age_ms > self.stale_after_ms
