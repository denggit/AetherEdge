from __future__ import annotations

import json
import logging
import os
import secrets
from pathlib import Path
from typing import Any, Mapping

from src.market_data.backfill.status_store import now_ms, process_id_exists, worker_heartbeat_ms

logger = logging.getLogger(__name__)

# Priority constants: higher = wins.
EXPEDITED_BACKFILL_PRIORITY = 100
BACKGROUND_BACKFILL_PRIORITY = 10

DEFAULT_GLOBAL_LOCK_PATH = "data/state/raw_trade_backfill_global.lock"
DEFAULT_GLOBAL_STATUS_PATH = "data/state/raw_trade_backfill_global_status.json"


class RawTradeBackfillCoordinator:
    """Coordinate workers sharing the same raw-trade archives.

    Priority rules:
    - Higher-priority work takes precedence over background work.
    - Active holders cannot be evicted unless stale or explicitly forced.
    - Stale lower-priority workers may be evicted.
    - All workers must release the lock on exit.
    """

    def __init__(
        self,
        lock_path: str | Path = DEFAULT_GLOBAL_LOCK_PATH,
        status_path: str | Path = DEFAULT_GLOBAL_STATUS_PATH,
        stale_after_seconds: int = 180,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.status_path = Path(status_path)
        self.stale_after_ms = max(0, int(stale_after_seconds)) * 1000
        self._acquired = False
        self._token: str | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def try_acquire(
        self,
        *,
        owner: str,
        priority: int,
        symbol: str = "",
        raw_days: int = 0,
        force: bool = False,
    ) -> bool:
        """Attempt to acquire the global raw-trade backfill lock.

        Returns True if the lock is acquired. If a higher-priority worker
        holds it, returns False. If a lower-priority worker holds it and
        is stale, evicts and takes over.
        """
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

        existing = self._read_lock()
        if existing is None and self.lock_path.exists():
            # Another process may still be atomically creating the file, or a
            # crashed process may have left a malformed lock behind.
            try:
                age_ms = now_ms() - int(self.lock_path.stat().st_mtime * 1000)
            except OSError:
                return False
            if age_ms <= self.stale_after_ms:
                return False
            self._delete_lock()
            self._delete_status()
        if existing is not None:
            existing_priority = int(existing.get("priority", 0))
            if existing_priority > priority:
                logger.info(
                    "raw-trade global lock held by higher-priority worker | "
                    "owner=%s priority=%s my_priority=%s",
                    existing.get("owner", "?"),
                    existing_priority,
                    priority,
                )
                return False

            if existing_priority == priority and not force:
                if not self._is_stale(existing):
                    logger.info(
                        "raw-trade global lock held by same-priority worker | owner=%s",
                        existing.get("owner", "?"),
                    )
                    return False
                logger.warning(
                    "raw-trade global lock: evicting stale same-priority worker | owner=%s",
                    existing.get("owner", "?"),
                )
            elif existing_priority < priority:
                if not self._is_stale(existing) and not force:
                    logger.info(
                        "raw-trade global lock: waiting for lower-priority worker | "
                        "owner=%s priority=%s my_priority=%s",
                        existing.get("owner", "?"),
                        existing_priority,
                        priority,
                    )
                    return False
                logger.info(
                    "raw-trade global lock: evicting stale lower-priority worker | owner=%s",
                    existing.get("owner", "?"),
                )

            # Release stale lock
            self._delete_lock()
            self._delete_status()

        self._token = secrets.token_hex(16)
        try:
            self._write_lock(
                owner=owner,
                priority=priority,
                symbol=symbol,
                raw_days=raw_days,
            )
        except FileExistsError:
            # Lost the simultaneous-create race to another worker.
            self._token = None
            return False
        try:
            self._write_status(
                owner=owner,
                priority=priority,
                symbol=symbol,
                raw_days=raw_days,
            )
        except Exception:
            self._delete_lock()
            self._token = None
            raise
        self._acquired = True
        return True

    def release(self) -> None:
        """Release the global lock."""
        if not self._acquired:
            return
        existing = self._read_lock()
        if existing is not None and existing.get("token") == self._token:
            self._delete_lock()
            self._finish_status()
        self._acquired = False
        self._token = None

    def heartbeat(self) -> None:
        """Update the status heartbeat."""
        if not self._acquired:
            return
        self._update_status_heartbeat()

    @property
    def is_held(self) -> bool:
        return self._acquired or self.lock_path.exists()

    def current_owner(self) -> Mapping[str, Any] | None:
        return self._read_lock()

    def has_fresh_holder(self) -> bool:
        """Return whether the lock is held by a non-stale worker."""
        if not self.lock_path.exists():
            return False
        existing = self._read_lock()
        if existing is not None:
            return not self._is_stale(existing)
        try:
            age_ms = now_ms() - int(self.lock_path.stat().st_mtime * 1000)
        except OSError:
            return True
        return age_ms <= self.stale_after_ms

    def status(self) -> Mapping[str, Any] | None:
        try:
            if not self.status_path.exists():
                return None
            data = json.loads(self.status_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except (OSError, json.JSONDecodeError):
            return None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _is_stale(self, lock_data: Mapping[str, Any]) -> bool:
        pid = lock_data.get("pid")
        if pid is not None and process_id_exists(pid) is False:
            return True

        status = self.status()
        if status is not None:
            heartbeat = worker_heartbeat_ms(status)
            if heartbeat is not None:
                return (now_ms() - heartbeat) > self.stale_after_ms

        try:
            age_ms = now_ms() - int(self.lock_path.stat().st_mtime * 1000)
        except OSError:
            return True
        return age_ms > self.stale_after_ms

    def _read_lock(self) -> dict[str, Any] | None:
        try:
            if not self.lock_path.exists():
                return None
            data = json.loads(self.lock_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except (OSError, json.JSONDecodeError):
            return None

    def _write_lock(self, *, owner: str, priority: int, symbol: str, raw_days: int) -> None:
        payload = {
            "pid": os.getpid(),
            "token": self._token,
            "owner": owner,
            "priority": int(priority),
            "symbol": symbol,
            "raw_days": int(raw_days),
            "started_at_ms": now_ms(),
        }
        fd = os.open(
            self.lock_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)

    def _write_status(self, *, owner: str, priority: int, symbol: str, raw_days: int) -> None:
        payload = {
            "version": 1,
            "pid": os.getpid(),
            "token": self._token,
            "owner": owner,
            "priority": int(priority),
            "symbol": symbol,
            "raw_days": int(raw_days),
            "running": True,
            "worker_heartbeat_ms": now_ms(),
            "started_at_ms": now_ms(),
        }
        self.status_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.status_path.with_name(self.status_path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.status_path)

    def _update_status_heartbeat(self) -> None:
        try:
            current = self.status()
            if current is None:
                return
            if current.get("token") != self._token:
                return
            current["worker_heartbeat_ms"] = now_ms()
            tmp = self.status_path.with_name(self.status_path.name + ".tmp")
            tmp.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(tmp, self.status_path)
        except OSError:
            pass

    def _finish_status(self) -> None:
        try:
            current = self.status()
            if current is None or current.get("token") != self._token:
                return
            current["running"] = False
            current["worker_heartbeat_ms"] = now_ms()
            current["finished_at_ms"] = now_ms()
            tmp = self.status_path.with_name(self.status_path.name + ".tmp")
            tmp.write_text(
                json.dumps(current, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(tmp, self.status_path)
        except OSError:
            pass

    def _delete_lock(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def _delete_status(self) -> None:
        try:
            self.status_path.unlink()
        except FileNotFoundError:
            pass

    def __enter__(self) -> "RawTradeBackfillCoordinator":
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()
