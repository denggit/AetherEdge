from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from src.utils.log import get_logger

logger = get_logger(__name__)

DEFAULT_HEARTBEAT_DB = "data/state/aether_runtime_heartbeat.sqlite3"


@dataclass(frozen=True)
class RuntimeHeartbeat:
    """Snapshot of runtime aliveness written to durable storage.

    Used to detect short-crash restarts and to annotate startup catch-up
    decisions.  Normal heartbeat writes are logged at DEBUG only so they
    never flood operator logs.
    """

    runtime_id: str
    pid: int
    started_at_ms: int
    last_alive_ms: int
    last_market_event_ms: int | None
    last_closed_bar_open_time_ms: int | None


class RuntimeHeartbeatStore:
    """SQLite-backed store for the most recent RuntimeHeartbeat row."""

    def __init__(self, db_path: str | Path = DEFAULT_HEARTBEAT_DB) -> None:
        self.db_path = str(db_path)
        self._ensure_table()

    def _ensure_table(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS runtime_heartbeat (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    runtime_id TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    started_at_ms INTEGER NOT NULL,
                    last_alive_ms INTEGER NOT NULL,
                    last_market_event_ms INTEGER,
                    last_closed_bar_open_time_ms INTEGER,
                    updated_at_ms INTEGER NOT NULL
                )"""
            )
            conn.commit()

    def write(self, heartbeat: RuntimeHeartbeat) -> None:
        now_ms = int(time.time() * 1000)
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(
                    """INSERT OR REPLACE INTO runtime_heartbeat
                       (id, runtime_id, pid, started_at_ms, last_alive_ms,
                        last_market_event_ms, last_closed_bar_open_time_ms,
                        updated_at_ms)
                       VALUES (1, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        heartbeat.runtime_id,
                        heartbeat.pid,
                        heartbeat.started_at_ms,
                        heartbeat.last_alive_ms,
                        heartbeat.last_market_event_ms,
                        heartbeat.last_closed_bar_open_time_ms,
                        now_ms,
                    ),
                )
                conn.commit()
            logger.debug(
                "Heartbeat written | runtime_id=%s pid=%s last_alive_ms=%s",
                heartbeat.runtime_id,
                heartbeat.pid,
                heartbeat.last_alive_ms,
            )
        except Exception:
            logger.warning(
                "Heartbeat write failed | runtime_id=%s pid=%s",
                heartbeat.runtime_id,
                heartbeat.pid,
            )

    def read(self) -> RuntimeHeartbeat | None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT * FROM runtime_heartbeat WHERE id = 1"
                ).fetchone()
                if row is None:
                    return None
                return RuntimeHeartbeat(
                    runtime_id=row["runtime_id"],
                    pid=row["pid"],
                    started_at_ms=row["started_at_ms"],
                    last_alive_ms=row["last_alive_ms"],
                    last_market_event_ms=row["last_market_event_ms"],
                    last_closed_bar_open_time_ms=row["last_closed_bar_open_time_ms"],
                )
        except Exception:
            logger.warning("Heartbeat read failed")
            return None

    def delete(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM runtime_heartbeat WHERE id = 1")
                conn.commit()
        except Exception:
            logger.warning("Heartbeat delete failed")


class RuntimeHeartbeatService:
    """Periodically persists a RuntimeHeartbeat row at the configured interval.

    * Normal writes are logged at DEBUG.
    * Write failures are logged at WARNING.
    * Startup read emits a single INFO summary.
    """

    def __init__(
        self,
        *,
        store: RuntimeHeartbeatStore | None = None,
        interval_seconds: float = 15.0,
    ) -> None:
        self._store = store or RuntimeHeartbeatStore()
        self._interval = interval_seconds
        self._runtime_id: str | None = None
        self._pid: int = os.getpid()
        self._started_at_ms: int = int(time.time() * 1000)
        self._last_market_event_ms: int | None = None
        self._last_closed_bar_open_time_ms: int | None = None

    @property
    def store(self) -> RuntimeHeartbeatStore:
        return self._store

    def start(self, *, runtime_id: str) -> None:
        self._runtime_id = runtime_id
        logger.info(
            "Heartbeat service started | runtime_id=%s pid=%s interval_seconds=%s",
            runtime_id,
            self._pid,
            self._interval,
        )

    def read_previous(self) -> RuntimeHeartbeat | None:
        """Read the previous heartbeat stored on disk (from a prior run)."""
        return self._store.read()

    def note_market_event(self, event_time_ms: int | None) -> None:
        self._last_market_event_ms = event_time_ms

    def note_closed_bar(self, open_time_ms: int) -> None:
        self._last_closed_bar_open_time_ms = open_time_ms

    def build(self) -> RuntimeHeartbeat:
        return RuntimeHeartbeat(
            runtime_id=self._runtime_id or "unknown",
            pid=self._pid,
            started_at_ms=self._started_at_ms,
            last_alive_ms=int(time.time() * 1000),
            last_market_event_ms=self._last_market_event_ms,
            last_closed_bar_open_time_ms=self._last_closed_bar_open_time_ms,
        )

    def write_now(self) -> None:
        self._store.write(self.build())

    async def run_periodic(self, stop_event) -> None:
        """Write heartbeat periodically until *stop_event* is set."""
        import asyncio
        while not stop_event.is_set():
            try:
                self.write_now()
            except Exception:
                logger.warning("Heartbeat periodic write failed")
            # Sleeping in small chunks allows graceful shutdown.
            remaining = self._interval
            while remaining > 0 and not stop_event.is_set():
                await asyncio.sleep(min(1.0, remaining))
                remaining -= 1.0
