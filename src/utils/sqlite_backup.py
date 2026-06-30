from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import sqlite3
from typing import Callable

DEFAULT_SQLITE_BACKUP_KEEP = 5


def backup_sqlite_database(
    source_path: str | Path,
    *,
    backup_dir: str | Path | None = None,
    keep: int = DEFAULT_SQLITE_BACKUP_KEEP,
    before_backup: Callable[[Path], None] | None = None,
) -> Path:
    source = Path(source_path)
    if not source.is_file():
        raise FileNotFoundError(source)
    target_dir = Path(backup_dir) if backup_dir is not None else source.parent / "backups"
    target_dir.mkdir(parents=True, exist_ok=True)
    label = _safe_backup_label(source.stem)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    target = target_dir / f"{label}.{timestamp}.sqlite3"
    if before_backup is not None:
        before_backup(target)
    with sqlite3.connect(source) as src, sqlite3.connect(target) as dst:
        src.backup(dst)
    _remove_sqlite_sidecars(target)
    _prune_old_backups(target_dir=target_dir, label=label, keep=keep)
    return target


def _prune_old_backups(*, target_dir: Path, label: str, keep: int) -> None:
    limit = max(1, int(keep))
    backups = sorted(
        target_dir.glob(f"{label}.*.sqlite3"),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
        reverse=True,
    )
    for stale in backups[limit:]:
        _remove_sqlite_sidecars(stale)
        try:
            stale.unlink()
        except FileNotFoundError:
            pass


def _remove_sqlite_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm"):
        try:
            path.with_name(path.name + suffix).unlink()
        except FileNotFoundError:
            pass


def _safe_backup_label(value: str) -> str:
    label = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value).strip())
    return label.strip("._-") or "sqlite"
