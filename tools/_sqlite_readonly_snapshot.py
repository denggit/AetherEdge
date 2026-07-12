from __future__ import annotations

import hashlib
import shutil
import sys
import tempfile
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class ReadOnlySnapshotError(RuntimeError):
    pass


@contextmanager
def stable_sqlite_snapshot(
    source: str | Path,
    *,
    attempts: int = 3,
) -> Iterator[Path]:
    """Copy a stable SQLite DB/WAL/SHM set and yield its temporary DB path."""

    source_path = Path(source).expanduser().resolve(strict=False)
    if not source_path.is_file():
        raise ReadOnlySnapshotError(f"database_missing:{source_path}")

    source_files = tuple(
        Path(f"{source_path}{suffix}") for suffix in ("", "-wal", "-shm")
    )
    with tempfile.TemporaryDirectory(
        prefix="aether-preflight-sqlite-"
    ) as raw_temp:
        temp_root = Path(raw_temp)
        for _ in range(attempts):
            before = _sqlite_file_manifest(source_files)
            for old in temp_root.iterdir():
                old.unlink()
            for source_file in source_files:
                if source_file.exists():
                    shutil.copy2(source_file, temp_root / source_file.name)
            after = _sqlite_file_manifest(source_files)
            if before == after:
                yield (temp_root / source_path.name).resolve()
                return
        raise ReadOnlySnapshotError(
            f"database_changed_during_snapshot:{source_path}"
        )


@contextmanager
def stable_sqlite_snapshots(
    sources: Mapping[str, str | Path],
) -> Iterator[dict[str, Path]]:
    """Create one stable temporary snapshot for each named source database."""

    with ExitStack() as stack:
        snapshots = {
            str(name): stack.enter_context(stable_sqlite_snapshot(path))
            for name, path in sources.items()
        }
        yield snapshots


def _sqlite_file_manifest(
    paths: tuple[Path, ...],
) -> tuple[tuple[Any, ...], ...]:
    records: list[tuple[Any, ...]] = []
    for path in paths:
        if not path.exists():
            records.append((path.name, False, None, None, None))
            continue
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        records.append(
            (path.name, True, stat.st_size, stat.st_mtime_ns, digest)
        )
    return tuple(records)


__all__ = [
    "ReadOnlySnapshotError",
    "stable_sqlite_snapshot",
    "stable_sqlite_snapshots",
]
