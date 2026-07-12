from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlsplit


BLOCKED_MESSAGE = "pytest blocked write access to repository runtime database"
_ORIGINAL_CONNECT: Callable[..., sqlite3.Connection] | None = None


def install_sqlite_guard(*, repo_root: str | Path, pytest_root: str | Path) -> None:
    """Block writable SQLite connections into repository runtime directories."""

    global _ORIGINAL_CONNECT
    if getattr(sqlite3.connect, "_aether_pytest_guard", False):
        return

    repo = Path(repo_root).resolve()
    allowed = Path(pytest_root).resolve()
    blocked_roots = tuple(
        (repo / relative).resolve()
        for relative in (Path("data/state"), Path("data/market_data"))
    )
    original = sqlite3.connect
    _ORIGINAL_CONNECT = original

    def guarded_connect(database: Any, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        path, read_only = resolve_sqlite_target(
            database,
            uri=bool(kwargs.get("uri", False)),
        )
        if path is not None and not read_only:
            if any(_is_within(path, blocked) for blocked in blocked_roots):
                raise RuntimeError(f"{BLOCKED_MESSAGE}: {path}")
            if not _is_within(path, allowed):
                raise RuntimeError(
                    "pytest blocked write access outside the temporary directory: "
                    f"{path}"
                )
        return original(database, *args, **kwargs)

    guarded_connect._aether_pytest_guard = True  # type: ignore[attr-defined]
    sqlite3.connect = guarded_connect  # type: ignore[assignment]


def uninstall_sqlite_guard() -> None:
    global _ORIGINAL_CONNECT
    if _ORIGINAL_CONNECT is not None:
        sqlite3.connect = _ORIGINAL_CONNECT  # type: ignore[assignment]
        _ORIGINAL_CONNECT = None


def resolve_sqlite_target(
    database: Any,
    *,
    uri: bool = False,
) -> tuple[Path | None, bool]:
    """Return the resolved filesystem target and whether the URI is read-only."""

    if isinstance(database, bytes):
        raw = os.fsdecode(database)
    else:
        raw = os.fspath(database)
    if raw == ":memory:":
        return None, False

    is_file_uri = raw.casefold().startswith("file:")
    if is_file_uri:
        parsed = urlsplit(raw)
        query = parse_qs(parsed.query, keep_blank_values=True)
        mode_values = [value.casefold() for value in query.get("mode", ())]
        read_only = uri and mode_values == ["ro"]
        uri_path = unquote(parsed.path)
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            uri_path = f"//{parsed.netloc}{uri_path}"
        # urlsplit("file:C:/x") preserves C:/x without a leading slash, while
        # urlsplit("file:///C:/x") returns /C:/x on Windows.
        if os.name == "nt" and len(uri_path) >= 3 and uri_path[0] == "/" and uri_path[2] == ":":
            uri_path = uri_path[1:]
        raw_path = uri_path
    else:
        read_only = False
        raw_path = raw

    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve(strict=False), read_only


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False
