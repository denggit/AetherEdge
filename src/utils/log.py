#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Project logging helpers.

The logging setup is intentionally small and boring:

- one root logger for the whole process;
- console output can be switched on/off;
- file output writes to ``logs/app.log`` by default;
- when a day ends, the previous day's file is rotated and gzip-compressed;
- old rotated logs are removed after the configured retention window.

Environment variables:

- ``LOG_LEVEL``: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default: INFO.
- ``LOG_DIR``: directory for log files. Default: logs.
- ``LOG_FILE_NAME``: active log file name. Default: app.log.
- ``LOG_TO_CONSOLE``: true/false. Default: false.
- ``LOG_TO_FILE``: true/false. Default: true.
- ``LOG_RETENTION_DAYS``: rotated log retention. Default: 14.
"""
from __future__ import annotations

import gzip
import logging
import logging.handlers
import os
import shutil
import sys
from pathlib import Path

_SETUP_DONE = False
_ENV_LOADED = False


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_log_env() -> None:
    """Load LOG_* values from .env without overriding real environment vars."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True

    env_file = _project_root() / ".env"
    if not env_file.exists():
        return

    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key.startswith("LOG_"):
                continue
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
    except OSError:
        return


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw not in (None, "") else default
    except ValueError:
        value = default
    return max(value, minimum)


def _level_env(default: int = logging.INFO) -> int:
    value = os.getenv("LOG_LEVEL", "").strip().upper()
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.CRITICAL,
    }.get(value, default)


def _resolve_log_dir(log_dir: str) -> Path:
    path = Path(os.getenv("LOG_DIR", log_dir)).expanduser()
    if not path.is_absolute():
        path = _project_root() / path
    return path


def _gzip_rotated_log(source: str, dest: str) -> None:
    """Compress a rotated log file and remove the uncompressed source."""
    with open(source, "rb") as src, gzip.open(dest, "wb") as dst:
        shutil.copyfileobj(src, dst)
    try:
        os.remove(source)
    except OSError:
        pass


class GzipTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """Timed rotating handler that keeps gzip archives under backupCount."""

    def getFilesToDelete(self) -> list[str]:  # noqa: N802 - stdlib override
        dir_name, base_name = os.path.split(self.baseFilename)
        prefix = f"{base_name}."
        candidates = []
        for file_name in os.listdir(dir_name):
            if not file_name.startswith(prefix):
                continue
            suffix = file_name[len(prefix) :]
            if suffix.endswith(".gz"):
                suffix = suffix[:-3]
            if self.extMatch.match(suffix):
                candidates.append(os.path.join(dir_name, file_name))
        candidates.sort()
        if len(candidates) <= self.backupCount:
            return []
        return candidates[: len(candidates) - self.backupCount]


def setup_logging(log_level: int | None = None, log_dir: str = "logs") -> None:
    """Configure root logging once for the current process."""
    global _SETUP_DONE
    if _SETUP_DONE:
        return

    _load_log_env()

    effective_level = _level_env() if log_level is None else log_level
    log_to_console = _bool_env("LOG_TO_CONSOLE", False)
    log_to_file = _bool_env("LOG_TO_FILE", True)
    if not log_to_console and not log_to_file:
        log_to_file = True

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(effective_level)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(effective_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    if log_to_file:
        resolved_log_dir = _resolve_log_dir(log_dir)
        resolved_log_dir.mkdir(parents=True, exist_ok=True)

        log_file_name = os.getenv("LOG_FILE_NAME", "app.log")
        retention_days = _int_env("LOG_RETENTION_DAYS", 14, minimum=1)
        file_handler = GzipTimedRotatingFileHandler(
            filename=str(resolved_log_dir / log_file_name),
            when="midnight",
            interval=1,
            backupCount=retention_days,
            encoding="utf-8",
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.namer = lambda name: f"{name}.gz"
        file_handler.rotator = _gzip_rotated_log
        file_handler.setLevel(effective_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    _SETUP_DONE = True
    root_logger.info(
        "Logging initialized | level=%s console=%s file=%s dir=%s file_name=%s retention_days=%s",
        logging.getLevelName(effective_level),
        log_to_console,
        log_to_file,
        str(_resolve_log_dir(log_dir)),
        os.getenv("LOG_FILE_NAME", "app.log"),
        _int_env("LOG_RETENTION_DAYS", 14, minimum=1),
    )


def get_logger(name: str) -> logging.Logger:
    """Return a configured module logger."""
    setup_logging()
    return logging.getLogger(name)


logger = get_logger(__name__)
