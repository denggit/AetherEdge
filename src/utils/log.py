#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日志模块，提供按日期分割的日志文件功能。

默认行为：
- 写入文件，不输出到 console；
- 每天一个日志文件；
- 默认保留最近 7 天日志，过期文件在启动和日期切换时由 TimedRotatingFileHandler 清理；
- 默认启用异步日志队列，业务线程只入队，后台线程写文件，降低 tick path I/O 阻塞。

可用环境变量：
- LOG_LEVEL=INFO
- LOG_DIR=logs
- LOG_FILE_NAME=app.log
- LOG_TO_CONSOLE=false
- LOG_TO_FILE=true
- LOG_RETENTION_DAYS=7
- LOG_BACKUP_COUNT=7
- LOG_ASYNC_ENABLED=true
- LOG_ASYNC_QUEUE_MAXSIZE=10000
- LOG_HOT_PATH_THROTTLE_ENABLED=true
- LOG_ARMED_EXTREME_UPDATE_THROTTLE_SECONDS=1
- LOG_ADD_SKIPPED_THROTTLE_SECONDS=5
"""
from __future__ import annotations

import atexit
import copy
import datetime as dt
import logging
import logging.handlers
import os
import queue as queue_module
import re
import sys
import time
from pathlib import Path

_setup_done = False
_env_loaded_for_logging = False
_queue_listener: logging.handlers.QueueListener | None = None


def _load_dotenv_for_logging() -> None:
    """Load LOG_* variables from project .env before logging is configured.

    Most live modules import loggers at module import time, which can happen before
    scripts call load_dotenv(). This lightweight parser only sets missing LOG_*/NUMBA_*
    values so process-level environment variables still win.
    """
    global _env_loaded_for_logging
    if _env_loaded_for_logging:
        return
    _env_loaded_for_logging = True

    root = Path(__file__).resolve().parents[2]
    env_file = root / ".env"
    if not env_file.exists():
        return

    allowed_prefixes = ("LOG_", "NUMBA_LOG_LEVEL")
    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key.startswith(allowed_prefixes):
                continue
            value = value.strip()
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            os.environ.setdefault(key, value)
    except OSError:
        # Logging bootstrap must never stop the trading process.
        return


def _bool_env(name: str, default: bool = True) -> bool:
    value = os.environ.get(name)
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return bool(default)


def _int_env(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        value = default
    else:
        try:
            value = int(str(raw).strip())
        except ValueError:
            value = default
    if minimum is not None:
        value = max(value, minimum)
    return value


def _float_env(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        value = default
    else:
        try:
            value = float(str(raw).strip())
        except ValueError:
            value = default
    if minimum is not None:
        value = max(value, minimum)
    return value


def _get_log_level_from_env(default_level: int = logging.INFO) -> int:
    log_level_str = os.environ.get("LOG_LEVEL", "").upper()
    if not log_level_str:
        return default_level

    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.CRITICAL,
    }
    return level_map.get(log_level_str, default_level)


class NonBlockingQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler that never blocks the caller when the log queue is full.

    Unlike stdlib QueueHandler.prepare(), this handler does not format the message
    in the caller thread. Formatting and file I/O are left to QueueListener target
    handlers, which keeps the live tick path lighter.
    """

    def __init__(self, log_queue: queue_module.Queue[logging.LogRecord]):
        super().__init__(log_queue)
        self.dropped_count = 0

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        # Queue is in-process, so records do not need to be pickle-safe. Copy the
        # record to avoid target handlers mutating the caller's LogRecord, but keep
        # msg/args/exc_info intact so formatting happens in the listener thread.
        return copy.copy(record)

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue_module.Full:
            self.dropped_count += 1
            return


class HotPathThrottleFilter(logging.Filter):
    """Throttle noisy INFO logs emitted from tick-path strategy code.

    The filter is intentionally based on record.msg templates, so it does not call
    record.getMessage() and does not force string formatting on the hot path.
    Business logic and strategy state updates are untouched; only repetitive log
    records are dropped before they reach the file writer.
    """

    def __init__(self) -> None:
        super().__init__()
        self.enabled = _bool_env("LOG_HOT_PATH_THROTTLE_ENABLED", True)
        self.armed_extreme_update_interval_seconds = _float_env(
            "LOG_ARMED_EXTREME_UPDATE_THROTTLE_SECONDS",
            1.0,
            minimum=0.0,
        )
        self.add_skipped_interval_seconds = _float_env(
            "LOG_ADD_SKIPPED_THROTTLE_SECONDS",
            5.0,
            minimum=0.0,
        )
        self._last_by_key: dict[str, float] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if not self.enabled or record.levelno >= logging.WARNING:
            return True

        msg = str(record.msg)
        key: str | None = None
        interval = 0.0

        if msg.startswith("LOWER_ARMED_EXTREME_UPDATED"):
            key = "LOWER_ARMED_EXTREME_UPDATED"
            interval = self.armed_extreme_update_interval_seconds
        elif msg.startswith("UPPER_ARMED_EXTREME_UPDATED"):
            key = "UPPER_ARMED_EXTREME_UPDATED"
            interval = self.armed_extreme_update_interval_seconds
        elif msg.startswith("ADD_SKIPPED | reason=add_gap"):
            key = "ADD_SKIPPED:add_gap"
            interval = self.add_skipped_interval_seconds
        elif msg.startswith("ADD_SKIPPED | reason=avg_improvement"):
            key = "ADD_SKIPPED:avg_improvement"
            interval = self.add_skipped_interval_seconds
        elif msg.startswith("ADD_SKIPPED | reason=add_interval"):
            key = "ADD_SKIPPED:add_interval"
            interval = self.add_skipped_interval_seconds
        elif msg.startswith("ADD_SKIPPED | reason=first_add_block"):
            key = "ADD_SKIPPED:first_add_block"
            interval = self.add_skipped_interval_seconds

        if key is None or interval <= 0:
            return True

        now = time.monotonic()
        last = self._last_by_key.get(key)
        if last is not None and now - last < interval:
            return False
        self._last_by_key[key] = now
        return True


def _cleanup_expired_daily_logs(log_dir: str, log_file_name: str, retention_days: int) -> None:
    """Best-effort cleanup for daily rotated files older than retention_days."""
    if retention_days <= 0:
        return
    directory = Path(log_dir)
    if not directory.exists():
        return

    cutoff_date = dt.date.today() - dt.timedelta(days=retention_days)
    pattern = re.compile(rf"^{re.escape(log_file_name)}\.(\d{{4}}-\d{{2}}-\d{{2}})(?:\..*)?$")
    for path in directory.iterdir():
        if not path.is_file():
            continue
        match = pattern.match(path.name)
        if not match:
            continue
        try:
            file_date = dt.datetime.strptime(match.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        if file_date < cutoff_date:
            try:
                path.unlink()
            except OSError:
                # 日志清理失败不应该影响交易主程序启动。
                pass


def _stop_queue_listener() -> None:
    global _queue_listener
    listener = _queue_listener
    if listener is None:
        return
    try:
        listener.stop()
    except Exception:
        pass
    _queue_listener = None


def setup_logging(log_level: int | None = None, log_dir: str = "logs") -> None:
    """
    配置根日志记录器。

    Args:
        log_level: 日志级别，如果为 None 则从 LOG_LEVEL 读取，默认为 INFO。
        log_dir: 日志目录，默认 logs，可被 LOG_DIR 覆盖。
    """
    global _setup_done, _queue_listener
    if _setup_done:
        return

    _load_dotenv_for_logging()

    if log_level is None:
        log_level = _get_log_level_from_env(logging.INFO)

    _stop_queue_listener()

    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    log_to_console = _bool_env("LOG_TO_CONSOLE", False)
    log_to_file = _bool_env("LOG_TO_FILE", True)
    log_async_enabled = _bool_env("LOG_ASYNC_ENABLED", True)
    async_queue_maxsize = _int_env("LOG_ASYNC_QUEUE_MAXSIZE", 10000, minimum=100)
    effective_log_dir = os.environ.get("LOG_DIR", log_dir)
    log_file_name = os.environ.get("LOG_FILE_NAME", "app.log")
    retention_days = _int_env("LOG_RETENTION_DAYS", 7, minimum=1)
    backup_count = _int_env("LOG_BACKUP_COUNT", retention_days, minimum=1)
    if not log_to_console and not log_to_file:
        log_to_file = True

    root_logger.setLevel(log_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    target_handlers: list[logging.Handler] = []
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        target_handlers.append(console_handler)

    if log_to_file:
        os.makedirs(effective_log_dir, exist_ok=True)
        _cleanup_expired_daily_logs(effective_log_dir, log_file_name, retention_days)
        log_file = os.path.join(effective_log_dir, log_file_name)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        target_handlers.append(file_handler)

    hot_path_filter = HotPathThrottleFilter()
    if log_async_enabled:
        log_queue: queue_module.Queue[logging.LogRecord] = queue_module.Queue(maxsize=async_queue_maxsize)
        queue_handler = NonBlockingQueueHandler(log_queue)
        queue_handler.setLevel(log_level)
        queue_handler.addFilter(hot_path_filter)
        root_logger.addHandler(queue_handler)
        _queue_listener = logging.handlers.QueueListener(log_queue, *target_handlers, respect_handler_level=True)
        _queue_listener.start()
        atexit.register(_stop_queue_listener)
    else:
        for handler in target_handlers:
            handler.addFilter(hot_path_filter)
            root_logger.addHandler(handler)

    numba_log_level_str = os.environ.get("NUMBA_LOG_LEVEL", "").upper()
    if numba_log_level_str:
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        numba_log_level = level_map.get(numba_log_level_str, logging.WARNING)
    else:
        numba_log_level = logging.WARNING if log_level <= logging.WARNING else log_level

    logging.getLogger("numba").setLevel(numba_log_level)
    for module_name in ["numba.core.ssa", "numba.core.byteflow", "numba.core.interpreter"]:
        logging.getLogger(module_name).setLevel(logging.WARNING)

    root_logger.info(
        "日志系统初始化完成 | log_dir=%s log_file=%s console=%s file=%s async=%s async_queue_maxsize=%s level=%s retention_days=%s backup_count=%s hot_path_throttle=%s",
        os.path.abspath(effective_log_dir),
        log_file_name,
        log_to_console,
        log_to_file,
        log_async_enabled,
        async_queue_maxsize if log_async_enabled else 0,
        logging.getLevelName(log_level),
        retention_days,
        backup_count,
        hot_path_filter.enabled,
    )
    root_logger.debug("Numba日志级别: %s", numba_log_level)

    _setup_done = True


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的 logger。"""
    setup_logging(None)
    return logging.getLogger(name)


setup_logging(None)
logger = get_logger(__name__)
