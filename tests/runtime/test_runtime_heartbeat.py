from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
from io import StringIO

import pytest

from src.runtime.heartbeat import (
    RuntimeHeartbeat,
    RuntimeHeartbeatService,
    RuntimeHeartbeatStore,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _temp_db_path() -> str:
    fd, path = tempfile.mkstemp(suffix=".sqlite3", prefix="test_hb_")
    os.close(fd)
    return path


def _sample_heartbeat(**overrides) -> RuntimeHeartbeat:
    kwargs = dict(
        runtime_id="test::ETH-USDT-PERP",
        pid=1234,
        started_at_ms=1_000_000,
        last_alive_ms=2_000_000,
        last_market_event_ms=None,
        last_closed_bar_open_time_ms=None,
    )
    kwargs.update(overrides)
    return RuntimeHeartbeat(**kwargs)


# ── Tests: store ─────────────────────────────────────────────────────────────


def test_runtime_heartbeat_store_write_and_read():
    path = _temp_db_path()
    try:
        store = RuntimeHeartbeatStore(path)
        hb = _sample_heartbeat()
        store.write(hb)
        got = store.read()
        assert got is not None
        assert got.runtime_id == "test::ETH-USDT-PERP"
        assert got.pid == 1234
        assert got.started_at_ms == 1_000_000
        assert got.last_alive_ms == 2_000_000
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_runtime_heartbeat_store_overwrites_previous():
    path = _temp_db_path()
    try:
        store = RuntimeHeartbeatStore(path)
        hb1 = _sample_heartbeat(runtime_id="first")
        store.write(hb1)
        hb2 = _sample_heartbeat(runtime_id="second")
        store.write(hb2)
        got = store.read()
        assert got is not None
        assert got.runtime_id == "second"
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_runtime_heartbeat_store_read_empty_returns_none():
    path = _temp_db_path()
    try:
        store = RuntimeHeartbeatStore(path)
        assert store.read() is None
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_runtime_heartbeat_store_delete():
    path = _temp_db_path()
    try:
        store = RuntimeHeartbeatStore(path)
        store.write(_sample_heartbeat())
        assert store.read() is not None
        store.delete()
        assert store.read() is None
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# ── Tests: heartbeat no INFO log ─────────────────────────────────────────────


def test_runtime_heartbeat_write_does_not_log_info():
    """Normal heartbeat writes must not emit INFO-level log records."""
    path = _temp_db_path()
    try:
        store = RuntimeHeartbeatStore(path)

        # Capture log output at INFO level.
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.INFO)
        logger = logging.getLogger("src.runtime.heartbeat")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            store.write(_sample_heartbeat())
            handler.flush()
            output = stream.getvalue()
            # At INFO level, normal heartbeat writes should produce no output.
            assert "Heartbeat written" not in output, (
                f"INFO log emitted but should be DEBUG only: {output}"
            )
        finally:
            logger.removeHandler(handler)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# ── Tests: service ───────────────────────────────────────────────────────────


def test_runtime_heartbeat_service_build():
    service = RuntimeHeartbeatService(store=RuntimeHeartbeatStore(_temp_db_path()))
    service.start(runtime_id="svc::test")
    hb = service.build()
    assert hb.runtime_id == "svc::test"
    assert hb.pid == os.getpid()
    assert hb.last_alive_ms > 0


def test_runtime_heartbeat_service_read_previous():
    path = _temp_db_path()
    try:
        store = RuntimeHeartbeatStore(path)
        store.write(_sample_heartbeat(runtime_id="prev-run"))
        service = RuntimeHeartbeatService(store=store)
        prev = service.read_previous()
        assert prev is not None
        assert prev.runtime_id == "prev-run"
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def test_runtime_heartbeat_service_note_market_event():
    service = RuntimeHeartbeatService(store=RuntimeHeartbeatStore(_temp_db_path()))
    service.start(runtime_id="test")
    service.note_market_event(5000)
    hb = service.build()
    assert hb.last_market_event_ms == 5000


def test_runtime_heartbeat_service_note_closed_bar():
    service = RuntimeHeartbeatService(store=RuntimeHeartbeatStore(_temp_db_path()))
    service.start(runtime_id="test")
    service.note_closed_bar(8 * 60 * 60_000)
    hb = service.build()
    assert hb.last_closed_bar_open_time_ms == 8 * 60 * 60_000
