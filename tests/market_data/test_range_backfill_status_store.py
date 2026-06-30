from __future__ import annotations

from src.market_data.backfill.status_store import RangeBackfillStatusStore


def test_status_store_atomic_write_and_read(tmp_path) -> None:
    store = RangeBackfillStatusStore(tmp_path / "state" / "status.json")

    store.write({"running": True, "pid": 123})
    data = store.read()

    assert data is not None
    assert data["version"] == 1
    assert data["running"] is True
    assert data["pid"] == 123


def test_status_store_read_failure_returns_none(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{bad", encoding="utf-8")

    assert RangeBackfillStatusStore(path).read() is None
