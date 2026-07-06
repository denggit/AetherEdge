from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from src.market_data.models import FixedTimeTradeBar
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataBuffer,
    MfDataReadiness,
    MfFeatureObserver,
)
import strategies.eth_portfolio_v1.domain.mf_data as mf_data_module


def _make_bar(open_time_ms: int, close_time_ms: int, *, large_share: str = "0.05") -> FixedTimeTradeBar:
    return FixedTimeTradeBar(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        timeframe="1m",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        available_time_ms=close_time_ms,
        open=Decimal("1000"),
        high=Decimal("1005"),
        low=Decimal("995"),
        close=Decimal("1002"),
        volume=Decimal("10"),
        buy_volume=Decimal("6"),
        sell_volume=Decimal("4"),
        buy_notional=Decimal("6000"),
        sell_notional=Decimal("4000"),
        delta_volume=Decimal("2"),
        delta_notional=Decimal("2000"),
        abs_delta_notional=Decimal("2000"),
        trade_count=5,
        large_trade_share=Decimal(large_share),
    )


# ---------------------------------------------------------------------------
# MfDataBuffer
# ---------------------------------------------------------------------------

def test_buffer_load_initial_from_store(tmp_path: Path) -> None:
    store_path = tmp_path / "test.sqlite3"
    store = SqliteTradeFeatureStore(path=store_path)
    base = int(time.time() * 1000) - 3600_000

    bars = [
        _make_bar(base + i * 60_000, base + (i + 1) * 60_000 - 1)
        for i in range(10)
    ]
    store.upsert_many(bars)

    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(store_path),
        decision_buffer_minutes=10,
        decision_buffer_max_minutes=20,
    )
    count = buffer.load_initial()
    assert count == 10
    assert buffer.loaded is True
    assert buffer.bar_count == 10


def test_buffer_maxlen_enforced(tmp_path: Path) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
        decision_buffer_minutes=3,
        decision_buffer_max_minutes=5,
    )
    base = int(time.time() * 1000)
    for i in range(10):
        buffer.append_tradebar(_make_bar(base + i * 60_000, base + (i + 1) * 60_000 - 1))

    assert buffer.bar_count <= 5


def test_buffer_large_trade_share_scalars(tmp_path: Path) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
        large_share_quantile_window_days=1,
    )
    base = int(time.time() * 1000)
    for i in range(10):
        share = str(0.01 * (i + 1))
        buffer.append_tradebar(_make_bar(base + i * 60_000, base + (i + 1) * 60_000 - 1, large_share=share))

    median = buffer.large_trade_share_median()
    assert median > 0
    q75 = buffer.large_trade_share_quantile(0.75)
    assert q75 > 0


def test_buffer_audit_is_json_safe(tmp_path: Path) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
    )
    base = int(time.time() * 1000)
    buffer.append_tradebar(_make_bar(base, base + 60_000 - 1))

    audit = buffer.last_audit()
    json.dumps(audit)
    assert "bar_count" in audit
    assert "latest_bar" in audit


def test_buffer_recent_bars_returns_subset(tmp_path: Path) -> None:
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
        decision_buffer_minutes=100,
        decision_buffer_max_minutes=200,
    )
    base = int(time.time() * 1000)
    for i in range(50):
        buffer.append_tradebar(_make_bar(base + i * 60_000, base + (i + 1) * 60_000 - 1))

    recent = buffer.recent_bars(10)
    assert len(recent) == 10
    assert recent[-1].open_time_ms > recent[0].open_time_ms


# ---------------------------------------------------------------------------
# MfDataReadiness
# ---------------------------------------------------------------------------

def test_readiness_mf_signal_ready_always_false(tmp_path: Path) -> None:
    readiness = MfDataReadiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
        required_minutes=10,
    )
    assert readiness.mf_signal_ready is False

    info = readiness.readiness()
    assert info["mf_signal_ready"] is False


def test_readiness_has_full_audit_info(tmp_path: Path) -> None:
    store_path = tmp_path / "test.sqlite3"
    store = SqliteTradeFeatureStore(path=store_path)
    base = int(time.time() * 1000) - 3600_000
    store.upsert_many([
        _make_bar(base + i * 60_000, base + (i + 1) * 60_000 - 1)
        for i in range(5)
    ])

    readiness = MfDataReadiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(store_path),
        required_minutes=5,
    )
    info = readiness.readiness()
    assert "coverage_ready" in info


def test_readiness_blocks_when_large_share_samples_are_insufficient(
    tmp_path: Path,
    monkeypatch,
) -> None:
    coverage = SimpleNamespace(required_minutes=4_320)
    monkeypatch.setattr(
        mf_data_module,
        "resolve_trade_feature_readiness",
        lambda **_: SimpleNamespace(
            coverage=coverage,
            audit=lambda: {
                "tradebar_ready": True,
                "fixed_time_footprint_ready": True,
                "range_footprint_ready": True,
                "coverage_ready": True,
                "coverage": {
                    "extra": {
                        "tradebar_complete_minutes": 4_320,
                    }
                },
            },
        ),
    )
    readiness = MfDataReadiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
        required_minutes=4_320,
        large_share_min_samples=43_200,
    )

    audit = readiness.readiness()

    assert audit["large_share_samples_ready"] is False
    assert audit["mf_signal_ready"] is False


def test_readiness_passes_large_share_sample_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    coverage = SimpleNamespace(required_minutes=129_600)
    monkeypatch.setattr(
        mf_data_module,
        "resolve_trade_feature_readiness",
        lambda **_: SimpleNamespace(
            coverage=coverage,
            audit=lambda: {
                "tradebar_ready": True,
                "fixed_time_footprint_ready": True,
                "range_footprint_ready": True,
                "coverage_ready": True,
                "coverage": {
                    "extra": {
                        "tradebar_complete_minutes": 129_600,
                    }
                },
            },
        ),
    )
    readiness = MfDataReadiness(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        store_path=str(tmp_path / "test.sqlite3"),
        required_minutes=129_600,
        large_share_min_samples=43_200,
        large_share_window_days=90,
    )

    audit = readiness.readiness()

    assert audit["large_share_samples_ready"] is True
    assert audit["mf_signal_ready"] is True


# ---------------------------------------------------------------------------
# MfFeatureObserver
# ---------------------------------------------------------------------------

def test_observer_always_returns_empty_signals() -> None:
    obs = MfFeatureObserver()

    assert obs.on_market_feature({"any": "event"}) == ()
    assert obs.on_kline("test") == ()
    assert obs.on_trade("test") == ()

    result = obs.on_market_feature(None)
    assert result == ()
    assert isinstance(result, tuple)


# ---------------------------------------------------------------------------
# R011: Tightened readiness observer gate tests
# ---------------------------------------------------------------------------

def test_observer_set_readiness_rejects_partial_gates() -> None:
    """When any of the 6 readiness gates is False, data_ready must be False."""
    obs = MfFeatureObserver()

    # mf_signal_feature_ready=True but individual gates are False
    obs.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": False,
            "coverage_ready": True,
            "large_share_samples_ready": True,
        },
        source="test",
    )
    assert obs.last_mf_signal_audit["data_ready"] is False

    # coverage_ready=False
    obs.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": True,
            "coverage_ready": False,
            "large_share_samples_ready": True,
        },
        source="test",
    )
    assert obs.last_mf_signal_audit["data_ready"] is False

    # large_share_samples_ready=False
    obs.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": True,
            "coverage_ready": True,
            "large_share_samples_ready": False,
        },
        source="test",
    )
    assert obs.last_mf_signal_audit["data_ready"] is False


def test_observer_set_readiness_all_gates_true() -> None:
    """When all 6 readiness gates are True, data_ready must be True."""
    obs = MfFeatureObserver()

    obs.set_readiness(
        {
            "mf_signal_feature_ready": True,
            "range_footprint_ready": True,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": True,
            "coverage_ready": True,
            "large_share_samples_ready": True,
        },
        source="test",
    )
    assert obs.last_mf_signal_audit["data_ready"] is True
    gates = obs.last_mf_signal_audit["readiness_gates"]
    assert len(gates) == 6
    assert all(gates.values())


def test_observer_readiness_audit_shows_missing_fields() -> None:
    """audit dict must contain readiness_gates for diagnostics."""
    obs = MfFeatureObserver()

    obs.set_readiness(
        {
            "mf_signal_feature_ready": False,
            "range_footprint_ready": False,
            "tradebar_ready": True,
            "fixed_time_footprint_ready": False,
            "coverage_ready": False,
            "large_share_samples_ready": False,
        },
        source="test",
    )
    assert obs.last_mf_signal_audit["data_ready"] is False
    gates = obs.last_mf_signal_audit["readiness_gates"]
    # Only tradebar is ready
    assert gates["tradebar_ready"] is True
    assert gates["range_footprint_ready"] is False
    assert gates["large_share_samples_ready"] is False
