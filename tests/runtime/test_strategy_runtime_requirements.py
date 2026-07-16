from __future__ import annotations

from decimal import Decimal

from src.runtime import StrategyRuntimeRequirements, resolve_strategy_runtime_requirements


class StrategyWithMappingRequirements:
    def runtime_requirements(self):
        return {
            "closed_kline": {"enabled": True, "interval": "4h", "warmup_days": 365, "close_buffer_ms": 60000},
            "trades": {"enabled": True, "stream_enabled": True, "warmup_enabled": True},
            "range_bars": {"enabled": True, "range_pct": "0.002", "aggregate_interval": "4h"},
            "order_book": {"enabled": False},
            "capabilities": {
                "manifest_version": 1,
                "strategy_id": "test-strategy",
                "position_snapshots": True,
                "recovery_status": False,
                "market_features": True,
                "range_speed_history": False,
                "startup_preview": False,
                "pending_work": False,
            },
            "account_state": {"poll_interval_seconds": 300},
            "order_state": {"poll_interval_seconds": 20},
        }


def test_strategy_runtime_requirements_from_mapping():
    req = StrategyRuntimeRequirements.from_mapping(StrategyWithMappingRequirements().runtime_requirements())

    assert req.closed_kline.enabled is True
    assert req.closed_kline.interval == "4h"
    assert req.closed_kline.warmup_days == 365
    assert req.trades.stream_enabled is True
    assert req.trades.warmup_enabled is True
    assert req.range_bars.enabled is True
    assert req.range_bars.range_pct == Decimal("0.002")
    assert req.order_book.enabled is False
    assert req.private_account_stream.enabled is False
    assert req.account_state.poll_interval_seconds == 300
    assert req.order_state.poll_interval_seconds == 20
    assert req.capabilities.strategy_id == "test-strategy"
    assert req.capabilities.position_snapshots is True
    assert req.capabilities.market_features is True
    assert req.capabilities.range_speed_history is False
    assert req.capabilities.manifest_version == 1
    assert req.capability_manifest_declared is True


def test_resolve_requirements_prefers_strategy_over_legacy_streams():
    req = resolve_strategy_runtime_requirements(StrategyWithMappingRequirements(), fallback_data_streams=("order_book",))

    assert req.trades.enabled is True
    assert req.order_book.enabled is False
    assert req.private_account_stream.enabled is False
    assert req.capability_manifest_declared is True


def test_legacy_data_streams_fallback_only_when_strategy_has_no_requirements():
    req = resolve_strategy_runtime_requirements(object(), fallback_data_streams=("trades", "order_book"))

    assert req.trades.enabled is True
    assert req.trades.stream_enabled is True
    assert req.order_book.enabled is True
    assert req.order_book.stream_enabled is True
    assert req.private_account_stream.enabled is False
    assert req.capability_manifest_declared is False
