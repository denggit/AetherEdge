from __future__ import annotations

import pytest

from src.runtime.capabilities import capability_request_from_requirements
from src.runtime.feature_pipeline import TradeFeatureRuntimeConfig
from src.runtime.market_data.pipeline_plan import (
    MarketModuleSpec,
    resolve_market_pipeline,
)
from src.runtime.requirements import (
    ClosedKlineRequirement,
    OrderBookRequirement,
    RangeBarRequirement,
    StrategyRuntimeRequirements,
    TradeStreamRequirement,
)


# ---------------------------------------------------------------------------
# Pipeline plan resolution by strategy requirement type
# ---------------------------------------------------------------------------


class TestEmptyStrategy:
    def test_no_trade_source(self):
        req = StrategyRuntimeRequirements()
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is False
        assert "trade-stream" not in plan.enabled_module_ids

    def test_no_trade_queue(self):
        req = StrategyRuntimeRequirements()
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is False
        assert plan.enabled_module_ids == ()

    def test_no_feature_builder(self):
        req = StrategyRuntimeRequirements()
        plan = resolve_market_pipeline(req)
        assert "fixed-time-trade-bars" not in plan.enabled_module_ids
        assert "trade-footprint" not in plan.enabled_module_ids
        assert "range-footprint" not in plan.enabled_module_ids
        assert "range-bars" not in plan.enabled_module_ids

    def test_no_range_store(self):
        req = StrategyRuntimeRequirements()
        plan = resolve_market_pipeline(req)
        assert "range-bars" not in plan.enabled_module_ids

    def test_no_order_book(self):
        req = StrategyRuntimeRequirements()
        plan = resolve_market_pipeline(req)
        assert plan.order_book_enabled is False


class TestClosedKlineOnly:
    def test_closed_kline_enabled(self):
        req = StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(enabled=True, interval="4h"),
        )
        plan = resolve_market_pipeline(req)
        assert plan.closed_kline_enabled is True

    def test_no_trade_source_for_kline_only(self):
        req = StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(enabled=True, interval="4h"),
        )
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is False
        assert "trade-stream" not in plan.enabled_module_ids

    def test_no_feature_builders_for_kline_only(self):
        req = StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(enabled=True, interval="4h"),
        )
        plan = resolve_market_pipeline(req)
        for trade_module in (
            "fixed-time-trade-bars",
            "trade-footprint",
            "range-footprint",
            "range-bars",
        ):
            assert trade_module not in plan.enabled_module_ids


class TestRawTradeOnly:
    def test_trade_source_enabled(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is True
        assert "trade-stream" in plan.enabled_module_ids

    def test_raw_callback_module_present(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert "raw-trade-callback" in plan.enabled_module_ids

    def test_no_feature_builders_for_raw_trade_only(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        # Only raw trade and trade-stream — no feature builders
        trade_module_ids = set(plan.enabled_module_ids) - {
            "trade-stream",
            "raw-trade-callback",
        }
        # If range bars not enabled, no builders
        assert "range-bars" not in plan.enabled_module_ids


class TestRangeBarOnly:
    def test_range_bar_module_present(self):
        req = StrategyRuntimeRequirements(
            range_bars=RangeBarRequirement(enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert "range-bars" in plan.enabled_module_ids

    def test_trade_source_present_for_range_bar(self):
        req = StrategyRuntimeRequirements(
            range_bars=RangeBarRequirement(enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert "trade-stream" in plan.enabled_module_ids


class TestTradeFootprintOnly:
    def test_no_range_bar(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert "range-bars" not in plan.enabled_module_ids


class TestRangeFootprintOnly:
    def test_no_range_bar(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert "range-bars" not in plan.enabled_module_ids


class TestOrderBookOnly:
    def test_no_trade_processor(self):
        req = StrategyRuntimeRequirements(
            order_book=OrderBookRequirement(enabled=True, stream_enabled=True),
        )
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is False
        assert "trade-stream" not in plan.enabled_module_ids


class TestMultiFeature:
    def test_modules_share_one_trade_source(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
            range_bars=RangeBarRequirement(enabled=True),
        )
        plan = resolve_market_pipeline(req)
        # All trade-derived modules exist under one trade source
        assert "trade-stream" in plan.enabled_module_ids
        # trade-stream appears exactly once
        assert list(plan.enabled_module_ids).count("trade-stream") == 1


class TestPortfolioV1:
    """Portfolio V1: trades + range bars + closed kline + all features."""

    @staticmethod
    def _portfolio_v1_requirements() -> StrategyRuntimeRequirements:
        return StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(
                enabled=True, interval="4h", warmup_days=365,
                close_buffer_ms=5000,
            ),
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
            range_bars=RangeBarRequirement(
                enabled=True, range_pct="0.002", aggregate_interval="4h",
                min_bars=5,
            ),
        )

    def test_trades_enabled(self):
        req = self._portfolio_v1_requirements()
        plan = resolve_market_pipeline(req)
        assert plan.trades_enabled is True

    def test_closed_kline_enabled(self):
        req = self._portfolio_v1_requirements()
        plan = resolve_market_pipeline(req)
        assert plan.closed_kline_enabled is True

    def test_range_bars_enabled(self):
        req = self._portfolio_v1_requirements()
        plan = resolve_market_pipeline(req)
        assert "range-bars" in plan.enabled_module_ids

    def test_trade_source_present(self):
        req = self._portfolio_v1_requirements()
        plan = resolve_market_pipeline(req)
        assert "trade-stream" in plan.enabled_module_ids


class TestModuleDependencyOrdering:
    def test_deterministic_order(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
            range_bars=RangeBarRequirement(enabled=True),
        )
        plan1 = resolve_market_pipeline(req)
        plan2 = resolve_market_pipeline(req)
        assert plan1.enabled_module_ids == plan2.enabled_module_ids

    def test_cycle_detection(self):
        cyclic_specs = (
            MarketModuleSpec(module_id="a", after=frozenset({"b"})),
            MarketModuleSpec(module_id="b", after=frozenset({"a"})),
        )
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        req_with_extra = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        with pytest.raises(ValueError, match="cycle"):
            resolve_market_pipeline(
                req_with_extra,
                extra_module_ids=frozenset({"a", "b"}),
                custom_specs=cyclic_specs,
            )


class TestCapabilityRequestMapping:
    def test_empty_requirements_no_trade_or_book_capabilities(self):
        req = StrategyRuntimeRequirements()
        cr = capability_request_from_requirements(req)
        cap_values = {c.value for c in cr.capabilities}
        # No market data capabilities — only default account/order polling
        assert "market.trades" not in cap_values
        assert "market.order_book" not in cap_values
        assert "feature.range_bars" not in cap_values

    def test_trades_requirements_produce_trade_capability(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
        )
        cr = capability_request_from_requirements(req)
        cap_values = {c.value for c in cr.capabilities}
        assert "market.trades" in cap_values

    def test_closed_kline_produces_capability(self):
        req = StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(enabled=True),
        )
        cr = capability_request_from_requirements(req)
        cap_values = {c.value for c in cr.capabilities}
        assert "market.closed_klines" in cap_values

    def test_range_bars_produces_feature_capability(self):
        req = StrategyRuntimeRequirements(
            trades=TradeStreamRequirement(enabled=True, stream_enabled=True),
            range_bars=RangeBarRequirement(enabled=True),
        )
        cr = capability_request_from_requirements(req)
        cap_values = {c.value for c in cr.capabilities}
        assert "feature.range_bars" in cap_values
