from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform.data.models import (
    MarketFullOrderBook,
    MarketOpenInterest,
    MarketOrderBookL2,
)
from src.platform.exchanges.models import ExchangeName
from src.runtime.capabilities import (
    MARKET_FULL_ORDER_BOOK,
    MARKET_OPEN_INTEREST,
    MARKET_ORDER_BOOK,
    MARKET_ORDER_BOOK_L2,
    MARKET_TRADES,
    capability_request_from_requirements,
)
from src.runtime.market_data.catalog import build_market_data_registry
from src.runtime.market_data.pipeline_plan import resolve_market_pipeline
from src.runtime.registry import DependencyResolver
from src.runtime.requirements import (
    FullOrderBookRequirement,
    OpenInterestRequirement,
    OrderBookL2Requirement,
    StrategyRuntimeRequirements,
)
from src.runtime.strategy_host import StrategyHost
from src.strategy.contracts import StrategyCapabilityError


def test_new_requirements_default_off_and_aliases_are_independent() -> None:
    defaults = StrategyRuntimeRequirements()
    assert defaults.order_book_l2.enabled is False
    assert defaults.full_order_book.enabled is False
    assert defaults.open_interest.enabled is False

    l2 = StrategyRuntimeRequirements.from_data_streams(("books400",))
    full = StrategyRuntimeRequirements.from_data_streams(("books5000",))
    oi = StrategyRuntimeRequirements.from_data_streams(("oi",))
    assert l2.order_book_l2.stream_enabled is True
    assert full.full_order_book.polling_enabled is True
    assert oi.open_interest.stream_enabled is True
    assert l2.order_book.enabled is False
    assert MARKET_ORDER_BOOK not in capability_request_from_requirements(
        l2
    ).capabilities
    assert MARKET_TRADES not in capability_request_from_requirements(
        l2
    ).capabilities


def test_full_order_book_requirement_mapping_and_bounds() -> None:
    requirements = StrategyRuntimeRequirements.from_mapping(
        {
            "full_order_book": {
                "enabled": True,
                "polling_enabled": True,
                "depth": 1234,
                "poll_interval_seconds": 4.5,
            }
        }
    )
    assert requirements.full_order_book.depth == 1234
    assert requirements.full_order_book.poll_interval_seconds == 4.5
    with pytest.raises(StrategyCapabilityError):
        StrategyRuntimeRequirements(
            full_order_book=FullOrderBookRequirement(depth=5001)
        )
    with pytest.raises(StrategyCapabilityError):
        StrategyRuntimeRequirements(
            full_order_book=FullOrderBookRequirement(
                poll_interval_seconds=0.5
            )
        )


def test_capability_and_pipeline_resolution_keeps_new_sources_separate() -> None:
    requirements = StrategyRuntimeRequirements(
        order_book_l2=OrderBookL2Requirement(
            enabled=True,
            stream_enabled=True,
        ),
        full_order_book=FullOrderBookRequirement(
            enabled=True,
            polling_enabled=True,
        ),
        open_interest=OpenInterestRequirement(
            enabled=True,
            stream_enabled=True,
        ),
    )
    request = capability_request_from_requirements(requirements)
    assert {
        MARKET_ORDER_BOOK_L2,
        MARKET_FULL_ORDER_BOOK,
        MARKET_OPEN_INTEREST,
    }.issubset(request.capabilities)
    assert MARKET_ORDER_BOOK not in request.capabilities
    assert MARKET_TRADES not in request.capabilities

    plan = resolve_market_pipeline(requirements)
    assert plan.order_book_l2_enabled is True
    assert plan.full_order_book_enabled is True
    assert plan.open_interest_enabled is True
    assert plan.enabled_module_ids == (
        "order-book-l2-stream",
        "full-order-book-poller",
        "open-interest-stream",
    )


def test_registry_factories_remain_lazy_and_resolve_exact_capability() -> None:
    calls = {"l2": 0, "full": 0, "oi": 0}

    class IdleStreams:
        async def stream_order_book_l2(self):
            if False:
                yield None

        async def stream_full_order_book(self):
            if False:
                yield None

        async def stream_open_interest(self):
            if False:
                yield None

    def mark(name: str):
        def factory():
            calls[name] += 1
            return IdleStreams()

        return factory

    registry = build_market_data_registry(
        create_trade_stream=lambda: object(),
        create_order_book_stream=lambda: object(),
        create_order_book_l2_stream=mark("l2"),
        create_full_order_book_stream=mark("full"),
        create_open_interest_stream=mark("oi"),
        publish_feature=lambda _event: None,
    )
    assert calls == {"l2": 0, "full": 0, "oi": 0}
    plan = DependencyResolver(registry).resolve({MARKET_OPEN_INTEREST})
    assert plan.module_ids == ("open-interest-stream",)
    registry.instantiate(plan)
    assert calls == {"l2": 0, "full": 0, "oi": 1}


@pytest.mark.asyncio
async def test_strategy_host_routes_each_new_event_to_its_own_callback() -> None:
    calls = []

    class Strategy:
        async def on_order_book_l2(self, event):
            calls.append(("l2", event))

        async def on_full_order_book(self, event):
            calls.append(("full", event))

        async def on_open_interest(self, event):
            calls.append(("oi", event))

    events = (
        MarketOrderBookL2(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            bids=(),
            asks=(),
            event_time_ms=1,
            sequence_id=1,
            previous_sequence_id=-1,
        ),
        MarketFullOrderBook(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            bids=(),
            asks=(),
            event_time_ms=2,
            requested_depth=5000,
        ),
        MarketOpenInterest(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            instrument_type="SWAP",
            open_interest_contracts=Decimal("1"),
            open_interest_base=None,
            open_interest_usd=None,
            event_time_ms=3,
        ),
    )
    host = StrategyHost(Strategy())
    for event in events:
        assert await host.on_market_event(event) == ()
    assert [name for name, _event in calls] == ["l2", "full", "oi"]

    ignored = StrategyHost(object())
    for event in events:
        assert await ignored.on_market_event(event) == ()
