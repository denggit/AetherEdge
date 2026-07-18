from __future__ import annotations

import pytest

from src.runtime.capabilities import (
    ACCOUNT_POLL,
    ACCOUNT_PRIVATE_EVENTS,
    ACCOUNT_SNAPSHOT,
    FEATURE_FIXED_TIME_TRADE_BARS,
    FEATURE_RANGE_BARS,
    FEATURE_RANGE_FOOTPRINT,
    FEATURE_TRADE_FOOTPRINT,
    MARKET_CLOSED_KLINES,
    MARKET_ORDER_BOOK,
    MARKET_TRADES,
    ORDER_POLL,
    capability_request_from_requirements,
)
from src.runtime.feature_pipeline import TradeFeatureRuntimeConfig
from src.runtime.requirements import StrategyRuntimeRequirements


def _requirements(**values) -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements.from_mapping(values)


def test_empty_manifest_requests_no_market_or_background_capability() -> None:
    request = capability_request_from_requirements(
        _requirements(
            account_state={
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
            },
            order_state={"poll_when_position_enabled": False},
        )
    )

    assert request.capabilities == frozenset()


def test_trade_only_manifest_does_not_infer_other_market_data() -> None:
    request = capability_request_from_requirements(
        _requirements(
            trades={"enabled": True, "stream_enabled": True},
            account_state={
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
            },
            order_state={"poll_when_position_enabled": False},
        )
    )

    assert request.capabilities == frozenset({MARKET_TRADES})


def test_order_book_only_manifest_does_not_infer_trades() -> None:
    request = capability_request_from_requirements(
        _requirements(
            order_book={"enabled": True, "stream_enabled": True},
            account_state={
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
            },
            order_state={"poll_when_position_enabled": False},
        )
    )

    assert request.capabilities == frozenset({MARKET_ORDER_BOOK})


def test_range_request_declares_range_and_leaves_trade_as_dependency() -> None:
    request = capability_request_from_requirements(
        _requirements(
            range_bars={"enabled": True},
            account_state={
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
            },
            order_state={"poll_when_position_enabled": False},
        )
    )

    assert request.capabilities == frozenset({FEATURE_RANGE_BARS})


def test_legacy_trade_feature_config_is_adapted_once_to_explicit_features() -> None:
    config = TradeFeatureRuntimeConfig(enabled=True)
    request = capability_request_from_requirements(
        _requirements(
            account_state={
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
            },
            order_state={"poll_when_position_enabled": False},
        ),
        trade_features=config,
    )

    assert request.trade_features is config
    assert request.capabilities == frozenset(
        {
            FEATURE_FIXED_TIME_TRADE_BARS,
            FEATURE_TRADE_FOOTPRINT,
            FEATURE_RANGE_FOOTPRINT,
        }
    )


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({}, frozenset()),
        (
            {"fixed_time_trade_bars_enabled": True},
            frozenset({FEATURE_FIXED_TIME_TRADE_BARS}),
        ),
        (
            {"trade_footprint_enabled": True},
            frozenset({FEATURE_TRADE_FOOTPRINT}),
        ),
        (
            {"range_footprint_enabled": True},
            frozenset({FEATURE_RANGE_FOOTPRINT}),
        ),
        (
            {
                "fixed_time_trade_bars_enabled": True,
                "range_footprint_enabled": True,
            },
            frozenset(
                {
                    FEATURE_FIXED_TIME_TRADE_BARS,
                    FEATURE_RANGE_FOOTPRINT,
                }
            ),
        ),
        (
            {"enabled": True},
            frozenset(
                {
                    FEATURE_FIXED_TIME_TRADE_BARS,
                    FEATURE_TRADE_FOOTPRINT,
                    FEATURE_RANGE_FOOTPRINT,
                }
            ),
        ),
    ],
)
def test_trade_features_resolve_independently(values, expected) -> None:
    config = TradeFeatureRuntimeConfig.from_strategy(
        type(
            "Strategy",
            (),
            {"trade_feature_runtime_config": lambda self: values},
        )()
    )

    request = capability_request_from_requirements(
        _requirements(
            account_state={
                "startup_snapshot_enabled": False,
                "poll_enabled": False,
            },
            order_state={"poll_when_position_enabled": False},
        ),
        trade_features=config,
    )

    assert request.capabilities == expected


def test_account_and_scheduler_requirements_are_explicit() -> None:
    request = capability_request_from_requirements(
        _requirements(
            closed_kline={"enabled": True},
            private_account_stream={"enabled": True},
        )
    )

    assert request.capabilities == frozenset(
        {
            MARKET_CLOSED_KLINES,
            ACCOUNT_PRIVATE_EVENTS,
            ACCOUNT_SNAPSHOT,
            ACCOUNT_POLL,
            ORDER_POLL,
        }
    )
