from __future__ import annotations

from dataclasses import dataclass

import pytest

from src.runtime import resolve_strategy_runtime_requirements
from src.runtime.models import RuntimeMode
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.strategy_capabilities import (
    StrategyCapabilityError,
    validate_strategy_capabilities,
)
from src.strategy import load_strategy


class _IdentityOnlyStrategy:
    def strategy_identity(self) -> str:
        return "test-strategy"


@dataclass
class _Observer:
    observer_id: str
    enabled: bool = True

    def on_market_feature(self, event):
        return ()


def _requirements(**capabilities: object) -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements.from_mapping(
        {"capabilities": capabilities}
    )


def _validate(strategy: object, requirements: StrategyRuntimeRequirements):
    return validate_strategy_capabilities(
        strategy,
        requirements,
        strategy_entry="tests.fake:Strategy",
        runtime_mode=RuntimeMode.LIVE_RUNTIME,
    )


def test_identity_is_required_and_must_match_declared_strategy_id() -> None:
    with pytest.raises(
        StrategyCapabilityError,
        match="missing=StrategyIdentityProvider",
    ):
        _validate(object(), _requirements())

    with pytest.raises(StrategyCapabilityError, match="identity mismatch"):
        _validate(
            _IdentityOnlyStrategy(),
            _requirements(strategy_id="different-strategy"),
        )


@pytest.mark.parametrize(
    ("capability", "provider"),
    [
        ("position_snapshots", "StrategyPositionProvider"),
        ("recovery_status", "StrategyRecoveryStatusProvider"),
        ("market_features", "MarketFeatureObserverProvider"),
        ("range_speed_history", "RangeSpeedHistoryProvider"),
        ("startup_preview", "StrategyStartupPreviewProvider"),
        ("pending_work", "StrategyPendingWorkProvider"),
    ],
)
def test_declared_required_provider_is_fail_fast(
    capability: str,
    provider: str,
) -> None:
    with pytest.raises(
        StrategyCapabilityError,
        match=f"missing={provider}",
    ) as exc_info:
        _validate(_IdentityOnlyStrategy(), _requirements(**{capability: True}))

    assert "runtime_mode=live_runtime" in str(exc_info.value)
    assert "strategy=test-strategy" in str(exc_info.value)


def test_closed_kline_requires_observer_provider_even_without_capability_flag() -> None:
    requirements = StrategyRuntimeRequirements.from_mapping(
        {"closed_kline": {"enabled": True}}
    )

    with pytest.raises(
        StrategyCapabilityError,
        match="missing=MarketFeatureObserverProvider",
    ):
        _validate(_IdentityOnlyStrategy(), requirements)


def test_legacy_on_market_feature_callback_is_not_a_provider() -> None:
    class LegacyCallbackOnly(_IdentityOnlyStrategy):
        def on_market_feature(self, event):
            return ()

    with pytest.raises(
        StrategyCapabilityError,
        match="missing=MarketFeatureObserverProvider",
    ):
        _validate(
            LegacyCallbackOnly(),
            _requirements(market_features=True),
        )


def test_required_market_feature_provider_rejects_empty_and_duplicate_observers() -> None:
    class Empty(_IdentityOnlyStrategy):
        def market_feature_observers(self):
            return ()

    with pytest.raises(StrategyCapabilityError, match="no enabled observers"):
        _validate(Empty(), _requirements(market_features=True))

    class Duplicate(_IdentityOnlyStrategy):
        def market_feature_observers(self):
            return (_Observer("same"), _Observer("same"))

    with pytest.raises(StrategyCapabilityError, match="duplicate market feature"):
        _validate(Duplicate(), _requirements(market_features=True))


def test_empty_strategy_explicitly_opts_out_of_optional_capabilities() -> None:
    strategy = load_strategy("strategies.empty_strategy:Strategy")
    requirements = resolve_strategy_runtime_requirements(strategy)

    capabilities = _validate(strategy, requirements)

    assert capabilities.identity == "empty_strategy"
    assert capabilities.position_snapshots is None
    assert capabilities.recovery_status is None
    assert capabilities.market_features is None
    assert capabilities.range_speed_history is None
    assert capabilities.startup_preview is None
    assert capabilities.pending_work is None


@pytest.mark.parametrize(
    ("entry", "expected_identity", "expects_range_speed"),
    [
        (
            "strategies.eth_lf_portfolio_v8:Strategy",
            "eth_lf_portfolio_v9e_range_exit_overlay",
            False,
        ),
        (
            "strategies.eth_lf_portfolio_v10b:Strategy",
            "eth_lf_portfolio_v10b_all_swing_structural_stop",
            True,
        ),
        (
            "strategies.eth_portfolio_v1:Strategy",
            "eth_portfolio_v1",
            True,
        ),
    ],
)
def test_formal_strategy_capability_declarations_validate(
    entry: str,
    expected_identity: str,
    expects_range_speed: bool,
) -> None:
    strategy = load_strategy(entry)
    requirements = resolve_strategy_runtime_requirements(strategy)

    capabilities = validate_strategy_capabilities(
        strategy,
        requirements,
        strategy_entry=entry,
        runtime_mode=RuntimeMode.LIVE_RUNTIME,
    )

    assert capabilities.identity == expected_identity
    assert capabilities.position_snapshots is strategy
    assert capabilities.recovery_status is strategy
    assert capabilities.market_features is strategy
    assert (capabilities.range_speed_history is strategy) is expects_range_speed
    assert capabilities.startup_preview is strategy
    assert capabilities.pending_work is strategy
