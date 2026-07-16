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
    manifest: dict[str, object] = {
        "manifest_version": 1,
        "strategy_id": "test-strategy",
        "position_snapshots": False,
        "recovery_status": False,
        "market_features": False,
        "range_speed_history": False,
        "startup_preview": False,
        "pending_work": False,
    }
    manifest.update(capabilities)
    return StrategyRuntimeRequirements.from_mapping(
        {"capabilities": manifest}
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
        {
            "closed_kline": {"enabled": True},
            "capabilities": {
                "manifest_version": 1,
                "strategy_id": "test-strategy",
                "position_snapshots": False,
                "recovery_status": False,
                "market_features": False,
                "range_speed_history": False,
                "startup_preview": False,
                "pending_work": False,
            },
        }
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


def test_undeclared_manifest_is_distinct_from_explicit_false_manifest() -> None:
    undeclared = resolve_strategy_runtime_requirements(object())
    declared = _requirements()

    assert undeclared.capability_manifest_declared is False
    assert declared.capability_manifest_declared is True
    with pytest.raises(
        StrategyCapabilityError,
        match="capabilities manifest is not declared",
    ):
        _validate(_IdentityOnlyStrategy(), undeclared)


def test_runtime_requirements_provider_error_is_fatal_and_preserves_cause() -> None:
    cause = ValueError("broken manifest provider")

    class BrokenRequirements:
        def runtime_requirements(self):
            raise cause

    with pytest.raises(StrategyCapabilityError) as exc_info:
        resolve_strategy_runtime_requirements(BrokenRequirements())

    assert exc_info.value.__cause__ is cause


@pytest.mark.parametrize("value", (None, [], "manifest"))
def test_capability_manifest_must_be_a_mapping(value: object) -> None:
    with pytest.raises(StrategyCapabilityError, match="must be a mapping"):
        StrategyRuntimeRequirements.from_mapping({"capabilities": value})


@pytest.mark.parametrize(
    "missing_field",
    (
        "manifest_version",
        "strategy_id",
        "position_snapshots",
        "recovery_status",
        "market_features",
        "range_speed_history",
        "startup_preview",
        "pending_work",
    ),
)
def test_capability_manifest_rejects_every_missing_field(
    missing_field: str,
) -> None:
    manifest = dict(_requirements().capabilities.__dict__)
    manifest.pop(missing_field)

    with pytest.raises(StrategyCapabilityError, match="missing="):
        StrategyRuntimeRequirements.from_mapping({"capabilities": manifest})


def test_capability_manifest_rejects_unknown_and_misspelled_fields() -> None:
    manifest = dict(_requirements().capabilities.__dict__)
    manifest["unexpected"] = False
    with pytest.raises(
        StrategyCapabilityError,
        match=r"unknown=\['unexpected'\]",
    ):
        StrategyRuntimeRequirements.from_mapping({"capabilities": manifest})

    manifest = dict(_requirements().capabilities.__dict__)
    manifest["postion_snapshots"] = manifest.pop("position_snapshots")
    with pytest.raises(StrategyCapabilityError, match="postion_snapshots"):
        StrategyRuntimeRequirements.from_mapping({"capabilities": manifest})


@pytest.mark.parametrize("version", (2, 0, True, "1", None))
def test_capability_manifest_supports_only_integer_version_one(
    version: object,
) -> None:
    manifest = dict(_requirements().capabilities.__dict__)
    manifest["manifest_version"] = version
    with pytest.raises(StrategyCapabilityError, match="manifest_version"):
        StrategyRuntimeRequirements.from_mapping({"capabilities": manifest})


@pytest.mark.parametrize("strategy_id", ("", "   ", None, 1))
def test_capability_manifest_requires_non_empty_string_identity(
    strategy_id: object,
) -> None:
    manifest = dict(_requirements().capabilities.__dict__)
    manifest["strategy_id"] = strategy_id
    with pytest.raises(StrategyCapabilityError, match="strategy_id"):
        StrategyRuntimeRequirements.from_mapping({"capabilities": manifest})


@pytest.mark.parametrize("value", ("true", "false", 1, 0, None))
@pytest.mark.parametrize(
    "field",
    (
        "position_snapshots",
        "recovery_status",
        "market_features",
        "range_speed_history",
        "startup_preview",
        "pending_work",
    ),
)
def test_capability_manifest_rejects_non_bool_capabilities(
    field: str,
    value: object,
) -> None:
    manifest = dict(_requirements().capabilities.__dict__)
    manifest[field] = value
    with pytest.raises(StrategyCapabilityError, match=field):
        StrategyRuntimeRequirements.from_mapping({"capabilities": manifest})


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
