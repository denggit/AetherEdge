from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.app import AppConfig
from src.platform import ExchangeName
from src.platform.config import ProjectEnvConfig
from src.runtime import (
    LiveRuntimeConfig,
    RuntimeMode,
    StrategyCapabilityRequirements,
    StrategyRuntimeRequirements,
    resolve_strategy_runtime_requirements,
    validate_strategy_runtime_requirements,
)
from src.runtime.runner import LiveRuntimeRunner
from src.strategy import StrategyCapabilityError


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


def _declared_capabilities() -> StrategyCapabilityRequirements:
    return StrategyCapabilityRequirements(
        manifest_version=1,
        strategy_id="test-strategy",
        position_snapshots=False,
        recovery_status=False,
        market_features=False,
        range_speed_history=False,
        startup_preview=False,
        pending_work=False,
    )


def _forged_invalid_requirements() -> StrategyRuntimeRequirements:
    requirements = StrategyRuntimeRequirements(
        capabilities=_declared_capabilities(),
        capability_manifest_declared=True,
    )
    object.__setattr__(
        requirements,
        "capabilities",
        replace(requirements.capabilities, recovery_status=1),
    )
    return requirements


def _runner_config() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


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


def test_valid_direct_runtime_requirements_are_returned_unchanged() -> None:
    requirements = StrategyRuntimeRequirements(
        capabilities=_declared_capabilities(),
        capability_manifest_declared=True,
    )

    assert validate_strategy_runtime_requirements(requirements) is requirements


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("manifest_version", None),
        ("manifest_version", True),
        ("manifest_version", "1"),
        ("strategy_id", None),
        ("strategy_id", ""),
        ("position_snapshots", "false"),
        ("recovery_status", 1),
        ("market_features", None),
    ),
)
def test_direct_declared_runtime_requirements_are_strict(
    field: str,
    value: object,
) -> None:
    capabilities = replace(_declared_capabilities(), **{field: value})

    with pytest.raises(StrategyCapabilityError, match=field):
        StrategyRuntimeRequirements(
            capabilities=capabilities,
            capability_manifest_declared=True,
        )


@pytest.mark.parametrize(
    "capabilities",
    (
        replace(StrategyCapabilityRequirements(), strategy_id="unexpected"),
        replace(StrategyCapabilityRequirements(), pending_work=True),
        replace(StrategyCapabilityRequirements(), pending_work=0),
    ),
)
def test_direct_undeclared_runtime_requirements_require_empty_false_manifest(
    capabilities: StrategyCapabilityRequirements,
) -> None:
    with pytest.raises(StrategyCapabilityError):
        StrategyRuntimeRequirements(capabilities=capabilities)


def test_direct_runtime_requirements_reject_wrong_manifest_container_types() -> None:
    with pytest.raises(StrategyCapabilityError, match="capabilities must be"):
        StrategyRuntimeRequirements(capabilities={})  # type: ignore[arg-type]

    with pytest.raises(StrategyCapabilityError, match="declared must be bool"):
        StrategyRuntimeRequirements(
            capability_manifest_declared=1,  # type: ignore[arg-type]
        )


def test_resolve_revalidates_direct_dataclass_provider_result() -> None:
    valid = StrategyRuntimeRequirements(
        capabilities=_declared_capabilities(),
        capability_manifest_declared=True,
    )

    class ValidProvider:
        def runtime_requirements(self):
            return valid

    class ForgedProvider:
        def runtime_requirements(self):
            return _forged_invalid_requirements()

    assert resolve_strategy_runtime_requirements(ValidProvider()) is valid
    with pytest.raises(StrategyCapabilityError, match="recovery_status"):
        resolve_strategy_runtime_requirements(ForgedProvider())


def test_runner_revalidates_injected_runtime_requirements() -> None:
    config = _runner_config()
    services = {
        "project_env_config": ProjectEnvConfig(
            values={},
            source_files=(),
            env_file=Path(".env"),
            example_file=None,
        ),
        "runtime_requirements": _forged_invalid_requirements(),
    }

    with pytest.raises(StrategyCapabilityError, match="recovery_status"):
        LiveRuntimeRunner(
            app_config=config,
            app_context=SimpleNamespace(strategy=object()),
            runtime_config=LiveRuntimeConfig(
                app=config,
                mode=RuntimeMode.LIVE_RUNTIME,
            ),
            services=services,
        )
