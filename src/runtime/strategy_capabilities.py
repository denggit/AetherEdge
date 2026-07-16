from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from src.strategy.contracts import (
    StrategyCapabilityError,
    StrategyContractError,
)
from src.runtime.market_features import resolve_market_feature_observers
from src.runtime.models import RuntimeMode
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.strategy_positions import resolve_strategy_position_snapshots
from src.strategy.market_features import MarketFeatureObserverProvider
from src.strategy.positions import StrategyPositionProvider
from src.strategy.positions import StrategyPositionSnapshot
from src.strategy.ports import (
    RangeSpeedHistoryProvider,
    StrategyIdentityProvider,
    StrategyPendingWorkProvider,
    StrategyRecoveryStatus,
    StrategyRecoveryStatusProvider,
    StrategyStartupPreviewProvider,
)


DYNAMIC_STRATEGY_CAPABILITIES_VALIDATED = (
    "strategy_dynamic_capabilities_validated"
)


@dataclass(frozen=True)
class ValidatedStrategyCapabilities:
    identity: str
    position_snapshots: StrategyPositionProvider | None
    recovery_status: StrategyRecoveryStatusProvider | None
    market_features: MarketFeatureObserverProvider | None
    range_speed_history: RangeSpeedHistoryProvider | None
    startup_preview: StrategyStartupPreviewProvider | None
    pending_work: StrategyPendingWorkProvider | None


@dataclass(frozen=True)
class ValidatedDynamicStrategyState:
    position_snapshots: tuple[StrategyPositionSnapshot, ...]
    recovery_status: StrategyRecoveryStatus
    pending_work: bool


def validate_strategy_capabilities(
    strategy: object,
    requirements: StrategyRuntimeRequirements,
    *,
    strategy_entry: str,
    runtime_mode: RuntimeMode,
) -> ValidatedStrategyCapabilities:
    """Validate required public capabilities before runtime startup work."""

    if not requirements.capability_manifest_declared:
        _raise_invalid(
            strategy=strategy_entry,
            provider="StrategyCapabilityManifest",
            detail="capabilities manifest is not declared",
            runtime_mode=runtime_mode,
        )

    if not isinstance(strategy, StrategyIdentityProvider):
        _raise_missing(
            strategy=strategy_entry,
            provider="StrategyIdentityProvider",
            required_by=("formal_live_runtime",),
            runtime_mode=runtime_mode,
        )
    try:
        identity = strategy.strategy_identity()
    except StrategyContractError:
        raise
    except Exception as exc:
        _raise_invalid(
            strategy=strategy_entry,
            provider="StrategyIdentityProvider",
            detail=f"{type(exc).__name__}: {exc}",
            runtime_mode=runtime_mode,
        )
    if not isinstance(identity, str) or not identity.strip():
        _raise_invalid(
            strategy=strategy_entry,
            provider="StrategyIdentityProvider",
            detail="strategy_identity() must return a non-empty string",
            runtime_mode=runtime_mode,
        )
    identity = identity.strip()
    expected_identity = requirements.capabilities.strategy_id
    if expected_identity is not None and identity != expected_identity:
        _raise_invalid(
            strategy=identity,
            provider="StrategyIdentityProvider",
            detail=(
                "identity mismatch | "
                f"declared={expected_identity} actual={identity}"
            ),
            runtime_mode=runtime_mode,
        )

    position_provider = (
        strategy if isinstance(strategy, StrategyPositionProvider) else None
    )
    if requirements.capabilities.position_snapshots and position_provider is None:
        _raise_missing(
            strategy=identity,
            provider="StrategyPositionProvider",
            required_by=("position_snapshots",),
            runtime_mode=runtime_mode,
        )
    if position_provider is not None:
        try:
            resolve_strategy_position_snapshots(strategy)
        except StrategyContractError:
            raise
        except Exception as exc:
            _raise_invalid(
                strategy=identity,
                provider="StrategyPositionProvider",
                detail=f"{type(exc).__name__}: {exc}",
                runtime_mode=runtime_mode,
            )

    recovery_provider = (
        strategy if isinstance(strategy, StrategyRecoveryStatusProvider) else None
    )
    if requirements.capabilities.recovery_status and recovery_provider is None:
        _raise_missing(
            strategy=identity,
            provider="StrategyRecoveryStatusProvider",
            required_by=("strategy_recovery",),
            runtime_mode=runtime_mode,
        )
    if recovery_provider is not None:
        try:
            recovery_status = recovery_provider.recovery_status()
        except StrategyContractError:
            raise
        except Exception as exc:
            _raise_invalid(
                strategy=identity,
                provider="StrategyRecoveryStatusProvider",
                detail=f"{type(exc).__name__}: {exc}",
                runtime_mode=runtime_mode,
            )
        if not isinstance(recovery_status, StrategyRecoveryStatus):
            _raise_invalid(
                strategy=identity,
                provider="StrategyRecoveryStatusProvider",
                detail="recovery_status() must return StrategyRecoveryStatus",
                runtime_mode=runtime_mode,
            )

    market_feature_reasons = _market_feature_reasons(requirements)
    market_feature_provider = (
        strategy if isinstance(strategy, MarketFeatureObserverProvider) else None
    )
    if market_feature_reasons and market_feature_provider is None:
        _raise_missing(
            strategy=identity,
            provider="MarketFeatureObserverProvider",
            required_by=market_feature_reasons,
            runtime_mode=runtime_mode,
        )
    if market_feature_provider is not None:
        try:
            observers = resolve_market_feature_observers(strategy)
        except StrategyContractError:
            raise
        except Exception as exc:
            _raise_invalid(
                strategy=identity,
                provider="MarketFeatureObserverProvider",
                detail=f"{type(exc).__name__}: {exc}",
                runtime_mode=runtime_mode,
            )
        if market_feature_reasons and not observers:
            _raise_invalid(
                strategy=identity,
                provider="MarketFeatureObserverProvider",
                detail="market_feature_observers() returned no enabled observers",
                runtime_mode=runtime_mode,
            )

    range_speed_provider = (
        strategy if isinstance(strategy, RangeSpeedHistoryProvider) else None
    )
    if requirements.capabilities.range_speed_history and range_speed_provider is None:
        _raise_missing(
            strategy=identity,
            provider="RangeSpeedHistoryProvider",
            required_by=("range_speed_history",),
            runtime_mode=runtime_mode,
        )
    if range_speed_provider is not None:
        try:
            range_speed_status = range_speed_provider.range_speed_history_status()
        except StrategyContractError:
            raise
        except Exception as exc:
            _raise_invalid(
                strategy=identity,
                provider="RangeSpeedHistoryProvider",
                detail=f"{type(exc).__name__}: {exc}",
                runtime_mode=runtime_mode,
            )
        required_status_keys = {
            "complete_history",
            "min_periods",
            "rolling_window_bars",
        }
        if not isinstance(range_speed_status, Mapping) or not required_status_keys <= set(
            range_speed_status
        ):
            _raise_invalid(
                strategy=identity,
                provider="RangeSpeedHistoryProvider",
                detail=(
                    "range_speed_history_status() must return a mapping with "
                    "complete_history, min_periods, and rolling_window_bars"
                ),
                runtime_mode=runtime_mode,
            )

    preview_provider = (
        strategy if isinstance(strategy, StrategyStartupPreviewProvider) else None
    )
    if requirements.capabilities.startup_preview and preview_provider is None:
        _raise_missing(
            strategy=identity,
            provider="StrategyStartupPreviewProvider",
            required_by=("startup_catchup_preview",),
            runtime_mode=runtime_mode,
        )

    pending_work_provider = (
        strategy if isinstance(strategy, StrategyPendingWorkProvider) else None
    )
    if requirements.capabilities.pending_work and pending_work_provider is None:
        _raise_missing(
            strategy=identity,
            provider="StrategyPendingWorkProvider",
            required_by=("pending_work_guards",),
            runtime_mode=runtime_mode,
        )
    if pending_work_provider is not None:
        try:
            pending_work = pending_work_provider.has_pending_strategy_work()
        except StrategyContractError:
            raise
        except Exception as exc:
            _raise_invalid(
                strategy=identity,
                provider="StrategyPendingWorkProvider",
                detail=f"{type(exc).__name__}: {exc}",
                runtime_mode=runtime_mode,
            )
        if not isinstance(pending_work, bool):
            _raise_invalid(
                strategy=identity,
                provider="StrategyPendingWorkProvider",
                detail="has_pending_strategy_work() must return bool",
                runtime_mode=runtime_mode,
            )

    return ValidatedStrategyCapabilities(
        identity=identity,
        position_snapshots=position_provider,
        recovery_status=recovery_provider,
        market_features=market_feature_provider,
        range_speed_history=range_speed_provider,
        startup_preview=preview_provider,
        pending_work=pending_work_provider,
    )


def validate_dynamic_strategy_capabilities(
    strategy: object,
    *,
    strategy_entry: str | None = None,
    runtime_mode: RuntimeMode = RuntimeMode.LIVE_RUNTIME,
) -> ValidatedDynamicStrategyState:
    """Validate provider outputs that can change during strategy recovery."""

    strategy_label = strategy_entry or (
        f"{type(strategy).__module__}.{type(strategy).__qualname__}"
    )

    snapshots: tuple[StrategyPositionSnapshot, ...] = ()
    if isinstance(strategy, StrategyPositionProvider):
        snapshots = resolve_strategy_position_snapshots(strategy)

    recovery_status = StrategyRecoveryStatus()
    if isinstance(strategy, StrategyRecoveryStatusProvider):
        try:
            recovery_status = strategy.recovery_status()
        except StrategyContractError:
            raise
        except Exception as exc:
            raise StrategyContractError(
                "strategy dynamic contract validation failed | "
                f"strategy={strategy_label} | "
                "invalid=StrategyRecoveryStatusProvider | "
                f"detail={type(exc).__name__}: {exc} | "
                f"runtime_mode={runtime_mode.value}"
            ) from exc
        if not isinstance(recovery_status, StrategyRecoveryStatus):
            raise StrategyContractError(
                "strategy dynamic contract validation failed | "
                f"strategy={strategy_label} | "
                "invalid=StrategyRecoveryStatusProvider | "
                "detail=recovery_status() must return StrategyRecoveryStatus | "
                f"runtime_mode={runtime_mode.value}"
            )

    pending_work = False
    if isinstance(strategy, StrategyPendingWorkProvider):
        try:
            pending_work = strategy.has_pending_strategy_work()
        except StrategyContractError:
            raise
        except Exception as exc:
            raise StrategyContractError(
                "strategy dynamic contract validation failed | "
                f"strategy={strategy_label} | "
                "invalid=StrategyPendingWorkProvider | "
                f"detail={type(exc).__name__}: {exc} | "
                f"runtime_mode={runtime_mode.value}"
            ) from exc
        if type(pending_work) is not bool:
            raise StrategyContractError(
                "strategy dynamic contract validation failed | "
                f"strategy={strategy_label} | "
                "invalid=StrategyPendingWorkProvider | "
                "detail=has_pending_strategy_work() must return bool | "
                f"runtime_mode={runtime_mode.value}"
            )

    return ValidatedDynamicStrategyState(
        position_snapshots=snapshots,
        recovery_status=recovery_status,
        pending_work=pending_work,
    )


def _market_feature_reasons(
    requirements: StrategyRuntimeRequirements,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if requirements.closed_kline.enabled:
        reasons.append("closed_kline")
    if requirements.range_bars.enabled:
        reasons.append("range_bars")
    if requirements.capabilities.market_features:
        reasons.append("declared_market_features")
    return tuple(dict.fromkeys(reasons))


def _raise_missing(
    *,
    strategy: str,
    provider: str,
    required_by: tuple[str, ...],
    runtime_mode: RuntimeMode,
) -> None:
    raise StrategyCapabilityError(
        "strategy capability validation failed | "
        f"strategy={strategy} | missing={provider} | "
        f"required_by={','.join(required_by)} | runtime_mode={runtime_mode.value}"
    )


def _raise_invalid(
    *,
    strategy: str,
    provider: str,
    detail: str,
    runtime_mode: RuntimeMode,
) -> None:
    raise StrategyCapabilityError(
        "strategy capability validation failed | "
        f"strategy={strategy} | invalid={provider} | detail={detail} | "
        f"runtime_mode={runtime_mode.value}"
    )


__all__ = [
    "DYNAMIC_STRATEGY_CAPABILITIES_VALIDATED",
    "StrategyCapabilityError",
    "StrategyContractError",
    "ValidatedDynamicStrategyState",
    "ValidatedStrategyCapabilities",
    "validate_dynamic_strategy_capabilities",
    "validate_strategy_capabilities",
]
