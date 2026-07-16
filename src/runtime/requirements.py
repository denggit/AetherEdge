from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping
import json

from src.strategy.contracts import StrategyCapabilityError


_CAPABILITY_BOOLEAN_FIELDS = (
    "position_snapshots",
    "recovery_status",
    "market_features",
    "range_speed_history",
    "startup_preview",
    "pending_work",
)
_CAPABILITY_MANIFEST_FIELDS = frozenset(
    ("manifest_version", "strategy_id", *_CAPABILITY_BOOLEAN_FIELDS)
)


@dataclass(frozen=True)
class ClosedKlineRequirement:
    enabled: bool = False
    interval: str = "4h"
    warmup_days: int = 0
    close_buffer_ms: int | None = None
    retry_interval_ms: int | None = None
    missing_alert_after_ms: int | None = None
    min_records: int = 1


@dataclass(frozen=True)
class TradeStreamRequirement:
    enabled: bool = False
    stream_enabled: bool = False
    warmup_enabled: bool = False
    warmup_days: int = 0


@dataclass(frozen=True)
class OrderBookRequirement:
    enabled: bool = False
    stream_enabled: bool = False


@dataclass(frozen=True)
class RangeBarRequirement:
    enabled: bool = False
    range_pct: Decimal = Decimal("0.002")
    aggregate_interval: str = "4h"
    min_bars: int = 1


@dataclass(frozen=True)
class PrivateAccountStreamRequirement:
    enabled: bool = False


@dataclass(frozen=True)
class AccountStateRequirement:
    startup_snapshot_enabled: bool = True
    poll_enabled: bool = True
    poll_interval_seconds: int = 300
    post_order_sync_enabled: bool = True
    consecutive_failure_alert_threshold: int = 3


@dataclass(frozen=True)
class OrderStateRequirement:
    post_submit_sync_enabled: bool = True
    poll_when_position_enabled: bool = True
    poll_interval_seconds: int = 20
    sync_open_orders: bool = True
    sync_open_stop_orders: bool = True
    sync_position: bool = True
    consecutive_failure_alert_threshold: int = 3


@dataclass(frozen=True)
class StrategyCapabilityRequirements:
    """Explicit public capabilities required from a strategy plugin."""

    manifest_version: int | None = None
    strategy_id: str | None = None
    position_snapshots: bool = False
    recovery_status: bool = False
    market_features: bool = False
    range_speed_history: bool = False
    startup_preview: bool = False
    pending_work: bool = False


@dataclass(frozen=True)
class StrategyRuntimeRequirements:
    """Strategy-declared runtime data requirements.

    Runtime uses this manifest to decide which warmup services, market-data
    producers and feature pipelines to start. Strategies declare requirements;
    they do not build exchange adapters or data pipelines themselves.
    """

    closed_kline: ClosedKlineRequirement = field(default_factory=ClosedKlineRequirement)
    trades: TradeStreamRequirement = field(default_factory=TradeStreamRequirement)
    order_book: OrderBookRequirement = field(default_factory=OrderBookRequirement)
    range_bars: RangeBarRequirement = field(default_factory=RangeBarRequirement)
    private_account_stream: PrivateAccountStreamRequirement = field(default_factory=PrivateAccountStreamRequirement)
    account_state: AccountStateRequirement = field(default_factory=AccountStateRequirement)
    order_state: OrderStateRequirement = field(default_factory=OrderStateRequirement)
    capabilities: StrategyCapabilityRequirements = field(
        default_factory=StrategyCapabilityRequirements
    )
    capability_manifest_declared: bool = False

    def __post_init__(self) -> None:
        validate_strategy_runtime_requirements(self)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "StrategyRuntimeRequirements":
        raw = dict(data or {})
        manifest_declared = "capabilities" in raw
        return cls(
            closed_kline=_closed_kline(raw.get("closed_kline")),
            trades=_trades(raw.get("trades")),
            order_book=_order_book(raw.get("order_book")),
            range_bars=_range_bars(raw.get("range_bars")),
            private_account_stream=_private_account(raw.get("private_account_stream")),
            account_state=_account_state(raw.get("account_state")),
            order_state=_order_state(raw.get("order_state")),
            capabilities=(
                _capabilities(raw["capabilities"])
                if manifest_declared
                else StrategyCapabilityRequirements()
            ),
            capability_manifest_declared=manifest_declared,
        )

    @classmethod
    def from_config_file(cls, path: str | Path) -> "StrategyRuntimeRequirements":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_mapping(data.get("runtime_requirements", data))

    @classmethod
    def from_data_streams(cls, data_streams: tuple[str, ...]) -> "StrategyRuntimeRequirements":
        streams = {item.strip().lower() for item in data_streams}
        return cls(
            trades=TradeStreamRequirement(enabled="trades" in streams, stream_enabled="trades" in streams),
            order_book=OrderBookRequirement(enabled=("order_book" in streams or "books" in streams), stream_enabled=("order_book" in streams or "books" in streams)),
        )


def validate_strategy_runtime_requirements(
    requirements: StrategyRuntimeRequirements,
) -> StrategyRuntimeRequirements:
    """Validate the capability manifest without coercing dataclass fields."""

    if not isinstance(requirements, StrategyRuntimeRequirements):
        raise StrategyCapabilityError(
            "strategy runtime requirements must be StrategyRuntimeRequirements"
        )
    if type(requirements.capability_manifest_declared) is not bool:
        raise StrategyCapabilityError(
            "strategy capability_manifest_declared must be bool"
        )

    capabilities = requirements.capabilities
    if type(capabilities) is not StrategyCapabilityRequirements:
        raise StrategyCapabilityError(
            "strategy capabilities must be StrategyCapabilityRequirements"
        )

    if requirements.capability_manifest_declared:
        if (
            type(capabilities.manifest_version) is not int
            or capabilities.manifest_version != 1
        ):
            raise StrategyCapabilityError(
                "strategy capability manifest_version must be integer 1"
            )
        if (
            not isinstance(capabilities.strategy_id, str)
            or not capabilities.strategy_id.strip()
        ):
            raise StrategyCapabilityError(
                "strategy capability strategy_id must be a non-empty string"
            )
        invalid_boolean_fields = [
            name
            for name in _CAPABILITY_BOOLEAN_FIELDS
            if type(getattr(capabilities, name)) is not bool
        ]
        if invalid_boolean_fields:
            raise StrategyCapabilityError(
                "strategy capability values must be bool | "
                f"invalid={invalid_boolean_fields}"
            )
        return requirements

    if capabilities.manifest_version is not None:
        raise StrategyCapabilityError(
            "undeclared strategy capability manifest_version must be None"
        )
    if capabilities.strategy_id is not None:
        raise StrategyCapabilityError(
            "undeclared strategy capability strategy_id must be None"
        )
    invalid_undeclared_fields = [
        name
        for name in _CAPABILITY_BOOLEAN_FIELDS
        if type(getattr(capabilities, name)) is not bool
        or getattr(capabilities, name) is not False
    ]
    if invalid_undeclared_fields:
        raise StrategyCapabilityError(
            "undeclared strategy capability values must be bool False | "
            f"invalid={invalid_undeclared_fields}"
        )
    return requirements


def resolve_strategy_runtime_requirements(strategy: object, *, fallback_data_streams: tuple[str, ...] = ()) -> StrategyRuntimeRequirements:
    """Resolve runtime requirements from a strategy object.

    Supported strategy forms, in order:
      1. ``strategy.runtime_requirements()`` method
      2. ``strategy.runtime_requirements`` attribute
      3. legacy fallback from app ``data_streams``
    """

    value = getattr(strategy, "runtime_requirements", None)
    if callable(value):
        try:
            value = value()
        except StrategyCapabilityError:
            raise
        except Exception as exc:
            raise StrategyCapabilityError(
                "strategy runtime requirements provider failed | "
                f"provider={type(strategy).__module__}.{type(strategy).__qualname__} | "
                f"error={type(exc).__name__}: {exc}"
            ) from exc
    if isinstance(value, StrategyRuntimeRequirements):
        return validate_strategy_runtime_requirements(value)
    if isinstance(value, Mapping):
        return StrategyRuntimeRequirements.from_mapping(value)
    return StrategyRuntimeRequirements.from_data_streams(tuple(fallback_data_streams))


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value or {}) if isinstance(value, Mapping) else {}


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _closed_kline(value: Any) -> ClosedKlineRequirement:
    raw = _mapping(value)
    return ClosedKlineRequirement(
        enabled=_bool(raw.get("enabled"), False),
        interval=str(raw.get("interval", "4h")),
        warmup_days=int(raw.get("warmup_days", 0) or 0),
        close_buffer_ms=None if raw.get("close_buffer_ms") is None else int(raw.get("close_buffer_ms")),
        retry_interval_ms=None if raw.get("closed_bar_retry_interval_ms", raw.get("retry_interval_ms")) is None else int(raw.get("closed_bar_retry_interval_ms", raw.get("retry_interval_ms"))),
        missing_alert_after_ms=None if raw.get("closed_bar_missing_alert_after_ms", raw.get("missing_alert_after_ms")) is None else int(raw.get("closed_bar_missing_alert_after_ms", raw.get("missing_alert_after_ms"))),
        min_records=int(raw.get("min_records", 1) or 1),
    )


def _trades(value: Any) -> TradeStreamRequirement:
    raw = _mapping(value)
    enabled = _bool(raw.get("enabled"), False)
    return TradeStreamRequirement(
        enabled=enabled,
        stream_enabled=_bool(raw.get("stream_enabled"), enabled),
        warmup_enabled=_bool(raw.get("warmup_enabled"), False),
        warmup_days=int(raw.get("warmup_days", 0) or 0),
    )


def _order_book(value: Any) -> OrderBookRequirement:
    raw = _mapping(value)
    enabled = _bool(raw.get("enabled"), False)
    return OrderBookRequirement(enabled=enabled, stream_enabled=_bool(raw.get("stream_enabled"), enabled))


def _range_bars(value: Any) -> RangeBarRequirement:
    raw = _mapping(value)
    return RangeBarRequirement(
        enabled=_bool(raw.get("enabled"), False),
        range_pct=Decimal(str(raw.get("range_pct", "0.002"))),
        aggregate_interval=str(raw.get("aggregate_interval", "4h")),
        min_bars=max(1, int(raw.get("min_bars", 1) or 1)),
    )


def _private_account(value: Any) -> PrivateAccountStreamRequirement:
    raw = _mapping(value)
    return PrivateAccountStreamRequirement(enabled=_bool(raw.get("enabled"), False))


def _account_state(value: Any) -> AccountStateRequirement:
    raw = _mapping(value)
    return AccountStateRequirement(
        startup_snapshot_enabled=_bool(raw.get("startup_snapshot_enabled"), True),
        poll_enabled=_bool(raw.get("poll_enabled"), True),
        poll_interval_seconds=int(raw.get("poll_interval_seconds", 300) or 300),
        post_order_sync_enabled=_bool(raw.get("post_order_sync_enabled"), True),
        consecutive_failure_alert_threshold=int(raw.get("consecutive_failure_alert_threshold", 3) or 3),
    )


def _order_state(value: Any) -> OrderStateRequirement:
    raw = _mapping(value)
    return OrderStateRequirement(
        post_submit_sync_enabled=_bool(raw.get("post_submit_sync_enabled"), True),
        poll_when_position_enabled=_bool(raw.get("poll_when_position_enabled"), True),
        poll_interval_seconds=int(raw.get("poll_interval_seconds", 20) or 20),
        sync_open_orders=_bool(raw.get("sync_open_orders"), True),
        sync_open_stop_orders=_bool(raw.get("sync_open_stop_orders"), True),
        sync_position=_bool(raw.get("sync_position"), True),
        consecutive_failure_alert_threshold=int(raw.get("consecutive_failure_alert_threshold", 3) or 3),
    )


def _capabilities(value: Any) -> StrategyCapabilityRequirements:
    if not isinstance(value, Mapping):
        raise StrategyCapabilityError(
            "strategy capability manifest must be a mapping"
        )
    try:
        raw = dict(value)
    except Exception as exc:
        raise StrategyCapabilityError(
            "strategy capability manifest could not be read | "
            f"error={type(exc).__name__}: {exc}"
        ) from exc
    actual_fields = set(raw)
    missing = sorted(_CAPABILITY_MANIFEST_FIELDS - actual_fields)
    unknown = sorted(
        actual_fields - _CAPABILITY_MANIFEST_FIELDS,
        key=repr,
    )
    if missing or unknown:
        raise StrategyCapabilityError(
            "strategy capability manifest fields must match schema exactly | "
            f"missing={missing} | unknown={unknown}"
        )

    manifest_version = raw["manifest_version"]
    if type(manifest_version) is not int or manifest_version != 1:
        raise StrategyCapabilityError(
            "strategy capability manifest_version must be integer 1"
        )

    strategy_id = raw["strategy_id"]
    if not isinstance(strategy_id, str) or not strategy_id.strip():
        raise StrategyCapabilityError(
            "strategy capability strategy_id must be a non-empty string"
        )

    invalid_boolean_fields = [
        name
        for name in _CAPABILITY_BOOLEAN_FIELDS
        if type(raw[name]) is not bool
    ]
    if invalid_boolean_fields:
        raise StrategyCapabilityError(
            "strategy capability values must be bool | "
            f"invalid={invalid_boolean_fields}"
        )

    return StrategyCapabilityRequirements(
        manifest_version=manifest_version,
        strategy_id=strategy_id.strip(),
        position_snapshots=raw["position_snapshots"],
        recovery_status=raw["recovery_status"],
        market_features=raw["market_features"],
        range_speed_history=raw["range_speed_history"],
        startup_preview=raw["startup_preview"],
        pending_work=raw["pending_work"],
    )
