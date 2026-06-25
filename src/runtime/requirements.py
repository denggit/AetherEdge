from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping
import json


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

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "StrategyRuntimeRequirements":
        raw = dict(data or {})
        return cls(
            closed_kline=_closed_kline(raw.get("closed_kline")),
            trades=_trades(raw.get("trades")),
            order_book=_order_book(raw.get("order_book")),
            range_bars=_range_bars(raw.get("range_bars")),
            private_account_stream=_private_account(raw.get("private_account_stream")),
            account_state=_account_state(raw.get("account_state")),
            order_state=_order_state(raw.get("order_state")),
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


def resolve_strategy_runtime_requirements(strategy: object, *, fallback_data_streams: tuple[str, ...] = ()) -> StrategyRuntimeRequirements:
    """Resolve runtime requirements from a strategy object.

    Supported strategy forms, in order:
      1. ``strategy.runtime_requirements()`` method
      2. ``strategy.runtime_requirements`` attribute
      3. legacy fallback from app ``data_streams``
    """

    value = getattr(strategy, "runtime_requirements", None)
    if callable(value):
        value = value()
    if isinstance(value, StrategyRuntimeRequirements):
        return value
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
