from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from src.market_data.events import MarketFeatureEvent
from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.reconcile.models import ReconcileReport
from src.signals import TradeSignal


@dataclass(frozen=True)
class StrategyRecoveryContext:
    """Strategy-facing recovery input.

    Runtime owns the generic recovery orchestration. Concrete strategies can use
    this context to rebuild their own internal state without importing runtime
    or exchange adapters.
    """

    snapshots: tuple[PlatformSnapshot, ...]
    reconcile_reports: tuple[ReconcileReport, ...]
    order_intent_ids: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyRecoveryStatus:
    blocking_manual_required: bool = False
    alerts: tuple[str, ...] = ()


class StrategyPort(Protocol):
    """Future strategy interface.

    A strategy observes normalized platform data and returns standardized
    signals. It must not import exchange adapters directly.
    """

    async def on_start(self, snapshot: PlatformSnapshot) -> Sequence[TradeSignal]:
        ...

    async def on_kline(self, kline: MarketKline) -> Sequence[TradeSignal]:
        ...

    async def on_ticker(self, ticker: MarketTicker) -> Sequence[TradeSignal]:
        ...

    async def on_trade(self, trade: MarketTrade) -> Sequence[TradeSignal]:
        ...

    async def on_order_book(self, order_book: MarketOrderBook) -> Sequence[TradeSignal]:
        ...

    async def on_account_event(self, event: AccountEvent) -> Sequence[TradeSignal]:
        ...


class RecoverableStrategyPort(Protocol):
    """Optional strategy extension used by runtime recovery."""

    async def recover(self, context: StrategyRecoveryContext) -> Sequence[TradeSignal]:
        ...


@runtime_checkable
class StrategyStopAdoptionProvider(Protocol):
    """Optional recovery output for stop references adopted by a plugin."""

    def pending_stop_adoptions(self) -> Sequence[Mapping[str, Any]]:
        ...

    def clear_pending_stop_adoptions(self) -> None:
        ...


@runtime_checkable
class RangeSpeedHistoryProvider(Protocol):
    """Optional strategy capability for past-only range-speed history."""

    def warmup_range_speed_history(self, rf_bar_counts: Sequence[int]) -> int:
        ...

    def replace_range_speed_history(self, rf_bar_counts: Sequence[int]) -> int:
        ...

    def range_speed_history_status(self) -> Mapping[str, int | bool]:
        ...


@runtime_checkable
class StrategyRecoveryStatusProvider(Protocol):
    def recovery_status(self) -> StrategyRecoveryStatus:
        ...


@runtime_checkable
class StrategyPositionPlanRecoveryUpdateProvider(Protocol):
    def position_plan_recovery_updates(self) -> Sequence[Mapping[str, Any]]:
        ...


@runtime_checkable
class StrategyRuntimeStateProvider(Protocol):
    """Optional runtime-facing view of plugin identity and transient state."""

    def strategy_identity(self) -> str:
        ...

    def has_pending_strategy_work(self) -> bool:
        ...

    def capture_startup_preview_state(self) -> object:
        ...

    def restore_startup_preview_state(self, state: object) -> None:
        ...


@runtime_checkable
class StrategyDecisionAuditProvider(Protocol):
    def decision_audit(self) -> Mapping[str, Any] | None:
        ...


class AccountSnapshotStrategyPort(Protocol):
    """Optional strategy extension for request-synced account snapshots."""

    async def on_account_snapshot(self, snapshot: PlatformSnapshot) -> None:
        ...


class MarketFeatureStrategyPort(Protocol):
    """Optional strategy extension for reusable market feature events."""

    async def on_market_feature(self, event: MarketFeatureEvent) -> Sequence[TradeSignal]:
        ...
