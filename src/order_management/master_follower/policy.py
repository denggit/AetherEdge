from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from src.order_management.master_follower.config import MasterFollowerPolicyConfig, RetryPolicy
from src.order_management.models import ExchangeOrderResult, OrderIntent
from src.platform.exchanges.models import ExchangeName


class MasterFollowerDecisionStatus(str, Enum):
    OK = "ok"
    MASTER_FAILED = "master_failed"
    FOLLOWER_FAILED_SKIPPED = "follower_failed_skipped"
    ORPHAN_FOLLOWER_REQUIRES_MANUAL = "orphan_follower_requires_manual"
    PRICE_DEVIATION_ALERT = "price_deviation_alert"


@dataclass(frozen=True)
class MasterFollowerExecutionPolicy:
    """Master/follower exchange execution policy.

    The master exchange defines strategy state. Followers try to mirror the master,
    but follower failures do not force master exits. The concrete exchange roles
    must come from app/runtime configuration rather than this generic policy.
    """

    master_exchange: ExchangeName
    follower_exchanges: tuple[ExchangeName, ...] = ()
    same_stop_price_as_master: bool = True
    entry_deviation_alert_pct: Decimal = Decimal("0.005")
    follower_entry_retry: RetryPolicy = field(default_factory=RetryPolicy)
    master_entry_retry: RetryPolicy = field(default_factory=RetryPolicy)
    manual_grace_seconds_after_master_fail: int = 1800
    close_orphan_follower_after_grace: bool = True
    do_not_rejoin_mid_position_after_follower_desync: bool = True

    @classmethod
    def from_config(cls, config: MasterFollowerPolicyConfig) -> "MasterFollowerExecutionPolicy":
        return cls(
            master_exchange=config.master_exchange,
            follower_exchanges=config.follower_exchanges,
            entry_deviation_alert_pct=config.entry_deviation_alert_pct,
            follower_entry_retry=config.follower_entry_retry,
            master_entry_retry=config.master_entry_retry,
            manual_grace_seconds_after_master_fail=config.manual_grace_seconds_after_master_fail,
            close_orphan_follower_after_grace=config.close_orphan_follower_after_grace,
            do_not_rejoin_mid_position_after_follower_desync=config.do_not_rejoin_mid_position_after_follower_desync,
        )

    @classmethod
    def from_env(
        cls,
        *,
        app_exchanges: tuple[ExchangeName, ...],
        data_exchange: ExchangeName,
        env: Mapping[str, str] | None = None,
    ) -> "MasterFollowerExecutionPolicy":
        return cls.from_config(
            MasterFollowerPolicyConfig.from_env(
                app_exchanges=app_exchanges,
                data_exchange=data_exchange,
                env=env,
            )
        )

    def __post_init__(self) -> None:
        followers: list[ExchangeName] = []
        seen: set[ExchangeName] = set()
        for exchange in self.follower_exchanges:
            if exchange is self.master_exchange or exchange in seen:
                continue
            seen.add(exchange)
            followers.append(exchange)
        object.__setattr__(self, "follower_exchanges", tuple(followers))
        if self.entry_deviation_alert_pct < 0:
            raise ValueError("entry_deviation_alert_pct must be >= 0")
        if self.manual_grace_seconds_after_master_fail < 0:
            raise ValueError("manual_grace_seconds_after_master_fail must be >= 0")

    def followers_for(self, target_exchanges: Sequence[ExchangeName]) -> tuple[ExchangeName, ...]:
        targets = set(target_exchanges)
        return tuple(exchange for exchange in self.follower_exchanges if exchange in targets)


@dataclass(frozen=True)
class MasterFollowerDecision:
    status: MasterFollowerDecisionStatus
    alerts: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def requires_alert(self) -> bool:
        return bool(self.alerts)


class MasterFollowerPolicyEvaluator:
    def __init__(self, policy: MasterFollowerExecutionPolicy) -> None:
        self.policy = policy

    def evaluate(self, *, intent: OrderIntent, results: Sequence[ExchangeOrderResult]) -> MasterFollowerDecision:
        by_exchange = {result.exchange: result for result in results}
        master = by_exchange.get(self.policy.master_exchange)
        followers = {exchange: by_exchange.get(exchange) for exchange in self.policy.followers_for(intent.target_exchanges)}
        metadata: dict[str, Any] = {
            "master_exchange": self.policy.master_exchange.value,
            "follower_exchanges": [exchange.value for exchange in followers],
            "same_stop_price_as_master": self.policy.same_stop_price_as_master,
        }
        alerts: list[str] = []
        actions: list[str] = []
        statuses: list[MasterFollowerDecisionStatus] = []

        master_ok = bool(master and master.ok)
        follower_ok = {exchange: bool(result and result.ok) for exchange, result in followers.items()}
        metadata["master_ok"] = master_ok
        metadata["follower_ok"] = {exchange.value: ok for exchange, ok in follower_ok.items()}

        if not master_ok:
            ok_followers = [exchange for exchange, ok in follower_ok.items() if ok]
            if ok_followers:
                statuses.append(MasterFollowerDecisionStatus.ORPHAN_FOLLOWER_REQUIRES_MANUAL)
                alerts.append("master_failed_with_follower_position")
                actions.append("alert_manual_handling")
                actions.append(f"wait_{self.policy.manual_grace_seconds_after_master_fail}s_before_orphan_close")
                if self.policy.close_orphan_follower_after_grace:
                    actions.append("close_orphan_follower_after_grace")
                metadata["orphan_followers"] = [exchange.value for exchange in ok_followers]
            else:
                statuses.append(MasterFollowerDecisionStatus.MASTER_FAILED)
                alerts.append("master_entry_failed")
                actions.append("alert_master_failed")
            return MasterFollowerDecision(status=_dominant_status(statuses), alerts=tuple(alerts), actions=tuple(actions), metadata=metadata)

        failed_followers = [exchange for exchange, ok in follower_ok.items() if not ok]
        if failed_followers:
            statuses.append(MasterFollowerDecisionStatus.FOLLOWER_FAILED_SKIPPED)
            alerts.append("follower_entry_failed_skipped")
            actions.append("retry_then_skip_failed_followers")
            metadata["skipped_followers"] = [exchange.value for exchange in failed_followers]

        deviation_alerts = self._entry_deviation_alerts(master, followers)
        if deviation_alerts:
            statuses.append(MasterFollowerDecisionStatus.PRICE_DEVIATION_ALERT)
            alerts.extend(alert for alert, _ in deviation_alerts)
            metadata["entry_deviation"] = [meta for _, meta in deviation_alerts]
            actions.append("alert_price_deviation_only")

        if not statuses:
            statuses.append(MasterFollowerDecisionStatus.OK)
        return MasterFollowerDecision(status=_dominant_status(statuses), alerts=tuple(alerts), actions=tuple(actions), metadata=metadata)

    def _entry_deviation_alerts(
        self,
        master: ExchangeOrderResult | None,
        followers: Mapping[ExchangeName, ExchangeOrderResult | None],
    ) -> list[tuple[str, dict[str, Any]]]:
        if master is None or master.avg_fill_price is None or master.avg_fill_price <= 0:
            return []
        alerts: list[tuple[str, dict[str, Any]]] = []
        for exchange, result in followers.items():
            if result is None or not result.ok or result.avg_fill_price is None:
                continue
            deviation = abs(result.avg_fill_price - master.avg_fill_price) / master.avg_fill_price
            if deviation >= self.policy.entry_deviation_alert_pct:
                alerts.append(
                    (
                        "entry_price_deviation_alert",
                        {
                            "exchange": exchange.value,
                            "master_avg_fill_price": str(master.avg_fill_price),
                            "follower_avg_fill_price": str(result.avg_fill_price),
                            "deviation_pct": str(deviation),
                            "threshold_pct": str(self.policy.entry_deviation_alert_pct),
                            "auto_fix": False,
                        },
                    )
                )
        return alerts


def _dominant_status(statuses: Sequence[MasterFollowerDecisionStatus]) -> MasterFollowerDecisionStatus:
    priority = [
        MasterFollowerDecisionStatus.ORPHAN_FOLLOWER_REQUIRES_MANUAL,
        MasterFollowerDecisionStatus.MASTER_FAILED,
        MasterFollowerDecisionStatus.FOLLOWER_FAILED_SKIPPED,
        MasterFollowerDecisionStatus.PRICE_DEVIATION_ALERT,
        MasterFollowerDecisionStatus.OK,
    ]
    for status in priority:
        if status in statuses:
            return status
    return MasterFollowerDecisionStatus.OK
