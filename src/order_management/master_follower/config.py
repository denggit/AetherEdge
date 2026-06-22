from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.platform.config import load_env_config
from src.platform.exchanges.models import ExchangeName


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    retry_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be >= 0")


@dataclass(frozen=True)
class MasterFollowerPolicyConfig:
    master_exchange: ExchangeName
    follower_exchanges: tuple[ExchangeName, ...]
    entry_deviation_alert_pct: Decimal = Decimal("0.005")
    follower_entry_retry: RetryPolicy = RetryPolicy()
    master_entry_retry: RetryPolicy = RetryPolicy()
    follower_close_retry: RetryPolicy = RetryPolicy(max_attempts=3, retry_delay_seconds=1.0)
    manual_grace_seconds_after_master_fail: int = 1800
    close_orphan_follower_after_grace: bool = True
    do_not_rejoin_mid_position_after_follower_desync: bool = True

    @classmethod
    def from_env(
        cls,
        *,
        app_exchanges: tuple[ExchangeName, ...],
        data_exchange: ExchangeName,
        env: Mapping[str, str] | None = None,
    ) -> "MasterFollowerPolicyConfig":
        values = load_env_config() if env is None else {str(key): str(value) for key, value in env.items()}
        exchanges = _unique(app_exchanges)
        if not exchanges:
            raise ValueError("at least one app exchange is required")

        master = _exchange(values.get("AETHER_MASTER_EXCHANGE"), default=data_exchange)
        if master not in exchanges:
            raise ValueError(f"master exchange {master.value!r} is not configured in AETHER_EXCHANGES")

        raw_followers = values.get("AETHER_FOLLOWER_EXCHANGES")
        if raw_followers is None:
            followers = tuple(exchange for exchange in exchanges if exchange is not master)
        else:
            followers = tuple(exchange for exchange in _exchange_tuple(raw_followers) if exchange is not master)
            followers = _unique(followers)
            unknown_followers = tuple(exchange for exchange in followers if exchange not in exchanges)
            if unknown_followers:
                names = ", ".join(exchange.value for exchange in unknown_followers)
                raise ValueError(f"follower exchanges are not configured in AETHER_EXCHANGES: {names}")

        return cls(
            master_exchange=master,
            follower_exchanges=followers,
            entry_deviation_alert_pct=Decimal(str(values.get("AETHER_ENTRY_DEVIATION_ALERT_PCT", "0.005"))),
            follower_entry_retry=RetryPolicy(
                max_attempts=int(values.get("AETHER_FOLLOWER_ENTRY_MAX_ATTEMPTS", 3)),
                retry_delay_seconds=float(values.get("AETHER_FOLLOWER_ENTRY_RETRY_DELAY_SECONDS", 10)),
            ),
            master_entry_retry=RetryPolicy(
                max_attempts=int(values.get("AETHER_MASTER_ENTRY_MAX_ATTEMPTS", 3)),
                retry_delay_seconds=float(values.get("AETHER_MASTER_ENTRY_RETRY_DELAY_SECONDS", 10)),
            ),
            follower_close_retry=RetryPolicy(
                max_attempts=int(values.get("AETHER_FOLLOWER_CLOSE_MAX_ATTEMPTS", 3)),
                retry_delay_seconds=float(values.get("AETHER_FOLLOWER_CLOSE_RETRY_DELAY_SECONDS", 1.0)),
            ),
            manual_grace_seconds_after_master_fail=int(values.get("AETHER_MASTER_FAIL_MANUAL_GRACE_SECONDS", 1800)),
            close_orphan_follower_after_grace=_bool(values.get("AETHER_CLOSE_ORPHAN_FOLLOWER_AFTER_GRACE", True)),
            do_not_rejoin_mid_position_after_follower_desync=_bool(values.get("AETHER_DO_NOT_REJOIN_MID_POSITION_AFTER_FOLLOWER_DESYNC", True)),
        )


def _exchange(raw: str | None, *, default: ExchangeName) -> ExchangeName:
    if raw is None or not str(raw).strip():
        return default
    return ExchangeName(str(raw).strip().lower())


def _exchange_tuple(raw: str) -> tuple[ExchangeName, ...]:
    return tuple(ExchangeName(value.strip().lower()) for value in raw.split(",") if value.strip())


def _unique(exchanges: Sequence[ExchangeName]) -> tuple[ExchangeName, ...]:
    seen: set[ExchangeName] = set()
    values: list[ExchangeName] = []
    for exchange in exchanges:
        if exchange in seen:
            continue
        seen.add(exchange)
        values.append(exchange)
    return tuple(values)


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
