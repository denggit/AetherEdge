from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.platform.account.ports import AccountClient
from src.platform.config import get_project_env_config, load_env_config
from src.platform.exchanges.models import ExchangeName, LeverageInfo, MarginMode, Order, Position
from src.platform.execution.ports import ExecutionClient
from src.utils.log import get_logger

logger = get_logger(__name__)


LEVERAGE_ENV_KEYS: Mapping[str, str] = {
    "okx": "OKX_LEVERAGE",
    "binance": "BINANCE_LEVERAGE",
}


class AccountConfigBootstrapError(RuntimeError):
    pass


_EXPOSURE_BLOCKED_REASONS: frozenset[str] = frozenset({
    "existing_exposure_config_unverified",
    "existing_exposure_config_mismatch",
})


@dataclass(frozen=True)
class AccountConfigTarget:
    exchange: ExchangeName
    symbol: str
    margin_mode: MarginMode
    leverage: Decimal


@dataclass(frozen=True)
class AccountConfigEnv:
    margin_mode: MarginMode
    targets: tuple[AccountConfigTarget, ...]
    missing_leverage: tuple[ExchangeName, ...] = ()

    def target_for(self, exchange: ExchangeName) -> AccountConfigTarget | None:
        return next((target for target in self.targets if target.exchange == exchange), None)


@dataclass(frozen=True)
class AccountConfigBootstrapResult:
    exchange: ExchangeName
    symbol: str
    expected_margin_mode: MarginMode
    expected_leverage: Decimal
    before_margin_mode: MarginMode | None
    before_leverage: Decimal | None
    after_margin_mode: MarginMode | None
    after_leverage: Decimal | None
    active_positions: tuple[Position, ...] = ()
    open_orders: tuple[Order, ...] = ()
    open_stop_orders: tuple[Order, ...] = ()
    applied: bool = False
    verified: bool = False
    skipped_write: bool = False
    reason: str | None = None
    error: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def has_blockers(self) -> bool:
        return bool(self.active_positions or self.open_orders or self.open_stop_orders)

    @property
    def ok(self) -> bool:
        return self.verified and self.error is None

    @property
    def runtime_start_allowed(self) -> bool:
        """Runtime may start even if config is not verified, as long as
        the blocker is existing exposure (positions/orders)."""
        if self.verified:
            return True
        return self.reason in _EXPOSURE_BLOCKED_REASONS

    @property
    def new_entries_allowed(self) -> bool:
        """New position entries are only allowed when config is fully verified."""
        return self.verified

    def detail(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange.value,
            "symbol": self.symbol,
            "expected_margin_mode": self.expected_margin_mode.value,
            "expected_leverage": str(self.expected_leverage),
            "before_margin_mode": None if self.before_margin_mode is None else self.before_margin_mode.value,
            "before_leverage": None if self.before_leverage is None else str(self.before_leverage),
            "after_margin_mode": None if self.after_margin_mode is None else self.after_margin_mode.value,
            "after_leverage": None if self.after_leverage is None else str(self.after_leverage),
            "active_positions": len(self.active_positions),
            "open_orders": len(self.open_orders),
            "open_stop_orders": len(self.open_stop_orders),
            "applied": self.applied,
            "verified": self.verified,
            "skipped_write": self.skipped_write,
            "reason": self.reason,
        }


def load_account_config_env(
    *,
    exchanges: Sequence[ExchangeName],
    symbol: str,
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    require_leverage: bool = False,
) -> AccountConfigEnv:
    values = _load_env(env_file=env_file, environ=environ)
    margin_mode = _parse_margin_mode(values.get("MARGIN_MODE", "isolated"))
    targets: list[AccountConfigTarget] = []
    missing: list[ExchangeName] = []

    for exchange in exchanges:
        key = LEVERAGE_ENV_KEYS.get(exchange.value)
        if key is None:
            continue
        raw = values.get(key)
        if raw in (None, ""):
            missing.append(exchange)
            continue
        targets.append(
            AccountConfigTarget(
                exchange=exchange,
                symbol=symbol,
                margin_mode=margin_mode,
                leverage=_parse_leverage(raw, key=key),
            )
        )

    if require_leverage and missing:
        missing_text = ", ".join(LEVERAGE_ENV_KEYS[exchange.value] for exchange in missing)
        raise AccountConfigBootstrapError(f"missing exchange leverage env: {missing_text}")

    return AccountConfigEnv(margin_mode=margin_mode, targets=tuple(targets), missing_leverage=tuple(missing))


async def bootstrap_account_config(
    *,
    targets: Sequence[AccountConfigTarget],
    account_clients: Sequence[AccountClient],
    execution_clients: Sequence[ExecutionClient],
    apply: bool,
    dry_run: bool,
    fail_on_error: bool = True,
) -> tuple[AccountConfigBootstrapResult, ...]:
    account_by_exchange = {client.exchange: client for client in account_clients}
    execution_by_exchange = {client.exchange: client for client in execution_clients}
    results: list[AccountConfigBootstrapResult] = []

    for target in targets:
        account = account_by_exchange.get(target.exchange)
        execution = execution_by_exchange.get(target.exchange)
        if account is None or execution is None:
            message = f"account config clients missing for {target.exchange.value}"
            if fail_on_error:
                raise AccountConfigBootstrapError(message)
            results.append(_error_result(target, message))
            continue

        try:
            results.append(await _bootstrap_one(target=target, account=account, execution=execution, apply=apply, dry_run=dry_run))
        except Exception as exc:
            if fail_on_error:
                raise
            results.append(_error_result(target, str(exc)))

    return tuple(results)


async def _bootstrap_one(
    *,
    target: AccountConfigTarget,
    account: AccountClient,
    execution: ExecutionClient,
    apply: bool,
    dry_run: bool,
) -> AccountConfigBootstrapResult:
    positions = tuple(await account.fetch_positions())
    open_orders = tuple(await execution.fetch_open_orders())
    open_stop_orders = tuple(await execution.fetch_open_stop_orders())
    before = await account.fetch_leverage(margin_mode=target.margin_mode)

    before_margin = _normalize_margin_mode(before.margin_mode)
    before_leverage = before.leverage
    already_verified = _matches(before, target)
    if already_verified:
        logger.info(
            "Account config verified | exchange=%s symbol=%s margin_mode=%s leverage=%s",
            target.exchange.value,
            target.symbol,
            target.margin_mode.value,
            target.leverage,
        )
        return AccountConfigBootstrapResult(
            exchange=target.exchange,
            symbol=target.symbol,
            expected_margin_mode=target.margin_mode,
            expected_leverage=target.leverage,
            before_margin_mode=before_margin,
            before_leverage=before_leverage,
            after_margin_mode=before_margin,
            after_leverage=before_leverage,
            active_positions=positions,
            open_orders=open_orders,
            open_stop_orders=open_stop_orders,
            verified=True,
            reason="already_configured",
        )

    if positions or open_orders or open_stop_orders:
        # Existing exposure detected — never block runtime startup.
        # Instead, classify the config state and let the runner decide
        # whether to allow new entries.
        if before_leverage is None:
            # Leverage could not be read — config is unverified but not
            # necessarily wrong.
            reason = "existing_exposure_config_unverified"
            logger.warning(
                "Account config unverified (leverage unreadable) — existing exposure "
                "detected, writes skipped, new entries will be blocked | "
                "exchange=%s symbol=%s positions=%s open_orders=%s stop_orders=%s",
                target.exchange.value,
                target.symbol,
                len(positions),
                len(open_orders),
                len(open_stop_orders),
            )
        else:
            # Leverage was read but does not match the target.
            reason = "existing_exposure_config_mismatch"
            logger.warning(
                "Account config mismatch with existing exposure — writes skipped, "
                "new entries will be blocked | exchange=%s symbol=%s "
                "current_leverage=%s current_margin=%s target_leverage=%s target_margin=%s "
                "positions=%s open_orders=%s stop_orders=%s",
                target.exchange.value,
                target.symbol,
                before_leverage,
                before_margin,
                target.leverage,
                target.margin_mode.value,
                len(positions),
                len(open_orders),
                len(open_stop_orders),
            )
        return AccountConfigBootstrapResult(
            exchange=target.exchange,
            symbol=target.symbol,
            expected_margin_mode=target.margin_mode,
            expected_leverage=target.leverage,
            before_margin_mode=before_margin,
            before_leverage=before_leverage,
            after_margin_mode=before_margin,
            after_leverage=before_leverage,
            active_positions=positions,
            open_orders=open_orders,
            open_stop_orders=open_stop_orders,
            verified=False,
            skipped_write=True,
            reason=reason,
            error=None,  # CRITICAL: no error — this is a soft block, not a fatal failure
        )

    if dry_run or not apply:
        return AccountConfigBootstrapResult(
            exchange=target.exchange,
            symbol=target.symbol,
            expected_margin_mode=target.margin_mode,
            expected_leverage=target.leverage,
            before_margin_mode=before_margin,
            before_leverage=before_leverage,
            after_margin_mode=before_margin,
            after_leverage=before_leverage,
            active_positions=positions,
            open_orders=open_orders,
            open_stop_orders=open_stop_orders,
            verified=False,
            skipped_write=True,
            reason="dry_run" if dry_run else "read_only",
            error="account config does not match target and writes are disabled",
        )

    logger.info(
        "Applying account config | exchange=%s symbol=%s margin_mode=%s leverage=%s",
        target.exchange.value,
        target.symbol,
        target.margin_mode.value,
        target.leverage,
    )
    margin_result = await account.set_margin_mode(target.margin_mode)
    leverage_result = await account.set_leverage(target.leverage, margin_mode=target.margin_mode)
    after = await account.fetch_leverage(margin_mode=target.margin_mode)
    after_margin = _normalize_margin_mode(after.margin_mode)
    verified, reason, error = _resolve_verification(
        after=after,
        after_margin=after_margin,
        target=target,
        leverage_result=leverage_result,
    )

    return AccountConfigBootstrapResult(
        exchange=target.exchange,
        symbol=target.symbol,
        expected_margin_mode=target.margin_mode,
        expected_leverage=target.leverage,
        before_margin_mode=before_margin,
        before_leverage=before_leverage,
        after_margin_mode=after_margin,
        after_leverage=after.leverage,
        active_positions=positions,
        open_orders=open_orders,
        open_stop_orders=open_stop_orders,
        applied=True,
        verified=verified,
        reason=reason,
        error=error,
        raw={"set_margin_mode": margin_result, "set_leverage": leverage_result.raw, "fetch_leverage": after.raw},
    )


def _is_existing_exposure_result(result: AccountConfigBootstrapResult) -> bool:
    """Return True when the result indicates existing exposure blocked
    config verification — these are non-fatal and must not prevent startup."""
    return result.reason in _EXPOSURE_BLOCKED_REASONS


def raise_on_failed_account_config(results: Sequence[AccountConfigBootstrapResult]) -> None:
    failures = [
        result for result in results
        if not result.ok and not _is_existing_exposure_result(result)
    ]
    if not failures:
        return
    detail = "; ".join(
        f"{result.exchange.value}: {result.error or result.reason or 'not verified'}"
        for result in failures
    )
    raise AccountConfigBootstrapError(f"account config bootstrap failed: {detail}")


def _resolve_verification(
    *,
    after: LeverageInfo,
    after_margin: MarginMode | None,
    target: AccountConfigTarget,
    leverage_result: LeverageInfo,
) -> tuple[bool, str | None, str | None]:
    """Determine verification result after applying margin/leverage.

    Returns ``(verified, reason, error)``.

    Rules (in order):

    1. If ``after.leverage`` was read back explicitly and does not match the
       target, fail.
    2. If ``after.margin_mode`` was read back explicitly and does not match
       the target, fail.
    3. If the exchange cannot read back leverage (``after.leverage is None``)
       but margin mode matches and the *set_leverage* API response confirms
       the target leverage, tolerate the missing readback (Binance).
    4. Otherwise fail.
    """
    # Standard match: both leverage and margin mode read back correctly.
    if _matches(after, target):
        return True, "applied", None

    # Leverage was read back explicitly and does not match target.
    if after.leverage is not None and after.leverage != target.leverage:
        return False, "verification_mismatch", "account config verification mismatch after apply"

    # Margin mode was read back explicitly and does not match target.
    if after_margin is not None and after_margin is not target.margin_mode:
        return False, "verification_mismatch", "account config verification mismatch after apply"

    # Leverage readback unavailable (after.leverage is None) but margin mode
    # matches.  Trust the set_leverage API response — this is the Binance
    # (and similar) path where the read-only leverage endpoint is unavailable.
    if after.leverage is None and after_margin is target.margin_mode:
        if leverage_result.leverage == target.leverage:
            return True, "applied_leverage_readback_unavailable_clean_slate", None

    # Any other mismatch.
    return False, "verification_mismatch", "account config verification mismatch after apply"


def _matches(info: LeverageInfo, target: AccountConfigTarget) -> bool:
    return info.leverage == target.leverage and _normalize_margin_mode(info.margin_mode) is target.margin_mode


def _normalize_margin_mode(value: MarginMode | str | None) -> MarginMode | None:
    if value in (None, ""):
        return None
    if isinstance(value, MarginMode):
        return value
    return MarginMode(str(value).strip().lower())


def _parse_margin_mode(value: str) -> MarginMode:
    try:
        return MarginMode(str(value or "isolated").strip().lower())
    except ValueError as exc:
        raise AccountConfigBootstrapError(f"invalid MARGIN_MODE: {value!r}") from exc


def _parse_leverage(value: str, *, key: str) -> Decimal:
    try:
        leverage = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise AccountConfigBootstrapError(f"invalid {key}: {value!r}") from exc
    if leverage <= 0:
        raise AccountConfigBootstrapError(f"{key} must be > 0")
    return leverage


def _load_env(*, env_file: str | Path | None, environ: Mapping[str, str] | None) -> dict[str, str]:
    if environ is None and env_file is None:
        return dict(get_project_env_config().values)
    if environ is not None and env_file is None:
        return {str(key): str(value) for key, value in environ.items()}
    return dict(load_env_config(env_file, environ=environ))


def _error_result(target: AccountConfigTarget, message: str) -> AccountConfigBootstrapResult:
    return AccountConfigBootstrapResult(
        exchange=target.exchange,
        symbol=target.symbol,
        expected_margin_mode=target.margin_mode,
        expected_leverage=target.leverage,
        before_margin_mode=None,
        before_leverage=None,
        after_margin_mode=None,
        after_leverage=None,
        reason="error",
        error=message,
    )
