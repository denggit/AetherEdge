from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import time
from typing import Any, Callable, Mapping, Sequence
from src.app.alerts import AppAlert
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange, WarmupRequest, WarmupResult
from src.market_data.ports import KlineRepository
from src.market_data.range_checkpoint import (
    RangeCheckpointRecovery,
    RangeCheckpointWriter,
    SqliteRangeCheckpointStore,
)
from src.market_data.storage import SqliteKlineStore
from src.market_data.warmup.service import KlineWarmupService
from src.platform.markets import get_market_profile
from src.runtime.account_config import (
    AccountConfigBootstrapResult,
    AccountConfigEnv,
    bootstrap_account_config,
    load_account_config_env,
    raise_on_failed_account_config,
)
from src.runtime.features import closed_kline_feature, range_aggregate_unavailable_feature
from src.runtime.position_mode_gate import (
    fetch_position_mode_statuses,
    resolve_position_mode_requirements,
)
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.tasks.scheduler import closed_bar_open_time_ms

from src.runtime.live_helpers import _all_exchange_sandbox
from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


@dataclass(frozen=True)
class _ClosedKlineWarmupAudit:
    time_range: TimeRange
    newly_loaded_records: int
    available_before_backfill: int
    available_after_backfill: int
    backfill_attempted: bool
    min_records: int
    store_class: str
    store_path: str


class StartupComponent(RuntimeComponent):
    async def _check_strategy_position_mode_requirements(
        self,
    ) -> None:
        try:
            requirements = resolve_position_mode_requirements(
                self.context.strategy
            )
        except Exception as exc:
            raise LiveRuntimeError(
                "strategy position mode requirement failed | "
                f"invalid_requirement={type(exc).__name__}: {exc}"
            ) from exc
        if not requirements:
            return

        strategy_id = self._strategy_capabilities().identity
        audit: dict[str, Any] = {
            "strategy": strategy_id,
            "symbol": self.app_config.symbol,
            "ok": True,
            "requirements": [],
            "source": "startup_hard_gate",
        }
        failures: list[str] = []
        for requirement in requirements:
            statuses = await fetch_position_mode_statuses(
                exchanges=requirement.exchanges,
                symbol=self.app_config.symbol,
                account_clients=self._get_account_clients(),
                source="startup_hard_gate",
            )
            requirement_ok = bool(statuses) and all(
                status.mode == requirement.required_mode.value
                for status in statuses
            )
            requirement_audit = {
                "required_mode": requirement.required_mode.value,
                "requirement_source": requirement.source,
                "ok": requirement_ok,
                "exchanges": [
                    status.audit(requirement.required_mode)
                    for status in statuses
                ],
            }
            audit["requirements"].append(requirement_audit)
            audit["ok"] = bool(audit["ok"]) and requirement_ok
            if not statuses:
                failures.append(
                    "position_mode_requirement_has_no_exchanges"
                )

            for status in statuses:
                status_ok = (
                    status.mode == requirement.required_mode.value
                )
                log_args = (
                    strategy_id,
                    status.exchange.value,
                    status.symbol,
                    requirement.required_mode.value,
                    status.mode,
                    status.error,
                )
                if status_ok:
                    logger.info(
                        "Strategy position mode validated | "
                        "strategy=%s exchange=%s symbol=%s "
                        "required_mode=%s actual_mode=%s "
                        "source=startup_hard_gate error=%s",
                        *log_args,
                    )
                    continue
                logger.error(
                    "Strategy position mode validation failed | "
                    "strategy=%s exchange=%s symbol=%s "
                    "required_mode=%s actual_mode=%s "
                    "source=startup_hard_gate error=%s",
                    *log_args,
                )
                failures.append(
                    f"{status.exchange.value}={status.mode}"
                )

        self._set_health(
            self._health.phase,
            metadata={
                **dict(self._health.metadata),
                "position_mode_requirements": audit,
            },
        )

        if failures:
            raise LiveRuntimeError(
                "strategy position mode requirement failed | "
                f"strategy={strategy_id} symbol={self.app_config.symbol} "
                f"issues={failures}"
            )

    async def _bootstrap_account_config_if_enabled(self) -> None:
        if self.runtime_config.mode is not RuntimeMode.LIVE_RUNTIME:
            return

        project_env = self._project_env
        live_trading = project_env.get_bool("AETHER_LIVE_TRADING", False)
        require_leverage = live_trading and not self.app_config.dry_run
        env = load_account_config_env(
            exchanges=self.app_config.exchanges,
            symbol=self.app_config.symbol,
            environ=project_env.values,
            require_leverage=require_leverage,
        )
        self._account_config_env = env
        if env.missing_leverage:
            logger.warning(
                "Account config leverage env missing; skipping exchanges | exchanges=%s dry_run=%s live_trading=%s",
                ",".join(exchange.value for exchange in env.missing_leverage),
                self.app_config.dry_run,
                live_trading,
            )
        if not env.targets:
            return

        apply_writes = (not self.app_config.dry_run) and (
            live_trading or _all_exchange_sandbox(self.app_config.exchanges, project_env)
        )
        self._account_config_apply_writes = apply_writes
        results = await bootstrap_account_config(
            targets=env.targets,
            account_clients=self._get_account_clients(),
            execution_clients=self._get_execution_clients(),
            apply=apply_writes,
            dry_run=self.app_config.dry_run,
            fail_on_error=require_leverage,
        )
        # Store results for downstream inspection
        self._account_config_results = tuple(results)

        for result in results:
            log = logger.info if result.ok else logger.warning
            log(
                "Account config bootstrap result | exchange=%s symbol=%s applied=%s verified=%s reason=%s error=%s",
                result.exchange.value,
                result.symbol,
                result.applied,
                result.verified,
                result.reason,
                result.error,
            )
        if require_leverage:
            raise_on_failed_account_config(results)

        # Check if any exchange has existing exposure that blocked config verification.
        # These are non-fatal — runtime starts but new entries are blocked.
        _EXPOSURE_BLOCKED = {"existing_exposure_config_unverified", "existing_exposure_config_mismatch"}
        exposure_blocked = [r for r in results if r.reason in _EXPOSURE_BLOCKED]
        if exposure_blocked:
            self._account_config_new_entries_blocked = True
            for blocked in exposure_blocked:
                severity = "critical" if blocked.reason == "existing_exposure_config_mismatch" else "warning"
                logger.log(
                    logging.CRITICAL if severity == "critical" else logging.WARNING,
                    "Account config: existing exposure detected — new entries blocked | "
                    "exchange=%s symbol=%s reason=%s positions=%s orders=%s stop_orders=%s",
                    blocked.exchange.value,
                    blocked.symbol,
                    blocked.reason,
                    len(blocked.active_positions),
                    len(blocked.open_orders),
                    len(blocked.open_stop_orders),
                )
                self.context.alerts.emit(
                    AppAlert(
                        subject=f"AetherEdge account config: {blocked.reason}",
                        severity=severity,
                        content=(
                            f"exchange={blocked.exchange.value}\n"
                            f"symbol={blocked.symbol}\n"
                            f"reason={blocked.reason}\n"
                            f"positions={len(blocked.active_positions)}\n"
                            f"open_orders={len(blocked.open_orders)}\n"
                            f"stop_orders={len(blocked.open_stop_orders)}\n"
                            f"new_entries_blocked=True\n"
                        ),
                    )
                )

    async def _recheck_account_config_after_recovery(self) -> None:
        """After recovery, if all positions are now flat, re-run account config
        bootstrap to try to clear the entry block."""
        env = self._account_config_env
        if env is None or not env.targets:
            return

        # Check if any exchange still has positions
        account_clients = self._get_account_clients()
        execution_clients = self._get_execution_clients()

        still_has_exposure = False
        for target in env.targets:
            account = next((a for a in account_clients if a.exchange == target.exchange), None)
            execution = next((e for e in execution_clients if e.exchange == target.exchange), None)
            if account is None or execution is None:
                continue
            try:
                positions = await account.fetch_positions()
                open_orders = await execution.fetch_open_orders()
                open_stop_orders = await execution.fetch_open_stop_orders()
                if positions or open_orders or open_stop_orders:
                    still_has_exposure = True
                    break
            except Exception:
                still_has_exposure = True
                break

        if still_has_exposure:
            logger.info(
                "Post-recovery account config re-check: exposure still exists, entries remain blocked"
            )
            return

        # All flat — re-run bootstrap
        logger.info("Post-recovery account config re-check: all flat, re-running bootstrap")
        try:
            results = await bootstrap_account_config(
                targets=env.targets,
                account_clients=account_clients,
                execution_clients=execution_clients,
                apply=self._account_config_apply_writes,
                dry_run=self.app_config.dry_run,
                fail_on_error=False,
            )
            all_verified = all(r.verified for r in results)
            if all_verified:
                self._account_config_new_entries_blocked = False
                logger.info("Post-recovery account config verified — new entries re-enabled")
                self.context.alerts.emit(
                    AppAlert(
                        subject="AetherEdge account config verified after recovery",
                        severity="info",
                        content="All exchanges verified after positions closed. New entries re-enabled.",
                    )
                )
            else:
                logger.warning(
                    "Post-recovery account config re-check: not all verified | results=%s",
                    [r.detail() for r in results],
                )
        except Exception as exc:
            logger.warning("Post-recovery account config re-check failed: %s", exc)
            self.context.alerts.emit(
                AppAlert(
                    subject="AetherEdge post-recovery account config re-check failed",
                    severity="warning",
                    content=(
                        f"error={exc}\n"
                        f"new_entries_blocked=True (entries remain blocked until next restart)\n"
                    ),
                )
            )

    def _initialize_rangebar_trust_window(self) -> None:
        if not self.requirements.range_bars.enabled or not self.requirements.trades.enabled:
            return
        module = self._require_range_module()
        now_ms = int(time.time() * 1000)
        recovery = (
            module.initial_recovery
            if getattr(self, "_market_modules_managed", False)
            else module.initialize_recovery()
        )
        if recovery is None:
            raise LiveRuntimeError("Range module recovery was not prepared")
        current_bucket = module.initial_bucket_ms
        configure_coverage = getattr(
            self.context.strategy, "configure_range_coverage", None
        )
        if callable(configure_coverage):
            configure_coverage(
                degraded_fast_margin=self.range_config.degraded_fast_margin
            )
        if not getattr(self, "_market_modules_managed", False):
            module.checkpoint_writer.start()
            self._launch_range_micro_repair_subprocess(recovery)
        logger.info(
            "Rangebar checkpoint recovery initialized | symbol=%s interval=%s now_ms=%s current_bucket_ms=%s trust_start_bucket_ms=%s coverage_status=%s checkpoint_age_ms=%s recovered=%s missing_gap_ms=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            now_ms,
            current_bucket,
            module.trust_start_bucket_ms,
            recovery.coverage_status,
            recovery.checkpoint_age_ms,
            recovery.recovered_from_checkpoint,
            recovery.missing_gap_ms,
        )

    def _launch_range_micro_repair_subprocess(
        self,
        recovery: RangeCheckpointRecovery,
    ) -> None:
        del recovery
        self._require_range_module().repair_now()

    async def _warmup_range_speed_history(self) -> int:
        warmup = self._range_speed_warmup
        if warmup is None:
            return 0
        if getattr(self, "_market_modules_managed", False):
            return warmup.complete_history
        return await warmup.warmup()

    async def _finish_range_speed_warmup_after_catchup(self) -> None:
        warmup = self._range_speed_warmup
        if warmup is not None:
            await warmup.finish_after_catchup(
                range_observed=self._startup_catchup_range_observed
            )

    async def _run_warmup(self) -> None:
        services = self.service_dependencies()
        warmup_services = services.warmup_services or services.warmup_service
        if warmup_services is not None:
            if not isinstance(warmup_services, (list, tuple)):
                warmup_services = (warmup_services,)
            for service in warmup_services:
                result = service() if callable(service) and not hasattr(service, "warmup") else service
                if hasattr(result, "warmup"):
                    maybe = result.warmup()
                else:
                    maybe = result
                if asyncio.iscoroutine(maybe):
                    await maybe
                self.stats.warmup_runs += 1
        await self._run_requirement_warmup()

    def _count_available_closed_klines(self, repository, *, symbol: str, interval: str, time_range: TimeRange) -> int:
        """Return the number of closed klines currently available in the repository.

        This counts **all** closed klines in the store for the given range,
        NOT just records that were newly saved by the most recent warmup pass.
        """
        rows = repository.load(symbol=symbol, interval=interval, time_range=time_range)
        return sum(1 for row in rows if row.is_closed)

    async def _run_requirement_warmup(self) -> None:
        services = self.service_dependencies()
        time_range = self._closed_kline_warmup_range()
        if time_range is None:
            return
        repository = services.kline_store or SqliteKlineStore()
        result = await KlineWarmupService(
            data_feed=self.context.data,
            repository=repository,
        ).warmup(
            WarmupRequest(
                symbol=self.app_config.symbol,
                dataset=MarketDataSet.KLINES,
                interval=self._closed_bar_interval,
                time_range=time_range,
            )
        )
        self.stats.warmup_runs += 1
        min_records = max(
            1,
            int(self.requirements.closed_kline.min_records or 1),
        )
        newly_loaded = result.records_loaded
        available_before = self._count_available_closed_klines(
            repository,
            symbol=self.app_config.symbol,
            interval=self._closed_bar_interval,
            time_range=time_range,
        )
        self._validate_closed_kline_warmup_result(
            result,
            newly_loaded=newly_loaded,
            available_records=available_before,
        )
        self._log_closed_kline_warmup(
            time_range=time_range,
            newly_loaded=newly_loaded,
            available_records=available_before,
            min_records=min_records,
            caught_up=result.caught_up,
        )

        store_path = str(getattr(repository, "path", ""))
        store_class = type(repository).__name__
        available_after, backfill_attempted = (
            await self._backfill_insufficient_closed_klines(
                repository=repository,
                time_range=time_range,
                newly_loaded=newly_loaded,
                available_records=available_before,
                min_records=min_records,
                store_class=store_class,
                store_path=store_path,
            )
        )
        await self._hydrate_strategy_closed_klines(
            repository,
            time_range=time_range,
        )
        self._handle_closed_kline_warmup_minimum(
            _ClosedKlineWarmupAudit(
                time_range=time_range,
                newly_loaded_records=newly_loaded,
                available_before_backfill=available_before,
                available_after_backfill=available_after,
                backfill_attempted=backfill_attempted,
                min_records=min_records,
                store_class=store_class,
                store_path=store_path,
            )
        )

    def _closed_kline_warmup_range(self) -> TimeRange | None:
        requirement = self.requirements.closed_kline
        if not requirement.enabled or requirement.warmup_days <= 0:
            return None
        end_open = closed_bar_open_time_ms(
            int(time.time() * 1000),
            interval_ms=self._closed_bar_interval_ms,
            close_buffer_ms=self._closed_bar_buffer_ms,
        )
        if end_open < 0:
            return None
        start_open = max(
            0,
            end_open - int(requirement.warmup_days) * 24 * 60 * 60_000,
        )
        return TimeRange(start_open, end_open)

    def _validate_closed_kline_warmup_result(
        self,
        result: WarmupResult,
        *,
        newly_loaded: int,
        available_records: int,
    ) -> None:
        if result.caught_up:
            return
        gap_details = [
            {
                "start_time_ms": gap.time_range.start_time_ms,
                "end_time_ms": gap.time_range.end_time_ms,
                "reason": gap.reason,
            }
            for gap in result.gaps_after[:10]
        ]
        logger.error(
            "Closed-kline warmup gaps remain | interval=%s gap_count=%s first_gaps=%s "
            "newly_loaded=%s available=%s",
            self._closed_bar_interval,
            len(result.gaps_after),
            gap_details,
            newly_loaded,
            available_records,
        )
        raise LiveRuntimeError(
            "closed-kline warmup did not catch up: "
            f"{len(result.gaps_after)} gaps remain"
        )

    def _log_closed_kline_warmup(
        self,
        *,
        time_range: TimeRange,
        newly_loaded: int,
        available_records: int,
        min_records: int,
        caught_up: bool,
    ) -> None:
        logger.info(
            "Closed-kline warmup completed | interval=%s start_open=%s end_open=%s "
            "newly_loaded=%s available=%s min_records=%s caught_up=%s",
            self._closed_bar_interval,
            time_range.start_time_ms,
            time_range.end_time_ms,
            newly_loaded,
            available_records,
            min_records,
            caught_up,
        )

    async def _backfill_insufficient_closed_klines(
        self,
        *,
        repository: KlineRepository,
        time_range: TimeRange,
        newly_loaded: int,
        available_records: int,
        min_records: int,
        store_class: str,
        store_path: str,
    ) -> tuple[int, bool]:
        if available_records >= min_records:
            return available_records, False
        logger.warning(
            "Closed-kline warmup insufficient — attempting REST backfill | "
            "symbol=%s interval=%s newly_loaded=%s available=%s min_records=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            newly_loaded,
            available_records,
            min_records,
        )
        try:
            from src.market_data.warmup.kline_provider import (
                MarketDataKlineProvider,
            )

            diagnostics = await MarketDataKlineProvider(
                data_feed=self.context.data,
                repository=repository,
            ).backfill_and_reload(
                symbol=self.app_config.symbol,
                interval=self._closed_bar_interval,
                time_range=time_range,
                min_records=min_records,
                store_class=store_class,
                store_path=store_path,
            )
            available_records = self._count_available_closed_klines(
                repository,
                symbol=self.app_config.symbol,
                interval=self._closed_bar_interval,
                time_range=time_range,
            )
            logger.info(
                "REST kline backfill completed | symbol=%s interval=%s "
                "fetched=%s saved=%s available_after=%s success=%s",
                diagnostics.symbol,
                diagnostics.interval,
                diagnostics.fetched_records,
                diagnostics.saved_records,
                available_records,
                diagnostics.success,
            )
            return available_records, True
        except Exception as exc:
            logger.error(
                "REST kline backfill failed | symbol=%s interval=%s error=%s",
                self.app_config.symbol,
                self._closed_bar_interval,
                exc,
            )
            return available_records, False

    def _handle_closed_kline_warmup_minimum(
        self,
        audit: _ClosedKlineWarmupAudit,
    ) -> None:
        if audit.available_after_backfill >= audit.min_records:
            return
        content = self._closed_kline_warmup_diagnostics(audit)
        if self.app_config.dry_run:
            logger.warning(
                "Closed-kline warmup loaded fewer records than required — continuing in dry-run mode | "
                "interval=%s warmup_days=%s available_records=%s min_records=%s",
                self._closed_bar_interval,
                self.requirements.closed_kline.warmup_days,
                audit.available_after_backfill,
                audit.min_records,
            )
            self.context.alerts.emit(
                AppAlert(
                    subject=(
                        "AetherEdge closed-kline warmup below minimum records"
                    ),
                    content=content,
                    severity="warning",
                )
            )
            return
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge closed-kline warmup failed",
                content=content,
                severity="error",
            )
        )
        raise LiveRuntimeError(
            "closed-kline warmup loaded insufficient records "
            f"(symbol={self.app_config.symbol} "
            f"interval={self._closed_bar_interval} "
            f"available_records={audit.available_after_backfill} "
            f"min_records={audit.min_records})"
        )

    def _closed_kline_warmup_diagnostics(
        self,
        audit: _ClosedKlineWarmupAudit,
    ) -> str:
        raw_aliases = "N/A"
        try:
            profile = get_market_profile(self.app_config.symbol)
            raw_aliases = ", ".join(
                f"{exchange.value}:{profile.raw_symbol(exchange)}"
                for exchange in profile.exchange_symbols
            )
        except Exception:
            pass
        start_open = audit.time_range.start_time_ms
        end_open = audit.time_range.end_time_ms
        start_utc = datetime.fromtimestamp(
            start_open / 1000,
            tz=timezone.utc,
        ).isoformat()
        end_utc = datetime.fromtimestamp(
            end_open / 1000,
            tz=timezone.utc,
        ).isoformat()
        return (
            f"symbol={self.app_config.symbol}\n"
            f"raw_aliases={raw_aliases}\n"
            f"interval={self._closed_bar_interval}\n"
            f"start_open_ms={start_open}\n"
            f"end_open_ms={end_open}\n"
            f"start_open_utc={start_utc}\n"
            f"end_open_utc={end_utc}\n"
            f"newly_loaded_records={audit.newly_loaded_records}\n"
            f"available_records_before_backfill={audit.available_before_backfill}\n"
            f"available_records_after_backfill={audit.available_after_backfill}\n"
            f"backfill_attempted={audit.backfill_attempted}\n"
            f"min_records={audit.min_records}\n"
            f"kline_store_class={audit.store_class}\n"
            f"kline_store_path={audit.store_path}\n"
            f"warmup_days={self.requirements.closed_kline.warmup_days}\n"
            f"dry_run={self.app_config.dry_run}\n"
        )

    async def _hydrate_strategy_closed_klines(self, repository, *, time_range: TimeRange) -> None:
        if not self._get_market_feature_pipeline().resolve_observers():
            return
        rows = repository.load(symbol=self.app_config.symbol, interval=self._closed_bar_interval, time_range=time_range)
        for row in rows:
            if not row.is_closed:
                continue
            await self.process_market_feature(closed_kline_feature(row))
