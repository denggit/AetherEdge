from __future__ import annotations

import time
from src.market_data.storage import SqliteKlineStore
from src.runtime.market_data.range_module import RangeBarModule
from src.runtime.range_repair_bootstrap import RangeRepairBootstrapService
from src.runtime.live_types import LiveRuntimeError
from src.runtime.components.base import RuntimeComponent


class RangeRuntimeComponent(RuntimeComponent):
    def _get_live_kline_store(self):
        services = self._runtime_service_bundle().market
        repository = services.kline_store
        if repository is None:
            repository = SqliteKlineStore(self.range_config.market_data_db_path)
            services.kline_store = repository
        return repository

    def _get_range_repair_bootstrap_service(
        self,
    ) -> RangeRepairBootstrapService:
        if self._range_repair_bootstrap_service is None:
            self._range_repair_bootstrap_service = (
                RangeRepairBootstrapService(
                    range_config=self.range_config,
                    exchange=self.app_config.data_exchange.value,
                    symbol=self.app_config.symbol,
                    range_pct=str(self._range_pct),
                    closed_bar_interval_ms=self._closed_bar_interval_ms,
                    checkpoint_store=self._require_range_module().checkpoint_store,
                    emit_alert=self.context.alerts.emit,
                    journal_store=self._require_range_module().repair_journal.store,
                    journal_writer=self._require_range_module().repair_journal.writer,
                    micro_repair_supervisor=(
                        None
                        if self._range_background is None
                        else self._range_background.micro_repair_supervisor
                    ),
                    clock_ms=lambda: int(time.time() * 1000),
                )
            )
        return self._range_repair_bootstrap_service

    def _start_range_speed_background_services(self) -> None:
        if not getattr(self, "_market_modules_managed", False):
            if self._range_background is not None:
                self._range_background.start(self._stop_event)

    async def _stop_market_data_modules(self) -> None:
        runtime = getattr(self, "_market_data_runtime", None)
        if runtime is not None:
            await runtime.stop()
            return
        module = self._range_module
        if module is None:
            return
        await module.stop()

    def _require_range_module(self) -> RangeBarModule:
        module = getattr(self, "_range_module", None)
        if module is None:
            raise LiveRuntimeError("Range capability is not enabled")
        return module
