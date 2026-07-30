from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.app import AppConfig, AppContext, build_app_context
from src.platform.data.factory import (
    create_full_order_book_stream,
    create_open_interest_stream,
    create_order_book_l2_stream,
    create_order_book_stream,
    create_trade_stream,
)
from src.platform.data.websocket import WebsocketsConnector
from src.platform.exchanges.config_loader import load_exchange_config
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.runtime.capabilities import capability_request_from_requirements
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.feature_pipeline import TradeFeatureRuntimeConfig
from src.runtime.market_data.catalog import (
    MarketDataModuleConfig,
    build_market_data_registry,
)
from src.runtime.market_data.features import (
    FixedTimeTradeBarModuleConfig,
    RangeFootprintModuleConfig,
    TradeFootprintModuleConfig,
)
from src.runtime.market_data.integrity import (
    IntegrityWindowState,
    OrderBookDataIntegrityTracker,
    TradeDataIntegrityTracker,
)
from src.runtime.market_data.pipeline_plan import (
    resolve_market_pipeline,
)
from src.runtime.market_data.processor import (
    MarketEventProcessor,
)
from src.runtime.market_data.range_config import (
    RangeRuntimeConfig,
    range_runtime_config_from_env,
)
from src.runtime.market_data.runtime import MarketDataRuntime
from src.runtime.runner import LiveRuntimeRunner, LiveRuntimeStats
from src.runtime.services import RuntimeServices
from src.utils.log import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class LiveRuntimeApplication:
    """The one composed live application exposed to the process entrypoint."""

    runner: LiveRuntimeRunner
    market_data: MarketDataRuntime

    async def run(
        self,
        *,
        max_market_events: int | None = None,
    ) -> LiveRuntimeStats:
        return await self.runner.run(max_market_events=max_market_events)


def compose_live_runtime(
    app_config: AppConfig,
    *,
    defaults_path: str | Path = "config/aether_defaults.json",
    app_context: AppContext | None = None,
    runtime_config: LiveRuntimeConfig | None = None,
    range_config: RangeRuntimeConfig | None = None,
    services: RuntimeServices | None = None,
) -> LiveRuntimeApplication:
    """Build every formal live dependency without opening market streams."""

    if app_config.data_exchange != ExchangeName.OKX:
        raise ValueError(
            "OKX is the only supported market-data exchange; "
            f"got {app_config.data_exchange.value}"
        )
    context = app_context or build_app_context(
        app_config,
        enable_market_streams=False,
    )
    runtime_settings = runtime_config or live_runtime_config_from_app(
        app_config,
        defaults_path=defaults_path,
    )
    range_settings = range_config or range_runtime_config_from_env(
        defaults_path=defaults_path,
    )
    runtime_services = services or RuntimeServices()

    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=context,
        runtime_config=runtime_settings,
        range_config=range_settings,
        managed_market_modules=True,
        services=runtime_services,
    )

    service_bundle = runner.service_bundle
    feature_config = service_bundle.market.trade_feature_config
    if not isinstance(feature_config, TradeFeatureRuntimeConfig):
        raise TypeError("composition did not produce TradeFeatureRuntimeConfig")

    pipeline_plan = resolve_market_pipeline(
        runner.requirements,
        feature_config=feature_config,
    )
    trade_integrity = TradeDataIntegrityTracker() if pipeline_plan.trades_enabled else None
    order_book_integrity = OrderBookDataIntegrityTracker() if pipeline_plan.order_book_enabled else None
    service_bundle.market.trade_data_integrity_tracker = trade_integrity
    service_bundle.market.order_book_data_integrity_tracker = (
        order_book_integrity
    )
    if trade_integrity is not None:
        _bind_trade_integrity_persistence(
            trade_integrity,
            context.state_store,
            exchange=app_config.data_exchange,
            symbol=app_config.symbol,
        )

    trade_processor = (
        MarketEventProcessor(
            closed_bar_handler=(
                runner.closed_bar
                if pipeline_plan.closed_kline_enabled
                else None
            ),
            raw_trade_callback=(
                runner.process_market_event
                if "raw-trade-callback" in pipeline_plan.enabled_module_ids
                else None
            ),
            integrity=trade_integrity,
            trade_processed_callback=(
                runner._heartbeat_service.note_market_event
            ),
            maxsize=max(1, app_config.market_queue_maxsize),
        )
        if pipeline_plan.trades_enabled
        else None
    )
    service_bundle.market.market_event_processor = trade_processor

    range_module = service_bundle.range.module
    configure_integrity = getattr(range_module, "configure_integrity", None)
    if trade_integrity is not None and callable(configure_integrity):
        configure_integrity(trade_integrity)

    module_config = _market_module_config(app_config, feature_config)
    exchange_config = load_exchange_config(app_config.data_exchange)
    connector = WebsocketsConnector()

    registry = build_market_data_registry(
        create_trade_stream=lambda: create_trade_stream(
            app_config.data_exchange,
            symbol=app_config.symbol,
            config=exchange_config,
            connector=connector,
            reconnect=False,
            reconnect_delay_seconds=1.0,
            max_reconnects=0,
        ),
        create_order_book_stream=lambda: create_order_book_stream(
            app_config.data_exchange,
            symbol=app_config.symbol,
            config=exchange_config,
            connector=connector,
            reconnect=True,
            reconnect_delay_seconds=1.0,
            max_reconnects=None,
        ),
        create_order_book_l2_stream=lambda: create_order_book_l2_stream(
            app_config.data_exchange,
            symbol=app_config.symbol,
            config=exchange_config,
            connector=connector,
            reconnect=True,
            reconnect_delay_seconds=1.0,
            max_reconnects=None,
        ),
        create_full_order_book_stream=lambda: create_full_order_book_stream(
            app_config.data_exchange,
            symbol=app_config.symbol,
            config=exchange_config,
            depth=runner.requirements.full_order_book.depth,
            poll_interval_seconds=(
                runner.requirements.full_order_book.poll_interval_seconds
            ),
        ),
        create_open_interest_stream=lambda: create_open_interest_stream(
            app_config.data_exchange,
            symbol=app_config.symbol,
            config=exchange_config,
            connector=connector,
            reconnect=True,
            reconnect_delay_seconds=1.0,
            max_reconnects=None,
        ),
        publish_feature=runner.process_market_feature,
        config=module_config,
        create_range_module=(
            None
            if service_bundle.range.module is None
            else lambda: service_bundle.range.module
        ),
        consume_dropped_trade=runner.handle_dropped_trade,
        consume_order_book=runner.enqueue_market_event,
        consume_order_book_l2=runner.enqueue_market_event,
        consume_full_order_book=runner.enqueue_market_event,
        consume_open_interest=runner.enqueue_market_event,
        trade_integrity=trade_integrity,
        order_book_integrity=order_book_integrity,
        trade_processor=trade_processor,
        on_first_live_trade=(
            runner.closed_bar.close_startup_trade_gap
            if pipeline_plan.trades_enabled
            else None
        ),
    )

    market_data = MarketDataRuntime(
        registry=registry,
        logger=logger,
        event_processor=trade_processor,
        pipeline_plan=pipeline_plan,
        before_prepare=runner.closed_bar.begin_startup_trade_gap
        if pipeline_plan.trades_enabled else None,
        before_source_start=runner.closed_bar.arm_initial_closed_bar_cutoff
        if pipeline_plan.trades_enabled else None,
    )

    request = capability_request_from_requirements(
        runner.requirements,
        trade_features=feature_config,
    )
    market_capabilities = frozenset(
        capability
        for capability in request.capabilities
        if capability in registry.capabilities.capabilities
    )
    runner.attach_market_data_runtime(market_data, market_capabilities)
    return LiveRuntimeApplication(runner=runner, market_data=market_data)


def _bind_trade_integrity_persistence(
    tracker: TradeDataIntegrityTracker,
    state_store: object,
    *, exchange: object, symbol: str,
) -> None:
    load = getattr(state_store, "load_trade_integrity_windows", None)
    save = getattr(state_store, "save_trade_integrity_window", None)
    if not callable(load) or not callable(save):
        return
    for row in load(exchange=exchange, symbol=symbol):
        tracker.restore_window(**row)

    def persist(state: IntegrityWindowState) -> None:
        save(exchange=exchange, symbol=symbol, start_ms=state.start_ms,
             end_ms=state.end_ms, last_issue_revision=state.last_issue_revision,
             repaired_through_revision=state.repaired_through_revision,
             reason=",".join(sorted(state.reasons)) or None,
             complete=state.complete and not state.forced_incomplete)

    tracker.set_window_persister(persist)


def _market_module_config(
    app_config: AppConfig,
    config: TradeFeatureRuntimeConfig,
) -> MarketDataModuleConfig:
    return MarketDataModuleConfig(
        order_book_queue_maxsize=max(1, app_config.market_queue_maxsize),
        order_book_l2_queue_maxsize=1,
        full_order_book_queue_maxsize=1,
        open_interest_queue_maxsize=1,
        fixed_time_trade_bars=FixedTimeTradeBarModuleConfig(
            contract_value=config.contract_value,
            large_trade_threshold_notional=config.large_trade_threshold,
        ),
        trade_footprint=TradeFootprintModuleConfig(
            contract_value=config.contract_value,
            price_bucket_size=config.price_bucket_size,
        ),
        range_footprint=RangeFootprintModuleConfig(
            contract_value=config.contract_value,
            range_pct=config.range_pct,
            price_step=config.range_price_step,
        ),
    )


__all__ = ["LiveRuntimeApplication", "compose_live_runtime"]
