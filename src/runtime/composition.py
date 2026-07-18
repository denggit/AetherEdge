from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.app import AppConfig, AppContext, build_app_context
from src.platform.data.factory import create_order_book_stream, create_trade_stream
from src.platform.data.websocket import WebsocketsConnector
from src.platform.exchanges.models import ExchangeConfig
from src.runtime.capabilities import capability_request_from_requirements
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.feature_pipeline import TradeFeatureRuntimeConfig
from src.runtime.market_data.catalog import (
    MarketDataModuleConfig,
    build_market_data_registry,
)
from src.runtime.market_data.dispatcher import BoundedOrderedEventDispatcher
from src.runtime.market_data.features import (
    FixedTimeTradeBarModuleConfig,
    RangeFootprintModuleConfig,
    TradeFootprintModuleConfig,
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
    trade_dispatcher = (
        runtime_services.range_trade_dispatcher
        or BoundedOrderedEventDispatcher(
            maxsize=max(1, app_config.market_queue_maxsize),
        )
    )
    runtime_services.range_trade_dispatcher = trade_dispatcher
    runner = LiveRuntimeRunner(
        app_config=app_config,
        app_context=context,
        runtime_config=runtime_settings,
        range_config=range_settings,
        managed_market_modules=True,
        services=runtime_services,
    )

    feature_config = runtime_services.trade_feature_config
    if not isinstance(feature_config, TradeFeatureRuntimeConfig):
        raise TypeError("composition did not produce TradeFeatureRuntimeConfig")
    module_config = _market_module_config(app_config, feature_config)
    exchange_config = ExchangeConfig.from_env(app_config.data_exchange)
    connector = WebsocketsConnector()
    registry = build_market_data_registry(
        create_trade_stream=lambda: create_trade_stream(
            app_config.data_exchange,
            symbol=app_config.symbol,
            config=exchange_config,
            connector=connector,
            reconnect=True,
            reconnect_delay_seconds=1.0,
            max_reconnects=None,
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
        publish_feature=runner.process_market_feature,
        config=module_config,
        create_range_module=(
            None
            if runtime_services.range_bar_module is None
            else lambda _dispatcher: runtime_services.range_bar_module
        ),
        trade_dispatcher=trade_dispatcher,
        consume_trade=runner.enqueue_market_event,
        consume_order_book=runner.enqueue_market_event,
    )
    market_data = MarketDataRuntime(registry=registry, logger=logger)
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


def _market_module_config(
    app_config: AppConfig,
    config: TradeFeatureRuntimeConfig,
) -> MarketDataModuleConfig:
    return MarketDataModuleConfig(
        trade_queue_maxsize=max(1, app_config.market_queue_maxsize),
        order_book_queue_maxsize=max(1, app_config.market_queue_maxsize),
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
