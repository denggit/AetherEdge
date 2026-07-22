from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal

import pytest

from src.market_data.derived import RangeBarBuilder
from src.order_management.coordinator.position_plan_updater import (
    PositionPlanUpdater,
)
from src.order_management.models import ExchangeOrderResult
from src.order_management.position_plan import SqlitePositionPlanStore
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import (
    MarketKline,
    MarketTrade,
    TradeSide,
)
from src.platform.exchanges.models import ExchangeName, OrderStatus
from src.runtime.features import (
    closed_kline_feature,
    range_bar_closed_feature,
)
from src.runtime.market_data.features import (
    FixedTimeTradeBarModule,
    FixedTimeTradeBarModuleConfig,
    RangeFootprintModule,
    RangeFootprintModuleConfig,
    TradeFootprintModule,
    TradeFootprintModuleConfig,
)
from src.runtime.market_data.integrity import TradeDataIntegrityTracker
from src.runtime.market_data.pipeline_plan import (
    ClosedBarControlEvent,
    ResolvedMarketPipelinePlan,
)
from src.runtime.market_data.processor import MarketEventProcessor
from src.runtime.market_features import MarketFeaturePipeline
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.strategy_host import StrategyHost
from strategies.eth_portfolio_v1 import Strategy


MINUTE_MS = 60_000
BASE_MS = 1_700_000_040_000
READY = {
    "mf_signal_feature_ready": True,
    "range_footprint_ready": True,
    "tradebar_ready": True,
    "fixed_time_footprint_ready": True,
    "coverage_ready": True,
    "large_share_samples_ready": True,
    "source": "real_replay_fixture",
}


def _fixture_trades() -> tuple[MarketTrade, ...]:
    lows = ("100", "99", "98", "97", "96", "95", "90", "94", "95", "96", "97", "89")
    closes = ("100",) * 11 + ("89.5",)
    highs = ("102",) * 11 + ("101",)
    rows: list[MarketTrade] = []
    sequence = 0
    for minute, (low, high, close) in enumerate(zip(lows, highs, closes)):
        open_ms = BASE_MS + minute * MINUTE_MS
        for offset, price in enumerate(("100", high, low, close), start=1):
            sequence += 1
            time_ms = open_ms + offset * 1_000
            rows.append(
                MarketTrade(
                    exchange=ExchangeName.OKX,
                    symbol="ETH-USDT-PERP",
                    raw_symbol="ETH-USDT-SWAP",
                    price=Decimal(price),
                    quantity=Decimal("1"),
                    side=TradeSide.BUY,
                    trade_id=f"replay-{sequence}",
                    trade_time_ms=time_ms,
                    event_time_ms=time_ms,
                )
            )
    # This first Trade of the next minute closes the signal minute and supplies
    # the real next-open execution price/time to FixedTimeTradeBarBuilder.
    next_open_ms = BASE_MS + len(lows) * MINUTE_MS + 1_000
    rows.append(
        MarketTrade(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal("92"),
            quantity=Decimal("1"),
            side=TradeSide.BUY,
            trade_id="replay-next-open",
            trade_time_ms=next_open_ms,
            event_time_ms=next_open_ms,
        )
    )
    return tuple(rows)


class _Repository:
    def add_event(self, _event) -> None:
        return None


def _configure_real_strategy(strategy: Strategy) -> None:
    strategy.equity = Decimal("1000")
    strategy.exchange_equity = {"okx": Decimal("1000")}
    strategy.exchange_available = {"okx": Decimal("500")}
    strategy.exchange_leverage = {"okx": Decimal("15")}
    strategy.exchange_margin_mode = {"okx": "isolated"}
    strategy.mf_feature_observer.set_readiness(READY)
    first_open = BASE_MS
    history_start = first_open - 43_200 * MINUTE_MS
    strategy.mf_data_buffer._large_trade_shares.extend(
        (history_start + index * MINUTE_MS, Decimal("1"))
        for index in range(43_200)
    )
    strategy.mf_data_buffer._latest_history_open_time_ms = (
        first_open - MINUTE_MS
    )


async def _replay(*, ordered: bool, root) -> dict[str, object]:
    root.mkdir(parents=True, exist_ok=True)
    strategy = Strategy(mf_store_path=root / "features.sqlite3")
    _configure_real_strategy(strategy)
    strategy_host = StrategyHost(strategy)
    feature_pipeline = MarketFeaturePipeline(strategy)
    integrity = TradeDataIntegrityTracker()
    feature_trace: list[tuple[str, dict[str, object]]] = []
    callback_trace: list[tuple[str, int]] = []
    closed_bar_observations: list[dict[str, int]] = []
    signals = []

    async def publish_feature(event) -> None:
        feature_trace.append((event.type_value, dict(event.data)))
        callback_trace.append((f"feature:{event.type_value}", event.event_time_ms))
        signals.extend(await feature_pipeline.dispatch(event))

    range_builder = RangeBarBuilder(range_pct="0.002", contract_value="1")

    async def process_range(trade: MarketTrade) -> None:
        for bar in range_builder.on_trade(trade):
            await publish_feature(
                range_bar_closed_feature(bar, exchange=trade.exchange)
            )

    async def process_raw(trade: MarketTrade) -> None:
        callback_trace.append(("strategy:raw_trade_skipped", trade.trade_time_ms or 0))
        if strategy.raw_trade_callbacks_enabled:
            signals.extend(await strategy_host.on_market_event(trade))

    modules = (
        RangeFootprintModule(
            config=RangeFootprintModuleConfig(
                contract_value="1",
                range_pct="0.002",
                price_step="1",
            ),
            publish=publish_feature,
            integrity=integrity,
        ),
        FixedTimeTradeBarModule(
            config=FixedTimeTradeBarModuleConfig(
                contract_value="1",
                large_trade_threshold_notional="1",
            ),
            publish=publish_feature,
            integrity=integrity,
        ),
        TradeFootprintModule(
            config=TradeFootprintModuleConfig(
                contract_value="1",
                price_bucket_size="1",
            ),
            publish=publish_feature,
            integrity=integrity,
        ),
    )
    class RangeReplayModule:
        module_id = "range-bars"

        async def process_trade(self, trade: MarketTrade) -> None:
            await process_range(trade)

    runtime_modules = (*modules, RangeReplayModule())

    trades = _fixture_trades()
    split = 24
    period_a, period_b = trades[:split], trades[split:]
    close_time_ms = period_a[-1].event_time_ms
    kline = MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4h",
        open_time_ms=close_time_ms - 4 * 60 * MINUTE_MS + 1,
        close_time_ms=close_time_ms,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("90"),
        close=Decimal("100"),
        volume=Decimal("10"),
    )

    class ClosedHandler:
        async def process_closed_bar(self, event: ClosedBarControlEvent) -> None:
            closed_event = closed_kline_feature(event.kline)
            feature_trace.append((closed_event.type_value, dict(closed_event.data)))
            callback_trace.append(("strategy:closed_kline", closed_event.event_time_ms))
            signals.extend(await feature_pipeline.dispatch(closed_event))
            latest = {
                name: max(
                    time_ms
                    for callback, time_ms in callback_trace
                    if callback == name
                )
                for name in (
                    "strategy:raw_trade_skipped",
                    "feature:fixed_time_trade_bar",
                    "feature:trade_footprint_feature",
                    "feature:range_footprint_feature",
                    "feature:range_bar_closed",
                )
            }
            observer = strategy.mf_feature_observer
            latest["strategy:observer"] = max(
                observer._last_tradebar_ms,
                observer._last_footprint_ms,
                observer._last_range_footprint_ms,
            )
            buffer = strategy.mf_data_buffer
            latest["strategy:buffer"] = max(
                buffer._bars[-1].available_time_ms,
                buffer._range_footprints[-1].available_time_ms,
            )
            assert all(value <= event.kline.close_time_ms for value in latest.values())
            closed_bar_observations.append(latest)

    control = ClosedBarControlEvent(open_time_ms=kline.open_time_ms, kline=kline)
    closed_handler = ClosedHandler()
    if ordered:
        processor = MarketEventProcessor(
            plan=ResolvedMarketPipelinePlan(
                trades_enabled=True,
                closed_kline_enabled=True,
                order_book_enabled=False,
                enabled_module_ids=tuple(
                    module.module_id for module in runtime_modules
                ) + ("raw-trade-callback",),
                execution_stages=(),
            ),
            trade_modules=runtime_modules,
            closed_bar_handler=closed_handler,
            raw_trade_callback=process_raw,
            maxsize=256,
        )
        await processor.start()
        for trade in period_a:
            processor.submit_trade(trade)
        processor.submit_closed_bar(control)
        for trade in period_b:
            processor.submit_trade(trade)
        await processor.stop()
    else:
        for period in (period_a, period_b):
            if period is period_b:
                await closed_handler.process_closed_bar(control)
            for trade in period:
                await modules[0].process_trade(trade)
                await modules[1].process_trade(trade)
                await modules[2].process_trade(trade)
                await process_range(trade)
                await process_raw(trade)
    account = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.POSITION,
        symbol="ETH-USDT-PERP",
        event_time_ms=trades[-1].event_time_ms + 1,
        quantity=Decimal("0"),
    )
    callback_trace.append(("strategy:account", account.event_time_ms))
    account_signals = await strategy_host.on_account_event(account)
    signals.extend(account_signals or ())

    assert signals, "real Portfolio V1 fixture must produce its own TradeSignal"
    signal = signals[0]
    factory = LiveOrderIntentFactory(
        strategy_id="strategies.eth_portfolio_v1:Strategy",
        target_exchanges=(ExchangeName.OKX,),
    )
    intent = factory.create(
        signal,
        source="fixed_time_trade_bar",
        event_time_ms=signal.created_time_ms,
    )
    result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="replay-order-1",
        status=OrderStatus.FILLED,
        quantity=signal.quantity,
        filled_quantity=signal.quantity,
        avg_fill_price=Decimal("92"),
    )
    callback_trace.append(("strategy:order_result", signal.created_time_ms))
    followups = await strategy_host.on_order_results(
        signal=signal,
        results=(result,),
        source="fixed_time_trade_bar",
        event_time_ms=signal.created_time_ms,
    )
    store = SqlitePositionPlanStore(root / "position-plan.sqlite3")
    updater = PositionPlanUpdater(
        repository=_Repository(),
        position_plan_store=store,
        master_follower_policy=None,
    )
    updater.record_position_plan(intent, (result,))
    position_id = str(signal.metadata["position_id"])
    position = store.get_position(position_id)
    legs = store.get_legs(position_id)

    def stable(value):
        data = asdict(value)
        data.pop("created_time_ms", None)
        data.pop("updated_time_ms", None)
        return data

    sleeve = asdict(strategy.mf_sleeve)
    return {
        "feature_trace": feature_trace,
        "callback_trace": callback_trace,
        "closed_bar_observations": closed_bar_observations,
        "signal": signal,
        "signal_metadata": dict(signal.metadata),
        "intent": (
            intent.intent_id,
            intent.signal,
            intent.target_exchanges,
            dict(intent.metadata),
        ),
        "position": None if position is None else stable(position),
        "legs": tuple(stable(leg) for leg in legs),
        "strategy_state": sleeve,
        "observer_audit": dict(strategy.last_mf_signal_audit),
        "order_result_followups": tuple(followups or ()),
    }


def _normalize_replay(replay: dict[str, object]) -> dict[str, object]:
    """Strip wall-clock fields so replays are deterministically comparable."""
    normalized = dict(replay)
    audit = normalized.get("observer_audit")
    if isinstance(audit, dict):
        audit.pop("live_feature_age_ms", None)
    return normalized


@pytest.mark.asyncio
async def test_real_portfolio_v1_parent_and_runtime_replay_parity(tmp_path) -> None:
    parent = _normalize_replay(await _replay(ordered=False, root=tmp_path / "parent"))
    runtime = _normalize_replay(await _replay(ordered=True, root=tmp_path / "runtime"))

    assert runtime == parent
    print("portfolio_v1_closed_bar_observations", runtime["closed_bar_observations"])
    assert runtime["signal_metadata"]["engine"] == "MF_LOW_SWEEP_TIME48"
    assert runtime["position"] is not None
    assert runtime["legs"]
