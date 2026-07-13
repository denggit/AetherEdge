from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform import ExchangeName
from src.platform.data.models import MarketTrade, TradeSide
from src.runtime import feature_pipeline as pipeline_module
from src.runtime.feature_pipeline import TradeDerivedFeaturePipeline


class _EnabledStrategy:
    def __init__(self, config=None) -> None:
        self.config = {"enabled": True} if config is None else config

    def trade_feature_runtime_config(self):
        return self.config


class _Builder:
    def __init__(self, name: str, outputs=(), calls=None) -> None:
        self.name = name
        self.outputs = outputs
        self.calls = calls if calls is not None else []
        self.trades = []

    def on_trade(self, trade):
        self.calls.append(f"{self.name}.on_trade")
        self.trades.append(trade)
        return self.outputs


def _trade(*, trade_time_ms=100, event_time_ms=90) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_time_ms=trade_time_ms,
        event_time_ms=event_time_ms,
    )


async def _discard(event) -> None:
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "strategy",
    [
        object(),
        type("NonCallable", (), {"trade_feature_runtime_config": 1})(),
        _EnabledStrategy([]),
        _EnabledStrategy({"enabled": False}),
    ],
)
async def test_disabled_config_does_not_create_builders_or_emit(strategy) -> None:
    emitted = []

    async def emit(event):
        emitted.append(event)

    pipeline = TradeDerivedFeaturePipeline(
        strategy=strategy,
        emit_feature=emit,
    )

    await pipeline.process_trade(_trade())

    assert pipeline.fixed_time_trade_bar_builder is None
    assert pipeline.trade_footprint_builder is None
    assert pipeline.range_footprint_builder is None
    assert emitted == []


@pytest.mark.asyncio
async def test_builders_are_created_lazily_once_and_reused() -> None:
    pipeline = TradeDerivedFeaturePipeline(
        strategy=_EnabledStrategy(),
        emit_feature=_discard,
    )
    assert pipeline.fixed_time_trade_bar_builder is None
    assert pipeline.trade_footprint_builder is None
    assert pipeline.range_footprint_builder is None

    await pipeline.process_trade(_trade(trade_time_ms=100))
    builders = (
        pipeline.fixed_time_trade_bar_builder,
        pipeline.trade_footprint_builder,
        pipeline.range_footprint_builder,
    )
    assert all(builder is not None for builder in builders)

    await pipeline.process_trade(_trade(trade_time_ms=101))

    assert pipeline.fixed_time_trade_bar_builder is builders[0]
    assert pipeline.trade_footprint_builder is builders[1]
    assert pipeline.range_footprint_builder is builders[2]


def _patch_feature_wrappers(monkeypatch, calls, fixed_args=None) -> None:
    def range_wrapper(value, *, exchange):
        return ("range", value, exchange)

    def fixed_wrapper(value, **kwargs):
        if fixed_args is not None:
            fixed_args.append((value, kwargs))
        return ("fixed", value, kwargs["exchange"])

    def footprint_wrapper(value, *, exchange):
        return ("footprint", value, exchange)

    monkeypatch.setattr(pipeline_module, "range_footprint_feature", range_wrapper)
    monkeypatch.setattr(pipeline_module, "fixed_time_trade_bar_feature", fixed_wrapper)
    monkeypatch.setattr(pipeline_module, "trade_footprint_feature", footprint_wrapper)


@pytest.mark.asyncio
async def test_injected_builders_are_used_in_exact_call_and_emit_order(monkeypatch) -> None:
    calls = []
    range_builder = _Builder("range_builder", ("r1",), calls)
    fixed_builder = _Builder("fixed_time_builder", ("b1",), calls)
    footprint_builder = _Builder("footprint_builder", ("f1",), calls)
    _patch_feature_wrappers(monkeypatch, calls)

    async def emit(event):
        calls.append(f"emit {event[0]} {event[1]}")

    pipeline = TradeDerivedFeaturePipeline(
        strategy=_EnabledStrategy(),
        emit_feature=emit,
        fixed_time_trade_bar_builder=fixed_builder,
        trade_footprint_builder=footprint_builder,
        range_footprint_builder=range_builder,
    )
    trade = _trade()

    await pipeline.process_trade(trade)

    assert calls == [
        "range_builder.on_trade",
        "fixed_time_builder.on_trade",
        "footprint_builder.on_trade",
        "emit range r1",
        "emit fixed b1",
        "emit footprint f1",
    ]
    assert range_builder.trades == [trade]
    assert fixed_builder.trades == [trade]
    assert footprint_builder.trades == [trade]
    assert pipeline.range_footprint_builder is range_builder
    assert pipeline.fixed_time_trade_bar_builder is fixed_builder
    assert pipeline.trade_footprint_builder is footprint_builder


@pytest.mark.asyncio
async def test_multiple_features_preserve_builder_and_group_order(monkeypatch) -> None:
    calls = []
    _patch_feature_wrappers(monkeypatch, calls)

    async def emit(event):
        calls.append((event[0], event[1]))

    pipeline = TradeDerivedFeaturePipeline(
        strategy=_EnabledStrategy(),
        emit_feature=emit,
        range_footprint_builder=_Builder("range", ("r1", "r2")),
        fixed_time_trade_bar_builder=_Builder("fixed", ("b1", "b2")),
        trade_footprint_builder=_Builder("footprint", ("f1", "f2")),
    )

    await pipeline.process_trade(_trade())

    assert calls == [
        ("range", "r1"),
        ("range", "r2"),
        ("fixed", "b1"),
        ("fixed", "b2"),
        ("footprint", "f1"),
        ("footprint", "f2"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("trade_time_ms", "event_time_ms", "expected"),
    [(123, 99, 123), (None, 99, 99), (0, 99, 99)],
)
async def test_fixed_bar_next_open_uses_existing_or_fallback_semantics(
    monkeypatch, trade_time_ms, event_time_ms, expected
) -> None:
    fixed_args = []
    _patch_feature_wrappers(monkeypatch, [], fixed_args)
    pipeline = TradeDerivedFeaturePipeline(
        strategy=_EnabledStrategy(),
        emit_feature=_discard,
        range_footprint_builder=_Builder("range"),
        fixed_time_trade_bar_builder=_Builder("fixed", ("bar",)),
        trade_footprint_builder=_Builder("footprint"),
    )
    trade = _trade(
        trade_time_ms=trade_time_ms,
        event_time_ms=event_time_ms,
    )

    await pipeline.process_trade(trade)

    assert fixed_args[0][1]["next_open_price"] is trade.price
    assert fixed_args[0][1]["next_open_time_ms"] == expected


@pytest.mark.asyncio
async def test_config_provider_exception_propagates() -> None:
    expected = RuntimeError("config failed")

    class Strategy:
        def trade_feature_runtime_config(self):
            raise expected

    with pytest.raises(RuntimeError) as raised:
        await TradeDerivedFeaturePipeline(
            strategy=Strategy(), emit_feature=_discard
        ).process_trade(_trade())

    assert raised.value is expected


@pytest.mark.asyncio
async def test_builder_exception_propagates() -> None:
    expected = RuntimeError("builder failed")

    class BrokenBuilder(_Builder):
        def on_trade(self, trade):
            raise expected

    pipeline = TradeDerivedFeaturePipeline(
        strategy=_EnabledStrategy(),
        emit_feature=_discard,
        range_footprint_builder=BrokenBuilder("range"),
        fixed_time_trade_bar_builder=_Builder("fixed"),
        trade_footprint_builder=_Builder("footprint"),
    )

    with pytest.raises(RuntimeError) as raised:
        await pipeline.process_trade(_trade())

    assert raised.value is expected


@pytest.mark.asyncio
async def test_emitter_exception_propagates_and_pipeline_has_no_execution_dependencies(
    monkeypatch,
) -> None:
    expected = RuntimeError("emit failed")
    _patch_feature_wrappers(monkeypatch, [])

    async def broken_emit(event):
        raise expected

    pipeline = TradeDerivedFeaturePipeline(
        strategy=_EnabledStrategy(),
        emit_feature=broken_emit,
        range_footprint_builder=_Builder("range", ("r1",)),
        fixed_time_trade_bar_builder=_Builder("fixed"),
        trade_footprint_builder=_Builder("footprint"),
    )

    with pytest.raises(RuntimeError) as raised:
        await pipeline.process_trade(_trade())

    assert raised.value is expected
    assert set(vars(pipeline)) == {
        "_strategy",
        "_emit_feature",
        "fixed_time_trade_bar_builder",
        "trade_footprint_builder",
        "range_footprint_builder",
    }
