from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import (
    BarReadyContext,
    ClosedKlineContext,
    MicroDecision,
    RoutedSignal,
    Side,
)
from strategies.eth_lf_portfolio_v10a.strategy import Strategy as V10AStrategy
from strategies.eth_lf_portfolio_v10b.execution.structural_stop import (
    StructuralStopConfig,
    evaluate_swing_structural_stop,
)
from strategies.eth_lf_portfolio_v10b.strategy import Strategy as V10BStrategy


FOUR_HOURS_MS = 4 * 60 * 60 * 1000


def test_full_window_requires_exactly_21_closed_bars() -> None:
    config = StructuralStopConfig()

    unavailable = _evaluate(
        _bars(20, low=Decimal("90"), high=Decimal("110")),
        old_stop=Decimal("80"),
        config=config,
    )
    available = _evaluate(
        _bars(21, low=Decimal("90"), high=Decimal("110")),
        old_stop=Decimal("80"),
        config=config,
    )

    assert unavailable.accepted is False
    assert unavailable.raw_candidate is None
    assert unavailable.reject_reason == "insufficient_closed_bars"
    assert available.accepted is True
    assert available.raw_candidate == Decimal("90")


@pytest.mark.parametrize(
    ("low", "old_stop", "close", "accepted", "reason"),
    [
        ("95", "90", "100", True, ""),
        ("90", "90", "100", False, "not_more_protective_than_old_stop"),
        ("100", "90", "100", False, "raw_candidate_crosses_close"),
    ],
)
def test_long_structural_stop_rules(
    low: str,
    old_stop: str,
    close: str,
    accepted: bool,
    reason: str,
) -> None:
    decision = _evaluate(
        _bars(21, low=Decimal(low), high=Decimal("110")),
        old_stop=Decimal(old_stop),
        close=Decimal(close),
    )

    assert decision.accepted is accepted
    assert decision.reject_reason == reason


@pytest.mark.parametrize(
    ("high", "old_stop", "close", "accepted", "reason"),
    [
        ("105", "110", "100", True, ""),
        ("110", "110", "100", False, "not_more_protective_than_old_stop"),
        ("100", "110", "100", False, "raw_candidate_crosses_close"),
    ],
)
def test_short_structural_stop_rules(
    high: str,
    old_stop: str,
    close: str,
    accepted: bool,
    reason: str,
) -> None:
    decision = _evaluate(
        _bars(21, low=Decimal("90"), high=Decimal(high)),
        side=Side.SHORT,
        old_stop=Decimal(old_stop),
        close=Decimal(close),
    )

    assert decision.accepted is accepted
    assert decision.reject_reason == reason


def test_current_bar_exit_never_commits_structural_update() -> None:
    decision = _evaluate(
        _bars(21, low=Decimal("95"), high=Decimal("110")),
        current_bar_exit=True,
    )

    assert decision.accepted is False
    assert decision.reject_reason == "current_bar_exit"
    assert decision.final_stop == Decimal("90")


def test_rounding_that_loosens_long_stop_is_rejected() -> None:
    decision = _evaluate(
        _bars(21, low=Decimal("100.06"), high=Decimal("110")),
        old_stop=Decimal("100.05"),
        close=Decimal("105"),
        config=StructuralStopConfig(price_tick=Decimal("0.1")),
    )

    assert decision.raw_candidate == Decimal("100.06")
    assert decision.rounded_candidate == Decimal("100.0")
    assert decision.accepted is False
    assert decision.reject_reason == "rounded_candidate_loosens_old_stop"


def test_rounding_that_crosses_short_close_is_rejected() -> None:
    decision = _evaluate(
        _bars(21, low=Decimal("90"), high=Decimal("100.06")),
        side=Side.SHORT,
        old_stop=Decimal("100.07"),
        close=Decimal("100.01"),
        config=StructuralStopConfig(price_tick=Decimal("0.1")),
    )

    assert decision.raw_candidate == Decimal("100.06")
    assert decision.rounded_candidate == Decimal("100.0")
    assert decision.accepted is False
    assert decision.reject_reason == "rounded_candidate_crosses_close"


def test_disabled_v10b_stop_output_matches_v10a(tmp_path: Path) -> None:
    config = json.loads(
        Path("strategies/eth_lf_portfolio_v10b/config.json").read_text(encoding="utf-8")
    )
    config["structural_stop"]["enabled"] = False
    config_path = tmp_path / "v10b-disabled.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    v10a = V10AStrategy()
    v10b = V10BStrategy(config_path=config_path)
    context = _context(
        close=Decimal("120"),
        high=Decimal("125"),
        low=Decimal("115"),
        atr=Decimal("5"),
    )
    _open_long(v10a, old_stop=Decimal("90"))
    _open_long(v10b, old_stop=Decimal("90"))

    v10a_signals = v10a._stop_update_signals_if_needed(context)
    v10b_signals = v10b._stop_update_signals_if_needed(context)

    assert [_signal_fingerprint(signal) for signal in v10b_signals] == [
        _signal_fingerprint(signal) for signal in v10a_signals
    ]
    assert v10b.last_structural_stop_audit is None


def test_insufficient_warmup_preserves_v10a_stop_update(caplog: pytest.LogCaptureFixture) -> None:
    strategy = V10BStrategy()
    _open_long(strategy, old_stop=Decimal("90"))
    context = _context(
        close=Decimal("120"),
        high=Decimal("125"),
        low=Decimal("115"),
        atr=Decimal("5"),
        close_index=20,
    )
    _fill_buffer(strategy, count=20, low=Decimal("110"), high=Decimal("125"), close=Decimal("120"))

    with caplog.at_level("WARNING"):
        signals = strategy._stop_update_signals_if_needed(context)

    place = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_LONG)
    assert place.reason == "V8_PROTECTED_TRAILING_STOP_UPDATE"
    assert strategy.last_structural_stop_audit["reject_reason"] == "insufficient_closed_bars"
    assert strategy.position.stop_price == Decimal("90")
    assert strategy.position.desired_stop_price == place.trigger_price
    assert "insufficient_closed_bars" in caplog.text


def test_structural_stop_uses_one_okx_canonical_price_for_follower() -> None:
    strategy = V10BStrategy()
    _open_long(strategy, old_stop=Decimal("80"))
    strategy.position.mark_leg_open(
        exchange="binance",
        avg_fill_price=Decimal("100"),
        base_qty=Decimal("1"),
    )
    context = _context(close=Decimal("120"), high=Decimal("125"), low=Decimal("110"), atr=Decimal("5"))
    _fill_buffer(strategy, count=21, low=Decimal("110"), high=Decimal("125"), close=Decimal("120"))

    signals = strategy._stop_update_signals_if_needed(context)

    place = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_LONG)
    assert place.reason == "STRUCTURAL_STOP"
    assert place.trigger_price == Decimal("110")
    assert place.metadata["canonical_exchange"] == "okx"
    assert place.metadata["canonical_source_exchange"] == "okx"
    assert place.metadata["canonical_stop_price"] == "110.0"
    assert place.metadata["target_exchanges"] == ["binance", "okx"]
    assert place.metadata["stop_source"] == "STRUCTURAL_STOP"
    assert place.metadata["effective_from_next_bar"] is True
    assert strategy.position.stop_price == Decimal("80")
    assert strategy.position.desired_stop_price == Decimal("110")


def test_position_lifecycle_does_not_submit_structural_stop_after_exit() -> None:
    strategy = V10BStrategy()
    _open_long(strategy, old_stop=Decimal("80"))
    _fill_buffer(strategy, count=21, low=Decimal("110"), high=Decimal("125"), close=Decimal("120"))
    context = _context(
        close=Decimal("120"),
        high=Decimal("125"),
        low=Decimal("110"),
        atr=Decimal("5"),
        routed=RoutedSignal(side=Side.SHORT, engine="MOMENTUM_V3", priority=100),
    )

    signals = strategy._position_lifecycle_signals(context)

    assert any(signal.action is SignalAction.CLOSE_LONG for signal in signals)
    assert not any(
        signal.action
        in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.CANCEL_ALL_STOP_ORDERS}
        for signal in signals
    )
    assert strategy.last_structural_stop_audit["accepted"] is False
    assert strategy.last_structural_stop_audit["reject_reason"] == "current_bar_exit"


def test_restored_old_stop_can_only_tighten() -> None:
    strategy = V10BStrategy()
    _open_long(strategy, old_stop=Decimal("112"))
    context = _context(close=Decimal("120"), high=Decimal("125"), low=Decimal("110"), atr=None)
    _fill_buffer(strategy, count=21, low=Decimal("110"), high=Decimal("125"), close=Decimal("120"))

    signals = strategy._stop_update_signals_if_needed(context)

    assert signals == []
    assert strategy.position.stop_price == Decimal("112")
    assert strategy.position.desired_stop_price is None
    assert strategy.last_structural_stop_audit["accepted"] is False
    assert strategy.last_structural_stop_audit["final_stop"] == "112"


def _evaluate(
    bars: list[SimpleNamespace],
    *,
    side: Side = Side.LONG,
    old_stop: Decimal = Decimal("90"),
    close: Decimal = Decimal("100"),
    config: StructuralStopConfig | None = None,
    current_bar_exit: bool = False,
):
    return evaluate_swing_structural_stop(
        closed_bars=bars,
        side=side,
        old_stop=old_stop,
        base_v10a_stop=old_stop,
        current_close=close,
        atr=Decimal("5"),
        engine="MOMENTUM_V3",
        hold_bars=1,
        mfe_r=Decimal("1"),
        bar_close_time=21 * FOUR_HOURS_MS,
        config=config or StructuralStopConfig(),
        current_bar_exit=current_bar_exit,
    )


def _bars(count: int, *, low: Decimal, high: Decimal) -> list[SimpleNamespace]:
    return [
        SimpleNamespace(low=low, high=high, close=(low + high) / Decimal("2"))
        for _ in range(count)
    ]


def _open_long(strategy, *, old_stop: Decimal) -> None:
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=0,
        avg_entry=Decimal("100"),
        qty=Decimal("1"),
        stop_price=old_stop,
        entry_engine="MOMENTUM_V3",
        entry_risk_mult=Decimal("1"),
        position_id="v10b-test-position",
    )
    strategy.position.first_entry = Decimal("100")
    strategy.position.risk_per_coin = Decimal("20")
    strategy.position.max_fav = Decimal("100")
    strategy.position.mark_leg_open(
        exchange="okx",
        avg_fill_price=Decimal("100"),
        base_qty=Decimal("1"),
    )


def _fill_buffer(
    strategy: V10BStrategy,
    *,
    count: int,
    low: Decimal,
    high: Decimal,
    close: Decimal,
) -> None:
    for index in range(1, count + 1):
        close_time = index * FOUR_HOURS_MS
        strategy.buffer.put_kline(
            ClosedKlineContext(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                timeframe="4h",
                open_time_ms=close_time - FOUR_HOURS_MS,
                close_time_ms=close_time,
                open=close,
                high=high,
                low=low,
                close=close,
                volume=Decimal("1"),
            )
        )


def _context(
    *,
    close: Decimal,
    high: Decimal,
    low: Decimal,
    atr: Decimal | None,
    routed: RoutedSignal | None = None,
    close_index: int = 21,
) -> BarReadyContext:
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=(close_index - 1) * FOUR_HOURS_MS,
            close_time_ms=close_index * FOUR_HOURS_MS,
            open=close,
            high=high,
            low=low,
            close=close,
            volume=Decimal("1"),
        ),
        range_aggregate=None,
        micro=MicroDecision(
            signal_side=Side.FLAT,
            context_available=False,
            aligned=False,
            contra=False,
            entry_risk_scale=Decimal("1"),
            action="NO_SIGNAL",
        ),
        global_risk_scale=Decimal("1.3"),
        routed_signal=routed or RoutedSignal.flat(),
        engine_features={"momentum": {}} if atr is None else {"momentum": {"atr": atr}},
    )


def _signal_fingerprint(signal) -> tuple:
    metadata = dict(signal.metadata)
    metadata.pop("strategy_id", None)
    return (
        signal.symbol,
        signal.action,
        signal.quantity,
        signal.trigger_price,
        signal.reason,
        metadata,
    )
