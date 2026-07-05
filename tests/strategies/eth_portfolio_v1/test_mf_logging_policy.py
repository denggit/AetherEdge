from __future__ import annotations

import logging
from decimal import Decimal

from src.platform.exchanges.models import ExchangeName
from src.runtime.features import fixed_time_trade_bar_feature
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataBuffer,
    MfFeatureObserver,
)
from strategies.eth_portfolio_v1.domain.mf_sleeve import MfSleeveState
from strategies.eth_portfolio_v1.execution.mf_signal_mapper import (
    MfSignalMapper,
    MfSizingInput,
)

from _mf_test_helpers import READY, config, range_footprint, setup_bars


def _observer(tmp_path):
    cfg = config()
    bars = setup_bars()
    buffer = MfDataBuffer(
        symbol="ETH-USDT-PERP",
        store_path=str(tmp_path / "features.sqlite3"),
        decision_buffer_minutes=100,
        decision_buffer_max_minutes=100,
        large_share_quantile_window_days=1,
    )
    buffer.append_many(bars[:-1])
    buffer.append_range_footprint(
        range_footprint(
            available_time_ms=bars[-1].open_time_ms - 1
        )
    )
    sleeve = MfSleeveState(
        strategy_id="eth_portfolio_v1",
        symbol="ETH-USDT-PERP",
        enabled=True,
    )
    observer = MfFeatureObserver(
        buffer,
        config=cfg,
        sleeve=sleeve,
        signal_mapper=MfSignalMapper(
            strategy_id="eth_portfolio_v1",
            symbol="ETH-USDT-PERP",
            config=cfg,
        ),
        readiness=READY,
        sizing_provider=lambda: MfSizingInput(
            Decimal("1000"), Decimal("1000")
        ),
    )
    return observer, bars[-1], sleeve


def test_no_per_minute_decision_completed_log(tmp_path, caplog) -> None:
    observer, latest, _ = _observer(tmp_path)
    with caplog.at_level(logging.INFO):
        observer.on_market_feature(
            fixed_time_trade_bar_feature(
                latest, exchange=ExchangeName.OKX
            )
        )
    assert not any(
        "MF decision completed" in record.message
        for record in caplog.records
    )
    assert sum(
        "MF entry signal generated" in record.message
        for record in caplog.records
    ) == 1
    assert sum(
        "MF data readiness changed" in record.message
        for record in caplog.records
    ) == 1


def test_exit_event_logs_once(tmp_path, caplog) -> None:
    observer, latest, sleeve = _observer(tmp_path)
    entry_time_ms = latest.close_time_ms + 1 - 48 * 60_000
    sleeve.reserve_open(
        position_id="mf-low-sweep-time48-log-exit",
        quantity=Decimal("0.10"),
        signal_time_ms=entry_time_ms,
        entry_execution_time_ms=entry_time_ms,
        tradebar_open_time_ms=entry_time_ms,
    )
    sleeve.confirm_open(
        quantity=Decimal("0.10"),
        average_entry_price=Decimal("100"),
        entry_time_ms=entry_time_ms,
    )
    with caplog.at_level(logging.INFO):
        observer.on_market_feature(
            fixed_time_trade_bar_feature(
                latest, exchange=ExchangeName.OKX
            )
        )
    assert sum(
        "MF exit signal generated" in record.message
        for record in caplog.records
    ) == 1
