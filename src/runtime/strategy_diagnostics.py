from __future__ import annotations

from typing import Any, Mapping

from src.platform.data.models import MarketKline
from src.utils.log import get_logger

logger = get_logger(__name__)


def log_closed_bar_decision(
    *,
    audit: Mapping[str, Any] | None,
    symbol: str,
    interval: str,
    close_buffer_ms: int,
    open_time_ms: int,
    closed_kline: MarketKline,
) -> None:
    """Log one closed-bar decision without owning strategy state."""

    if not isinstance(audit, Mapping) or audit.get("bar_open_time_ms") != open_time_ms:
        logger.info(
            "4H decision completed | "
            f"symbol={symbol} interval={interval} "
            f"open_time_ms={closed_kline.open_time_ms} close_time_ms={closed_kline.close_time_ms} "
            "decision=no_audit reason=no_strategy_audit actions= selected_engine=None selected_side=None "
            "risk_mult=None quality_mult=None micro_action=None micro_scale=None micro_aligned=None micro_contra=None "
            "range_available=False range_status=no_audit range_bar_count=None range_min_required=None "
            "range_imbalance=None range_taker_buy_ratio=None range_close_pos=None range_micro_return_pct=None "
            "range_exit_triggered=False range_exit_reason=None range_exit_peak_r=None "
            "range_exit_current_r=None range_exit_giveback_frac=None "
            f"position=None position_side=None position_engine=None position_stop=None close={closed_kline.close} "
            f"close_buffer_ms={close_buffer_ms}"
        )
        return

    actions = audit.get("actions") or []
    logger.info(
        "4H decision completed | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s decision=%s actions=%s selected_engine=%s selected_side=%s risk_mult=%s quality_mult=%s micro_action=%s micro_scale=%s micro_aligned=%s micro_contra=%s range_available=%s range_status=%s range_bar_count=%s range_min_required=%s range_imbalance=%s range_taker_buy_ratio=%s range_close_pos=%s range_micro_return_pct=%s range_exit_triggered=%s range_exit_reason=%s range_exit_peak_r=%s range_exit_current_r=%s range_exit_giveback_frac=%s position=%s position_side=%s position_engine=%s position_stop=%s close=%s close_buffer_ms=%s",
        symbol,
        interval,
        audit.get("bar_open_time_ms"),
        audit.get("bar_close_time_ms"),
        audit.get("reason"),
        ",".join(actions),
        audit.get("selected_engine"),
        audit.get("selected_side"),
        audit.get("risk_mult"),
        audit.get("quality_mult"),
        audit.get("micro_action"),
        audit.get("micro_entry_risk_scale"),
        audit.get("micro_aligned"),
        audit.get("micro_contra"),
        audit.get("range_available"),
        audit.get("range_status"),
        audit.get("range_bar_count"),
        audit.get("range_min_required"),
        audit.get("range_imbalance"),
        audit.get("range_taker_buy_ratio"),
        audit.get("range_close_pos"),
        audit.get("range_micro_return_pct"),
        audit.get("range_exit_triggered"),
        audit.get("range_exit_reason"),
        audit.get("range_exit_peak_r"),
        audit.get("range_exit_current_r"),
        audit.get("range_exit_giveback_frac"),
        audit.get("position_in_pos"),
        audit.get("position_side"),
        audit.get("position_engine"),
        audit.get("position_stop"),
        closed_kline.close,
        close_buffer_ms,
    )
    engine_diag_text = audit.get("engine_diag_text")
    if isinstance(engine_diag_text, str) and engine_diag_text.strip():
        logger.info(
            "4H engine diagnostics | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s\n%s",
            symbol,
            interval,
            audit.get("bar_open_time_ms"),
            audit.get("bar_close_time_ms"),
            engine_diag_text,
        )


__all__ = ["log_closed_bar_decision"]
