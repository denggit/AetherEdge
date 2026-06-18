#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : semantic_models.py
@Description: Business-semantic models for broker operations.

These models bridge the gap between strategy intent and low-level broker
calls.  They carry *what* the strategy wants to do (action + role) without
knowing *how* a particular exchange implements it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from src.exchanges.models import (
    BrokerOrder,
    BrokerPosition,
    BrokerPositionSide,
    BrokerQuantityUnit,
    ExchangeName,
)


class BrokerSemanticAction(str, Enum):
    OPEN_POSITION = "OPEN_POSITION"
    ADD_POSITION = "ADD_POSITION"
    PLACE_REDUCE_ONLY_TP = "PLACE_REDUCE_ONLY_TP"
    PLACE_PROTECTIVE_STOP = "PLACE_PROTECTIVE_STOP"
    CANCEL_ORDER = "CANCEL_ORDER"
    CANCEL_REDUCE_ONLY_TP = "CANCEL_REDUCE_ONLY_TP"
    CANCEL_PROTECTIVE_STOP = "CANCEL_PROTECTIVE_STOP"
    CANCEL_ALL_OPEN_ORDERS = "CANCEL_ALL_OPEN_ORDERS"
    MARKET_EXIT = "MARKET_EXIT"
    MARKET_EXIT_RUNNER = "MARKET_EXIT_RUNNER"
    SIDECAR_ENTRY = "SIDECAR_ENTRY"
    SIDECAR_TP = "SIDECAR_TP"
    FETCH_POSITION = "FETCH_POSITION"
    FETCH_OPEN_ORDERS = "FETCH_OPEN_ORDERS"
    FETCH_ALGO_ORDERS = "FETCH_ALGO_ORDERS"
    RECOVER_OPEN_ORDERS = "RECOVER_OPEN_ORDERS"


class BrokerSemanticOrderRole(str, Enum):
    ENTRY = "ENTRY"
    ADD = "ADD"
    CORE_TP = "CORE_TP"
    TP1 = "TP1"
    TP2 = "TP2"
    RUNNER_TP = "RUNNER_TP"
    PROTECTIVE_SL = "PROTECTIVE_SL"
    MIDDLE_RUNNER_SL = "MIDDLE_RUNNER_SL"
    MIDDLE_BUCKET_FAST_SL = "MIDDLE_BUCKET_FAST_SL"
    THREE_STAGE_POST_TP1_SL = "THREE_STAGE_POST_TP1_SL"
    TREND_RUNNER_SL = "TREND_RUNNER_SL"
    SIDECAR_ENTRY = "SIDECAR_ENTRY"
    SIDECAR_TP = "SIDECAR_TP"
    MARKET_EXIT = "MARKET_EXIT"
    RECOVERY = "RECOVERY"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class BrokerSemanticRequest:
    """A strategy-level request expressed in business semantics.

    The *action* describes *what* to do; *role* tags the order for
    downstream tracking / reconciliation.
    """

    exchange: ExchangeName
    symbol: str
    action: BrokerSemanticAction
    role: BrokerSemanticOrderRole
    side: BrokerPositionSide | None = None
    quantity: Decimal | None = None
    quantity_unit: BrokerQuantityUnit | None = None
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    reduce_only: bool = False
    close_position: bool = False
    label: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerSemanticResult:
    """The result of executing a ``BrokerSemanticRequest``."""

    exchange: ExchangeName
    symbol: str
    action: BrokerSemanticAction
    role: BrokerSemanticOrderRole
    ok: bool
    message: str = ""
    order: BrokerOrder | None = None
    orders: tuple[BrokerOrder, ...] = ()
    position: BrokerPosition | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
