#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : semantics.py
@Description: Business-semantic executor port (BrokerSemanticExecutor ABC).

This is the high-level port that strategies talk to.  It translates
semantic requests into low-level ``BrokerClient`` calls.

The convenience methods defined here are pure — they only construct a
``BrokerSemanticRequest`` and delegate to ``execute()``.  No exchange-
specific logic lives here.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal

from src.exchanges.models import (
    BrokerPositionSide,
    BrokerQuantityUnit,
    ExchangeName,
)
from src.exchanges.semantic_models import (
    BrokerSemanticAction,
    BrokerSemanticOrderRole,
    BrokerSemanticRequest,
    BrokerSemanticResult,
)


class BrokerSemanticExecutor(ABC):
    """High-level port for strategy → broker communication.

    Subclasses implement ``execute()`` by translating the semantic request
    into one or more ``BrokerClient`` calls.
    """

    @property
    @abstractmethod
    def exchange(self) -> ExchangeName:
        """The exchange this executor targets."""
        ...

    @abstractmethod
    async def execute(self, request: BrokerSemanticRequest) -> BrokerSemanticResult:
        """Execute a semantic request and return a structured result."""
        ...

    # ------------------------------------------------------------------
    # Convenience methods – each builds a BrokerSemanticRequest then
    # delegates to self.execute().
    # ------------------------------------------------------------------

    async def open_position(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.OPEN_POSITION,
                role=BrokerSemanticOrderRole.ENTRY,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                price=price,
                client_order_id=client_order_id,
                label=label,
            )
        )

    async def add_position(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.ADD_POSITION,
                role=BrokerSemanticOrderRole.ADD,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                price=price,
                client_order_id=client_order_id,
                label=label,
            )
        )

    async def place_reduce_only_tp(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        trigger_price: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        order_price: Decimal | None = None,
        role: BrokerSemanticOrderRole = BrokerSemanticOrderRole.CORE_TP,
        client_order_id: str | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        effective_price = order_price if order_price is not None else trigger_price
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.PLACE_REDUCE_ONLY_TP,
                role=role,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                price=effective_price,
                trigger_price=None,
                reduce_only=True,
                client_order_id=client_order_id,
                label=label,
            )
        )

    async def place_protective_stop(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        trigger_price: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        role: BrokerSemanticOrderRole = BrokerSemanticOrderRole.PROTECTIVE_SL,
        client_order_id: str | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.PLACE_PROTECTIVE_STOP,
                role=role,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                trigger_price=trigger_price,
                reduce_only=True,
                client_order_id=client_order_id,
                label=label,
            )
        )

    async def cancel_order(
        self,
        symbol: str,
        order_id: str,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.CANCEL_ORDER,
                role=BrokerSemanticOrderRole.UNKNOWN,
                order_id=order_id,
                label=label,
            )
        )

    async def cancel_reduce_only_tp(
        self,
        symbol: str,
        order_id: str,
        role: BrokerSemanticOrderRole = BrokerSemanticOrderRole.CORE_TP,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.CANCEL_REDUCE_ONLY_TP,
                role=role,
                order_id=order_id,
                label=label,
            )
        )

    async def cancel_protective_stop(
        self,
        symbol: str,
        order_id: str,
        role: BrokerSemanticOrderRole = BrokerSemanticOrderRole.PROTECTIVE_SL,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.CANCEL_PROTECTIVE_STOP,
                role=role,
                order_id=order_id,
                label=label,
            )
        )

    async def market_exit(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.MARKET_EXIT,
                role=BrokerSemanticOrderRole.MARKET_EXIT,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                reduce_only=True,
                label=label,
            )
        )

    async def market_exit_runner(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.MARKET_EXIT_RUNNER,
                role=BrokerSemanticOrderRole.MARKET_EXIT,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                reduce_only=True,
                label=label,
            )
        )

    async def sidecar_entry(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        price: Decimal | None = None,
        client_order_id: str | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.SIDECAR_ENTRY,
                role=BrokerSemanticOrderRole.SIDECAR_ENTRY,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                price=price,
                client_order_id=client_order_id,
                label=label,
            )
        )

    async def sidecar_tp(
        self,
        symbol: str,
        side: BrokerPositionSide,
        quantity: Decimal,
        trigger_price: Decimal,
        quantity_unit: BrokerQuantityUnit | None = None,
        order_price: Decimal | None = None,
        client_order_id: str | None = None,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        effective_price = order_price if order_price is not None else trigger_price
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.SIDECAR_TP,
                role=BrokerSemanticOrderRole.SIDECAR_TP,
                side=side,
                quantity=quantity,
                quantity_unit=quantity_unit,
                price=effective_price,
                trigger_price=None,
                reduce_only=True,
                client_order_id=client_order_id,
                label=label,
            )
        )

    async def fetch_position(
        self,
        symbol: str,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.FETCH_POSITION,
                role=BrokerSemanticOrderRole.UNKNOWN,
                label=label,
            )
        )

    async def fetch_open_orders(
        self,
        symbol: str,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.FETCH_OPEN_ORDERS,
                role=BrokerSemanticOrderRole.UNKNOWN,
                label=label,
            )
        )

    async def fetch_algo_orders(
        self,
        symbol: str,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.FETCH_ALGO_ORDERS,
                role=BrokerSemanticOrderRole.UNKNOWN,
                label=label,
            )
        )

    async def recover_open_orders(
        self,
        symbol: str,
        label: str | None = None,
    ) -> BrokerSemanticResult:
        return await self.execute(
            BrokerSemanticRequest(
                exchange=self.exchange,
                symbol=symbol,
                action=BrokerSemanticAction.RECOVER_OPEN_ORDERS,
                role=BrokerSemanticOrderRole.RECOVERY,
                label=label,
            )
        )
