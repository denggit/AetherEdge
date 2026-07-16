"""Fake strategy for testing generic (non-provider) Preflight paths.

This strategy intentionally omits ``live_preflight_provider`` so that
``live_preflight_check.py`` exercises the generic code path. It also exposes
``runtime_requirements`` with kline warmup enabled so that the Kline-warmup
check can be exercised end-to-end without mocking ``_check_kline_warmup``.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from src.runtime.requirements import (
    ClosedKlineRequirement,
    StrategyRuntimeRequirements,
)


@dataclass
class _FakeStrategyConfig:
    strategy_id: str = "test-fake-generic"


class Strategy:
    """Minimal strategy for testing the generic Preflight path.

    * No ``live_preflight_provider`` → triggers the generic code path.
    * ``runtime_requirements`` enables closed-kline warmup.
    * No SQLite stores, no import-time side effects.
    """

    def __init__(self) -> None:
        self.config = _FakeStrategyConfig()

    def strategy_identity(self) -> str:
        return self.config.strategy_id

    @property
    def runtime_requirements(self) -> StrategyRuntimeRequirements:
        return StrategyRuntimeRequirements(
            closed_kline=ClosedKlineRequirement(
                enabled=True,
                interval="4h",
                warmup_days=30,
                min_records=1,
            ),
        )

    async def on_start(self, snapshot):
        return []

    async def on_kline(self, kline):
        return []

    async def on_ticker(self, ticker):
        return []

    async def on_trade(self, trade):
        return []

    async def on_order_book(self, order_book):
        return []

    async def on_account_event(self, event):
        return []
