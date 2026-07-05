from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.platform import ExchangeName
from src.runtime.no_mutation import (
    MutationAttemptError,
    NoMutationExecutionClient,
)


class _Client:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = SimpleNamespace(symbol=symbol)

    async def fetch_open_orders(self):
        return ["read-ok"]


@pytest.mark.parametrize(
    "method",
    (
        "place_order",
        "place_stop_market_order",
        "cancel_order",
        "cancel_all_orders",
        "cancel_stop_order",
        "cancel_all_stop_orders",
        "set_position_mode",
    ),
)
@pytest.mark.asyncio
async def test_smoke_wrapper_blocks_all_required_mutations(
    method: str,
) -> None:
    wrapper = NoMutationExecutionClient(_Client())

    with pytest.raises(MutationAttemptError, match=method):
        await getattr(wrapper, method)(object())

    assert wrapper.mutation_attempted is True
    assert wrapper.mutation_attempts == [method]


@pytest.mark.asyncio
async def test_smoke_wrapper_delegates_read_methods() -> None:
    wrapper = NoMutationExecutionClient(_Client())

    assert await wrapper.fetch_open_orders() == ["read-ok"]
    assert wrapper.mutation_attempted is False
