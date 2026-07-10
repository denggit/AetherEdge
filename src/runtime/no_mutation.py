from __future__ import annotations

from typing import Any


class MutationAttemptError(RuntimeError):
    """Raised when a read-only smoke path attempts an external mutation."""


class NoMutationExecutionClient:
    """Delegate reads while making every exchange mutation fail closed."""

    def __init__(self, client: object) -> None:
        self._client = client
        self.mutation_attempted = False
        self.mutation_attempts: list[str] = []

    @property
    def exchange(self):
        return self._client.exchange

    @property
    def symbol(self):
        return self._client.symbol

    @property
    def market_profile(self):
        return self._client.market_profile

    async def fetch_balance(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_balance(*args, **kwargs)

    async def fetch_positions(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_positions(*args, **kwargs)

    async def fetch_leverage(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_leverage(*args, **kwargs)

    async def fetch_position_mode(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_position_mode(*args, **kwargs)

    async def fetch_order_status(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_order_status(*args, **kwargs)

    async def fetch_open_orders(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_open_orders(*args, **kwargs)

    async def fetch_stop_order_status(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_stop_order_status(*args, **kwargs)

    async def fetch_open_stop_orders(self, *args: Any, **kwargs: Any):
        return await self._client.fetch_open_stop_orders(*args, **kwargs)

    async def fetch_instrument_rule(self, *args: Any, **kwargs: Any):
        fetch_rule = getattr(self._client, "fetch_instrument_rule", None)
        if callable(fetch_rule):
            return await fetch_rule(*args, **kwargs)
        return None

    async def place_order(self, *args: Any, **kwargs: Any):
        self._block("place_order")

    async def place_stop_market_order(self, *args: Any, **kwargs: Any):
        self._block("place_stop_market_order")

    async def place_stop_loss_for_position(
        self, *args: Any, **kwargs: Any
    ):
        self._block("place_stop_loss_for_position")

    async def cancel_order(self, *args: Any, **kwargs: Any):
        self._block("cancel_order")

    async def cancel_all_orders(self, *args: Any, **kwargs: Any):
        self._block("cancel_all_orders")

    async def cancel_stop_order(self, *args: Any, **kwargs: Any):
        self._block("cancel_stop_order")

    async def cancel_all_stop_orders(self, *args: Any, **kwargs: Any):
        self._block("cancel_all_stop_orders")

    async def amend_order(self, *args: Any, **kwargs: Any):
        self._block("amend_order")

    async def replace_order(self, *args: Any, **kwargs: Any):
        self._block("replace_order")

    async def set_position_mode(self, *args: Any, **kwargs: Any):
        self._block("set_position_mode")

    async def set_leverage(self, *args: Any, **kwargs: Any):
        self._block("set_leverage")

    async def set_margin_mode(self, *args: Any, **kwargs: Any):
        self._block("set_margin_mode")

    def _block(self, method: str) -> None:
        self.mutation_attempted = True
        self.mutation_attempts.append(method)
        raise MutationAttemptError(
            f"read-only live smoke blocked mutation: {method}"
        )


__all__ = ["MutationAttemptError", "NoMutationExecutionClient"]
