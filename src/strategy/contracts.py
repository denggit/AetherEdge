from __future__ import annotations


class StrategyCapabilityError(RuntimeError):
    """A strategy's public runtime capability contract is unsafe."""


class StrategyContractError(StrategyCapabilityError):
    """A strategy provider violated its declared runtime contract."""


class StrategyPositionContractError(StrategyContractError):
    """A strategy exposed an invalid logical position state."""


__all__ = [
    "StrategyCapabilityError",
    "StrategyContractError",
    "StrategyPositionContractError",
]
