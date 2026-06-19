from __future__ import annotations

import importlib
from typing import Any

from src.strategy.ports import StrategyPort


class StrategyLoadError(RuntimeError):
    pass


def load_strategy(path: str, **kwargs: Any) -> StrategyPort:
    """Load a strategy plugin from ``module:attribute``.

    The loader only imports and instantiates. It does not know about K-line,
    tick, orderbook, range bars, footprint, or any strategy internals.
    """

    if ":" not in path:
        raise StrategyLoadError("strategy path must be 'module:attribute'")
    module_name, attr_name = path.split(":", 1)
    try:
        module = importlib.import_module(module_name)
        attr = getattr(module, attr_name)
    except Exception as exc:
        raise StrategyLoadError(f"failed to load strategy {path}: {exc}") from exc
    strategy = attr(**kwargs) if isinstance(attr, type) else attr
    _validate_strategy(strategy, path)
    return strategy


def _validate_strategy(strategy: Any, path: str) -> None:
    required = ["on_start", "on_kline", "on_ticker", "on_trade", "on_order_book", "on_account_event"]
    missing = [name for name in required if not callable(getattr(strategy, name, None))]
    if missing:
        raise StrategyLoadError(f"strategy {path} is missing methods: {', '.join(missing)}")
