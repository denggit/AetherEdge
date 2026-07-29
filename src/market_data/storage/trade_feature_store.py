from __future__ import annotations

from src.market_data.storage.trade_feature_repository import (
    LargeTradeShareSample,
    SqliteTradeFeatureRepository,
)
from src.market_data.trade_features.compat import (
    CoverageRepositoryCompatibility,
)


class LegacySqliteTradeFeatureStoreFacade(
    SqliteTradeFeatureRepository,
    CoverageRepositoryCompatibility,
):
    """Deprecated strategy-facing facade over the pure SQLite repository."""


SqliteTradeFeatureStore = LegacySqliteTradeFeatureStoreFacade


__all__ = [
    "LargeTradeShareSample",
    "LegacySqliteTradeFeatureStoreFacade",
    "SqliteTradeFeatureStore",
]
