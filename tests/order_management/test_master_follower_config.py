from __future__ import annotations

import pytest

from src.order_management import MasterFollowerPolicyConfig
from src.platform import ExchangeName


def test_explicit_master_and_follower_config() -> None:
    config = MasterFollowerPolicyConfig.from_env(
        app_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        env={
            "AETHER_EXCHANGES": "okx,binance",
            "AETHER_DATA_EXCHANGE": "okx",
            "AETHER_MASTER_EXCHANGE": "okx",
            "AETHER_FOLLOWER_EXCHANGES": "binance",
        },
    )

    assert config.master_exchange is ExchangeName.OKX
    assert config.follower_exchanges == (ExchangeName.BINANCE,)


def test_defaults_master_to_data_exchange_and_followers_to_other_exchanges() -> None:
    config = MasterFollowerPolicyConfig.from_env(
        app_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        env={
            "AETHER_EXCHANGES": "okx,binance",
            "AETHER_DATA_EXCHANGE": "okx",
        },
    )

    assert config.master_exchange is ExchangeName.OKX
    assert config.follower_exchanges == (ExchangeName.BINANCE,)


def test_single_exchange_has_no_followers() -> None:
    config = MasterFollowerPolicyConfig.from_env(
        app_exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        env={
            "AETHER_EXCHANGES": "okx",
            "AETHER_DATA_EXCHANGE": "okx",
        },
    )

    assert config.master_exchange is ExchangeName.OKX
    assert config.follower_exchanges == ()


def test_master_must_be_in_app_exchanges() -> None:
    with pytest.raises(ValueError):
        MasterFollowerPolicyConfig.from_env(
            app_exchanges=(ExchangeName.OKX,),
            data_exchange=ExchangeName.OKX,
            env={
                "AETHER_EXCHANGES": "okx",
                "AETHER_MASTER_EXCHANGE": "binance",
            },
        )


def test_unsupported_master_exchange_fails_during_parsing() -> None:
    with pytest.raises(ValueError):
        MasterFollowerPolicyConfig.from_env(
            app_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
            data_exchange=ExchangeName.OKX,
            env={
                "AETHER_EXCHANGES": "okx,binance",
                "AETHER_MASTER_EXCHANGE": "bybit",
            },
        )


def test_follower_config_dedupes_and_excludes_master() -> None:
    config = MasterFollowerPolicyConfig.from_env(
        app_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        env={
            "AETHER_EXCHANGES": "okx,binance",
            "AETHER_MASTER_EXCHANGE": "okx",
            "AETHER_FOLLOWER_EXCHANGES": "okx,binance,binance",
        },
    )

    assert config.master_exchange is ExchangeName.OKX
    assert config.follower_exchanges == (ExchangeName.BINANCE,)


def test_reverse_master_follower_config_is_supported() -> None:
    config = MasterFollowerPolicyConfig.from_env(
        app_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.BINANCE,
        env={
            "AETHER_EXCHANGES": "okx,binance",
            "AETHER_DATA_EXCHANGE": "binance",
            "AETHER_MASTER_EXCHANGE": "binance",
            "AETHER_FOLLOWER_EXCHANGES": "okx",
        },
    )

    assert config.master_exchange is ExchangeName.BINANCE
    assert config.follower_exchanges == (ExchangeName.OKX,)
