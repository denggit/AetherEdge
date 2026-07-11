import pytest

from src.platform.config import (
    load_env_config,
    load_project_env_config,
    reset_project_env_config_for_tests,
    set_project_env_config,
)
from src.platform.exchanges import ExchangeConfig, ExchangeName
from src.platform.exchanges.binance.credentials import resolve_binance_credentials
from src.platform.exchanges.okx.credentials import resolve_okx_credentials


@pytest.fixture(autouse=True)
def _reset_project_env_config():
    reset_project_env_config_for_tests()
    yield
    reset_project_env_config_for_tests()


def test_load_env_config_reads_dotenv_and_process_env_wins(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text('OKX_API_KEY="from_file"\nOKX_SECRET_KEY=file_secret\n', encoding="utf-8")

    values = load_env_config(env_file, environ={"OKX_API_KEY": "from_process"})

    assert values["OKX_API_KEY"] == "from_process"
    assert values["OKX_SECRET_KEY"] == "file_secret"


def test_okx_credentials_use_only_maintained_key_names():
    cfg = ExchangeConfig()
    env = {
        "OKX_API_KEY": "okx_key",
        "OKX_SECRET_KEY": "okx_secret",
        "OKX_PASSPHRASE": "okx_passphrase",
        "OKX_API_SECRET": "wrong_old_secret",
        "OKX_PASSPHASE": "wrong_typo_passphrase",
        "EXCHANGE_API_KEY": "wrong_unified_key",
        "EXCHANGE_API_SECRET": "wrong_unified_secret",
        "EXCHANGE_API_PASSPHRASE": "wrong_unified_pass",
    }

    assert resolve_okx_credentials(cfg, env) == ("okx_key", "okx_secret", "okx_passphrase")


def test_explicit_config_still_wins_for_okx():
    cfg = ExchangeConfig(api_key="cfg_key", api_secret="cfg_secret", passphrase="cfg_pass")
    env = {"OKX_API_KEY": "env_key", "OKX_SECRET_KEY": "env_secret", "OKX_PASSPHRASE": "env_pass"}

    assert resolve_okx_credentials(cfg, env) == ("cfg_key", "cfg_secret", "cfg_pass")


def test_binance_credentials_use_only_maintained_key_names():
    cfg = ExchangeConfig()
    env = {
        "BINANCE_API_KEY": "binance_key",
        "BINANCE_SECRET_KEY": "binance_secret",
        "BINANCE_API_SECRET": "wrong_old_secret",
        "EXCHANGE_API_KEY": "wrong_unified_key",
        "EXCHANGE_API_SECRET": "wrong_unified_secret",
    }

    assert resolve_binance_credentials(cfg, env) == ("binance_key", "binance_secret")


def test_exchange_config_from_env_resolves_okx_strict_keys(monkeypatch):
    monkeypatch.setenv("OKX_API_KEY", "okx_key")
    monkeypatch.setenv("OKX_SECRET_KEY", "okx_secret")
    monkeypatch.setenv("OKX_PASSPHRASE", "okx_pass")

    cfg = ExchangeConfig.from_env(ExchangeName.OKX)

    assert cfg.api_key == "okx_key"
    assert cfg.api_secret == "okx_secret"
    assert cfg.passphrase == "okx_pass"


def test_exchange_config_from_env_resolves_binance_strict_keys(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", "binance_key")
    monkeypatch.setenv("BINANCE_SECRET_KEY", "binance_secret")
    monkeypatch.setenv("BINANCE_API_SECRET", "wrong_old_secret")

    cfg = ExchangeConfig.from_env(ExchangeName.BINANCE)

    assert cfg.api_key == "binance_key"
    assert cfg.api_secret == "binance_secret"


def test_exchange_config_uses_process_keys_from_global_project_snapshot(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "OKX_API_KEY=okx_file_key\n"
        "OKX_SECRET_KEY=okx_file_secret\n"
        "OKX_PASSPHRASE=okx_file_pass\n"
        "BINANCE_API_KEY=binance_file_key\n"
        "BINANCE_SECRET_KEY=binance_file_secret\n",
        encoding="utf-8",
    )
    project_env = load_project_env_config(
        env_file=env_file,
        process_env={
            "OKX_API_KEY": "okx_process_key",
            "OKX_SECRET_KEY": "okx_process_secret",
            "OKX_PASSPHRASE": "okx_process_pass",
            "BINANCE_API_KEY": "binance_process_key",
            "BINANCE_SECRET_KEY": "binance_process_secret",
        },
    )
    set_project_env_config(project_env)

    okx = ExchangeConfig.from_env(ExchangeName.OKX)
    binance = ExchangeConfig.from_env(ExchangeName.BINANCE)

    assert (okx.api_key, okx.api_secret, okx.passphrase) == (
        "okx_process_key",
        "okx_process_secret",
        "okx_process_pass",
    )
    assert (binance.api_key, binance.api_secret) == (
        "binance_process_key",
        "binance_process_secret",
    )
