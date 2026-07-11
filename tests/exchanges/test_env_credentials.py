import pytest

from src.platform.config import (
    load_env_config,
    load_project_env_config,
    reset_project_env_config_for_tests,
    set_project_env_config,
)
from src.platform.exchanges import ExchangeConfig, ExchangeName
from src.platform.exchanges.binance.credentials import resolve_binance_credentials
from src.platform.exchanges.credentials import validate_private_credentials
from src.platform.exchanges.errors import PrivateCredentialValidationError
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


def test_exchange_config_from_env_resolves_okx_strict_keys(tmp_path):
    env_file = tmp_path / "okx.env"
    env_file.write_text(
        "OKX_API_KEY=fake_okx_file_key\n"
        "OKX_SECRET_KEY=fake_okx_file_secret\n"
        "OKX_PASSPHRASE=fake_okx_file_pass\n",
        encoding="utf-8",
    )
    set_project_env_config(
        load_project_env_config(
            env_file=env_file,
            process_env={
                "OKX_API_KEY": "fake_okx_process_key",
                "OKX_SECRET_KEY": "fake_okx_process_secret",
                "OKX_PASSPHRASE": "fake_okx_process_pass",
                "OKX_API_SECRET": "fake_legacy_secret",
                "OKX_PASSPHASE": "fake_typo_passphrase",
            },
        )
    )

    cfg = ExchangeConfig.from_env(ExchangeName.OKX)

    assert cfg.api_key == "fake_okx_process_key"
    assert cfg.api_secret == "fake_okx_process_secret"
    assert cfg.passphrase == "fake_okx_process_pass"


def test_exchange_config_from_env_resolves_binance_strict_keys(tmp_path):
    env_file = tmp_path / "binance.env"
    env_file.write_text(
        "BINANCE_API_KEY=fake_binance_file_key\n"
        "BINANCE_SECRET_KEY=fake_binance_file_secret\n",
        encoding="utf-8",
    )
    set_project_env_config(
        load_project_env_config(
            env_file=env_file,
            process_env={
                "BINANCE_API_KEY": "fake_binance_process_key",
                "BINANCE_SECRET_KEY": "fake_binance_process_secret",
                "BINANCE_API_SECRET": "fake_legacy_secret",
            },
        )
    )

    cfg = ExchangeConfig.from_env(ExchangeName.BINANCE)

    assert cfg.api_key == "fake_binance_process_key"
    assert cfg.api_secret == "fake_binance_process_secret"


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


@pytest.mark.parametrize(
    ("exchange", "factory"),
    (
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="fake_okx_key",
                api_secret="fake_okx_secret",
                passphrase="fake_okx_passphrase",
            ),
        ),
        (
            ExchangeName.BINANCE,
            lambda: ExchangeConfig(
                api_key="fake_binance_key",
                api_secret="fake_binance_secret",
            ),
        ),
    ),
)
def test_private_credential_validator_accepts_complete_fake_credentials(
    exchange,
    factory,
):
    validate_private_credentials(exchange, factory())


@pytest.mark.parametrize(
    ("exchange", "factory", "expected_field"),
    (
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="",
                api_secret="canary_okx_secret",
                passphrase="canary_okx_passphrase",
            ),
            "api_key",
        ),
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="canary_okx_key",
                api_secret="   ",
                passphrase="canary_okx_passphrase",
            ),
            "api_secret",
        ),
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="canary_okx_key",
                api_secret="canary_okx_secret",
                passphrase=None,
            ),
            "passphrase",
        ),
        (
            ExchangeName.BINANCE,
            lambda: ExchangeConfig(
                api_key="",
                api_secret="canary_binance_secret",
            ),
            "api_key",
        ),
        (
            ExchangeName.BINANCE,
            lambda: ExchangeConfig(
                api_key="canary_binance_key",
                api_secret="\t",
            ),
            "api_secret",
        ),
    ),
)
def test_private_credential_validator_rejects_missing_or_blank_fields(
    exchange,
    factory,
    expected_field,
):
    with pytest.raises(PrivateCredentialValidationError) as exc_info:
        validate_private_credentials(exchange, factory())

    text = str(exc_info.value)
    assert exc_info.value.code == "missing_private_credentials"
    assert f"exchange={exchange.value}" in text
    assert f"missing_fields={expected_field}" in text
    assert "canary_" not in text


@pytest.mark.parametrize(
    ("exchange", "factory", "expected_field"),
    (
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="  你的_okx_api_key  ",
                api_secret="canary_okx_secret",
                passphrase="canary_okx_passphrase",
            ),
            "api_key",
        ),
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="canary_okx_key",
                api_secret="YOUR_OKX_SECRET_KEY",
                passphrase="canary_okx_passphrase",
            ),
            "api_secret",
        ),
        (
            ExchangeName.OKX,
            lambda: ExchangeConfig(
                api_key="canary_okx_key",
                api_secret="canary_okx_secret",
                passphrase="<OKX_PASSPHRASE>",
            ),
            "passphrase",
        ),
        (
            ExchangeName.BINANCE,
            lambda: ExchangeConfig(
                api_key="${BINANCE_API_KEY}",
                api_secret="canary_binance_secret",
            ),
            "api_key",
        ),
        (
            ExchangeName.BINANCE,
            lambda: ExchangeConfig(
                api_key="canary_binance_key",
                api_secret="your_binance_secret_key",
            ),
            "api_secret",
        ),
    ),
)
def test_private_credential_validator_rejects_documented_placeholders(
    exchange,
    factory,
    expected_field,
):
    with pytest.raises(PrivateCredentialValidationError) as exc_info:
        validate_private_credentials(exchange, factory())

    text = str(exc_info.value)
    assert exc_info.value.code == "placeholder_private_credentials"
    assert f"placeholder_fields={expected_field}" in text
    assert "canary_" not in text


def test_private_credential_validator_allows_sandbox_and_testnet_values():
    validate_private_credentials(
        ExchangeName.OKX,
        ExchangeConfig(
            api_key="sandbox-okx-key",
            api_secret="testnet-okx-secret",
            passphrase="demo-passphrase",
            sandbox=True,
        ),
    )
    validate_private_credentials(
        ExchangeName.BINANCE,
        ExchangeConfig(
            api_key="sandbox-binance-key",
            api_secret="testnet-binance-secret",
            sandbox=True,
        ),
    )


def test_exchange_config_repr_redacts_private_credentials_and_headers():
    config = ExchangeConfig(
        api_key="canary_api_key",
        api_secret="canary_secret",
        passphrase="canary_passphrase",
        extra_headers={"Authorization": "canary_header"},
    )

    text = repr(config)

    assert "canary_api_key" not in text
    assert "canary_secret" not in text
    assert "canary_passphrase" not in text
    assert "canary_header" not in text


def test_credential_validation_failure_never_leaks_canaries(caplog):
    config = ExchangeConfig(
        api_key="<OKX_API_KEY>",
        api_secret="canary_secret",
        passphrase="canary_passphrase",
    )

    with pytest.raises(PrivateCredentialValidationError) as exc_info:
        validate_private_credentials(ExchangeName.OKX, config)

    exception_text = str(exc_info.value)
    exception_repr = repr(exc_info.value)
    assert "canary_api_key" not in exception_text
    assert "canary_secret" not in exception_text
    assert "canary_passphrase" not in exception_text
    assert "canary_secret" not in exception_repr
    assert "canary_passphrase" not in exception_repr
    assert "canary_secret" not in caplog.text
    assert "canary_passphrase" not in caplog.text
