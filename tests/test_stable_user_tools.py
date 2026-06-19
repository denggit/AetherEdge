import inspect

from config.env_loader import EMAIL_CONFIG, load_env_config
from src.utils import email_sender


def test_env_loader_keeps_email_config_contract():
    assert callable(load_env_config)
    assert {"sender", "password", "receiver"} <= set(EMAIL_CONFIG)


def test_email_sender_keeps_async_send_email_contract():
    assert inspect.iscoroutinefunction(email_sender.send_email)
    params = inspect.signature(email_sender.send_email).parameters
    assert "subject" in params
    assert "content" in params
    assert "content_type" in params
