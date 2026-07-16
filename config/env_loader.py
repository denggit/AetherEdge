#!/usr/bin/env python
"""Stable legacy imports for project environment and email configuration."""

# Bootstrap project logging before scripts configure the standard library.
try:
    from src.utils import log as _reclaimedge_log  # noqa: F401
except Exception:
    _reclaimedge_log = None

from src.platform.config import load_env_config as _load_env_config


def load_env_config() -> dict[str, str]:
    """Load the project environment through the canonical platform loader."""

    return dict(_load_env_config())


def get_email_config() -> dict[str, str]:
    config = load_env_config()
    return {
        "sender": config.get("EMAIL_SENDER", ""),
        "password": config.get("EMAIL_PASSWORD", ""),
        "receiver": config.get("EMAIL_RECEIVER", ""),
    }


EMAIL_CONFIG = get_email_config()


__all__ = ["EMAIL_CONFIG", "get_email_config", "load_env_config"]
