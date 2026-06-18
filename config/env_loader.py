#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
环境变量配置加载器，用于读取 .env 文件中的配置。

注意：导入本模块时会先初始化 src.utils.log，确保实盘脚本在调用
logging.basicConfig 之前已经安装统一文件日志 handler。
"""
import os

# Bootstrap project logging early. This import is intentionally kept for its
# side effect: src.utils.log configures root logging with daily file rotation.
# If a script later calls logging.basicConfig(...), Python will no-op because
# handlers already exist, so logs will not be duplicated to console by default.
try:
    from src.utils import log as _reclaimedge_log  # noqa: F401
except Exception:
    # Config loading must remain usable in minimal/offline contexts even if the
    # logging package path is unavailable.
    _reclaimedge_log = None


def load_env_config() -> dict:
    """
    加载 .env 文件中的配置。

    Returns:
        dict: 包含所有环境变量配置的字典
    """
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
    config = {}

    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # 跳过注释和空行
                if not line or line.startswith('#') or '=' not in line:
                    continue

                # 分割键值对
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # 去除可能的引号
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]

                config[key] = value

    # os.environ 覆盖 .env 文件同名 key（生产部署可能使用 shell/systemd env）
    config.update({key: value for key, value in os.environ.items()})

    return config


def get_email_config() -> dict:
    """
    获取邮件配置。

    Returns:
        dict: 包含邮件发送者、密码、接收者的字典
    """
    config = load_env_config()
    return {
        'sender': config.get('EMAIL_SENDER', ''),
        'password': config.get('EMAIL_PASSWORD', ''),
        'receiver': config.get('EMAIL_RECEIVER', ''),
    }


# 提供全局配置变量
EMAIL_CONFIG = get_email_config()
