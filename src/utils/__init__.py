#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 3/26/26 9:32 PM
@File       : __init__.py.py
@Description:
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any


def to_json_safe(value: Any) -> Any:
    """Recursively convert Decimal values to float for JSON serialization.

    Handles dict, list, tuple, and nested combinations thereof.
    All other types (None, bool, int, float, str) pass through unchanged.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(v) for v in value]
    return value
