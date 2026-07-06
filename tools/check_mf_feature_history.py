#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 7/6/2026 9:17 PM
@File       : check_mf_feature_history.py
@Description: 
"""
import sqlite3, datetime

db = "data/market_data/aether_market_data.sqlite3"
tz = datetime.timezone(datetime.timedelta(hours=8))

def fmt(ms):
    if ms is None:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S+08")

conn = sqlite3.connect(db)
cur = conn.cursor()

specs = [
    ("tradebar_1m_features", "open_time_ms", "open_time_ms"),
    ("trade_footprint_1m_features", "open_time_ms", "open_time_ms"),
    ("range_footprint_features", "range_start_ms", "available_time_ms"),
]

for table, start_col, end_col in specs:
    count, start_ms, end_ms = cur.execute(
        f"select count(*), min({start_col}), max({end_col}) from {table}"
    ).fetchone()
    print(table, {"count": count, "start_okx": fmt(start_ms), "end_okx": fmt(end_ms)})

conn.close()