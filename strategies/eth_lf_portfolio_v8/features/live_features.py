from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from strategies.eth_lf_portfolio_v8.domain.models import ClosedKlineContext
from strategies.eth_lf_portfolio_v8.features.indicators import adx, atr, ema, resample_ohlcv, rsi


@dataclass(frozen=True)
class MomentumV3FeatureConfig:
    entry_lookback: int = 12
    exit_lookback: int = 12
    atr_period: int = 20
    adx_period: int = 14
    min_adx_long: float = 10.0
    min_adx_short: float = 16.0
    max_adx_long: float = 38.0
    max_adx_short: float = 42.0
    min_atr_pct: float = 0.0030
    max_atr_pct: float = 0.0700
    volume_window: int = 60
    volume_mult: float = 1.05
    max_d1_distance_long: float = 0.120
    max_d1_distance_short: float = 0.140
    enable_short: bool = True
    d1_ema_fast: int = 8
    d1_ema_slow: int = 30
    d1_slope_lookback: int = 10
    bull_slope_min: float = -0.0300
    bear_slope_max: float = -0.0030
    w_ema_fast: int = 10
    w_ema_mid: int = 20
    w_slope_lookback: int = 4
    min_risk_mult: float = 0.35
    max_risk_mult: float = 2.0
    weak_long_quality_mult: float = 0.25
    mature_long_adx_threshold: float = 16.0
    mature_long_quality_mult: float = 0.50


@dataclass(frozen=True)
class BearV3FeatureConfig:
    min_risk_mult: float = 0.25
    max_risk_mult: float = 2.3
    style: str = "bear_permission_v3"
    d1_ema_fast: int = 20
    d1_ema_mid: int = 50
    d1_ema_slow: int = 100
    d1_ema_major: int = 200
    d1_slope_lookback: int = 10
    w_ema_fast: int = 10
    w_ema_mid: int = 20
    w_ema_slow: int = 40
    w_slope_lookback: int = 4


@dataclass(frozen=True)
class BullReclaimV2FeatureConfig:
    min_risk_mult: float = 0.35
    max_risk_mult: float = 1.8
    atr_period: int = 20
    adx_period: int = 14
    rsi_period: int = 14
    ema_fast: int = 20
    ema_mid: int = 50
    ema_slow: int = 100
    pullback_lookback: int = 8
    pb_dist50: float = 0.015
    pb_dist100: float = 0.030
    d1_close_mult: float = 0.980
    d1_fast_mult: float = 0.970
    d1_slope_min: float = 0.000
    d1_mid_vs_slow_min: float = 0.980
    d1_max_dist: float = 0.200
    d1_min_dist: float = -0.080
    reclaim_mult: float = 1.000
    rsi_min: float = 48.0
    adx_min: float = 6.0
    adx_max: float = 16.0
    atr_min: float = 0.003
    atr_max: float = 0.050
    vol_mult: float = 0.80
    h4_max_dist50: float = 0.080
    secondary_adx_max: float = 22.0
    secondary_rsi_min: float = 52.0
    secondary_quality_mult: float = 0.35
    exit_ema50_mult: float = 0.970
    d1_ema_fast: int = 20
    d1_ema_mid: int = 50
    d1_ema_slow: int = 100
    d1_slope_lookback: int = 10


@dataclass(frozen=True)
class V8EngineFeatureRows:
    momentum: Mapping[str, Any] | None
    bear: Mapping[str, Any] | None
    bull: Mapping[str, Any] | None


class V8LiveFeatureBuilder:
    def __init__(
        self,
        *,
        momentum: MomentumV3FeatureConfig | None = None,
        bear: BearV3FeatureConfig | None = None,
        bull: BullReclaimV2FeatureConfig | None = None,
    ) -> None:
        self.momentum_cfg = momentum or MomentumV3FeatureConfig()
        self.bear_cfg = bear or BearV3FeatureConfig()
        self.bull_cfg = bull or BullReclaimV2FeatureConfig()

    def build_latest(self, klines: Mapping[int, ClosedKlineContext], *, target_close_time_ms: int) -> V8EngineFeatureRows:
        base = klines_to_dataframe(klines, target_close_time_ms=target_close_time_ms)
        if base.empty:
            return V8EngineFeatureRows(momentum=None, bear=None, bull=None)
        target_open_time_ms = int(base.iloc[-1]["open_time_ms"])
        target_index = base.index[-1]
        momentum = _row_or_none(build_momentum_features(base, self.momentum_cfg), target_index)
        bear = _row_or_none(build_bear_features(base, self.bear_cfg), target_index)
        bull = _row_or_none(build_bull_features(base, self.bull_cfg), target_index)
        return V8EngineFeatureRows(momentum=momentum, bear=bear, bull=bull)


def klines_to_dataframe(klines: Mapping[int, ClosedKlineContext], *, target_close_time_ms: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    target_open_time_ms: int | None = None
    for kline in klines.values():
        if kline.close_time_ms == target_close_time_ms:
            target_open_time_ms = kline.open_time_ms
            break
    if target_open_time_ms is None:
        return pd.DataFrame()
    for kline in sorted(klines.values(), key=lambda item: item.open_time_ms):
        if kline.open_time_ms > target_open_time_ms:
            continue
        rows.append(
            {
                "timestamp": pd.to_datetime(kline.open_time_ms, unit="ms", utc=True).tz_convert(None),
                "open_time_ms": kline.open_time_ms,
                "close_time_ms": kline.close_time_ms,
                "open": float(kline.open),
                "high": float(kline.high),
                "low": float(kline.low),
                "close": float(kline.close),
                "volume": float(kline.volume),
            }
        )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).set_index("timestamp").sort_index()
    return out


def build_v8_base_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["atr"] = atr(out, 20)
    out["atr_pct"] = out["atr"] / out["close"]
    out["adx"] = adx(out, 14)
    out["ema20"] = ema(out["close"], 20)
    out["ema50"] = ema(out["close"], 50)
    out["ema89"] = ema(out["close"], 89)
    out["ema100"] = ema(out["close"], 100)
    out["ema200"] = ema(out["close"], 200)
    out["entry_high"] = out["high"].rolling(12, min_periods=12).max().shift(1)
    out["entry_low"] = out["low"].rolling(12, min_periods=12).min().shift(1)
    out["exit_high"] = out["high"].rolling(12, min_periods=12).max().shift(1)
    out["exit_low"] = out["low"].rolling(12, min_periods=12).min().shift(1)
    return out


def build_momentum_features(base_4h: pd.DataFrame, cfg: MomentumV3FeatureConfig) -> pd.DataFrame:
    out = base_4h.copy()
    out["atr"] = atr(out, cfg.atr_period)
    out["atr_pct"] = out["atr"] / out["close"]
    out["adx"] = adx(out, cfg.adx_period)
    out["ema20"] = ema(out["close"], 20)
    out["ema50"] = ema(out["close"], 50)
    out["ema100"] = ema(out["close"], 100)
    out["ema200"] = ema(out["close"], 200)
    d1 = _momentum_daily_regime(base_4h, cfg)
    out = out.join(d1.reindex(out.index, method="ffill"))
    wk = _momentum_weekly_regime(base_4h, cfg)
    out = out.join(wk.reindex(out.index, method="ffill"))
    out["entry_high"] = out["high"].rolling(cfg.entry_lookback, min_periods=cfg.entry_lookback).max().shift(1)
    out["entry_low"] = out["low"].rolling(cfg.entry_lookback, min_periods=cfg.entry_lookback).min().shift(1)
    out["exit_low"] = out["low"].rolling(cfg.exit_lookback, min_periods=cfg.exit_lookback).min().shift(1)
    out["exit_high"] = out["high"].rolling(cfg.exit_lookback, min_periods=cfg.exit_lookback).max().shift(1)
    out["volume_median"] = out["volume"].rolling(cfg.volume_window, min_periods=30).median().shift(1)
    d1_bull = out["d1_bull"].astype("boolean").fillna(False).astype(bool)
    d1_bear = out["d1_bear"].astype("boolean").fillna(False).astype(bool)
    d1_distance = out["close"] / out["d1_ema_slow"] - 1.0
    out["d1_distance"] = d1_distance
    vol_ok = out["volume"] > out["volume_median"] * cfg.volume_mult
    atr_ok = out["atr_pct"].between(cfg.min_atr_pct, cfg.max_atr_pct)
    long_filter = d1_bull & atr_ok & out["adx"].between(cfg.min_adx_long, cfg.max_adx_long) & (d1_distance.abs() < cfg.max_d1_distance_long)
    short_filter = d1_bear & atr_ok & out["adx"].between(cfg.min_adx_short, cfg.max_adx_short) & (d1_distance.abs() < cfg.max_d1_distance_short) & cfg.enable_short
    out["long_breakout_setup"] = (out["close"] > out["entry_high"]) & (out["close"] > out["open"]) & (out["close"] > out["ema50"]) & (out["ema20"] > out["ema50"]) & vol_ok
    out["short_breakout_setup"] = (out["close"] < out["entry_low"]) & (out["close"] < out["open"]) & (out["close"] < out["ema50"]) & (out["ema20"] < out["ema50"]) & vol_ok
    out["long_signal"] = long_filter & out["long_breakout_setup"]
    out["short_signal"] = short_filter & out["short_breakout_setup"]
    weekly_bull = (out["w_close"] > out["w_ema_mid"]) | (out["w_slope_mid"] > 0)
    out["long_quality_full"] = out["long_signal"] & weekly_bull.fillna(False).astype(bool) & (out["d1_slow_slope"] > 0.004) & (out["adx"] < 32.0) & (d1_distance.abs() < 0.110)
    out["long_quality_weak"] = out["long_signal"] & ~out["long_quality_full"]
    out["long_mature_breakout"] = out["long_signal"] & (out["adx"] > cfg.mature_long_adx_threshold)
    out["signal"] = 0
    out.loc[out["long_signal"], "signal"] = 1
    out.loc[out["short_signal"], "signal"] = -1
    out["long_exit_channel"] = out["close"] < out["exit_low"]
    out["short_exit_channel"] = out["close"] > out["exit_high"]
    out["risk_mult"] = 1.0
    out.loc[out["adx"].between(14.0, 30.0), "risk_mult"] += 0.20
    out.loc[out["atr_pct"] > 0.040, "risk_mult"] -= 0.25
    out.loc[d1_distance.abs() > 0.100, "risk_mult"] -= 0.20
    out["risk_mult"] = out["risk_mult"].clip(cfg.min_risk_mult, cfg.max_risk_mult)
    out["quality_mult"] = 1.0
    out.loc[out["long_quality_weak"], "quality_mult"] *= cfg.weak_long_quality_mult
    out.loc[out["long_mature_breakout"], "quality_mult"] *= cfg.mature_long_quality_mult
    out.loc[out["volume"] > out["volume_median"] * 1.50, "quality_mult"] *= 1.10
    return out.dropna().copy()


def build_bear_features(base_4h: pd.DataFrame, cfg: BearV3FeatureConfig) -> pd.DataFrame:
    out = build_v8_base_features(base_4h)
    out = _bear_add_shifted_htf(base_4h, out, cfg)
    out["long_signal"] = False
    weekly_bear = (out["w_close"] < out["w_ema20"]) & (out["w_ema20_slope"] < 0)
    d1_major_bear = (out["d1_close"] < out["d1_ema100"]) & (out["d1_ema50_slope"] < -0.008)
    bear_permission_v2 = d1_major_bear & (out["d1_ema100_slope"] > -0.025) & (out["ret_12"] < 0.005) & ((out["close"] / out["d1_ema100"] - 1.0) > -0.110)
    four_h_bear = (out["ema20"] < out["ema50"]) & (out["close"] < out["ema20"]) & (out["close"] < out["open"]) & out["adx"].between(12.0, 32.0) & out["atr_pct"].between(0.006, 0.030) & ((out["close"] / out["d1_ema100"] - 1.0).between(-0.18, 0.02))
    breakdown = weekly_bear & (out["d1_close"] < out["d1_ema100"]) & (out["ema20"] < out["ema50"]) & (out["close"] < out["entry_low"]) & (out["close"] < out["open"]) & out["adx"].between(10.0, 30.0) & out["atr_pct"].between(0.004, 0.032)
    crash_continuation = d1_major_bear & four_h_bear
    permission_continuation = bear_permission_v2 & four_h_bear
    if cfg.style == "breakdown":
        short_signal = breakdown
    elif cfg.style == "crash_continuation":
        short_signal = crash_continuation
    elif cfg.style in {"bear_permission_v2", "bear_permission_v3"}:
        short_signal = permission_continuation
    elif cfg.style == "combo":
        short_signal = breakdown | permission_continuation
    else:
        raise ValueError(f"Unsupported style: {cfg.style}")
    out["weekly_bear"] = weekly_bear.fillna(False).astype(bool)
    out["bear_permission_v3"] = bear_permission_v2.fillna(False).astype(bool)
    out["bear_permission_v2"] = out["bear_permission_v3"]
    out["short_signal"] = short_signal.fillna(False).astype(bool)
    out["signal"] = 0
    out.loc[out["short_signal"], "signal"] = -1
    out["short_exit_channel"] = (out["close"] > out["ema50"]) | ((out["close"] > out["ema89"]) & (out["ema20"] > out["ema50"])) | (out["close"] > out["exit_high"])
    out["long_exit_channel"] = False
    out["risk_mult"] = 0.60
    out.loc[out["adx"].between(14.0, 28.0), "risk_mult"] += 0.30
    out.loc[out["d1_ema100_slope"] < -0.006, "risk_mult"] += 0.30
    out.loc[out["atr_pct"].between(0.006, 0.026), "risk_mult"] += 0.20
    out.loc[out["atr_pct"] > 0.030, "risk_mult"] -= 0.35
    out["risk_mult"] = out["risk_mult"].clip(cfg.min_risk_mult, cfg.max_risk_mult)
    out["quality_mult"] = 1.0
    trend_cont = (out["close"] < out["ema20"]) & (out["ema20"] < out["ema50"])
    out.loc[trend_cont, "quality_mult"] *= 1.35
    out.loc[out["close"] < out["entry_low"], "quality_mult"] *= 0.75
    out.loc[out["adx"] > 32.0, "quality_mult"] *= 0.60
    out.loc[out["atr_pct"] > 0.025, "quality_mult"] *= 0.70
    out["quality_mult"] = out["quality_mult"].clip(0.20, 1.70)
    return out.dropna().copy()


def build_bull_features(base_4h: pd.DataFrame, cfg: BullReclaimV2FeatureConfig) -> pd.DataFrame:
    out = base_4h.copy()
    out["ema20"] = ema(out["close"], cfg.ema_fast)
    out["ema50"] = ema(out["close"], cfg.ema_mid)
    out["ema100"] = ema(out["close"], cfg.ema_slow)
    out["atr"] = atr(out, cfg.atr_period)
    out["atr_pct"] = out["atr"] / out["close"]
    out["adx"] = adx(out, cfg.adx_period)
    out["rsi"] = rsi(out["close"], cfg.rsi_period)
    out["volume_med"] = out["volume"].rolling(30, min_periods=10).median().shift(1)
    d1 = _bull_daily_regime(base_4h, cfg)
    out = out.join(d1.reindex(out.index, method="ffill"))
    out["prev_close_below_ema20"] = out["close"].shift(1) < out["ema20"].shift(1)
    out["pb_min_dist50"] = (out["low"] / out["ema50"] - 1.0).rolling(cfg.pullback_lookback, min_periods=1).min().shift(1)
    out["pb_min_dist100"] = (out["low"] / out["ema100"] - 1.0).rolling(cfg.pullback_lookback, min_periods=1).min().shift(1)
    out["recent_pullback"] = (out["pb_min_dist50"] < cfg.pb_dist50) | (out["pb_min_dist100"] < cfg.pb_dist100) | out["prev_close_below_ema20"]
    out["reclaim"] = (out["close"] > out["ema20"] * cfg.reclaim_mult) & (out["close"] > out["open"]) & (out["close"] > out["close"].shift(1)) & (out["rsi"] > cfg.rsi_min)
    out["range_ok"] = out["adx"].between(cfg.adx_min, cfg.adx_max) & out["atr_pct"].between(cfg.atr_min, cfg.atr_max)
    out["volume_ok"] = out["volume"] > out["volume_med"] * cfg.vol_mult
    out["h4_dist50"] = out["close"] / out["ema50"] - 1.0
    out["not_extended"] = out["h4_dist50"] < cfg.h4_max_dist50
    out["daily_ok"] = out["d1_not_bear"].astype("boolean").fillna(False).astype(bool)
    out["macro_bull_ok"] = out["daily_ok"] & ((out["d1_ema_mid"] / out["d1_ema_slow"]) > cfg.d1_mid_vs_slow_min)
    out["quality_bucket_a"] = out["macro_bull_ok"] & out["recent_pullback"] & out["reclaim"] & out["range_ok"] & out["volume_ok"] & out["not_extended"]
    out["secondary_reclaim"] = out["macro_bull_ok"] & out["recent_pullback"] & (out["close"] > out["ema20"]) & (out["close"] > out["open"]) & (out["close"] > out["close"].shift(1)) & (out["rsi"] > cfg.secondary_rsi_min) & out["adx"].between(cfg.adx_min, cfg.secondary_adx_max) & out["atr_pct"].between(cfg.atr_min, cfg.atr_max) & out["volume_ok"] & out["not_extended"] & (~out["quality_bucket_a"])
    out["quality_bucket_b"] = out["secondary_reclaim"]
    out["long_signal"] = out["quality_bucket_a"] | out["quality_bucket_b"]
    out["short_signal"] = False
    out["signal"] = 0
    out.loc[out["long_signal"], "signal"] = 1
    out["exit_low"] = out["low"].rolling(16, min_periods=4).min().shift(1)
    out["long_exit_channel"] = (out["close"] < out["ema50"] * cfg.exit_ema50_mult) | (out["close"] < out["exit_low"])
    out["short_exit_channel"] = False
    out["risk_mult"] = 1.0
    out.loc[out["adx"].between(10.0, 18.0), "risk_mult"] += 0.15
    out.loc[out["atr_pct"].between(0.004, 0.030), "risk_mult"] += 0.15
    out.loc[out["atr_pct"] > 0.040, "risk_mult"] -= 0.25
    out["risk_mult"] = out["risk_mult"].clip(cfg.min_risk_mult, cfg.max_risk_mult)
    out["quality_mult"] = 0.0
    out.loc[out["quality_bucket_a"], "quality_mult"] = 1.00
    out.loc[out["quality_bucket_b"], "quality_mult"] = cfg.secondary_quality_mult
    out["quality_mult"] = out["quality_mult"].clip(0.20, 1.20)
    return out.dropna().copy()


def _momentum_daily_regime(base_4h: pd.DataFrame, cfg: MomentumV3FeatureConfig) -> pd.DataFrame:
    d1 = resample_ohlcv(base_4h, "1D")
    d1["d1_ema_fast"] = ema(d1["close"], cfg.d1_ema_fast)
    d1["d1_ema_slow"] = ema(d1["close"], cfg.d1_ema_slow)
    d1["d1_slow_slope"] = d1["d1_ema_slow"] / d1["d1_ema_slow"].shift(cfg.d1_slope_lookback) - 1.0
    d1["d1_bull"] = (d1["close"] > d1["d1_ema_slow"]) & (d1["d1_ema_fast"] > d1["d1_ema_slow"] * 0.995) & (d1["d1_slow_slope"] > cfg.bull_slope_min)
    d1["d1_bear"] = (d1["close"] < d1["d1_ema_slow"]) & (d1["d1_ema_fast"] < d1["d1_ema_slow"]) & (d1["d1_slow_slope"] < cfg.bear_slope_max)
    return d1[["d1_ema_fast", "d1_ema_slow", "d1_slow_slope", "d1_bull", "d1_bear"]].shift(1)


def _momentum_weekly_regime(base_4h: pd.DataFrame, cfg: MomentumV3FeatureConfig) -> pd.DataFrame:
    wk = resample_ohlcv(base_4h, "1W")
    wk["w_close"] = wk["close"]
    wk["w_ema_fast"] = ema(wk["close"], cfg.w_ema_fast)
    wk["w_ema_mid"] = ema(wk["close"], cfg.w_ema_mid)
    wk["w_slope_mid"] = wk["w_ema_mid"] / wk["w_ema_mid"].shift(cfg.w_slope_lookback) - 1.0
    return wk[["w_close", "w_ema_fast", "w_ema_mid", "w_slope_mid"]].shift(1)


def _bear_add_shifted_htf(base_4h: pd.DataFrame, features: pd.DataFrame, cfg: BearV3FeatureConfig) -> pd.DataFrame:
    out = features.copy()
    d1 = resample_ohlcv(base_4h, "1D")
    d1["d1_close"] = d1["close"]
    d1["d1_ema20"] = ema(d1["close"], cfg.d1_ema_fast)
    d1["d1_ema50"] = ema(d1["close"], cfg.d1_ema_mid)
    d1["d1_ema100"] = ema(d1["close"], cfg.d1_ema_slow)
    d1["d1_ema200"] = ema(d1["close"], cfg.d1_ema_major)
    d1["d1_ema50_slope"] = d1["d1_ema50"] / d1["d1_ema50"].shift(cfg.d1_slope_lookback) - 1.0
    d1["d1_ema100_slope"] = d1["d1_ema100"] / d1["d1_ema100"].shift(cfg.d1_slope_lookback) - 1.0
    d1_cols = ["d1_close", "d1_ema20", "d1_ema50", "d1_ema100", "d1_ema200", "d1_ema50_slope", "d1_ema100_slope"]
    out = out.join(pd.DataFrame({col: d1[col].shift(1) for col in d1_cols}, index=d1.index).reindex(out.index, method="ffill"))
    wk = resample_ohlcv(base_4h, "1W")
    wk["w_close"] = wk["close"]
    wk["w_ema10"] = ema(wk["close"], cfg.w_ema_fast)
    wk["w_ema20"] = ema(wk["close"], cfg.w_ema_mid)
    wk["w_ema40"] = ema(wk["close"], cfg.w_ema_slow)
    wk["w_ema20_slope"] = wk["w_ema20"] / wk["w_ema20"].shift(cfg.w_slope_lookback) - 1.0
    w_cols = ["w_close", "w_ema10", "w_ema20", "w_ema40", "w_ema20_slope"]
    out = out.join(pd.DataFrame({col: wk[col].shift(1) for col in w_cols}, index=wk.index).reindex(out.index, method="ffill"))
    out["ret_6"] = out["close"] / out["close"].shift(6) - 1.0
    out["ret_12"] = out["close"] / out["close"].shift(12) - 1.0
    out["ret_30"] = out["close"] / out["close"].shift(30) - 1.0
    return out


def _bull_daily_regime(base_4h: pd.DataFrame, cfg: BullReclaimV2FeatureConfig) -> pd.DataFrame:
    d1 = resample_ohlcv(base_4h, "1D")
    d1["d1_close"] = d1["close"]
    d1["d1_ema_fast"] = ema(d1["close"], cfg.d1_ema_fast)
    d1["d1_ema_mid"] = ema(d1["close"], cfg.d1_ema_mid)
    d1["d1_ema_slow"] = ema(d1["close"], cfg.d1_ema_slow)
    d1["d1_mid_slope"] = d1["d1_ema_mid"] / d1["d1_ema_mid"].shift(cfg.d1_slope_lookback) - 1.0
    d1["d1_slow_slope"] = d1["d1_ema_slow"] / d1["d1_ema_slow"].shift(cfg.d1_slope_lookback) - 1.0
    d1["d1_dist_mid"] = d1["close"] / d1["d1_ema_mid"] - 1.0
    d1["d1_not_bear"] = (d1["close"] > d1["d1_ema_mid"] * cfg.d1_close_mult) & (d1["d1_ema_fast"] > d1["d1_ema_mid"] * cfg.d1_fast_mult) & (d1["d1_mid_slope"] > cfg.d1_slope_min) & (d1["d1_dist_mid"].between(cfg.d1_min_dist, cfg.d1_max_dist))
    cols = ["d1_close", "d1_ema_fast", "d1_ema_mid", "d1_ema_slow", "d1_mid_slope", "d1_slow_slope", "d1_dist_mid", "d1_not_bear"]
    return pd.DataFrame({col: d1[col].shift(1) for col in cols}, index=d1.index)


def _row_or_none(df: pd.DataFrame, target_index) -> Mapping[str, Any] | None:
    if df.empty or target_index not in df.index:
        return None
    row = df.loc[target_index]
    return {key: _python_value(value) for key, value in row.to_dict().items()}


def _python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value
