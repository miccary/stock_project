"""Calculate basic technical indicators from OHLCV data."""

from __future__ import annotations

import pandas as pd


def calc_ma(df: pd.DataFrame, windows=(5, 10, 20, 60)) -> pd.DataFrame:
    out = df.copy()
    for w in windows:
        out[f"ma{w}"] = out["close"].rolling(w).mean()
    return out


def calc_macd(df: pd.DataFrame, fast=12, slow=26, signal=9) -> pd.DataFrame:
    out = df.copy()
    ema_fast = out["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = out["close"].ewm(span=slow, adjust=False).mean()
    out["dif"] = ema_fast - ema_slow
    out["dea"] = out["dif"].ewm(span=signal, adjust=False).mean()
    out["macd_hist"] = (out["dif"] - out["dea"]) * 2
    return out


def calc_rsi(df: pd.DataFrame, period=14) -> pd.DataFrame:
    out = df.copy()
    delta = out["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    out["rsi"] = 100 - (100 / (1 + rs))
    return out


def calc_volume_ma(df: pd.DataFrame, windows=(5, 20)) -> pd.DataFrame:
    out = df.copy()
    for w in windows:
        out[f"vol_ma{w}"] = out["volume"].rolling(w).mean()
    return out


def add_basic_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = calc_ma(out)
    out = calc_macd(out)
    out = calc_rsi(out)
    out = calc_volume_ma(out)
    return out
