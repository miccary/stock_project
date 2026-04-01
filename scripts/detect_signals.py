"""Detect first-stage technical signals from indicator data."""

from __future__ import annotations

import pandas as pd


def latest_row(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        raise ValueError("empty dataframe")
    return df.iloc[-1]


def detect_signals(df: pd.DataFrame) -> list[dict]:
    row = latest_row(df)
    prev = df.iloc[-2] if len(df) > 1 else row
    signals: list[dict] = []

    if pd.notna(row.get("ma5")) and pd.notna(row.get("ma20")) and pd.notna(row.get("ma60")):
        if row["ma5"] > row["ma20"] > row["ma60"]:
            signals.append({"type": "trend", "name": "多头排列", "strength": "high"})

    if pd.notna(row.get("ma5")) and pd.notna(row.get("ma20")):
        if prev.get("ma5") is not None and prev.get("ma20") is not None:
            if prev["ma5"] <= prev["ma20"] and row["ma5"] > row["ma20"]:
                signals.append({"type": "trend", "name": "MA5上穿MA20", "strength": "high"})

    if pd.notna(row.get("dif")) and pd.notna(row.get("dea")):
        if prev.get("dif") is not None and prev.get("dea") is not None:
            if prev["dif"] <= prev["dea"] and row["dif"] > row["dea"]:
                signals.append({"type": "momentum", "name": "MACD金叉", "strength": "high"})

    if pd.notna(row.get("rsi")):
        if row["rsi"] <= 30:
            signals.append({"type": "momentum", "name": "RSI超卖", "strength": "medium"})
        elif row["rsi"] >= 70:
            signals.append({"type": "momentum", "name": "RSI超买", "strength": "medium"})

    return signals
