"""Run a minimal daily scan for technical signals with fallback data."""

from __future__ import annotations

from pathlib import Path
import json
import os

import pandas as pd
import akshare as ak
import tushare as ts

from calc_indicators import add_basic_indicators
from detect_signals import detect_signals


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_DIR = DATA_DIR / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")

if TUSHARE_TOKEN:
    ts.set_token(TUSHARE_TOKEN)
PRO = ts.pro_api() if TUSHARE_TOKEN else None


def load_sample_data() -> pd.DataFrame:
    sample_path = DATA_DIR / "sample_ohlcv.csv"
    if not sample_path.exists():
        raise FileNotFoundError(f"missing sample data: {sample_path}")
    df = pd.read_csv(sample_path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_tushare_daily(symbol: str = "000001.SZ") -> tuple[pd.DataFrame, dict]:
    if PRO is None:
        raise RuntimeError("TUSHARE_TOKEN not set")
    df = PRO.daily(ts_code=symbol, start_date="20240101", end_date="20241231")
    if df.empty:
        raise ValueError("tushare returned empty dataframe")
    df = df.rename(
        columns={
            "trade_date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    cols = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[cols].copy()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna().sort_values("date").reset_index(drop=True)
    meta = {"source": "tushare", "status": "ok", "symbol": symbol}
    return df, meta


def load_market_data(symbol: str = "000001.SZ") -> tuple[pd.DataFrame, dict]:
    try:
        return load_tushare_daily(symbol)
    except Exception as e_tushare:
        try:
            df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20241231", adjust="qfq")
            if df.empty:
                raise ValueError("akshare returned empty dataframe")
            rename_map = {
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            }
            df = df.rename(columns=rename_map)
            cols = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
            df = df[cols].copy()
            df["date"] = pd.to_datetime(df["date"])
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna().sort_values("date").reset_index(drop=True)
            return df, {"source": "akshare", "status": "ok", "symbol": "000001", "tushare_error": str(e_tushare)}
        except Exception as e_akshare:
            meta = {"source": "sample", "status": "fallback", "symbol": symbol, "tushare_error": str(e_tushare), "akshare_error": str(e_akshare)}
            return load_sample_data(), meta


def main() -> None:
    df, source_meta = load_market_data()
    df = add_basic_indicators(df)
    signals = detect_signals(df)

    payload = {
        "rows": int(len(df)),
        "source": source_meta,
        "signals": signals,
    }

    out_json = OUT_DIR / "daily_scan_result.json"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
