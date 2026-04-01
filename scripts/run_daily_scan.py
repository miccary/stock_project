"""Run a minimal daily scan for technical signals with fallback data."""

from __future__ import annotations

from pathlib import Path
import json

import pandas as pd
import akshare as ak

from calc_indicators import add_basic_indicators
from detect_signals import detect_signals


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUT_DIR = DATA_DIR / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_sample_data() -> pd.DataFrame:
    sample_path = DATA_DIR / "sample_ohlcv.csv"
    if not sample_path.exists():
        raise FileNotFoundError(f"missing sample data: {sample_path}")
    df = pd.read_csv(sample_path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_market_data(symbol: str = "000001") -> tuple[pd.DataFrame, dict]:
    meta = {"source": "akshare", "status": "ok", "symbol": symbol}
    try:
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date="20240101", end_date="20241231", adjust="qfq")
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
        return df, meta
    except Exception as e:
        meta = {"source": "sample", "status": "fallback", "error": str(e), "symbol": symbol}
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
