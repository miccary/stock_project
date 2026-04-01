"""Fetch and store Tushare stock_basic for stock pool mapping.

Cache-first with TTL: reuse local cache if it is fresh enough, and only query
Tushare when cache is missing or expired.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import tushare as ts

from db import get_conn, init_db


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "processed"
CACHE_PATH = OUT_DIR / "stock_basic_cache.csv"
META_PATH = OUT_DIR / "stock_basic_cache_meta.json"
OUT_DIR.mkdir(parents=True, exist_ok=True)
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
CACHE_TTL_HOURS = int(os.getenv("STOCK_BASIC_CACHE_TTL_HOURS", "24"))


def cache_is_fresh() -> bool:
    if not CACHE_PATH.exists() or not META_PATH.exists():
        return False
    try:
        meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        fetched_at = datetime.fromisoformat(meta["fetched_at"])
        return datetime.now() - fetched_at < timedelta(hours=CACHE_TTL_HOURS)
    except Exception:
        return False


def load_stock_basic() -> tuple[pd.DataFrame, bool]:
    if CACHE_PATH.exists() and cache_is_fresh():
        return pd.read_csv(CACHE_PATH), True
    if not TUSHARE_TOKEN:
        raise RuntimeError("TUSHARE_TOKEN not set")
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,area,industry,list_date")
    if df.empty:
        raise ValueError("stock_basic returned empty dataframe")
    df.to_csv(CACHE_PATH, index=False)
    META_PATH.write_text(
        json.dumps({"fetched_at": datetime.now().isoformat(), "ttl_hours": CACHE_TTL_HOURS}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return df, False


def main() -> None:
    init_db()
    df, cached = load_stock_basic()
    payload = {"rows": int(len(df)), "cached": cached, "ttl_hours": CACHE_TTL_HOURS}
    out_json = OUT_DIR / "stock_basic_result.json"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_basic (
                    ts_code TEXT PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    area TEXT,
                    industry TEXT,
                    list_date TEXT
                )
                """
            )
            cur.execute("TRUNCATE stock_basic")
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO stock_basic (ts_code, symbol, name, area, industry, list_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (row["ts_code"], row["symbol"], row["name"], row["area"], row["industry"], row["list_date"]),
                )
        conn.commit()
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
