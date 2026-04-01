"""Minimal Streamlit dashboard for phase 1."""

from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import akshare as ak
except Exception:
    ak = None

try:
    import tushare as ts
except Exception:
    ts = None

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None
    RealDictCursor = None


ROOT = Path(__file__).resolve().parent
RESULT_PATH = ROOT / "data" / "processed" / "daily_scan_result.json"
STOCK_BASIC_CACHE = ROOT / "data" / "processed" / "stock_basic_cache.csv"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:112345@localhost:5432/postgres")


st.set_page_config(page_title="stock_project", layout="wide")
st.title("stock_project")
st.caption("Phase 1 · Stock research and trading-assist dashboard")


def read_recent_scans_from_db(limit: int = 10):
    if not DATABASE_URL or psycopg2 is None:
        return []
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT scanned_at, source, status, symbol, rows_count, error, payload
                FROM scan_runs
                ORDER BY scanned_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def read_latest_scan_from_file():
    if not RESULT_PATH.exists():
        return None
    return json.loads(RESULT_PATH.read_text(encoding="utf-8"))


def extract_signals(latest):
    if not latest:
        return []
    if "payload" in latest and isinstance(latest["payload"], dict):
        return latest["payload"].get("signals", [])
    return latest.get("signals", [])


def get_source_statuses():
    statuses = {
        "akshare": "unknown",
        "tushare": "unknown",
        "postgres": "unknown",
    }
    statuses["akshare"] = "import_ok" if ak is not None else "import_fail"
    statuses["tushare"] = "import_ok" if ts is not None else "import_fail"
    if psycopg2 is not None:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            statuses["postgres"] = "connected"
        except Exception:
            statuses["postgres"] = "connect_fail"
    else:
        statuses["postgres"] = "import_fail"
    return statuses


def read_stock_basic_cache():
    if not STOCK_BASIC_CACHE.exists():
        return None
    return pd.read_csv(STOCK_BASIC_CACHE)


recent_scans = []
db_error = None
try:
    recent_scans = read_recent_scans_from_db(limit=10)
except Exception as e:
    db_error = str(e)

latest = recent_scans[0] if recent_scans else read_latest_scan_from_file()
signals = extract_signals(latest)
source_statuses = get_source_statuses()
stock_basic_df = read_stock_basic_cache()

latest_rows = None
latest_source = "-"
latest_status = "-"
latest_scanned_at = "-"
if isinstance(latest, dict):
    latest_rows = latest.get("rows_count") if latest.get("rows_count") is not None else latest.get("rows")
    latest_source = latest.get("source", latest_source)
    latest_status = latest.get("status", latest_status)
    latest_scanned_at = str(latest.get("scanned_at", latest_scanned_at))

st.subheader("Overview")
overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
with overview_col1:
    st.metric("Latest Rows", latest_rows if latest_rows is not None else "-")
with overview_col2:
    st.metric("Latest Source", latest_source)
with overview_col3:
    st.metric("Latest Status", latest_status)
with overview_col4:
    st.metric("Signal Count", len(signals))

st.caption(f"Latest Scan Time: {latest_scanned_at}")

health_col1, health_col2 = st.columns(2)
with health_col1:
    st.subheader("Data Source Health")
    st.json(source_statuses)
with health_col2:
    st.subheader("Data Source Snapshot")
    if db_error:
        st.warning(f"DB read failed, fallback to file. Error: {db_error}")
    if latest:
        if "payload" in latest and isinstance(latest["payload"], dict):
            st.json(latest.get("payload", {}).get("source", latest))
        else:
            st.json(latest.get("source", latest))
    else:
        st.info("No scan result yet. Run scripts/run_pipeline.py first.")

signal_col1, signal_col2 = st.columns(2)
with signal_col1:
    st.subheader("Signals")
    if signals:
        st.json(signals)
    else:
        st.write("No signals detected.")
with signal_col2:
    st.subheader("Signal Statistics")
    if signals:
        type_counter = Counter([s.get("type", "unknown") for s in signals])
        strength_counter = Counter([s.get("strength", "unknown") for s in signals])
        st.write("By type")
        st.json(type_counter)
        st.write("By strength")
        st.json(strength_counter)
    else:
        st.write("No signal stats yet.")

st.subheader("Stock Pool Cache")
if stock_basic_df is not None:
    st.metric("Cached Stocks", len(stock_basic_df))
    if "industry" in stock_basic_df.columns:
        industry_counts = stock_basic_df["industry"].fillna("Unknown").value_counts().head(10)
        st.write("Top industries")
        st.json(industry_counts.to_dict())
else:
    st.write("No stock_basic cache found yet. Run scripts/update_stock_basic.py when token/permission is ready.")

st.subheader("Recent Scan Records")
if recent_scans:
    st.dataframe(recent_scans, use_container_width=True)
else:
    st.write("No recent scan records found.")

with st.expander("Raw Result"):
    if latest:
        if "payload" in latest and isinstance(latest["payload"], dict):
            st.code(json.dumps(latest["payload"], ensure_ascii=False, indent=2), language="json")
        else:
            st.code(json.dumps(latest, ensure_ascii=False, indent=2), language="json")
    else:
        st.write("No result file found.")
