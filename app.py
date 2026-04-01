"""Minimal Streamlit dashboard for phase 1."""

from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path

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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:112345@localhost:5432/postgres")


st.set_page_config(page_title="stock_project", layout="wide")
st.title("stock_project - Phase 1 Dashboard")


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
    if ak is not None:
        try:
            statuses["akshare"] = "import_ok"
        except Exception:
            statuses["akshare"] = "import_fail"
    else:
        statuses["akshare"] = "import_fail"
    if ts is not None:
        try:
            statuses["tushare"] = "import_ok"
        except Exception:
            statuses["tushare"] = "import_fail"
    else:
        statuses["tushare"] = "import_fail"
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


latest = None
db_error = None
recent_scans = []
try:
    recent_scans = read_recent_scans_from_db(limit=10)
    latest = recent_scans[0] if recent_scans else None
except Exception as e:
    db_error = str(e)

if latest is None:
    latest = read_latest_scan_from_file()

signals = extract_signals(latest)
source_statuses = get_source_statuses()

# Summary cards
card1, card2, card3, card4 = st.columns(4)
with card1:
    st.metric("Latest Rows", latest.get("rows_count") if isinstance(latest, dict) and latest.get("rows_count") is not None else (latest.get("rows") if isinstance(latest, dict) else "-"))
with card2:
    if isinstance(latest, dict):
        st.metric("Latest Source", latest.get("source", latest.get("source", "-")))
    else:
        st.metric("Latest Source", "-")
with card3:
    if isinstance(latest, dict):
        st.metric("Latest Status", latest.get("status", latest.get("status", "-")))
    else:
        st.metric("Latest Status", "-")
with card4:
    st.metric("Signal Count", len(signals))

status_col1, status_col2 = st.columns(2)
with status_col1:
    st.subheader("Data Source Health")
    st.json(source_statuses)
with status_col2:
    st.subheader("Data Source Status")
    if db_error:
        st.warning(f"DB read failed, fallback to file. Error: {db_error}")
    if latest:
        if "payload" in latest and isinstance(latest["payload"], dict):
            st.json(latest.get("payload", {}).get("source", latest))
        else:
            st.json(latest.get("source", latest))
    else:
        st.info("No scan result yet. Run scripts/run_pipeline.py first.")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Signals")
    if signals:
        st.json(signals)
    else:
        st.write("No signals detected.")

with col2:
    st.subheader("Signal Statistics")
    if signals:
        type_counter = Counter([s.get("type", "unknown") for s in signals])
        strength_counter = Counter([s.get("strength", "unknown") for s in signals])
        stat_col1, stat_col2 = st.columns(2)
        with stat_col1:
            st.write("By type")
            st.json(type_counter)
        with stat_col2:
            st.write("By strength")
            st.json(strength_counter)
    else:
        st.write("No signal stats yet.")

st.subheader("Recent Scan Records")
if recent_scans:
    st.dataframe(recent_scans, use_container_width=True)
else:
    st.write("No recent scan records found.")

st.subheader("Raw Result")
if latest:
    if "payload" in latest and isinstance(latest["payload"], dict):
        st.code(json.dumps(latest["payload"], ensure_ascii=False, indent=2), language="json")
    else:
        st.code(json.dumps(latest, ensure_ascii=False, indent=2), language="json")
else:
    st.write("No result file found.")
