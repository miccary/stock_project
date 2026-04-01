"""Minimal Streamlit dashboard for phase 1."""

from __future__ import annotations

import json
import os
from pathlib import Path

import streamlit as st

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
                SELECT scanned_at, source, status, symbol, rows_count, error
                FROM scan_runs
                ORDER BY scanned_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def read_latest_scan_from_db():
    scans = read_recent_scans_from_db(limit=1)
    return scans[0] if scans else None


def read_latest_scan_from_file():
    if not RESULT_PATH.exists():
        return None
    return json.loads(RESULT_PATH.read_text(encoding="utf-8"))


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

col1, col2 = st.columns(2)

with col1:
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

with col2:
    st.subheader("Signals")
    if latest:
        if "payload" in latest and isinstance(latest["payload"], dict):
            signals = latest["payload"].get("signals", [])
        else:
            signals = latest.get("signals", [])
        if signals:
            st.json(signals)
        else:
            st.write("No signals detected.")
    else:
        st.write("No signals yet.")

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
