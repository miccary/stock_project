"""Minimal Streamlit dashboard for phase 1."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parent
RESULT_PATH = ROOT / "data" / "processed" / "daily_scan_result.json"


st.set_page_config(page_title="stock_project", layout="wide")
st.title("stock_project - Phase 1 Dashboard")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Data Source Status")
    if RESULT_PATH.exists():
        result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
        st.json(result.get("source", {}))
    else:
        st.info("No scan result yet. Run scripts/run_daily_scan.py first.")

with col2:
    st.subheader("Signals")
    if RESULT_PATH.exists():
        result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
        signals = result.get("signals", [])
        if signals:
            st.json(signals)
        else:
            st.write("No signals detected.")
    else:
        st.write("No signals yet.")

st.subheader("Raw Result")
if RESULT_PATH.exists():
    st.code(RESULT_PATH.read_text(encoding="utf-8"), language="json")
else:
    st.write("No result file found.")
