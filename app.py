"""第1阶段中文看板：股票研究与交易辅助系统。"""

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
st.title("股票研究与交易辅助系统")
st.caption("第一阶段 · 中文看板")


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


def read_recent_signals_from_db(limit: int = 50):
    if not DATABASE_URL or psycopg2 is None:
        return []
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT recorded_at, source, symbol, signal_type, signal_name, strength, payload
                FROM signals
                ORDER BY recorded_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
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
        "akshare": "未知",
        "tushare": "未知",
        "postgres": "未知",
    }
    statuses["akshare"] = "导入成功" if ak is not None else "导入失败"
    statuses["tushare"] = "导入成功" if ts is not None else "导入失败"
    if psycopg2 is not None:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            statuses["postgres"] = "已连接"
        except Exception:
            statuses["postgres"] = "连接失败"
    else:
        statuses["postgres"] = "导入失败"
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
recent_signals = read_recent_signals_from_db(limit=50)

latest_rows = None
latest_source = "-"
latest_status = "-"
latest_scanned_at = "-"
if isinstance(latest, dict):
    latest_rows = latest.get("rows_count") if latest.get("rows_count") is not None else latest.get("rows")
    latest_source = latest.get("source", latest_source)
    latest_status = latest.get("status", latest_status)
    latest_scanned_at = str(latest.get("scanned_at", latest_scanned_at))

st.subheader("总览")
overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
with overview_col1:
    st.metric("最新行数", latest_rows if latest_rows is not None else "-")
with overview_col2:
    st.metric("最新数据源", latest_source)
with overview_col3:
    st.metric("最新状态", latest_status)
with overview_col4:
    st.metric("信号数量", len(signals))

if latest_source == "sample" or latest_status == "fallback":
    st.warning("当前看板显示的是回退/样例数据，不是实时 Tushare 日线数据。")
else:
    st.success(f"当前看板显示的是来自 {latest_source} 的实时数据。")

st.caption(f"最新扫描时间：{latest_scanned_at}")

health_col1, health_col2 = st.columns(2)
with health_col1:
    st.subheader("数据源健康")
    st.json(source_statuses)
with health_col2:
    st.subheader("数据源快照")
    if db_error:
        st.warning(f"数据库读取失败，回退到本地文件。错误：{db_error}")
    if latest:
        if "payload" in latest and isinstance(latest["payload"], dict):
            st.json(latest.get("payload", {}).get("source", latest))
        else:
            st.json(latest.get("source", latest))
    else:
        st.info("当前没有扫描结果，请先运行 scripts/run_pipeline.py")

signal_col1, signal_col2 = st.columns(2)
with signal_col1:
    st.subheader("信号列表")
    if signals:
        st.json(signals)
    else:
        st.write("暂无信号。")
with signal_col2:
    st.subheader("信号统计")
    if signals:
        type_counter = Counter([s.get("type", "unknown") for s in signals])
        strength_counter = Counter([s.get("strength", "unknown") for s in signals])
        st.write("按类型统计")
        st.json(type_counter)
        st.write("按强度统计")
        st.json(strength_counter)
    else:
        st.write("暂无信号统计。")

st.subheader("信号历史")
if recent_signals:
    signal_rows = []
    for row in recent_signals:
        signal_rows.append({
            "记录时间": row.get("recorded_at"),
            "来源": row.get("source"),
            "代码": row.get("symbol"),
            "信号类型": row.get("signal_type"),
            "信号名称": row.get("signal_name"),
            "强度": row.get("strength"),
        })
    st.dataframe(signal_rows, use_container_width=True)
else:
    st.write("暂无信号历史，请先运行 scripts/record_signal.py")

st.subheader("股票池缓存")
if stock_basic_df is not None:
    st.metric("缓存股票数", len(stock_basic_df))
    if "industry" in stock_basic_df.columns:
        industry_counts = stock_basic_df["industry"].fillna("未知").value_counts().head(10)
        st.write("行业前十")
        st.json(industry_counts.to_dict())
else:
    st.write("暂无股票池缓存，请在权限/Token 准备好后运行 scripts/update_stock_basic.py")

st.subheader("最近扫描记录")
if recent_scans:
    display_scans = []
    for row in recent_scans:
        display_scans.append({
            "扫描时间": row.get("scanned_at"),
            "来源": row.get("source"),
            "状态": row.get("status"),
            "代码": row.get("symbol"),
            "行数": row.get("rows_count"),
            "错误": row.get("error"),
        })
    st.dataframe(display_scans, use_container_width=True)
else:
    st.write("暂无扫描记录。")

with st.expander("原始结果"):
    if latest:
        if "payload" in latest and isinstance(latest["payload"], dict):
            st.code(json.dumps(latest["payload"], ensure_ascii=False, indent=2), language="json")
        else:
            st.code(json.dumps(latest, ensure_ascii=False, indent=2), language="json")
    else:
        st.write("暂无原始结果。")
