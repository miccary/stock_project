"""Minimal PostgreSQL helper."""

from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg2


DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id BIGSERIAL PRIMARY KEY,
                    scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source TEXT NOT NULL,
                    status TEXT NOT NULL,
                    symbol TEXT,
                    rows_count INTEGER,
                    error TEXT,
                    payload JSONB NOT NULL
                )
                """
            )
        conn.commit()
