"""Save scan result to PostgreSQL."""

from __future__ import annotations

import json
from pathlib import Path

from db import get_conn, init_db


RESULT_PATH = Path(__file__).resolve().parents[1] / "data" / "processed" / "daily_scan_result.json"


def main() -> None:
    init_db()
    payload = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
    source = payload.get("source", {})
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scan_runs (source, status, symbol, rows_count, error, payload)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    source.get("source", "unknown"),
                    source.get("status", "unknown"),
                    source.get("symbol"),
                    payload.get("rows"),
                    source.get("error"),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        conn.commit()
    print("saved scan result")


if __name__ == "__main__":
    main()
