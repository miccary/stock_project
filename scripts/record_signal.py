"""Record signals from the latest scan into PostgreSQL."""

from __future__ import annotations

import json
from pathlib import Path

from db import get_conn, init_db


RESULT_PATH = Path(__file__).resolve().parents[1] / "data" / "processed" / "daily_scan_result.json"


def main() -> None:
    init_db()
    payload = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
    source = payload.get("source", {})
    signals = payload.get("signals", [])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id BIGSERIAL PRIMARY KEY,
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source TEXT NOT NULL,
                    symbol TEXT,
                    signal_type TEXT,
                    signal_name TEXT,
                    strength TEXT,
                    payload JSONB NOT NULL
                )
                """
            )
            for sig in signals:
                cur.execute(
                    """
                    INSERT INTO signals (source, symbol, signal_type, signal_name, strength, payload)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        source.get("source", "unknown"),
                        source.get("symbol"),
                        sig.get("type"),
                        sig.get("name"),
                        sig.get("strength"),
                        json.dumps(sig, ensure_ascii=False),
                    ),
                )
        conn.commit()
    print(json.dumps({"recorded": len(signals)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
