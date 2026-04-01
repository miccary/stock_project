"""Run scan -> save result to PostgreSQL -> record signals -> print summary."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:112345@localhost:5432/postgres")


def run(cmd):
    env = os.environ.copy()
    env["DATABASE_URL"] = DATABASE_URL
    subprocess.run([str(PYTHON), *cmd], cwd=ROOT, env=env, check=True)


def main() -> None:
    run(["scripts/run_daily_scan.py"])
    run(["scripts/save_scan_result.py"])
    run(["scripts/record_signal.py"])


if __name__ == "__main__":
    main()
