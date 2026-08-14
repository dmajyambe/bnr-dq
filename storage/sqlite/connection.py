# Single source for the dq_rules.db connection
# in dq_auth.py, dq_issue_tracker.py, dq_change_request.py, and dq_notifications.py.
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "dq_rules.db"

def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con
