# Persistence for dq_history.json — split out of dq/reports/history.py per
# review feedback, so that module stays a pure entry-builder and this module
# owns the actual read-merge-write-to-disk cycle.
from __future__ import annotations

import logging
from pathlib import Path

from storage.files.json_store import read_json, write_json

log = logging.getLogger("storage.files.history_store")

SCRIPT_DIR   = Path(__file__).resolve().parents[2]
HISTORY_FILE = SCRIPT_DIR / "dq_history.json"


def append_history_entry(entry: dict, path: Path = HISTORY_FILE) -> None:
    """Append (or replace same-date entry) in the history log."""
    history: list = read_json(path, default=[]) or []
    # idempotent: replace if same date was already written today
    history = [e for e in history if e.get("date") != entry["date"]]
    history.append(entry)
    history.sort(key=lambda e: e.get("date", ""))
    write_json(path, history)
    log.info("History log updated → %s  (%d entries total)", path, len(history))
