"""Persistent evidence store for issue failing rows.

Each detection run upserts all failing rows for a (le_book, rule_id,
table_name) combination.  Rows are stored as JSON objects so no schema
migration is needed when Greenplum columns change.

Older run_dates for the same (le_book, rule_id, table_name) are replaced
on each write — we keep only the most recent run so storage doesn't grow
unboundedly over time.
"""
from __future__ import annotations

import json
import logging

from storage.postgres.app_db import get_connection

log = logging.getLogger("storage.evidence_store")

_tables_ensured = False


def ensure_table() -> None:
    global _tables_ensured
    if _tables_ensured:
        return
    from storage.postgres.init_tables import init_all
    init_all()
    _tables_ensured = True


def store_rows(
    le_book: str,
    rule_id: str,
    table_name: str,
    run_date: str,
    rows: list[dict],
) -> None:
    """Write evidence for this (le_book, rule_id, table_name).

    First scan → written as snapshot_type='original' (locked forever).
    Subsequent scans → overwrite snapshot_type='latest' (remaining rows).
    """
    if not rows:
        return
    ensure_table()
    con = get_connection()
    try:
        original_exists = con.execute(
            "SELECT 1 FROM dq_issue_evidence "
            "WHERE le_book=%s AND rule_id=%s AND table_name=%s AND snapshot_type='original' LIMIT 1",
            (le_book, rule_id, table_name),
        ).fetchone()
        stype = "latest" if original_exists else "original"
        con.execute(
            "DELETE FROM dq_issue_evidence "
            "WHERE le_book=%s AND rule_id=%s AND table_name=%s AND snapshot_type=%s",
            (le_book, rule_id, table_name, stype),
        )
        con.executemany(
            "INSERT INTO dq_issue_evidence "
            "(le_book, rule_id, table_name, run_date, row_data, snapshot_type) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (le_book, rule_id, table_name, run_date,
                 json.dumps(row, default=str), stype)
                for row in rows
            ],
        )
        con.commit()
        log.debug("Stored %d evidence rows (%s) for %s / %s / %s",
                  len(rows), stype, le_book, rule_id, table_name)
    finally:
        con.close()


def load_rows(
    le_book: str,
    rule_id: str,
    table_name: str,
    snapshot_type: str = "original",
) -> tuple[str, list[dict]]:
    """Return (run_date, rows) for the given snapshot, or ('', []) if none.

    snapshot_type='original' → rows at first detection (used for ZIP reports).
    snapshot_type='latest'   → most recent scan rows (remaining failing rows).
    """
    ensure_table()
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT run_date, row_data FROM dq_issue_evidence "
            "WHERE le_book=%s AND rule_id=%s AND table_name=%s AND snapshot_type=%s "
            "ORDER BY run_date",
            (le_book, rule_id, table_name, snapshot_type),
        ).fetchall()
        if not rows:
            return ("", [])
        run_date = rows[0]["run_date"]
        return run_date, [json.loads(r["row_data"]) for r in rows]
    except Exception:
        return ("", [])
    finally:
        con.close()


def clear_original(le_book: str, rule_id: str, table_name: str) -> None:
    """Delete the 'original' snapshot so the next detection run re-seeds it.

    Called on issue reopen so the resolved ZIP shows rows from the current
    recurrence cycle, not the first-ever detection.
    """
    ensure_table()
    con = get_connection()
    try:
        con.execute(
            "DELETE FROM dq_issue_evidence "
            "WHERE le_book=%s AND rule_id=%s AND table_name=%s AND snapshot_type='original'",
            (le_book, rule_id, table_name),
        )
        con.commit()
        log.debug("Cleared original evidence for %s / %s / %s", le_book, rule_id, table_name)
    except Exception as exc:
        log.warning("Could not clear original evidence %s/%s/%s: %s", le_book, rule_id, table_name, exc)
    finally:
        con.close()


def has_evidence(le_book: str, rule_id: str, table_name: str) -> bool:
    ensure_table()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT 1 FROM dq_issue_evidence "
            "WHERE le_book=%s AND rule_id=%s AND table_name=%s LIMIT 1",
            (le_book, rule_id, table_name),
        ).fetchone()
        return row is not None
    except Exception:
        return False
    finally:
        con.close()
