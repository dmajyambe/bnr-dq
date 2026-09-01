#Persistent evidence store for issue failing rows.
from __future__ import annotations
import json
import logging
from sqlalchemy import text
from storage.postgres.connection import get_engine

log = logging.getLogger("storage.evidence_store")

_tables_ensured = False
def ensure_table() -> None:
    global _tables_ensured
    if _tables_ensured:
        return
    from storage.postgres.init_tables import init_all
    init_all()
    _tables_ensured = True

#write rows to the evidence store for a given (le_book, rule_id, table_name).
#changes to latest rows are overwritten, original rows remain.
def store_rows(
    le_book: str,
    rule_id: str,
    table_name: str,
    run_date: str,
    rows: list[dict],
) -> None:
  
    if not rows:
        return
    ensure_table()
    with get_engine().begin() as con:
        original_exists = con.execute(
            text(
                "SELECT 1 FROM dq_issue_evidence "
                "WHERE le_book=:lb AND rule_id=:rid AND table_name=:tbl AND snapshot_type='original' LIMIT 1"
            ),
            {"lb": le_book, "rid": rule_id, "tbl": table_name},
        ).mappings().fetchone()
        stype = "latest" if original_exists else "original"
        con.execute(
            text(
                "DELETE FROM dq_issue_evidence "
                "WHERE le_book=:lb AND rule_id=:rid AND table_name=:tbl AND snapshot_type=:stype"
            ),
            {"lb": le_book, "rid": rule_id, "tbl": table_name, "stype": stype},
        )
        con.execute(
            text(
                "INSERT INTO dq_issue_evidence "
                "(le_book, rule_id, table_name, run_date, row_data, snapshot_type) "
                "VALUES (:lb, :rid, :tbl, :run_date, :row_data, :stype)"
            ),
            [
                {"lb": le_book, "rid": rule_id, "tbl": table_name,
                 "run_date": run_date, "row_data": json.dumps(row, default=str), "stype": stype}
                for row in rows
            ],
        )
        log.debug("Stored %d evidence rows (%s) for %s / %s / %s",
                  len(rows), stype, le_book, rule_id, table_name)


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
    try:
        with get_engine().connect() as con:
            rows = con.execute(
                text(
                    "SELECT run_date, row_data FROM dq_issue_evidence "
                    "WHERE le_book=:lb AND rule_id=:rid AND table_name=:tbl AND snapshot_type=:stype "
                    "ORDER BY run_date"
                ),
                {"lb": le_book, "rid": rule_id, "tbl": table_name, "stype": snapshot_type},
            ).mappings().fetchall()
            if not rows:
                return ("", [])
            run_date = rows[0]["run_date"]
            return run_date, [json.loads(r["row_data"]) for r in rows]
    except Exception:
        return ("", [])

#delete original data for reopned issues so the next detection run re-seeds it.
def clear_original(le_book: str, rule_id: str, table_name: str) -> None:
    
    ensure_table()
    try:
        with get_engine().begin() as con:
            con.execute(
                text(
                    "DELETE FROM dq_issue_evidence "
                    "WHERE le_book=:lb AND rule_id=:rid AND table_name=:tbl AND snapshot_type='original'"
                ),
                {"lb": le_book, "rid": rule_id, "tbl": table_name},
            )
            log.debug("Cleared original evidence for %s / %s / %s", le_book, rule_id, table_name)
    except Exception as exc:
        log.warning("Could not clear original evidence %s/%s/%s: %s", le_book, rule_id, table_name, exc)


def has_evidence(le_book: str, rule_id: str, table_name: str) -> bool:
    ensure_table()
    try:
        with get_engine().connect() as con:
            row = con.execute(
                text(
                    "SELECT 1 FROM dq_issue_evidence "
                    "WHERE le_book=:lb AND rule_id=:rid AND table_name=:tbl LIMIT 1"
                ),
                {"lb": le_book, "rid": rule_id, "tbl": table_name},
            ).mappings().fetchone()
            return row is not None
    except Exception:
        return False
