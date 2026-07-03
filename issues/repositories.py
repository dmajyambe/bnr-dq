# Raw SQLite CRUD against dq_open_issues / dq_institution_contacts.
# No urgency-band computation or rule-metadata lookups here — that's
# issues/tracker.py. This module is dumb storage access only.
from __future__ import annotations

import logging
import sqlite3
from datetime import date, timedelta

from issues.state_machine import urgency_band, issue_id, OPEN_SQL, SLA_DAYS
from storage.sqlite.connection import get_connection

log = logging.getLogger("issues.repositories")


def ensure_tables() -> None:
    """Create dq_open_issues, dq_penalties, dq_institution_contacts if absent."""
    con = get_connection()
    try:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS dq_open_issues (
                issue_id           TEXT PRIMARY KEY,
                le_book            TEXT NOT NULL,
                institution_name   TEXT,
                table_name         TEXT NOT NULL,
                rule_id            TEXT NOT NULL,
                rule_name          TEXT,
                dimension          TEXT NOT NULL,
                failing_rows       INTEGER NOT NULL DEFAULT 0,
                last_failing_rows  INTEGER,
                detected_at        TEXT NOT NULL,
                sla_deadline       TEXT NOT NULL,
                urgency_band       TEXT NOT NULL DEFAULT 'new',
                assigned_to        TEXT,
                notified_at        TEXT,
                resolved_at        TEXT,
                resolution_run_id  TEXT,
                recurrence_count   INTEGER NOT NULL DEFAULT 0,
                status             TEXT NOT NULL DEFAULT 'open'
            );

            CREATE TABLE IF NOT EXISTS dq_penalties (
                penalty_id           TEXT PRIMARY KEY,
                le_book              TEXT NOT NULL,
                institution_name     TEXT,
                dimension            TEXT NOT NULL,
                table_name           TEXT NOT NULL,
                rule_id              TEXT NOT NULL,
                period               TEXT NOT NULL,
                failing_rows         INTEGER NOT NULL DEFAULT 0,
                penalty_pct          REAL NOT NULL,
                applied_at           TEXT NOT NULL,
                original_detected_at TEXT
            );

            CREATE TABLE IF NOT EXISTS dq_institution_contacts (
                le_book        TEXT PRIMARY KEY,
                contact_email  TEXT,
                contact_name   TEXT,
                updated_at     TEXT
            );
        """)
        _migrate_schema(con)
        con.commit()
    finally:
        con.close()


def _migrate_schema(con: sqlite3.Connection) -> None:
    """Add columns introduced after initial deploy (idempotent)."""
    existing = {row[1] for row in
                con.execute("PRAGMA table_info(dq_open_issues)").fetchall()}
    additions = {
        "rule_name":          "TEXT",
        "last_failing_rows":  "INTEGER",
        "resolution_run_id":  "TEXT",
        "recurrence_count":   "INTEGER NOT NULL DEFAULT 0",
    }
    for col, defn in additions.items():
        if col not in existing:
            con.execute(f"ALTER TABLE dq_open_issues ADD COLUMN {col} {defn}")


def upsert_issue(con: sqlite3.Connection, le_book: str, inst_name: str,
                 table: str, rule_id: str, dimension: str,
                 failing_rows: int, run_date: str, rule_name: str) -> None:
    iid = issue_id(le_book, table, rule_id)

    # currently open/penalized → just refresh count
    open_row = con.execute(
        "SELECT issue_id, detected_at FROM dq_open_issues "
        "WHERE issue_id=? AND status IN ('open','penalized')",
        (iid,),
    ).fetchone()
    if open_row:
        band = urgency_band(open_row["detected_at"])
        con.execute("""
            UPDATE dq_open_issues
               SET failing_rows=?, last_failing_rows=failing_rows,
                   urgency_band=?, institution_name=?, rule_name=?
             WHERE issue_id=?
        """, (failing_rows, band, inst_name, rule_name, iid))
        return

    # recently resolved (≤30 days) → reopen with incremented recurrence
    resolved_row = con.execute("""
        SELECT issue_id, recurrence_count FROM dq_open_issues
        WHERE issue_id=? AND status='resolved'
          AND resolved_at >= date(?, '-30 days')
        ORDER BY resolved_at DESC LIMIT 1
    """, (iid, run_date)).fetchone()
    if resolved_row:
        new_recurrence = (resolved_row["recurrence_count"] or 0) + 1
        deadline = (date.fromisoformat(run_date) + timedelta(days=SLA_DAYS)).isoformat()
        con.execute("""
            UPDATE dq_open_issues
               SET status='open', failing_rows=?, last_failing_rows=NULL,
                   detected_at=?, sla_deadline=?, urgency_band='new',
                   resolved_at=NULL, resolution_run_id=NULL,
                   recurrence_count=?, institution_name=?, rule_name=?
             WHERE issue_id=?
        """, (failing_rows, run_date, deadline, new_recurrence, inst_name, rule_name, iid))
        log.warning("  REOPENED   %s  %s / %s  (recurrence #%d, %d rows)",
                    le_book, table, rule_id, new_recurrence, failing_rows)
        return

    # new issue
    deadline = (date.fromisoformat(run_date) + timedelta(days=SLA_DAYS)).isoformat()
    con.execute("""
        INSERT OR REPLACE INTO dq_open_issues
            (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
             dimension, failing_rows, detected_at, sla_deadline,
             urgency_band, status, recurrence_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,'new','open',0)
    """, (iid, le_book, inst_name, table, rule_id, rule_name,
          dimension, failing_rows, run_date, deadline))
    log.info("  NEW ISSUE  %s  %s / %s  (%d rows)", le_book, table, rule_id, failing_rows)


def maybe_resolve(con: sqlite3.Connection, le_book: str, table: str, rule_id: str) -> None:
    """Pipeline 1 watermark-based tentative resolve — Pipeline 2 confirms."""
    iid = issue_id(le_book, table, rule_id)
    row = con.execute(
        "SELECT issue_id FROM dq_open_issues "
        "WHERE issue_id=? AND status IN ('open','penalized')",
        (iid,),
    ).fetchone()
    if row:
        # Mark as pending_resolution so Pipeline 2 does the full-scan confirmation.
        con.execute(
            "UPDATE dq_open_issues SET status='pending_resolution' WHERE issue_id=?",
            (iid,),
        )
        log.info("  PENDING    %s  %s / %s  (awaiting full-scan confirmation)", le_book, table, rule_id)


# ── public resolution API (called by issues/resolution.py) ────────────────────

def resolve_issue(issue_id_: str, run_id: str) -> None:
    """Mark one issue as resolved after a full-scan confirms zero failing rows."""
    ensure_tables()
    today = date.today().isoformat()
    con   = get_connection()
    try:
        con.execute("""
            UPDATE dq_open_issues
               SET status='resolved', resolved_at=?, resolution_run_id=?
             WHERE issue_id=? AND status IN ('open','penalized','pending_resolution')
        """, (today, run_id, issue_id_))
        con.commit()
    finally:
        con.close()


def mark_pending_resolution(issue_id_: str) -> None:
    """First clean full-scan → park an open issue in pending_resolution (debounce).
    A second consecutive clean scan resolves it; any failing scan flips it back to
    open. Guards against a transient 0 (e.g. reading the warehouse mid-reload)
    permanently resolving a still-failing issue."""
    ensure_tables()
    con = get_connection()
    try:
        con.execute(
            "UPDATE dq_open_issues SET status='pending_resolution' "
            "WHERE issue_id=? AND status IN ('open','penalized')",
            (issue_id_,),
        )
        con.commit()
    finally:
        con.close()


def reopen_issue(issue_id_: str) -> None:
    """Revert a pending_resolution back to open (full-scan still failing)."""
    ensure_tables()
    con = get_connection()
    try:
        con.execute("""
            UPDATE dq_open_issues
               SET status='open'
             WHERE issue_id=? AND status='pending_resolution'
        """, (issue_id_,))
        con.commit()
    finally:
        con.close()


def update_issue_count(issue_id_: str, new_failing_rows: int) -> None:
    """Update the failing row count after a re-scan (partial fix or unchanged)."""
    ensure_tables()
    con = get_connection()
    try:
        con.execute("""
            UPDATE dq_open_issues
               SET last_failing_rows = failing_rows,
                   failing_rows      = ?,
                   status            = CASE WHEN status='pending_resolution'
                                            THEN 'open' ELSE status END
             WHERE issue_id=?
        """, (new_failing_rows, issue_id_))
        con.commit()
    finally:
        con.close()


def get_pending_resolution() -> list[dict]:
    """Return all issues in pending_resolution state (awaiting full-scan)."""
    ensure_tables()
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT * FROM dq_open_issues WHERE status='pending_resolution'"
            " ORDER BY table_name, le_book"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_issue_by_id(issue_id_: str) -> dict | None:
    """Fetch one issue by primary key, any status."""
    ensure_tables()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT * FROM dq_open_issues WHERE issue_id=?", (issue_id_,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def count_open() -> int:
    con = get_connection()
    try:
        return con.execute(f"SELECT COUNT(*) FROM dq_open_issues WHERE status IN {OPEN_SQL}").fetchone()[0]
    finally:
        con.close()


def get_open_issues(le_book: str | None = None,
                    include_pending: bool = True) -> list[dict]:
    """
    Return actionable issues: open + (optionally) pending_resolution.
    pending_resolution issues are included by default so callers (CR form,
    emails, alerts) see the full picture — they are flagged with
    status='pending_resolution' so the UI can render them differently.
    """
    ensure_tables()
    statuses = "('open','penalized','pending_resolution')" if include_pending \
               else "('open','penalized')"
    con = get_connection()
    try:
        if le_book:
            rows = con.execute(
                f"SELECT * FROM dq_open_issues"
                f" WHERE status IN {statuses} AND le_book=?"
                f" ORDER BY sla_deadline",
                (le_book,),
            ).fetchall()
        else:
            rows = con.execute(
                f"SELECT * FROM dq_open_issues"
                f" WHERE status IN {statuses}"
                f" ORDER BY sla_deadline"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_issues(status: str | None = None, le_book: str | None = None) -> list[dict]:
    """Return issues filtered by status ('open','penalized','resolved') and/or le_book."""
    ensure_tables()
    con = get_connection()
    try:
        clauses, params = [], []
        if status == "open":
            # "open" means the whole unresolved bucket (incl. pending_resolution)
            clauses.append(f"status IN {OPEN_SQL}")
        elif status:
            clauses.append("status=?");  params.append(status)
        if le_book:
            clauses.append("le_book=?"); params.append(le_book)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows  = con.execute(
            f"SELECT * FROM dq_open_issues {where} ORDER BY sla_deadline", params
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_contact(le_book: str) -> dict:
    ensure_tables()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT * FROM dq_institution_contacts WHERE le_book=?", (le_book,)
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def set_contact(le_book: str, email: str, name: str = "") -> None:
    ensure_tables()
    con = get_connection()
    try:
        con.execute("""
            INSERT INTO dq_institution_contacts (le_book, contact_email, contact_name, updated_at)
            VALUES (?,?,?,date('now'))
            ON CONFLICT(le_book) DO UPDATE SET
                contact_email=excluded.contact_email,
                contact_name=excluded.contact_name,
                updated_at=excluded.updated_at
        """, (le_book, email, name))
        con.commit()
    finally:
        con.close()


def set_assigned_to(issue_id_: str, email: str) -> None:
    ensure_tables()
    con = get_connection()
    try:
        con.execute("UPDATE dq_open_issues SET assigned_to=? WHERE issue_id=?", (email, issue_id_))
        con.commit()
    finally:
        con.close()


# ── penalty sweep — already disabled in the source this was ported from,
# kept commented out rather than deleted (not newly dead, pre-existing) ──────

# PENALTY_PCT = ...  # was never defined in the source either — apply_penalties
# referenced a PENALTY_PCT that doesn't exist anywhere in the codebase.

# def apply_penalties(run_date: str) -> int:
#     """
#     Sweep open issues past their SLA deadline → mark penalized + write dq_penalties.
#     Returns count of newly penalised issues.
#     """
#     ensure_tables()
#     con = get_connection()
#     penalised = 0
#     try:
#         breached = con.execute("""
#             SELECT * FROM dq_open_issues
#             WHERE status='open' AND sla_deadline < ?
#         """, (run_date,)).fetchall()
#
#         for row in breached:
#             period     = row["detected_at"][:7]   # YYYY-MM
#             penalty_id = issue_id(row["le_book"], row["table_name"], row["rule_id"]) + "_pen"
#
#             con.execute("""
#                 INSERT OR REPLACE INTO dq_penalties
#                     (penalty_id, le_book, institution_name, dimension, table_name, rule_id,
#                      period, failing_rows, penalty_pct, applied_at, original_detected_at)
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?)
#             """, (penalty_id, row["le_book"], row["institution_name"],
#                   row["dimension"], row["table_name"], row["rule_id"],
#                   period, row["failing_rows"], PENALTY_PCT, run_date, row["detected_at"]))
#
#             con.execute(
#                 "UPDATE dq_open_issues SET status='penalized' WHERE issue_id=?",
#                 (row["issue_id"],)
#             )
#             penalised += 1
#             log.warning("  PENALIZED  %s  %s / %s — SLA breached (deadline %s)",
#                         row["le_book"], row["table_name"], row["rule_id"], row["sla_deadline"])
#
#         con.commit()
#     finally:
#         con.close()
#
#     if penalised:
#         log.warning("Penalties applied: %d issue(s) past SLA", penalised)
#     return penalised


# def get_penalties(le_book: str | None = None) -> list[dict]:
#     ensure_tables()
#     con = get_connection()
#     try:
#         if le_book:
#             rows = con.execute(
#                 "SELECT * FROM dq_penalties WHERE le_book=? ORDER BY applied_at DESC", (le_book,)
#             ).fetchall()
#         else:
#             rows = con.execute(
#                 "SELECT * FROM dq_penalties ORDER BY applied_at DESC"
#             ).fetchall()
#         return [dict(r) for r in rows]
#     finally:
#         con.close()
