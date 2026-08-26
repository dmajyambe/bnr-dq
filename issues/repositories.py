# Raw CRUD against dq_open_issues / dq_resolved_issues / dq_institution_contacts.
# No urgency-band computation or rule-metadata lookups here — that's
# issues/tracker.py. This module is dumb storage access only.
from __future__ import annotations

import logging
from datetime import date, timedelta

from issues.state_machine import urgency_band, issue_id, OPEN_SQL, SLA_DAYS
from storage.postgres.app_db import get_connection

log = logging.getLogger("issues.repositories")


def ensure_tables() -> None:
    """Create dq_open_issues, dq_resolved_issues, dq_institution_contacts if absent."""
    from storage.postgres.init_tables import init_all
    init_all()


def upsert_issue(con, le_book: str, inst_name: str,
                 table: str, rule_id: str, dimension: str,
                 failing_rows: int, run_date: str, rule_name: str) -> None:
    iid = issue_id(le_book, table, rule_id)

    # currently open → just refresh count
    open_row = con.execute(
        "SELECT issue_id, detected_at, original_failing_rows FROM dq_open_issues "
        "WHERE issue_id=%s AND status IN ('open')",
        (iid,),
    ).fetchone()
    if open_row:
        band = urgency_band(open_row["detected_at"])
        # set original_failing_rows once if not yet recorded
        if not open_row["original_failing_rows"]:
            con.execute("""
                UPDATE dq_open_issues
                   SET failing_rows=%s, last_failing_rows=failing_rows,
                       urgency_band=%s, institution_name=%s, rule_name=%s,
                       original_failing_rows=%s
                 WHERE issue_id=%s
            """, (failing_rows, band, inst_name, rule_name, failing_rows, iid))
        else:
            con.execute("""
                UPDATE dq_open_issues
                   SET failing_rows=%s, last_failing_rows=failing_rows,
                       urgency_band=%s, institution_name=%s, rule_name=%s
                 WHERE issue_id=%s
            """, (failing_rows, band, inst_name, rule_name, iid))
        # log scan history for progress tracking
        con.execute(
            "INSERT INTO dq_issue_progress (issue_id, scan_date, failing_rows) "
            "SELECT %s,%s,%s WHERE NOT EXISTS "
            "(SELECT 1 FROM dq_issue_progress WHERE issue_id=%s AND scan_date=%s)",
            (iid, run_date, failing_rows, iid, run_date),
        )
        return

    # recently resolved (≤30 days) → reopen with incremented recurrence
    resolved_row = con.execute("""
        SELECT issue_id, recurrence_count FROM dq_resolved_issues
        WHERE issue_id=%s
          AND resolved_at::DATE >= %s::DATE - INTERVAL '30 days'
        ORDER BY resolved_at DESC LIMIT 1
    """, (iid, run_date)).fetchone()
    if resolved_row:
        new_recurrence = (resolved_row["recurrence_count"] or 0) + 1
        deadline = (date.fromisoformat(run_date) + timedelta(days=SLA_DAYS)).isoformat()
        updated = con.execute("""
            UPDATE dq_open_issues SET
                status='open', failing_rows=%s,
                original_failing_rows=%s, last_failing_rows=NULL,
                detected_at=%s, sla_deadline=%s, urgency_band='new',
                resolution_run_id=NULL, recurrence_count=%s,
                institution_name=%s, rule_name=%s
            WHERE issue_id=%s
        """, (failing_rows, failing_rows, run_date, deadline, new_recurrence,
              inst_name, rule_name, iid)).rowcount
        if not updated:
            con.execute("""
                INSERT INTO dq_open_issues
                    (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                     dimension, failing_rows, original_failing_rows, last_failing_rows,
                     detected_at, sla_deadline, urgency_band, status, recurrence_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,'new','open',%s)
            """, (iid, le_book, inst_name, table, rule_id, rule_name,
                  dimension, failing_rows, failing_rows, run_date, deadline, new_recurrence))
        log.warning("  REOPENED   %s  %s / %s  (recurrence #%d, %d rows)",
                    le_book, table, rule_id, new_recurrence, failing_rows)
        # Clear stale original evidence so the next ZIP export re-seeds it
        # with rows from this recurrence cycle, not the first-ever detection.
        try:
            from storage.evidence_store import clear_original
            clear_original(le_book, rule_id, table)
        except Exception as _exc:
            log.warning("Could not clear evidence on reopen %s/%s/%s: %s",
                        le_book, rule_id, table, _exc)
        return

    # new issue
    deadline = (date.fromisoformat(run_date) + timedelta(days=SLA_DAYS)).isoformat()
    updated = con.execute("""
        UPDATE dq_open_issues SET
            le_book=%s, institution_name=%s, table_name=%s, rule_id=%s,
            rule_name=%s, dimension=%s, failing_rows=%s,
            original_failing_rows=COALESCE(original_failing_rows, %s),
            detected_at=%s, sla_deadline=%s, urgency_band='new',
            status='open', recurrence_count=0
        WHERE issue_id=%s
    """, (le_book, inst_name, table, rule_id, rule_name, dimension,
          failing_rows, failing_rows, run_date, deadline, iid)).rowcount
    if not updated:
        con.execute("""
            INSERT INTO dq_open_issues
                (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                 dimension, failing_rows, original_failing_rows, detected_at, sla_deadline,
                 urgency_band, status, recurrence_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'new','open',0)
        """, (iid, le_book, inst_name, table, rule_id, rule_name,
              dimension, failing_rows, failing_rows, run_date, deadline))
    # log first scan for progress tracking
    con.execute(
        "INSERT INTO dq_issue_progress (issue_id, scan_date, failing_rows) "
        "SELECT %s,%s,%s WHERE NOT EXISTS "
        "(SELECT 1 FROM dq_issue_progress WHERE issue_id=%s AND scan_date=%s)",
        (iid, run_date, failing_rows, iid, run_date),
    )
    log.info("  NEW ISSUE  %s  %s / %s  (%d rows)", le_book, table, rule_id, failing_rows)


def maybe_resolve(con, le_book: str, table: str, rule_id: str) -> None:
    """Pipeline 1 watermark-based tentative resolve — Pipeline 2 confirms."""
    iid = issue_id(le_book, table, rule_id)
    row = con.execute(
        "SELECT issue_id FROM dq_open_issues "
        "WHERE issue_id=%s AND status IN ('open')",
        (iid,),
    ).fetchone()
    if row:
        con.execute(
            "UPDATE dq_open_issues SET status='pending_resolution' WHERE issue_id=%s",
            (iid,),
        )
        log.info("  PENDING    %s  %s / %s  (awaiting full-scan confirmation)", le_book, table, rule_id)


# ── public resolution API (called by issues/resolution.py) ───────────────────

def resolve_issue(issue_id_: str, run_id: str) -> None:
    """Move one issue from dq_open_issues to dq_resolved_issues after a clean full-scan."""
    ensure_tables()
    today = date.today().isoformat()
    con   = get_connection()
    try:
        row = con.execute(
            "SELECT * FROM dq_open_issues "
            "WHERE issue_id=%s AND status IN ('open','pending_resolution')",
            (issue_id_,),
        ).fetchone()
        if not row:
            return
        deadline = row["sla_deadline"] or ""
        on_time  = (today <= deadline) if deadline else None
        con.execute("DELETE FROM dq_resolved_issues WHERE issue_id=%s", (row["issue_id"],))
        con.execute("""
            INSERT INTO dq_resolved_issues
                (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                 dimension, failing_rows, last_failing_rows, detected_at, sla_deadline,
                 resolved_at, resolution_run_id, recurrence_count, on_time, resolution_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'fixed')
        """, (
            row["issue_id"], row["le_book"], row["institution_name"],
            row["table_name"], row["rule_id"], row["rule_name"],
            row["dimension"], 0, row["failing_rows"],
            row["detected_at"], row["sla_deadline"],
            today, run_id, row["recurrence_count"], on_time,
        ))
        con.execute("DELETE FROM dq_open_issues WHERE issue_id=%s", (issue_id_,))
        con.commit()
    finally:
        con.close()


def mark_rule_removed(issue_id_: str, run_id: str) -> None:
    """Rule no longer exists — retire the issue without crediting a fix."""
    ensure_tables()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT * FROM dq_open_issues WHERE issue_id=%s AND status NOT IN ('rule_removed')",
            (issue_id_,),
        ).fetchone()
        if not row:
            return
        today = date.today().isoformat()
        already = con.execute(
            "SELECT 1 FROM dq_resolved_issues WHERE issue_id=%s", (row["issue_id"],)
        ).fetchone()
        if not already:
            con.execute("""
            INSERT INTO dq_resolved_issues
                (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                 dimension, failing_rows, last_failing_rows, detected_at, sla_deadline,
                 resolved_at, resolution_run_id, recurrence_count, on_time, resolution_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,'rule_removed')
        """, (
            row["issue_id"], row["le_book"], row["institution_name"],
            row["table_name"], row["rule_id"], row["rule_name"],
            row["dimension"], row["failing_rows"], row["last_failing_rows"],
            row["detected_at"], row["sla_deadline"],
            today, run_id, row["recurrence_count"],
        ))
        con.execute("DELETE FROM dq_open_issues WHERE issue_id=%s", (issue_id_,))
        con.commit()
    finally:
        con.close()


def mark_pending_resolution(issue_id_: str) -> None:
    """First clean full-scan → park an open issue in pending_resolution (debounce)."""
    ensure_tables()
    con = get_connection()
    try:
        con.execute(
            "UPDATE dq_open_issues SET status='pending_resolution' "
            "WHERE issue_id=%s AND status IN ('open')",
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
             WHERE issue_id=%s AND status='pending_resolution'
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
                   failing_rows      = %s,
                   status            = CASE WHEN status='pending_resolution'
                                            THEN 'open' ELSE status END
             WHERE issue_id=%s
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
    """Fetch one issue by primary key — checks open then resolved."""
    ensure_tables()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT *, 'open' AS status FROM dq_open_issues WHERE issue_id=%s",
            (issue_id_,),
        ).fetchone()
        if row:
            return dict(row)
        row = con.execute(
            "SELECT *, 'resolved' AS status FROM dq_resolved_issues WHERE issue_id=%s",
            (issue_id_,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def count_open() -> int:
    con = get_connection()
    try:
        return con.execute(
            f"SELECT COUNT(*) AS n FROM dq_open_issues WHERE status IN {OPEN_SQL}"
        ).fetchone()["n"]
    finally:
        con.close()


def get_open_issues(le_book: str | None = None,
                    include_pending: bool = True) -> list[dict]:
    """Return actionable issues: open + (optionally) pending_resolution."""
    ensure_tables()
    statuses = "('open','pending_resolution')" if include_pending \
               else "('open')"
    con = get_connection()
    try:
        if le_book:
            rows = con.execute(
                f"SELECT * FROM dq_open_issues"
                f" WHERE status IN {statuses} AND le_book=%s"
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
    """Return issues filtered by status and/or le_book.

    status='open'     → active bucket (open + pending_resolution) from dq_open_issues
    status='resolved' → from dq_resolved_issues
    status=None       → union of both tables
    """
    ensure_tables()
    con = get_connection()
    try:
        if status == "open":
            clauses, params = [f"status IN {OPEN_SQL}"], []
            if le_book:
                clauses.append("le_book=%s")
                params.append(le_book)
            where = "WHERE " + " AND ".join(clauses)
            rows = con.execute(
                f"SELECT * FROM dq_open_issues {where} ORDER BY sla_deadline", params
            ).fetchall()
            return [dict(r) for r in rows]

        if status == "resolved":
            if le_book:
                rows = con.execute(
                    "SELECT *, 'resolved' AS status FROM dq_resolved_issues "
                    "WHERE le_book=%s ORDER BY resolved_at DESC",
                    (le_book,),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT *, 'resolved' AS status FROM dq_resolved_issues "
                    "ORDER BY resolved_at DESC"
                ).fetchall()
            return [dict(r) for r in rows]

        if status:
            # explicit non-open/non-resolved status
            clauses, params = ["status=%s"], [status]
            if le_book:
                clauses.append("le_book=%s")
                params.append(le_book)
            where = "WHERE " + " AND ".join(clauses)
            rows = con.execute(
                f"SELECT * FROM dq_open_issues {where} ORDER BY sla_deadline", params
            ).fetchall()
            return [dict(r) for r in rows]

        # no status filter → union open + resolved
        lb_clause = "WHERE le_book=%s" if le_book else ""
        params = [le_book] if le_book else []
        open_sql = f"SELECT *, status FROM dq_open_issues {lb_clause} ORDER BY sla_deadline"
        res_sql  = (f"SELECT *, 'resolved' AS status FROM dq_resolved_issues "
                    f"{lb_clause} ORDER BY resolved_at DESC")
        open_rows = con.execute(open_sql, params).fetchall()
        res_rows  = con.execute(res_sql,  params).fetchall()
        return [dict(r) for r in open_rows] + [dict(r) for r in res_rows]
    finally:
        con.close()


def get_resolved_issues(le_book: str | None = None,
                        month: str | None = None) -> list[dict]:
    """Return resolved issues, optionally scoped to institution and/or YYYY-MM month."""
    ensure_tables()
    con = get_connection()
    try:
        clauses, params = [], []
        if le_book:
            clauses.append("le_book=%s");    params.append(le_book)
        if month:
            clauses.append("resolved_at >= %s AND resolved_at < %s")
            y, m = (int(p) for p in month.split("-")[:2])
            ny, nm = y + (m == 12), m % 12 + 1
            params += [f"{month}-01", f"{ny:04d}-{nm:02d}-01"]
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = con.execute(
            f"SELECT *, 'resolved' AS status FROM dq_resolved_issues "
            f"{where} ORDER BY resolved_at DESC",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_contact(le_book: str) -> dict:
    ensure_tables()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT * FROM dq_institution_contacts WHERE le_book=%s", (le_book,)
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def set_contact(le_book: str, email: str, name: str = "") -> None:
    ensure_tables()
    con = get_connection()
    try:
        updated = con.execute("""
            UPDATE dq_institution_contacts SET
                contact_email=%s, contact_name=%s, updated_at=CURRENT_DATE
            WHERE le_book=%s
        """, (email, name, le_book)).rowcount
        if not updated:
            con.execute("""
                INSERT INTO dq_institution_contacts (le_book, contact_email, contact_name, updated_at)
                VALUES (%s,%s,%s,CURRENT_DATE)
            """, (le_book, email, name))
        con.commit()
    finally:
        con.close()


def set_assigned_to(issue_id_: str, email: str) -> None:
    ensure_tables()
    con = get_connection()
    try:
        con.execute("UPDATE dq_open_issues SET assigned_to=%s WHERE issue_id=%s", (email, issue_id_))
        con.commit()
    finally:
        con.close()
