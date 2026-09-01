# CRUD against dq_open_issues / dq_resolved_issues / dq_institution_contacts.
from __future__ import annotations

from datetime import date, timedelta
import logging
from issues.state_machine import OPEN_SQL, SLA_DAYS, issue_id, urgency_band
from sqlalchemy import text
from storage.postgres.connection import get_engine

log = logging.getLogger("issues.repositories")

#Call init_all to create dq_open_issues, dq_resolved_issues, dq_institution_contacts if absent.
def ensure_tables() -> None:
    from storage.postgres.init_tables import init_all
    init_all()


def upsert_issue(
    con,
    le_book: str,
    inst_name: str,
    table: str,
    rule_id: str,
    dimension: str,
    failing_rows: int,
    run_date: str,
    rule_name: str,
) -> None:
    iid = issue_id(le_book, table, rule_id)

    # currently open: just refresh count
    open_row = (
        con.execute(
            text(
                "SELECT issue_id, detected_at, original_failing_rows FROM dq_open_issues "
                "WHERE issue_id=:iid AND status IN ('open')"
            ),
            {"iid": iid},
        )
        .mappings()
        .fetchone()
    )
    if open_row:
        band = urgency_band(open_row["detected_at"])
        # set original_failing_rows once if not yet recorded
        if not open_row["original_failing_rows"]:
            con.execute(
                text("""
                UPDATE dq_open_issues
                   SET failing_rows=:failing_rows, last_failing_rows=failing_rows,
                       urgency_band=:band, institution_name=:inst_name, rule_name=:rule_name,
                       original_failing_rows=:original_failing_rows
                 WHERE issue_id=:iid
            """),
                {
                    "failing_rows": failing_rows,
                    "band": band,
                    "inst_name": inst_name,
                    "rule_name": rule_name,
                    "original_failing_rows": failing_rows,
                    "iid": iid,
                },
            )
        else:
            con.execute(
                text("""
                UPDATE dq_open_issues
                   SET failing_rows=:failing_rows, last_failing_rows=failing_rows,
                       urgency_band=:band, institution_name=:inst_name, rule_name=:rule_name
                 WHERE issue_id=:iid
            """),
                {
                    "failing_rows": failing_rows,
                    "band": band,
                    "inst_name": inst_name,
                    "rule_name": rule_name,
                    "iid": iid,
                },
            )
        # log scan history for progress tracking
        con.execute(
            text(
                "INSERT INTO dq_issue_progress (issue_id, scan_date, failing_rows) "
                "SELECT :iid,:scan_date,:failing_rows WHERE NOT EXISTS "
                "(SELECT 1 FROM dq_issue_progress WHERE issue_id=:iid AND scan_date=:scan_date)"
            ),
            {"iid": iid, "scan_date": run_date, "failing_rows": failing_rows},
        )
        return

    # recently resolved (≤30 days) → reopen with incremented recurrence
    resolved_row = (
        con.execute(
            text("""
        SELECT issue_id, recurrence_count FROM dq_resolved_issues
        WHERE issue_id=:iid
          AND resolved_at::DATE >= :run_date::DATE - INTERVAL '30 days'
        ORDER BY resolved_at DESC LIMIT 1
    """),
            {"iid": iid, "run_date": run_date},
        )
        .mappings()
        .fetchone()
    )
    if resolved_row:
        new_recurrence = (resolved_row["recurrence_count"] or 0) + 1
        deadline = (
            date.fromisoformat(run_date) + timedelta(days=SLA_DAYS)
        ).isoformat()
        updated = con.execute(
            text("""
            UPDATE dq_open_issues SET
                status='open', failing_rows=:failing_rows,
                original_failing_rows=:original_failing_rows, last_failing_rows=NULL,
                detected_at=:run_date, sla_deadline=:deadline, urgency_band='new',
                resolution_run_id=NULL, recurrence_count=:new_recurrence,
                institution_name=:inst_name, rule_name=:rule_name
            WHERE issue_id=:iid
        """),
            {
                "failing_rows": failing_rows,
                "original_failing_rows": failing_rows,
                "run_date": run_date,
                "deadline": deadline,
                "new_recurrence": new_recurrence,
                "inst_name": inst_name,
                "rule_name": rule_name,
                "iid": iid,
            },
        ).rowcount
        if not updated:
            con.execute(
                text("""
                INSERT INTO dq_open_issues
                    (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                     dimension, failing_rows, original_failing_rows, last_failing_rows,
                     detected_at, sla_deadline, urgency_band, status, recurrence_count)
                VALUES (:iid,:le_book,:inst_name,:table,:rule_id,:rule_name,
                        :dimension,:failing_rows,:failing_rows,NULL,:run_date,:deadline,'new','open',:new_recurrence)
            """),
                {
                    "iid": iid,
                    "le_book": le_book,
                    "inst_name": inst_name,
                    "table": table,
                    "rule_id": rule_id,
                    "rule_name": rule_name,
                    "dimension": dimension,
                    "failing_rows": failing_rows,
                    "run_date": run_date,
                    "deadline": deadline,
                    "new_recurrence": new_recurrence,
                },
            )
        log.warning(
            "  REOPENED   %s  %s / %s  (recurrence #%d, %d rows)",
            le_book,
            table,
            rule_id,
            new_recurrence,
            failing_rows,
        )
        # Clear stale original evidence so the next ZIP export re-seeds it
        # with rows from this recurrence cycle, not the first-ever detection.
        try:
            from storage.evidence_store import clear_original

            clear_original(le_book, rule_id, table)
        except Exception as _exc:
            log.warning(
                "Could not clear evidence on reopen %s/%s/%s: %s",
                le_book,
                rule_id,
                table,
                _exc,
            )
        return

    # new issue
    deadline = (
        date.fromisoformat(run_date) + timedelta(days=SLA_DAYS)
    ).isoformat()
    updated = con.execute(
        text("""
        UPDATE dq_open_issues SET
            le_book=:le_book, institution_name=:inst_name, table_name=:table, rule_id=:rule_id,
            rule_name=:rule_name, dimension=:dimension, failing_rows=:failing_rows,
            original_failing_rows=COALESCE(original_failing_rows, :original_failing_rows),
            detected_at=:run_date, sla_deadline=:deadline, urgency_band='new',
            status='open', recurrence_count=0
        WHERE issue_id=:iid
    """),
        {
            "le_book": le_book,
            "inst_name": inst_name,
            "table": table,
            "rule_id": rule_id,
            "rule_name": rule_name,
            "dimension": dimension,
            "failing_rows": failing_rows,
            "original_failing_rows": failing_rows,
            "run_date": run_date,
            "deadline": deadline,
            "iid": iid,
        },
    ).rowcount
    if not updated:
        con.execute(
            text("""
            INSERT INTO dq_open_issues
                (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                 dimension, failing_rows, original_failing_rows, detected_at, sla_deadline,
                 urgency_band, status, recurrence_count)
            VALUES (:iid,:le_book,:inst_name,:table,:rule_id,:rule_name,
                    :dimension,:failing_rows,:failing_rows,:run_date,:deadline,'new','open',0)
        """),
            {
                "iid": iid,
                "le_book": le_book,
                "inst_name": inst_name,
                "table": table,
                "rule_id": rule_id,
                "rule_name": rule_name,
                "dimension": dimension,
                "failing_rows": failing_rows,
                "run_date": run_date,
                "deadline": deadline,
            },
        )
    # log first scan for progress tracking
    con.execute(
        text(
            "INSERT INTO dq_issue_progress (issue_id, scan_date, failing_rows) "
            "SELECT :iid,:scan_date,:failing_rows WHERE NOT EXISTS "
            "(SELECT 1 FROM dq_issue_progress WHERE issue_id=:iid AND scan_date=:scan_date)"
        ),
        {"iid": iid, "scan_date": run_date, "failing_rows": failing_rows},
    )
    log.info(
        "  NEW ISSUE  %s  %s / %s  (%d rows)",
        le_book,
        table,
        rule_id,
        failing_rows,
    )


def maybe_resolve(con, le_book: str, table: str, rule_id: str) -> None:
    """Pipeline 1 watermark-based tentative resolve — Pipeline 2 confirms."""
    iid = issue_id(le_book, table, rule_id)
    row = (
        con.execute(
            text(
                "SELECT issue_id FROM dq_open_issues "
                "WHERE issue_id=:iid AND status IN ('open')"
            ),
            {"iid": iid},
        )
        .mappings()
        .fetchone()
    )
    if row:
        con.execute(
            text(
                "UPDATE dq_open_issues SET status='pending_resolution' WHERE issue_id=:iid"
            ),
            {"iid": iid},
        )
        log.info(
            "  PENDING    %s  %s / %s  (awaiting full-scan confirmation)",
            le_book,
            table,
            rule_id,
        )


# ── public resolution API (called by issues/resolution.py) ───────────────────


def resolve_issue(issue_id_: str, run_id: str) -> None:
    """Move one issue from dq_open_issues to dq_resolved_issues after a clean full-scan."""
    ensure_tables()
    today = date.today().isoformat()
    with get_engine().begin() as con:
        row = (
            con.execute(
                text(
                    "SELECT * FROM dq_open_issues "
                    "WHERE issue_id=:iid AND status IN ('open','pending_resolution')"
                ),
                {"iid": issue_id_},
            )
            .mappings()
            .fetchone()
        )
        if not row:
            return
        deadline = row["sla_deadline"] or ""
        on_time = (today <= deadline) if deadline else None
        con.execute(
            text("DELETE FROM dq_resolved_issues WHERE issue_id=:iid"),
            {"iid": row["issue_id"]},
        )
        con.execute(
            text("""
            INSERT INTO dq_resolved_issues
                (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                 dimension, failing_rows, last_failing_rows, detected_at, sla_deadline,
                 resolved_at, resolution_run_id, recurrence_count, on_time, resolution_type)
            VALUES (:issue_id,:le_book,:institution_name,:table_name,:rule_id,:rule_name,
                    :dimension,:failing_rows,:last_failing_rows,:detected_at,:sla_deadline,
                    :resolved_at,:resolution_run_id,:recurrence_count,:on_time,'fixed')
        """),
            {
                "issue_id": row["issue_id"],
                "le_book": row["le_book"],
                "institution_name": row["institution_name"],
                "table_name": row["table_name"],
                "rule_id": row["rule_id"],
                "rule_name": row["rule_name"],
                "dimension": row["dimension"],
                "failing_rows": 0,
                "last_failing_rows": row["failing_rows"],
                "detected_at": row["detected_at"],
                "sla_deadline": row["sla_deadline"],
                "resolved_at": today,
                "resolution_run_id": run_id,
                "recurrence_count": row["recurrence_count"],
                "on_time": on_time,
            },
        )
        con.execute(
            text("DELETE FROM dq_open_issues WHERE issue_id=:iid"),
            {"iid": issue_id_},
        )


def mark_rule_removed(issue_id_: str, run_id: str) -> None:
    """Rule no longer exists — retire the issue without crediting a fix."""
    ensure_tables()
    with get_engine().begin() as con:
        row = (
            con.execute(
                text(
                    "SELECT * FROM dq_open_issues WHERE issue_id=:iid AND status NOT IN ('rule_removed')"
                ),
                {"iid": issue_id_},
            )
            .mappings()
            .fetchone()
        )
        if not row:
            return
        today = date.today().isoformat()
        already = (
            con.execute(
                text("SELECT 1 FROM dq_resolved_issues WHERE issue_id=:iid"),
                {"iid": row["issue_id"]},
            )
            .mappings()
            .fetchone()
        )
        if not already:
            con.execute(
                text("""
            INSERT INTO dq_resolved_issues
                (issue_id, le_book, institution_name, table_name, rule_id, rule_name,
                 dimension, failing_rows, last_failing_rows, detected_at, sla_deadline,
                 resolved_at, resolution_run_id, recurrence_count, on_time, resolution_type)
            VALUES (:issue_id,:le_book,:institution_name,:table_name,:rule_id,:rule_name,
                    :dimension,:failing_rows,:last_failing_rows,:detected_at,:sla_deadline,
                    :resolved_at,:resolution_run_id,:recurrence_count,NULL,'rule_removed')
        """),
                {
                    "issue_id": row["issue_id"],
                    "le_book": row["le_book"],
                    "institution_name": row["institution_name"],
                    "table_name": row["table_name"],
                    "rule_id": row["rule_id"],
                    "rule_name": row["rule_name"],
                    "dimension": row["dimension"],
                    "failing_rows": row["failing_rows"],
                    "last_failing_rows": row["last_failing_rows"],
                    "detected_at": row["detected_at"],
                    "sla_deadline": row["sla_deadline"],
                    "resolved_at": today,
                    "resolution_run_id": run_id,
                    "recurrence_count": row["recurrence_count"],
                },
            )
        con.execute(
            text("DELETE FROM dq_open_issues WHERE issue_id=:iid"),
            {"iid": issue_id_},
        )


def mark_pending_resolution(issue_id_: str) -> None:
    """First clean full-scan → park an open issue in pending_resolution (debounce)."""
    ensure_tables()
    with get_engine().begin() as con:
        con.execute(
            text(
                "UPDATE dq_open_issues SET status='pending_resolution' "
                "WHERE issue_id=:iid AND status IN ('open')"
            ),
            {"iid": issue_id_},
        )


def reopen_issue(issue_id_: str) -> None:
    """Revert a pending_resolution back to open (full-scan still failing)."""
    ensure_tables()
    with get_engine().begin() as con:
        con.execute(
            text("""
            UPDATE dq_open_issues
               SET status='open'
             WHERE issue_id=:iid AND status='pending_resolution'
        """),
            {"iid": issue_id_},
        )


def update_issue_count(issue_id_: str, new_failing_rows: int) -> None:
    """Update the failing row count after a re-scan (partial fix or unchanged)."""
    ensure_tables()
    with get_engine().begin() as con:
        con.execute(
            text("""
            UPDATE dq_open_issues
               SET last_failing_rows = failing_rows,
                   failing_rows      = :new_failing_rows,
                   status            = CASE WHEN status='pending_resolution'
                                            THEN 'open' ELSE status END
             WHERE issue_id=:iid
        """),
            {"new_failing_rows": new_failing_rows, "iid": issue_id_},
        )


def get_pending_resolution() -> list[dict]:
    """Return all issues in pending_resolution state (awaiting full-scan)."""
    ensure_tables()
    with get_engine().connect() as con:
        rows = (
            con.execute(
                text(
                    "SELECT * FROM dq_open_issues WHERE status='pending_resolution'"
                    " ORDER BY table_name, le_book"
                )
            )
            .mappings()
            .fetchall()
        )
        return [dict(r) for r in rows]


def get_issue_by_id(issue_id_: str) -> dict | None:
    """Fetch one issue by primary key — checks open then resolved."""
    ensure_tables()
    with get_engine().connect() as con:
        row = (
            con.execute(
                text(
                    "SELECT *, 'open' AS status FROM dq_open_issues WHERE issue_id=:iid"
                ),
                {"iid": issue_id_},
            )
            .mappings()
            .fetchone()
        )
        if row:
            return dict(row)
        row = (
            con.execute(
                text(
                    "SELECT *, 'resolved' AS status FROM dq_resolved_issues WHERE issue_id=:iid"
                ),
                {"iid": issue_id_},
            )
            .mappings()
            .fetchone()
        )
        return dict(row) if row else None


def count_open() -> int:
    with get_engine().connect() as con:
        return con.execute(
            text(
                f"SELECT COUNT(*) AS n FROM dq_open_issues WHERE status IN {OPEN_SQL}"
            )
        ).mappings().fetchone()["n"]


def get_open_issues(
    le_book: str | None = None, include_pending: bool = True
) -> list[dict]:
    """Return actionable issues: open + (optionally) pending_resolution."""
    ensure_tables()
    statuses = (
        "('open','pending_resolution')" if include_pending else "('open')"
    )
    with get_engine().connect() as con:
        if le_book:
            rows = (
                con.execute(
                    text(
                        f"SELECT * FROM dq_open_issues"
                        f" WHERE status IN {statuses} AND le_book=:lb"
                        f" ORDER BY sla_deadline"
                    ),
                    {"lb": le_book},
                )
                .mappings()
                .fetchall()
            )
        else:
            rows = (
                con.execute(
                    text(
                        f"SELECT * FROM dq_open_issues"
                        f" WHERE status IN {statuses}"
                        f" ORDER BY sla_deadline"
                    )
                )
                .mappings()
                .fetchall()
            )
        return [dict(r) for r in rows]


def get_issues(
    status: str | None = None, le_book: str | None = None
) -> list[dict]:
    """Return issues filtered by status and/or le_book.

    status='open'     → active bucket (open + pending_resolution) from dq_open_issues
    status='resolved' → from dq_resolved_issues
    status=None       → union of both tables
    """
    ensure_tables()
    with get_engine().connect() as con:
        if status == "open":
            if le_book:
                rows = (
                    con.execute(
                        text(
                            f"SELECT * FROM dq_open_issues WHERE status IN {OPEN_SQL} AND le_book=:lb ORDER BY sla_deadline"
                        ),
                        {"lb": le_book},
                    )
                    .mappings()
                    .fetchall()
                )
            else:
                rows = (
                    con.execute(
                        text(
                            f"SELECT * FROM dq_open_issues WHERE status IN {OPEN_SQL} ORDER BY sla_deadline"
                        )
                    )
                    .mappings()
                    .fetchall()
                )
            return [dict(r) for r in rows]

        if status == "resolved":
            if le_book:
                rows = (
                    con.execute(
                        text(
                            "SELECT *, 'resolved' AS status FROM dq_resolved_issues "
                            "WHERE le_book=:lb ORDER BY resolved_at DESC"
                        ),
                        {"lb": le_book},
                    )
                    .mappings()
                    .fetchall()
                )
            else:
                rows = (
                    con.execute(
                        text(
                            "SELECT *, 'resolved' AS status FROM dq_resolved_issues "
                            "ORDER BY resolved_at DESC"
                        )
                    )
                    .mappings()
                    .fetchall()
                )
            return [dict(r) for r in rows]

        if status:
            # explicit non-open/non-resolved status
            if le_book:
                rows = (
                    con.execute(
                        text(
                            "SELECT * FROM dq_open_issues WHERE status=:status AND le_book=:lb ORDER BY sla_deadline"
                        ),
                        {"status": status, "lb": le_book},
                    )
                    .mappings()
                    .fetchall()
                )
            else:
                rows = (
                    con.execute(
                        text(
                            "SELECT * FROM dq_open_issues WHERE status=:status ORDER BY sla_deadline"
                        ),
                        {"status": status},
                    )
                    .mappings()
                    .fetchall()
                )
            return [dict(r) for r in rows]

        # no status filter → union open + resolved
        if le_book:
            open_rows = (
                con.execute(
                    text(
                        "SELECT *, status FROM dq_open_issues WHERE le_book=:lb ORDER BY sla_deadline"
                    ),
                    {"lb": le_book},
                )
                .mappings()
                .fetchall()
            )
            res_rows = (
                con.execute(
                    text(
                        "SELECT *, 'resolved' AS status FROM dq_resolved_issues WHERE le_book=:lb ORDER BY resolved_at DESC"
                    ),
                    {"lb": le_book},
                )
                .mappings()
                .fetchall()
            )
        else:
            open_rows = con.execute(
                text(
                    "SELECT *, status FROM dq_open_issues ORDER BY sla_deadline"
                )
            ).mappings().fetchall()
            res_rows = con.execute(
                text(
                    "SELECT *, 'resolved' AS status FROM dq_resolved_issues ORDER BY resolved_at DESC"
                )
            ).mappings().fetchall()
        return [dict(r) for r in open_rows] + [dict(r) for r in res_rows]


def get_resolved_issues(
    le_book: str | None = None, month: str | None = None
) -> list[dict]:
    """Return resolved issues, optionally scoped to institution and/or YYYY-MM month."""
    ensure_tables()
    with get_engine().connect() as con:
        clauses, params = [], {}
        if le_book:
            clauses.append("le_book=:lb")
            params["lb"] = le_book
        if month:
            clauses.append(
                "resolved_at >= :month_start AND resolved_at < :month_end"
            )
            y, m = (int(p) for p in month.split("-")[:2])
            ny, nm = y + (m == 12), m % 12 + 1
            params["month_start"] = f"{month}-01"
            params["month_end"] = f"{ny:04d}-{nm:02d}-01"
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = (
            con.execute(
                text(
                    f"SELECT *, 'resolved' AS status FROM dq_resolved_issues "
                    f"{where} ORDER BY resolved_at DESC"
                ),
                params,
            )
            .mappings()
            .fetchall()
        )
        return [dict(r) for r in rows]


def get_contact(le_book: str) -> dict:
    ensure_tables()
    with get_engine().connect() as con:
        row = (
            con.execute(
                text(
                    "SELECT * FROM dq_institution_contacts WHERE le_book=:lb"
                ),
                {"lb": le_book},
            )
            .mappings()
            .fetchone()
        )
        return dict(row) if row else {}


def set_contact(le_book: str, email: str, name: str = "") -> None:
    ensure_tables()
    with get_engine().begin() as con:
        updated = con.execute(
            text("""
            UPDATE dq_institution_contacts SET
                contact_email=:email, contact_name=:name, updated_at=CURRENT_DATE
            WHERE le_book=:lb
        """),
            {"email": email, "name": name, "lb": le_book},
        ).rowcount
        if not updated:
            con.execute(
                text("""
                INSERT INTO dq_institution_contacts (le_book, contact_email, contact_name, updated_at)
                VALUES (:lb,:email,:name,CURRENT_DATE)
            """),
                {"lb": le_book, "email": email, "name": name},
            )


def set_assigned_to(issue_id_: str, email: str) -> None:
    ensure_tables()
    with get_engine().begin() as con:
        con.execute(
            text(
                "UPDATE dq_open_issues SET assigned_to=:email WHERE issue_id=:iid"
            ),
            {"email": email, "iid": issue_id_},
        )