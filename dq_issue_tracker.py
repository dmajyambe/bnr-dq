#tracks issue lifecycle: detection → open issue → (escalation) → resolution
#Issue definition: (le_book, table_name, rule_id) with failing_rows > 0 → open issue.
#urgency definition: days since detected_at, escalates if past sla_deadline (default 30 days).
#new: 1-3 days,attention: 4-15 days, urgent: 16-20 days, critical: 21-30 days, overdue: past SLA deadline( send notification  from "attention" onwards)

#imports
from __future__ import annotations
import hashlib
import logging
import os
import smtplib
import sqlite3
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
log = logging.getLogger("dq_issue_tracker")
#Path setup
SCRIPT_DIR  = Path(__file__).parent
DB_PATH     = SCRIPT_DIR / "dq_rules.db"
SLA_DAYS    = 30   # days before SLA warning escalates to overdue

# Urgency bands 
_URGENCY_STEPS = [(3, "new"), (15, "attention"), (20, "urgent"), (30, "critical")]
URGENCY_COLORS = {
    "new":       "#2563EB",
    "attention": "#D97706",
    "urgent":    "#EA580C",
    "critical":  "#DC2626",
    "overdue":   "#7C3D1E",
}

# Notification cadence per urgency band (minimum days between notification( email or dashboard alert) for the same issue_id)
_NOTIFY_INTERVAL = {"new": None, "attention": 7, "urgent": 3, "critical": 1, "overdue": 1}

# Completeness
_COMP_TABLE_RULE: dict[str, str] = {
    "customers_expanded":    "COMP-001",
    "accounts":              "COMP-002",
    "contracts_disburse":    "COMP-003",
    "contract_loans":        "COMP-004",
    "contract_schedules":    "COMP-005",
    "contracts_expanded":    "COMP-006",
    "loan_applications_2":   "COMP-007",
    "prev_loan_applications": "COMP-008",
}


# SQlite helpers
def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con

def ensure_tables() -> None:
    """Create dq_open_issues, dq_penalties, dq_institution_contacts if absent."""
    con = _conn()
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


# issue urgency helper functions

def _urgency_band(detected_at: str, sla_deadline: str | None = None) -> str:
    try:
        days = (date.today() - date.fromisoformat(detected_at)).days
    except Exception:
        return "critical"
    # Past SLA → overdue warning (no penalization, just escalated label)
    if sla_deadline:
        try:
            if date.today() > date.fromisoformat(sla_deadline):
                return "overdue"
        except Exception:
            pass
    for max_days, band in _URGENCY_STEPS:
        if days <= max_days:
            return band
    return "critical"


def _issue_id(le_book: str, table: str, rule_id: str) -> str:
    raw = f"{le_book}|{table}|{rule_id}"
    return hashlib.sha1(raw.encode()).hexdigest()


#issue upsert / resolve helpers

def _lookup_rule_name(rule_id: str) -> str:
    """Return the human-readable rule name from the registry, or the rule_id itself."""
    try:
        from dq_rules import (ACC_RULE_META, TIM_RULE_META, VAL_RULE_META,
                               COMP_RULE_META, REL_RULE_META)
        all_meta = {**COMP_RULE_META, **ACC_RULE_META, **TIM_RULE_META,
                    **VAL_RULE_META, **REL_RULE_META}
        return all_meta.get(rule_id, {}).get("name") or rule_id
    except Exception:
        return rule_id


def _upsert_issue(con: sqlite3.Connection, le_book: str, inst_name: str,
                  table: str, rule_id: str, dimension: str,
                  failing_rows: int, run_date: str) -> None:
    iid  = _issue_id(le_book, table, rule_id)
    name = _lookup_rule_name(rule_id)

    #currently open/penalized → just refresh count
    open_row = con.execute(
        "SELECT issue_id, detected_at FROM dq_open_issues "
        "WHERE issue_id=? AND status IN ('open','penalized')",
        (iid,),
    ).fetchone()
    if open_row:
        band = _urgency_band(open_row["detected_at"])
        con.execute("""
            UPDATE dq_open_issues
               SET failing_rows=?, last_failing_rows=failing_rows,
                   urgency_band=?, institution_name=?, rule_name=?
             WHERE issue_id=?
        """, (failing_rows, band, inst_name, name, iid))
        return

    #recently resolved (≤60 days) → reopen with incremented recurrence
    resolved_row = con.execute("""
        SELECT issue_id, recurrence_count FROM dq_open_issues
        WHERE issue_id=? AND status='resolved'
          AND resolved_at >= date(?, '-60 days')
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
        """, (failing_rows, run_date, deadline, new_recurrence, inst_name, name, iid))
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
    """, (iid, le_book, inst_name, table, rule_id, name,
          dimension, failing_rows, run_date, deadline))
    log.info("  NEW ISSUE  %s  %s / %s  (%d rows)", le_book, table, rule_id, failing_rows)


def _maybe_resolve(con: sqlite3.Connection, le_book: str, table: str,
                   rule_id: str, run_date: str) -> None:
    """Pipeline 1 watermark-based tentative resolve — Pipeline 2 confirms."""
    iid = _issue_id(le_book, table, rule_id)
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


# ── public resolution API (called by dq_resolution_pipeline.py) ───────────────

def resolve_issue(issue_id: str, run_id: str) -> None:
    """Mark one issue as resolved after a full-scan confirms zero failing rows."""
    ensure_tables()
    today = date.today().isoformat()
    con   = _conn()
    try:
        con.execute("""
            UPDATE dq_open_issues
               SET status='resolved', resolved_at=?, resolution_run_id=?
             WHERE issue_id=? AND status IN ('open','penalized','pending_resolution')
        """, (today, run_id, issue_id))
        con.commit()
    finally:
        con.close()


def reopen_issue(issue_id: str) -> None:
    """Revert a pending_resolution back to open (full-scan still failing)."""
    ensure_tables()
    con = _conn()
    try:
        con.execute("""
            UPDATE dq_open_issues
               SET status='open'
             WHERE issue_id=? AND status='pending_resolution'
        """, (issue_id,))
        con.commit()
    finally:
        con.close()


def update_issue_count(issue_id: str, new_failing_rows: int) -> None:
    """Update the failing row count after a re-scan (partial fix or unchanged)."""
    ensure_tables()
    con = _conn()
    try:
        con.execute("""
            UPDATE dq_open_issues
               SET last_failing_rows = failing_rows,
                   failing_rows      = ?,
                   status            = CASE WHEN status='pending_resolution'
                                            THEN 'open' ELSE status END
             WHERE issue_id=?
        """, (new_failing_rows, issue_id))
        con.commit()
    finally:
        con.close()


def get_pending_resolution() -> list[dict]:
    """Return all issues in pending_resolution state (awaiting full-scan)."""
    ensure_tables()
    con = _conn()
    try:
        rows = con.execute(
            "SELECT * FROM dq_open_issues WHERE status='pending_resolution'"
            " ORDER BY table_name, le_book"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_issue_by_id(issue_id: str) -> dict | None:
    """Fetch one issue by primary key, any status."""
    ensure_tables()
    con = _conn()
    try:
        row = con.execute(
            "SELECT * FROM dq_open_issues WHERE issue_id=?", (issue_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def sync_cr_failing_rows() -> None:
    """
    Recompute each active CR's failing_rows as the sum of its linked issues'
    current counts.  Called at the end of each detection run so the CR list
    always reflects the latest pipeline numbers.
    """
    import json as _json
    try:
        import dq_change_request as _cr
        active_crs = [c for c in _cr.get_crs()
                      if c["status"] not in ("closed",)]
        if not active_crs:
            return
        con = _conn()
        try:
            for cr in active_crs:
                iids = _json.loads(cr.get("issue_ids") or "[]")
                if not iids:
                    continue
                placeholders = ",".join("?" * len(iids))
                row = con.execute(
                    f"SELECT COALESCE(SUM(failing_rows),0) FROM dq_open_issues"
                    f" WHERE issue_id IN ({placeholders})", iids
                ).fetchone()
                total = int(row[0])
                con.execute(
                    "UPDATE dq_change_requests SET failing_rows=?, updated_at=?"
                    " WHERE cr_id=?",
                    (total, date.today().isoformat(), cr["cr_id"]),
                )
            con.commit()
        finally:
            con.close()
    except Exception as exc:
        log.warning("sync_cr_failing_rows failed: %s", exc)


# ── main detection logic ───────────────────────────────────────────────────────

def detect_and_update_issues(R: dict, categories: dict, run_date: str) -> None:
    """
    Parse engine results (R) and write/update dq_open_issues.

    Any rule with invalid > 0 for a given le_book immediately becomes an open issue.
    Issues where invalid == 0 are marked resolved.
    No score threshold — one failing row is enough.
    """
    ensure_tables()
    con = _conn()
    try:
        _process_completeness(con, R.get("comp") or {}, categories, run_date)
        _process_rule_dimension(con, R.get("acc") or {},  "accuracy",    "accuracy_score",   categories, run_date)
        _process_rule_dimension(con, R.get("val") or {},  "validity",    "validity_score",    categories, run_date)
        _process_rule_dimension(con, R.get("uni") or {},  "uniqueness",  "uniqueness_score",  categories, run_date)
        _process_relationship(con, R.get("rel") or {}, categories, run_date)
        _refresh_urgency_bands(con)
        con.commit()
    finally:
        con.close()

    total = _count_open()
    log.info("Issue tracker: %d open issue(s) after run %s", total, run_date)
    sync_cr_failing_rows()


def ingest_issues(issues: list[dict], run_date: str) -> None:
    """
    Accept a flat list of pre-detected issues from an external pipeline.

    Each dict must have: le_book, table, rule_id, dimension, failing_rows, score.
    Optional: institution_name.
    """
    ensure_tables()
    con = _conn()
    try:
        for item in issues:
            lb      = item["le_book"]
            table   = item["table"]
            rule_id = item["rule_id"]
            failing = int(item.get("failing_rows", 0))
            inst    = item.get("institution_name") or lb.title()

            if failing > 0:
                _upsert_issue(con, lb, inst, table, rule_id,
                              item["dimension"], failing, run_date)
            else:
                _maybe_resolve(con, lb, table, rule_id, run_date)
        _refresh_urgency_bands(con)
        con.commit()
    finally:
        con.close()

    log.info("ingest_issues: processed %d item(s) for %s", len(issues), run_date)


def _inst_name(lb: str, categories: dict) -> str:
    return (categories.get(lb, {}).get("name") or lb).title()


def _refresh_urgency_bands(con: sqlite3.Connection) -> None:
    """Recompute urgency_band for all open issues (picks up overdue once SLA passes)."""
    rows = con.execute(
        "SELECT issue_id, detected_at, sla_deadline FROM dq_open_issues WHERE status='open'"
    ).fetchall()
    for row in rows:
        band = _urgency_band(row["detected_at"], row["sla_deadline"])
        con.execute("UPDATE dq_open_issues SET urgency_band=? WHERE issue_id=?",
                    (band, row["issue_id"]))


def _process_completeness(con, report: dict, categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        rule_id = _COMP_TABLE_RULE.get(table)
        if not rule_id:
            continue
        for lb, lb_data in tdata.get("le_book_breakdown", {}).items():
            failing_rows = int(lb_data.get("null_cells") or 0)
            if failing_rows > 0:
                _upsert_issue(con, lb, _inst_name(lb, categories), table,
                              rule_id, "completeness", failing_rows, run_date)
            else:
                _maybe_resolve(con, lb, table, rule_id, run_date)


def _process_rule_dimension(con, report: dict, dimension: str, score_key: str,
                             categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        for lb, lb_data in tdata.get("le_book_breakdown", {}).items():
            inst  = _inst_name(lb, categories)
            rules = lb_data.get("rules", {})
            if not rules:
                failing = int(lb_data.get("invalid") or lb_data.get("null_cells") or 0)
                rule_id = f"{dimension[:3].upper()}-ALL"
                if failing > 0:
                    _upsert_issue(con, lb, inst, table, rule_id, dimension, failing, run_date)
                else:
                    _maybe_resolve(con, lb, table, rule_id, run_date)
                continue
            for rule_id, rule_data in rules.items():
                failing = int(rule_data.get("invalid") or 0)
                if failing > 0:
                    _upsert_issue(con, lb, inst, table, rule_id, dimension, failing, run_date)
                else:
                    _maybe_resolve(con, lb, table, rule_id, run_date)


def _process_relationship(con, report: dict, categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        for rule_id, rule_data in tdata.get("rules", {}).items():
            for lb, lb_data in rule_data.get("le_book_breakdown", {}).items():
                failing = int(lb_data.get("invalid") or 0)
                if failing > 0:
                    _upsert_issue(con, lb, _inst_name(lb, categories),
                                  table, rule_id, "relationship", failing, run_date)
                else:
                    _maybe_resolve(con, lb, table, rule_id, run_date)


# # ── penalty sweep ──────────────────────────────────────────────────────────────

# def apply_penalties(run_date: str) -> int:
#     """
#     Sweep open issues past their SLA deadline → mark penalized + write dq_penalties.
#     Returns count of newly penalised issues.
#     """
#     ensure_tables()
#     con = _conn()
#     penalised = 0
#     try:
#         breached = con.execute("""
#             SELECT * FROM dq_open_issues
#             WHERE status='open' AND sla_deadline < ?
#         """, (run_date,)).fetchall()

#         for row in breached:
#             period     = row["detected_at"][:7]   # YYYY-MM
#             penalty_id = _issue_id(row["le_book"], row["table_name"], row["rule_id"]) + "_pen"

#             con.execute("""
#                 INSERT OR REPLACE INTO dq_penalties
#                     (penalty_id, le_book, institution_name, dimension, table_name, rule_id,
#                      period, failing_rows, penalty_pct, applied_at, original_detected_at)
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?)
#             """, (penalty_id, row["le_book"], row["institution_name"],
#                   row["dimension"], row["table_name"], row["rule_id"],
#                   period, row["failing_rows"], PENALTY_PCT, run_date, row["detected_at"]))

#             con.execute(
#                 "UPDATE dq_open_issues SET status='penalized' WHERE issue_id=?",
#                 (row["issue_id"],)
#             )
#             penalised += 1
#             log.warning("  PENALIZED  %s  %s / %s — SLA breached (deadline %s)",
#                         row["le_book"], row["table_name"], row["rule_id"], row["sla_deadline"])

#         con.commit()
#     finally:
#         con.close()

#     if penalised:
#         log.warning("Penalties applied: %d issue(s) past SLA", penalised)
#     return penalised


# ── query helpers (used by dashboard) ─────────────────────────────────────────

def _count_open() -> int:
    con = _conn()
    try:
        return con.execute("SELECT COUNT(*) FROM dq_open_issues WHERE status='open'").fetchone()[0]
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
    con = _conn()
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
    con = _conn()
    try:
        clauses, params = [], []
        if status:
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


def get_institution_issue_summary() -> dict[str, dict]:
    """
    Return {le_book: {worst_urgency, total, new, attention, urgent, critical, overdue}}
    for all institutions with open issues.
    """
    ensure_tables()
    con = _conn()
    try:
        rows = con.execute(
            "SELECT le_book, detected_at, sla_deadline FROM dq_open_issues WHERE status='open'"
        ).fetchall()
    finally:
        con.close()

    today  = date.today()
    summary: dict[str, dict] = {}
    _order = ["new", "attention", "urgent", "critical", "overdue"]

    for row in rows:
        lb   = row["le_book"]
        band = _urgency_band(row["detected_at"], row["sla_deadline"])

        if lb not in summary:
            summary[lb] = {"total": 0, "new": 0, "attention": 0,
                           "urgent": 0, "critical": 0, "overdue": 0,
                           "worst_urgency": "new"}
        s = summary[lb]
        s["total"] += 1
        s[band]    += 1
        if _order.index(band) > _order.index(s["worst_urgency"]):
            s["worst_urgency"] = band

    return summary


def get_issues_by_table(
    status: str = "open",
    le_book: str | None = None,
    include_pending: bool = True,
) -> dict[str, list[dict]]:
    """
    Return open (+ pending_resolution) issues grouped by table → rule_id.

    inst_rec fields include: le_book, institution_name, failing_rows,
    urgency_band, detected_at, sla_deadline, days_left, issue_id,
    recurrence_count, rule_name, pending (bool — True if pending_resolution).
    """
    ensure_tables()
    statuses = (f"('{status}','pending_resolution')" if include_pending
                else f"('{status}')")
    con = _conn()
    try:
        clauses = [f"status IN {statuses}"]
        params: list = []
        if le_book:
            clauses.append("le_book=?")
            params.append(le_book)
        where = "WHERE " + " AND ".join(clauses)
        rows  = con.execute(
            f"SELECT * FROM dq_open_issues {where} ORDER BY sla_deadline", params
        ).fetchall()
    finally:
        con.close()

    today = date.today()

    try:
        from dq_rules import (ACC_RULE_META, TIM_RULE_META, VAL_RULE_META,
                               UNI_RULE_META, REL_RULE_META, COMP_RULE_META)
        _all_meta = {**COMP_RULE_META, **ACC_RULE_META, **TIM_RULE_META,
                     **VAL_RULE_META, **UNI_RULE_META, **REL_RULE_META}
    except Exception:
        _all_meta = {}

    _band_order = ["new", "attention", "urgent", "critical", "overdue"]

    table_rule: dict[str, dict[str, list]] = {}
    for row in rows:
        tbl  = row["table_name"]
        rid  = row["rule_id"]
        band = _urgency_band(row["detected_at"], row["sla_deadline"])
        try:
            days_left = (date.fromisoformat(row["sla_deadline"]) - today).days
        except Exception:
            days_left = 0

        # rule_name: stored on the row (from our schema), fall back to registry
        rname = (row["rule_name"] if row["rule_name"]
                 else _all_meta.get(rid, {}).get("name", rid))

        inst_rec = {
            "le_book":          row["le_book"],
            "institution_name": (row["institution_name"] or row["le_book"]).title(),
            "failing_rows":     row["failing_rows"],
            "urgency_band":     band,
            "detected_at":      row["detected_at"],
            "sla_deadline":     row["sla_deadline"],
            "days_left":        days_left,
            "issue_id":         row["issue_id"],
            "recurrence_count": int(row["recurrence_count"] or 0),
            "rule_name":        rname,
            "pending":          row["status"] == "pending_resolution",
        }
        table_rule.setdefault(tbl, {}).setdefault(rid, []).append(inst_rec)

    # Build final structure
    result: dict[str, list[dict]] = {}
    for tbl, rules in table_rule.items():
        rule_list = []
        for rid, inst_list in rules.items():
            meta       = _all_meta.get(rid, {})
            all_bands  = [i["urgency_band"] for i in inst_list]
            worst      = max(all_bands, key=lambda b: _band_order.index(b)
                             if b in _band_order else 0)
            days_vals  = [i["days_left"] for i in inst_list]
            rule_list.append({
                "rule_id":            rid,
                "rule_name":          inst_list[0].get("rule_name") or meta.get("name", rid),
                "dimension":          inst_list[0].get("dimension",
                                       rows[0]["dimension"] if rows else ""),
                "worst_urgency":      worst,
                "institution_count":  len(inst_list),
                "total_failing_rows": sum(i["failing_rows"] for i in inst_list),
                "min_sla_days":       min(days_vals),
                "sla_overdue":        any(i["urgency_band"] == "overdue" for i in inst_list),
                "any_pending":        any(i.get("pending") for i in inst_list),
                "institutions":       sorted(inst_list,
                                             key=lambda i: _band_order.index(i["urgency_band"])
                                             if i["urgency_band"] in _band_order else 0,
                                             reverse=True),
            })

        # Sort rules: worst urgency first, then most failing rows
        rule_list.sort(
            key=lambda r: (_band_order.index(r["worst_urgency"])
                           if r["worst_urgency"] in _band_order else 0,
                           r["total_failing_rows"]),
            reverse=True,
        )
        result[tbl] = rule_list

    # Sort tables: most rules first
    return dict(sorted(result.items(), key=lambda kv: len(kv[1]), reverse=True))


# def get_penalties(le_book: str | None = None) -> list[dict]:
#     ensure_tables()
#     con = _conn()
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


def get_contact(le_book: str) -> dict:
    ensure_tables()
    con = _conn()
    try:
        row = con.execute(
            "SELECT * FROM dq_institution_contacts WHERE le_book=?", (le_book,)
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def set_contact(le_book: str, email: str, name: str = "") -> None:
    ensure_tables()
    con = _conn()
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


def set_assigned_to(issue_id: str, email: str) -> None:
    ensure_tables()
    con = _conn()
    try:
        con.execute("UPDATE dq_open_issues SET assigned_to=? WHERE issue_id=?", (email, issue_id))
        con.commit()
    finally:
        con.close()


# ── email / notification ───────────────────────────────────────────────────────

def _smtp_configured() -> bool:
    return bool(os.environ.get("SMTP_HOST") and os.environ.get("SMTP_USER"))


def _send_email(to_addr: str, subject: str, body_text: str, body_html: str) -> None:
    host     = os.environ["SMTP_HOST"]
    port     = int(os.environ.get("SMTP_PORT", 587))
    user     = os.environ["SMTP_USER"]
    password = os.environ.get("SMTP_PASSWORD", "")
    from_addr = os.environ.get("SMTP_FROM", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP(host, port) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(user, password)
        smtp.sendmail(from_addr, [to_addr], msg.as_string())
    log.info("  Email sent → %s", to_addr)


def _build_email(inst_name: str, lb: str, issues: list[dict]) -> tuple[str, str, str]:
    """Return (subject, plain_text, html) for a notification email."""
    today     = date.today()
    worst     = max((_urgency_band(i["detected_at"]) for i in issues),
                    key=lambda b: ["new","attention","urgent","critical"].index(b))
    band_label = {"new": "New Issues", "attention": "Needs Attention",
                  "urgent": "URGENT", "critical": "CRITICAL — About to Breach"}[worst]

    subject = f"[BNR DQ Alert] {band_label} — {inst_name} (LE Book {lb})"

    lines = [
        f"Institution : {inst_name}  (LE Book: {lb})",
        f"Alert Level : {band_label}",
        f"Date        : {today.isoformat()}",
        "",
        f"You have {len(issues)} open data quality issue(s) requiring attention:",
        "",
    ]
    for i, iss in enumerate(issues, 1):
        days_left = (date.fromisoformat(iss["sla_deadline"]) - today).days
        urgency   = _urgency_band(iss["detected_at"])
        lines += [
            f"{i}. {iss['dimension'].upper()} — {iss['table_name']} ({iss['rule_id']})",
            f"   Failing rows : {iss['failing_rows']:,}",
            f"   Detected     : {iss['detected_at']}",
            f"   SLA deadline : {iss['sla_deadline']}  ({days_left} day(s) remaining)",
            f"   Urgency      : {urgency.upper()}",
            "",
        ]

    lines += [
        "─" * 60,
        "ACTION REQUIRED:",
        "Download the DQ Issue Report for this institution from the BNR",
        "Data Quality Dashboard to see the exact affected records per rule.",
        "The report lists every failing row under the same dimension and",
        "rule referenced above — it is your evidence document.",
        "",
        "Issues past their SLA deadline are flagged as OVERDUE and escalate",
        "to daily notifications until resolved.",
        "",
        "This is an automated notification from the BNR Data Quality",
        "Monitoring System. Do not reply to this message.",
    ]
    plain = "\n".join(lines)

    # HTML version
    rows_html = ""
    for iss in issues:
        days_left = (date.fromisoformat(iss["sla_deadline"]) - today).days
        color     = URGENCY_COLORS.get(_urgency_band(iss["detected_at"]), "#666")
        rows_html += f"""
        <tr>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">{iss['dimension'].title()}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">{iss['table_name']}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">{iss['rule_id']}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:right">{iss['failing_rows']:,}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">{iss['sla_deadline']}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee;color:{color};font-weight:700">{days_left}d left</td>
        </tr>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#1a1a2e;max-width:720px">
    <div style="background:#1A3A6B;padding:20px 32px">
      <h2 style="color:#fff;margin:0">BNR Data Quality Alert</h2>
      <p style="color:rgba(255,255,255,.7);margin:4px 0 0">{band_label}</p>
    </div>
    <div style="padding:24px 32px">
      <p><strong>Institution:</strong> {inst_name} &nbsp;|&nbsp; <strong>LE Book:</strong> {lb}</p>
      <p>You have <strong>{len(issues)}</strong> open data quality issue(s) requiring attention:</p>
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="background:#F4F6F9">
            <th style="padding:8px 10px;text-align:left">Dimension</th>
            <th style="padding:8px 10px;text-align:left">Table</th>
            <th style="padding:8px 10px;text-align:left">Rule</th>
            <th style="padding:8px 10px;text-align:right">Failing Rows</th>
            <th style="padding:8px 10px;text-align:left">Deadline</th>
            <th style="padding:8px 10px;text-align:left">Remaining</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      <div style="background:#FFF8E7;border-left:4px solid #D97706;padding:14px 18px;margin:20px 0">
        <strong>Action Required:</strong> Download the DQ Issue Report for this institution
        from the <strong>BNR Data Quality Dashboard</strong> to see the exact affected records.
        The report lists every failing row under the same dimensions and rules shown above —
        it is your official evidence document.
      </div>
      <p style="color:#DC2626;font-size:13px">
        Issues past their SLA deadline are flagged as <strong>OVERDUE</strong>
        and escalate to daily notifications until resolved.
      </p>
    </div>
    <div style="background:#F4F6F9;padding:14px 32px;font-size:11px;color:#6B7280">
      Automated notification — BNR Data Quality Monitoring System. Do not reply.
    </div>
    </body></html>"""

    return subject, plain, html


def send_notification(le_book: str, inst_name: str,
                      issues: list[dict], force: bool = False) -> bool:
    """
    Send a notification email for one institution.
    Respects per-urgency cadence unless force=True (manual Send Reminder).
    Returns True if email was sent.
    """
    if not issues:
        return False

    to_addr = None
    # 1. Prefer institution contact table
    contact = get_contact(le_book)
    if contact.get("contact_email"):
        to_addr = contact["contact_email"]
    # 2. Fall back to any assigned_to on the most urgent issue
    if not to_addr:
        for iss in issues:
            if iss.get("assigned_to"):
                to_addr = iss["assigned_to"]
                break

    if not to_addr:
        log.debug("  No contact email for %s — skipping notification", le_book)
        return False

    if not _smtp_configured():
        log.warning("SMTP not configured — cannot send notification for %s", le_book)
        return False

    # Decide whether to send based on cadence (unless forced)
    if not force:
        today = date.today()
        should_send = False
        for iss in issues:
            band     = _urgency_band(iss["detected_at"])
            interval = _NOTIFY_INTERVAL.get(band)
            if interval is None:
                continue   # 'new' — no auto-notify
            last = iss.get("notified_at")
            if not last or (today - date.fromisoformat(last)).days >= interval:
                should_send = True
                break
        if not should_send:
            return False

    subject, plain, html = _build_email(inst_name, le_book, issues)
    try:
        _send_email(to_addr, subject, plain, html)
    except Exception as exc:
        log.error("  Failed to send email to %s: %s", to_addr, exc)
        return False

    # Update notified_at for all issues in this batch
    today_str = date.today().isoformat()
    con = _conn()
    try:
        ids = [iss["issue_id"] for iss in issues]
        con.executemany(
            "UPDATE dq_open_issues SET notified_at=? WHERE issue_id=?",
            [(today_str, iid) for iid in ids]
        )
        con.commit()
    finally:
        con.close()

    return True


def run_notification_sweep(categories: dict) -> int:
    """Auto-notify all institutions with issues due for a reminder. Returns count sent."""
    issues_by_lb: dict[str, list[dict]] = {}
    for iss in get_open_issues():
        issues_by_lb.setdefault(iss["le_book"], []).append(iss)

    sent = 0
    for lb, issues in issues_by_lb.items():
        inst = _inst_name(lb, categories)
        if send_notification(lb, inst, issues, force=False):
            sent += 1
    if sent:
        log.info("Notification sweep: %d email(s) sent", sent)
    return sent
