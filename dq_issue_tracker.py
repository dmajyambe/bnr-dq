"""
dq_issue_tracker.py — DQ issue lifecycle: detect, track urgency, penalise, notify.

An issue is a (le_book, table_name, rule_id) combination whose score fell below
ISSUE_THRESHOLD in a pipeline run.  It persists in SQLite independent of the
rolling window so nothing is lost after 30 days.

Lifecycle:
  open      → score < threshold, within SLA window
  resolved  → score ≥ threshold (institution fixed the data)
  penalized → SLA deadline passed without resolution

Urgency bands (days since detected_at):
  new       1–3 d   🔵  no auto-notification yet
  attention 4–15 d  🟡  notify every 7 days
  urgent    16–20 d 🟠  notify every 3 days
  critical  21–30 d 🔴  notify every day
"""
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

SCRIPT_DIR       = Path(__file__).parent
DB_PATH          = SCRIPT_DIR / "dq_rules.db"
ISSUE_THRESHOLD  = 70.0    # score below this creates / keeps an issue
SLA_DAYS         = 30      # days until breach
PENALTY_PCT      = 5.0     # % deducted per penalised issue (logged, not auto-applied to scores)

# Urgency: (max_days_inclusive, band_name)
_URGENCY_STEPS = [(3, "new"), (15, "attention"), (20, "urgent"), (30, "critical")]

URGENCY_COLORS = {
    "new":       "#2563EB",
    "attention": "#D97706",
    "urgent":    "#EA580C",
    "critical":  "#DC2626",
}

# Notification cadence per urgency band (minimum days between emails)
_NOTIFY_INTERVAL = {"new": None, "attention": 7, "urgent": 3, "critical": 1}

# Completeness: one COMP rule per table (matches dq_rules.py _COMP_TABLE_NAMES)
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


# ── SQLite helpers ─────────────────────────────────────────────────────────────

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
                issue_id          TEXT PRIMARY KEY,
                le_book           TEXT NOT NULL,
                institution_name  TEXT,
                table_name        TEXT NOT NULL,
                rule_id           TEXT NOT NULL,
                dimension         TEXT NOT NULL,
                failing_rows      INTEGER NOT NULL DEFAULT 0,
                detected_at       TEXT NOT NULL,
                sla_deadline      TEXT NOT NULL,
                urgency_band      TEXT NOT NULL DEFAULT 'new',
                assigned_to       TEXT,
                notified_at       TEXT,
                resolved_at       TEXT,
                status            TEXT NOT NULL DEFAULT 'open'
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
        con.commit()
    finally:
        con.close()


# ── urgency helpers ────────────────────────────────────────────────────────────

def _urgency_band(detected_at: str) -> str:
    try:
        days = (date.today() - date.fromisoformat(detected_at)).days
    except Exception:
        return "critical"
    for max_days, band in _URGENCY_STEPS:
        if days <= max_days:
            return band
    return "critical"


def _issue_id(le_book: str, table: str, rule_id: str) -> str:
    raw = f"{le_book}|{table}|{rule_id}"
    return hashlib.sha1(raw.encode()).hexdigest()


# ── issue upsert / resolve ─────────────────────────────────────────────────────

def _upsert_issue(con: sqlite3.Connection, le_book: str, inst_name: str,
                  table: str, rule_id: str, dimension: str,
                  failing_rows: int, run_date: str) -> None:
    iid   = _issue_id(le_book, table, rule_id)
    today = run_date

    row = con.execute(
        "SELECT issue_id, detected_at FROM dq_open_issues WHERE issue_id=? AND status='open'",
        (iid,)
    ).fetchone()

    if row:
        band = _urgency_band(row["detected_at"])
        con.execute("""
            UPDATE dq_open_issues
               SET failing_rows=?, urgency_band=?, institution_name=?
             WHERE issue_id=?
        """, (failing_rows, band, inst_name, iid))
    else:
        deadline = (date.fromisoformat(today) + timedelta(days=SLA_DAYS)).isoformat()
        con.execute("""
            INSERT INTO dq_open_issues
                (issue_id, le_book, institution_name, table_name, rule_id, dimension,
                 failing_rows, detected_at, sla_deadline, urgency_band, status)
            VALUES (?,?,?,?,?,?,?,?,?,'new','open')
        """, (iid, le_book, inst_name, table, rule_id, dimension, failing_rows, today, deadline))
        log.info("  NEW ISSUE  %s  %s / %s / %s  (%d failing rows)", le_book, table, rule_id, dimension, failing_rows)


def _maybe_resolve(con: sqlite3.Connection, le_book: str, table: str,
                   rule_id: str, run_date: str) -> None:
    iid = _issue_id(le_book, table, rule_id)
    row = con.execute(
        "SELECT issue_id FROM dq_open_issues WHERE issue_id=? AND status='open'", (iid,)
    ).fetchone()
    if row:
        con.execute(
            "UPDATE dq_open_issues SET status='resolved', resolved_at=? WHERE issue_id=?",
            (run_date, iid)
        )
        log.info("  RESOLVED   %s  %s / %s", le_book, table, rule_id)


# ── main detection logic ───────────────────────────────────────────────────────

def detect_and_update_issues(R: dict, categories: dict, run_date: str) -> None:
    """
    Parse engine results (R) and write/update dq_open_issues.

    R keys used: comp, acc, tim, val, rel.
    An issue is created when a (le_book, table, rule_id) score < ISSUE_THRESHOLD.
    Issues that are now ≥ threshold are marked resolved.
    """
    ensure_tables()
    con = _conn()
    try:
        _process_completeness(con, R.get("comp") or {}, categories, run_date)
        _process_rule_dimension(con, R.get("acc") or {},  "accuracy",   "accuracy_score",   categories, run_date)
        _process_rule_dimension(con, R.get("tim") or {},  "timeliness", "timeliness_score",  categories, run_date)
        _process_rule_dimension(con, R.get("val") or {},  "validity",   "validity_score",    categories, run_date)
        _process_relationship(con, R.get("rel") or {}, categories, run_date)
        con.commit()
    finally:
        con.close()

    total = _count_open()
    log.info("Issue tracker: %d open issue(s) after run %s", total, run_date)


def _inst_name(lb: str, categories: dict) -> str:
    return (categories.get(lb, {}).get("name") or lb).title()


def _process_completeness(con, report: dict, categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        rule_id = _COMP_TABLE_RULE.get(table)
        if not rule_id:
            continue
        for lb, lb_data in tdata.get("le_book_breakdown", {}).items():
            score        = float(lb_data.get("completeness_score") or 100.0)
            failing_rows = int(lb_data.get("null_cells") or 0)
            if score < ISSUE_THRESHOLD and failing_rows > 0:
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
                # Older report format without per-rule breakdown — use table-level score
                score   = float(lb_data.get(score_key) or 100.0)
                failing = int(lb_data.get("invalid") or lb_data.get("null_cells") or 0)
                rule_id = f"{dimension[:3].upper()}-ALL"
                if score < ISSUE_THRESHOLD and failing > 0:
                    _upsert_issue(con, lb, inst, table, rule_id, dimension, failing, run_date)
                else:
                    _maybe_resolve(con, lb, table, rule_id, run_date)
                continue
            for rule_id, rule_data in rules.items():
                score   = float(rule_data.get(score_key) or 100.0)
                failing = int(rule_data.get("invalid") or 0)
                if score < ISSUE_THRESHOLD and failing > 0:
                    _upsert_issue(con, lb, inst, table, rule_id, dimension, failing, run_date)
                else:
                    _maybe_resolve(con, lb, table, rule_id, run_date)


def _process_relationship(con, report: dict, categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        for rule_id, rule_data in tdata.get("rules", {}).items():
            for lb, lb_data in rule_data.get("le_book_breakdown", {}).items():
                score   = float(lb_data.get("ri_score") or 100.0)
                failing = int(lb_data.get("invalid") or 0)
                if score < ISSUE_THRESHOLD and failing > 0:
                    _upsert_issue(con, lb, _inst_name(lb, categories),
                                  table, rule_id, "relationship", failing, run_date)
                else:
                    _maybe_resolve(con, lb, table, rule_id, run_date)


# ── penalty sweep ──────────────────────────────────────────────────────────────

def apply_penalties(run_date: str) -> int:
    """
    Sweep open issues past their SLA deadline → mark penalized + write dq_penalties.
    Returns count of newly penalised issues.
    """
    ensure_tables()
    con = _conn()
    penalised = 0
    try:
        breached = con.execute("""
            SELECT * FROM dq_open_issues
            WHERE status='open' AND sla_deadline < ?
        """, (run_date,)).fetchall()

        for row in breached:
            period     = row["detected_at"][:7]   # YYYY-MM
            penalty_id = _issue_id(row["le_book"], row["table_name"], row["rule_id"]) + "_pen"

            con.execute("""
                INSERT OR REPLACE INTO dq_penalties
                    (penalty_id, le_book, institution_name, dimension, table_name, rule_id,
                     period, failing_rows, penalty_pct, applied_at, original_detected_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (penalty_id, row["le_book"], row["institution_name"],
                  row["dimension"], row["table_name"], row["rule_id"],
                  period, row["failing_rows"], PENALTY_PCT, run_date, row["detected_at"]))

            con.execute(
                "UPDATE dq_open_issues SET status='penalized' WHERE issue_id=?",
                (row["issue_id"],)
            )
            penalised += 1
            log.warning("  PENALIZED  %s  %s / %s — SLA breached (deadline %s)",
                        row["le_book"], row["table_name"], row["rule_id"], row["sla_deadline"])

        con.commit()
    finally:
        con.close()

    if penalised:
        log.warning("Penalties applied: %d issue(s) past SLA", penalised)
    return penalised


# ── query helpers (used by dashboard) ─────────────────────────────────────────

def _count_open() -> int:
    con = _conn()
    try:
        return con.execute("SELECT COUNT(*) FROM dq_open_issues WHERE status='open'").fetchone()[0]
    finally:
        con.close()


def get_open_issues(le_book: str | None = None) -> list[dict]:
    """Return open issues, optionally filtered to one institution."""
    ensure_tables()
    con = _conn()
    try:
        if le_book:
            rows = con.execute(
                "SELECT * FROM dq_open_issues WHERE status='open' AND le_book=? ORDER BY sla_deadline",
                (le_book,)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM dq_open_issues WHERE status='open' ORDER BY sla_deadline"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_institution_issue_summary() -> dict[str, dict]:
    """
    Return {le_book: {worst_urgency, total, new, attention, urgent, critical, expiring_in_5}}
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

    today    = date.today()
    summary: dict[str, dict] = {}
    _order   = ["new", "attention", "urgent", "critical"]

    for row in rows:
        lb   = row["le_book"]
        band = _urgency_band(row["detected_at"])
        days_left = (date.fromisoformat(row["sla_deadline"]) - today).days

        if lb not in summary:
            summary[lb] = {"total": 0, "new": 0, "attention": 0,
                           "urgent": 0, "critical": 0, "expiring_in_5": 0,
                           "worst_urgency": "new"}
        s = summary[lb]
        s["total"]     += 1
        s[band]        += 1
        if days_left <= 5:
            s["expiring_in_5"] += 1
        if _order.index(band) > _order.index(s["worst_urgency"]):
            s["worst_urgency"] = band

    return summary


def get_penalties(le_book: str | None = None) -> list[dict]:
    ensure_tables()
    con = _conn()
    try:
        if le_book:
            rows = con.execute(
                "SELECT * FROM dq_penalties WHERE le_book=? ORDER BY applied_at DESC", (le_book,)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM dq_penalties ORDER BY applied_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


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
        "Issues not resolved within 30 days of detection attract a",
        f"{int(PENALTY_PCT)}% compliance score penalty per unresolved issue.",
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
        Issues not resolved within 30 days of detection attract a
        <strong>{int(PENALTY_PCT)}% compliance score penalty</strong> per unresolved issue.
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
