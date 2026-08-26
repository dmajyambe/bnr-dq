# Data Correction Request (CR) lifecycle — moved from dq_change_request.py.
#
# A Data Correction Request is created by a data specialist to track the correction of
# one or more open DQ issues (from issues/tracker.py). Lifecycle:
#
#   open -> in_progress -> submitted -> approved / rejected
#                                ^_____________|
#   Any active status can be moved to "closed" (cancelled / expired).
#
# This is a *different* state machine from issues/state_machine.py's issue
# status (open -> pending_resolution -> resolved) — see that module's
# docstring. Stored in dq_rules.db (same SQLite file as issues/notifications)
# so the dashboard can join data without a second connection.
from __future__ import annotations

import json
import logging
import os
import smtplib
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from storage.postgres.app_db import get_connection

log = logging.getLogger("remediation.change_requests")

# ── status metadata ────────────────────────────────────────────────────────────

STATUS_LABELS: dict[str, str] = {
    "open":        "Open",
    "in_progress": "In Progress",
    "submitted":   "Submitted",
    "approved":    "Approved",
    "rejected":    "Rejected",
    "closed":      "Closed",
}

STATUS_COLORS: dict[str, str] = {
    "open":        "#2563EB",   # blue
    "in_progress": "#D97706",   # amber
    "submitted":   "#7C3D1E",   # burnt rust
    "approved":    "#16A34A",   # green
    "rejected":    "#DC2626",   # red
    "closed":      "#6B7280",   # grey
}

# Which statuses a given status may transition to
VALID_TRANSITIONS: dict[str, list[str]] = {
    "open":        ["in_progress", "closed"],
    "in_progress": ["submitted",   "closed"],
    "submitted":   ["approved",    "rejected"],
    "rejected":    ["in_progress", "closed"],
    "approved":    ["closed"],
    "closed":      [],
}


# ── schema ─────────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    """Create dq_change_requests if it doesn't already exist."""
    from storage.postgres.init_tables import init_all
    init_all()


# ── ID generation ──────────────────────────────────────────────────────────────

def _next_cr_id() -> str:
    today = date.today().strftime("%Y%m%d")
    con   = get_connection()
    try:
        row = con.execute(
            "SELECT COUNT(*) AS n FROM dq_change_requests WHERE cr_id LIKE ?",
            (f"CR-{today}-%",),
        ).fetchone()
        seq = (row["n"] if row else 0) + 1
    finally:
        con.close()
    return f"CR-{today}-{seq:04d}"


# ── CRUD ──────────────────────────────────────────────────────────────────────

def create_cr(
    tables:           list[str],
    le_book:          str,
    institution_name: str,
    title:            str,
    description:      str = "",
    assigned_to:      str = "",
    created_by:       str = "",
    target_date:      str = "",
    dimension:        str = "",
    failing_rows:     int = 0,
) -> str:
    """
    Persist a new Data Correction Request.  Returns the generated cr_id.
    The CR starts in 'open' status.
    """
    ensure_table()
    now   = datetime.utcnow().strftime("%Y-%m-%d")
    cr_id = _next_cr_id()

    # Build per-table approval state
    table_approvals = {
        tbl: {"status": "pending", "approved_at": None, "approved_by": None}
        for tbl in tables
    }

    # Derive issue_ids from open issues for this le_book + tables
    issue_ids: list[str] = []
    if tables:
        try:
            con_q = get_connection()
            placeholders = ",".join("?" * len(tables))
            rows = con_q.execute(
                f"SELECT issue_id FROM dq_open_issues WHERE le_book=? AND table_name IN ({placeholders})"
                f" AND status IN ('open','pending_resolution')",
                [le_book] + list(tables),
            ).fetchall()
            issue_ids = [r["issue_id"] for r in rows]
            con_q.close()
        except Exception as exc:
            log.warning("Could not derive issue_ids for CR %s: %s", cr_id, exc)
            issue_ids = []

    con = get_connection()
    try:
        con.execute("""
            INSERT INTO dq_change_requests
                (cr_id, title, description, issue_ids, le_book, institution_name,
                 dimension, assigned_to, created_by, created_at, updated_at,
                 target_date, status, failing_rows, tables, table_approvals)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'open',?,?,?)
        """, (
            cr_id,
            title.strip(),
            (description or "").strip(),
            json.dumps(issue_ids),
            le_book,
            institution_name,
            dimension,
            (assigned_to or "").strip(),
            (created_by  or "").strip(),
            now, now,
            (target_date or ""),
            failing_rows,
            json.dumps(tables),
            json.dumps(table_approvals),
        ))
        con.commit()
        log.info("CR created: %s  (%s / %s)", cr_id, le_book, title)
    finally:
        con.close()

    # Portal notifications to all inst_users of this le_book
    try:
        from remediation.notifications import notify_le_book
        inst = institution_name or le_book
        notify_le_book(
            le_book, "new_cr",
            f"A new Data Correction Request ({cr_id}) has been created for {inst}: {title}",
            cr_id=cr_id,
        )
    except Exception as exc:
        log.warning("Portal notification failed for %s: %s", cr_id, exc)

    # Fire assignment email in a daemon thread so it never blocks CR creation
    if assigned_to:
        import threading
        def _notify():
            try:
                send_assignment_notification(cr_id)
            except Exception as exc:
                log.warning("Assignment email failed for %s: %s", cr_id, exc)
        threading.Thread(target=_notify, daemon=True).start()

    return cr_id


def get_crs(status: str | None = None, le_book: str | None = None) -> list[dict]:
    """Return all CRs, optionally filtered by status and/or institution."""
    ensure_table()
    con = get_connection()
    try:
        clauses: list[str] = []
        params:  list      = []
        if status and status != "all":
            clauses.append("status=?")
            params.append(status)
        if le_book:
            clauses.append("le_book=?")
            params.append(le_book)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows  = con.execute(
            f"SELECT * FROM dq_change_requests {where} ORDER BY created_at DESC",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_cr(cr_id: str) -> dict | None:
    """Fetch one CR by ID, or None if not found."""
    ensure_table()
    con = get_connection()
    try:
        row = con.execute(
            "SELECT * FROM dq_change_requests WHERE cr_id=?", (cr_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def update_status(
    cr_id:      str,
    new_status: str,
    actor:      str = "",
    notes:      str = "",
) -> tuple[bool, str]:
    """
    Transition a CR to new_status.  Returns (ok, message).
    Enforces VALID_TRANSITIONS — invalid moves return (False, reason).
    """
    ensure_table()
    now = datetime.utcnow().strftime("%Y-%m-%d")
    con = get_connection()
    try:
        row = con.execute(
            "SELECT status FROM dq_change_requests WHERE cr_id=?", (cr_id,)
        ).fetchone()
        if not row:
            return False, f"CR {cr_id} not found."
        cur     = row["status"]
        allowed = VALID_TRANSITIONS.get(cur, [])
        if new_status not in allowed:
            return False, (
                f"Cannot move {cr_id} from '{cur}' to '{new_status}'. "
                f"Allowed: {allowed or ['none']}."
            )
        if new_status in ("approved", "rejected"):
            con.execute("""
                UPDATE dq_change_requests
                   SET status=?, updated_at=?, reviewed_by=?, reviewed_at=?, review_notes=?
                 WHERE cr_id=?
            """, (new_status, now, actor, now, notes or "", cr_id))
        else:
            con.execute(
                "UPDATE dq_change_requests SET status=?, updated_at=? WHERE cr_id=?",
                (new_status, now, cr_id),
            )
        con.commit()
        log.info("CR %s: %s → %s  (actor=%s)", cr_id, cur, new_status, actor or "—")

        # Fetch CR details for notifications
        cr_row = con.execute(
            "SELECT le_book, institution_name, title FROM dq_change_requests WHERE cr_id=?",
            (cr_id,)
        ).fetchone()
    finally:
        con.close()

    # Fire portal notifications (non-blocking)
    if cr_row:
        try:
            from remediation.notifications import notify_bnr_admins, notify_le_book
            lb    = cr_row["le_book"]
            inst  = (cr_row["institution_name"] or lb)
            title = cr_row["title"]

            if new_status == "approved":
                notify_le_book(lb, "cr_approved",
                    f"Data Correction Request {cr_id} for {inst} has been approved: {title}",
                    cr_id=cr_id)
            elif new_status == "rejected":
                notify_le_book(lb, "cr_rejected",
                    f"Data Correction Request {cr_id} for {inst} has been rejected: {title}",
                    cr_id=cr_id)
            elif new_status == "submitted":
                notify_bnr_admins("cr_submitted",
                    f"{inst} submitted Data Correction Request {cr_id} for review: {title}",
                    cr_id=cr_id)
        except Exception as exc:
            log.warning("Portal notification failed for CR %s status change: %s", cr_id, exc)

    return True, f"{cr_id} moved to '{STATUS_LABELS[new_status]}'."


def get_stats() -> dict[str, int]:
    """Return {status: count} for all CRs."""
    ensure_table()
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT status, COUNT(*) AS n FROM dq_change_requests GROUP BY status"
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}
    finally:
        con.close()


def approve_table(cr_id: str, table_name: str, actor: str = "") -> tuple[bool, str]:
    """
    Mark one table within a CR as approved.
    If all tables are now approved, auto-transitions the CR to 'approved'.
    Returns (ok, message).
    """
    cr = get_cr(cr_id)
    if not cr:
        return False, f"CR {cr_id} not found."

    try:
        table_approvals: dict = json.loads(cr.get("table_approvals") or "{}")
    except Exception:
        table_approvals = {}

    if table_name not in table_approvals:
        return False, f"Table '{table_name}' is not part of CR {cr_id}."

    table_approvals[table_name] = {
        "status":      "approved",
        "approved_at": date.today().isoformat(),
        "approved_by": actor,
    }

    now = datetime.utcnow().strftime("%Y-%m-%d")
    con = get_connection()
    try:
        con.execute(
            "UPDATE dq_change_requests SET table_approvals=?, updated_at=? WHERE cr_id=?",
            (json.dumps(table_approvals), now, cr_id),
        )
        con.commit()
    finally:
        con.close()

    log.info("CR %s: table '%s' approved by %s", cr_id, table_name, actor or "—")

    # If every table is now approved, auto-transition CR to approved
    all_approved = all(v.get("status") == "approved" for v in table_approvals.values())
    if all_approved and table_approvals:
        update_status(cr_id, "approved", actor=actor, notes="All tables approved")
        return True, f"Table '{table_name}' approved — CR {cr_id} fully approved."

    pending = [t for t, v in table_approvals.items() if v.get("status") != "approved"]
    return True, f"Table '{table_name}' approved. Still pending: {', '.join(pending)}."


def auto_close_approved_crs(run_id: str = "") -> list[str]:
    """
    After a resolution scan, close any 'approved' CR whose every linked table
    has been approved (or all linked issues are resolved for backward compat).
    Returns the list of cr_ids that were auto-closed.
    """
    closed: list[str] = []
    for cr in get_crs(status="approved"):
        # Primary check: table_approvals
        tables_raw = cr.get("tables") or "[]"
        try:
            tables_list = json.loads(tables_raw)
        except Exception:
            tables_list = []

        if tables_list:
            try:
                table_approvals = json.loads(cr.get("table_approvals") or "{}")
            except Exception:
                table_approvals = {}
            all_done = all(
                table_approvals.get(t, {}).get("status") == "approved"
                for t in tables_list
            )
        else:
            # Backward compat: fall back to issue_ids check
            from issues.repositories import get_issue_by_id
            iids = json.loads(cr.get("issue_ids") or "[]")
            if not iids:
                continue
            all_done = all(
                (get_issue_by_id(iid) or {}).get("status") == "resolved"
                for iid in iids
            )

        if not all_done:
            continue

        note = (
            "Auto-closed by pipeline: all linked tables/issues confirmed resolved"
            + (f" (run {run_id})" if run_id else "")
        )
        ok, _ = update_status(cr["cr_id"], "closed", actor="pipeline", notes=note)
        if ok:
            closed.append(cr["cr_id"])
            log.info("CR %s auto-closed", cr["cr_id"])
    return closed


def get_issue_cr_map() -> dict[str, dict]:
    """
    Return {issue_id: {'cr_id': ..., 'status': ...}} for all non-closed CRs.
    Used by the Alerts page to show which issues have an active CR.
    """
    ensure_table()
    result: dict[str, dict] = {}
    for cr in get_crs():
        if cr["status"] == "closed":
            continue
        try:
            for iid in json.loads(cr.get("issue_ids") or "[]"):
                result[iid] = {"cr_id": cr["cr_id"], "status": cr["status"]}
        except Exception:
            pass
    return result


# ── email notifications ────────────────────────────────────────────────────────

def _smtp_ready() -> bool:
    return bool(os.environ.get("SMTP_HOST") and os.environ.get("SMTP_USER"))


def _send(to_addr: str, subject: str, plain: str, html: str) -> None:
    host     = os.environ["SMTP_HOST"]
    port     = int(os.environ.get("SMTP_PORT", 587))
    user     = os.environ.get("SMTP_USER", "")
    password = os.environ.get("SMTP_PASSWORD", "")
    sender   = os.environ.get("SMTP_FROM", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = to_addr
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html,  "html"))

    # localhost relay (postfix) — no TLS or auth needed
    local_relay = host in ("localhost", "127.0.0.1")

    with smtplib.SMTP(host, port, timeout=10) as smtp:
        smtp.ehlo()
        if not local_relay:
            smtp.starttls()
            smtp.login(user, password)
        smtp.sendmail(sender, [to_addr], msg.as_string())

    log.info("CR assignment email sent → %s", to_addr)


def _build_assignment_email(cr: dict, issues: list[dict]) -> tuple[str, str, str]:
    """Return (subject, plain, html) for a CR assignment notification."""
    inst   = (cr.get("institution_name") or cr["le_book"]).title()
    cr_id  = cr["cr_id"]
    today  = date.today().isoformat()

    subject = f"[BNR DQ] Data Correction Request Assigned — {cr_id} — {inst}"

    # ── plain text ────────────────────────────────────────────────────────────
    lines = [
        f"Data Correction Request : {cr_id}",
        f"Institution    : {inst}  (LE Book: {cr['le_book']})",
        f"Assigned To    : {cr.get('assigned_to', '—')}",
        f"Created By     : {cr.get('created_by',  '—')}",
        f"Target Date    : {cr.get('target_date',  '—')}",
        f"Date           : {today}",
        "",
        f"Title          : {cr['title']}",
    ]
    if cr.get("description"):
        lines += ["", "Correction Plan:", cr["description"]]

    lines += ["", "─" * 60, f"LINKED ISSUES ({len(issues)})", "─" * 60]
    for i, iss in enumerate(issues, 1):
        try:
            days_left = (date.fromisoformat(iss["sla_deadline"]) - date.today()).days
        except Exception:
            days_left = "?"
        lines += [
            f"{i}. [{iss['rule_id']}]  {iss['table_name']}  —  {iss['dimension'].title()}",
            f"   Failing rows : {iss.get('failing_rows', 0):,}",
            f"   Detected     : {iss.get('detected_at', '—')}",
            f"   SLA deadline : {iss.get('sla_deadline', '—')}  ({days_left} day(s) remaining)",
            f"   Urgency      : {iss.get('urgency_band', '—').upper()}",
            "",
        ]

    lines += [
        "─" * 60,
        "ACTION REQUIRED:",
        "Review the linked issues above, correct the data in your source system,",
        "then return to the BNR Data Quality Dashboard → Remediation tab and mark",
        "this Data Correction Request as 'In Progress' and then 'Submitted for Review'.",
        "",
        "This is an automated notification from the BNR Data Quality",
        "Monitoring System. Do not reply to this message.",
    ]
    plain = "\n".join(lines)

    # ── HTML ──────────────────────────────────────────────────────────────────
    URGENCY_COLOR = {
        "new": "#2563EB", "attention": "#D97706",
        "urgent": "#EA580C", "critical": "#DC2626",
    }
    issue_rows = ""
    for iss in issues:
        try:
            days_left = (date.fromisoformat(iss["sla_deadline"]) - date.today()).days
        except Exception:
            days_left = "?"
        band  = iss.get("urgency_band", "new")
        color = URGENCY_COLOR.get(band, "#666")
        issue_rows += f"""
        <tr>
          <td style="padding:6px 10px;border-bottom:1px solid #eee;font-weight:700">
            {iss['rule_id']}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">
            {iss['table_name']}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">
            {iss['dimension'].title()}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee;text-align:right">
            {iss.get('failing_rows', 0):,}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee">
            {iss.get('sla_deadline', '—')}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #eee;
                     color:{color};font-weight:700">
            {days_left}d left</td>
        </tr>"""

    desc_block = (
        f'<p style="background:#F9F5F1;border-left:4px solid #753918;'
        f'padding:12px 16px;margin:16px 0;font-size:13px">'
        f'{cr["description"]}</p>'
    ) if cr.get("description") else ""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#1a1a2e;max-width:720px">
      <div style="background:#753918;padding:20px 32px">
        <h2 style="color:#fff;margin:0">BNR Data Quality — Data Correction Request Assigned</h2>
        <p style="color:rgba(255,255,255,.7);margin:4px 0 0">{cr_id}  ·  {inst}</p>
      </div>
      <div style="padding:24px 32px">
        <table style="width:100%;font-size:13px;margin-bottom:16px">
          <tr><td style="color:#6B7280;width:140px">Assigned To</td>
              <td><strong>{cr.get('assigned_to','—')}</strong></td></tr>
          <tr><td style="color:#6B7280">Created By</td>
              <td>{cr.get('created_by','—')}</td></tr>
          <tr><td style="color:#6B7280">Target Date</td>
              <td>{cr.get('target_date','—')}</td></tr>
          <tr><td style="color:#6B7280">Institution</td>
              <td>{inst}  (LE Book: {cr['le_book']})</td></tr>
        </table>
        <h3 style="font-size:14px;color:#753918;margin:0 0 4px">{cr['title']}</h3>
        {desc_block}
        <h4 style="font-size:12px;text-transform:uppercase;letter-spacing:.05em;
                   color:#6B7280;margin:20px 0 8px">
          Linked Issues ({len(issues)})</h4>
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead>
            <tr style="background:#F4F6F9">
              <th style="padding:8px 10px;text-align:left">Rule</th>
              <th style="padding:8px 10px;text-align:left">Table</th>
              <th style="padding:8px 10px;text-align:left">Dimension</th>
              <th style="padding:8px 10px;text-align:right">Failing Rows</th>
              <th style="padding:8px 10px;text-align:left">Deadline</th>
              <th style="padding:8px 10px;text-align:left">Remaining</th>
            </tr>
          </thead>
          <tbody>{issue_rows}</tbody>
        </table>
        <div style="background:#FFF8E7;border-left:4px solid #D97706;
                    padding:14px 18px;margin:20px 0">
          <strong>Action Required:</strong> Correct the data in your source system for
          the rules and tables listed above, then open the
          <strong>BNR Data Quality Dashboard → Remediation tab</strong>
          and move this Data Correction Request to <em>In Progress</em>, then
          <em>Submit for Review</em> when corrections are complete.
        </div>
      </div>
      <div style="background:#F4F6F9;padding:14px 32px;font-size:11px;color:#6B7280">
        Automated notification — BNR Data Quality Monitoring System. Do not reply.
      </div>
    </body></html>"""

    return subject, plain, html


def send_assignment_notification(cr_id: str) -> bool:
    """
    Send a CR assignment email to cr['assigned_to'].
    Fetches the CR and its linked issues, then emails the assignment details.
    Returns True if sent, False otherwise.
    """
    if not _smtp_ready():
        log.warning("SMTP not configured — skipping CR assignment email for %s", cr_id)
        return False

    cr = get_cr(cr_id)
    if not cr or not cr.get("assigned_to"):
        log.warning("No CR or no assigned_to for %s", cr_id)
        return False

    # Fetch linked issue details
    try:
        from issues.repositories import get_open_issues
        all_issues = get_open_issues(cr["le_book"])
        issue_ids  = set(json.loads(cr.get("issue_ids") or "[]"))
        issues     = [i for i in all_issues if i["issue_id"] in issue_ids]
    except Exception as exc:
        log.warning("Could not fetch issues for CR %s: %s", cr_id, exc)
        issues = []

    subject, plain, html = _build_assignment_email(cr, issues)
    try:
        _send(cr["assigned_to"], subject, plain, html)
        return True
    except Exception as exc:
        log.error("Failed to send CR assignment email for %s: %s", cr_id, exc)
        return False
