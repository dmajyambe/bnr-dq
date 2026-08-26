# Outbound SMTP email for issue urgency — split out of issues/tracker.py per
# review feedback. Named "alerts" (not "notifications") to stay distinct from
# remediation/notifications.py's in-app bell, which is a separate system
# triggered by CR events rather than issue urgency — the two have never
# called each other and aren't unified here.
from __future__ import annotations

import logging
import os
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from issues import repositories as repo
from issues.state_machine import URGENCY_COLORS, NOTIFY_INTERVAL, urgency_band
from storage.postgres.app_db import get_connection

log = logging.getLogger("issues.alerts")


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
    worst     = max((urgency_band(i["detected_at"]) for i in issues),
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
        urgency   = urgency_band(iss["detected_at"])
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
        color     = URGENCY_COLORS.get(urgency_band(iss["detected_at"]), "#666")
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
    Respects per-urgency cadence unless force=True (manual Send Reminder —
    this is the path the dashboard's Alerts tab calls).
    Returns True if email was sent.
    """
    if not issues:
        return False

    to_addr = None
    # 1. Prefer institution contact table
    contact = repo.get_contact(le_book)
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
            band     = urgency_band(iss["detected_at"])
            interval = NOTIFY_INTERVAL.get(band)
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
    con = get_connection()
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


# Dead: zero callers anywhere in the codebase (confirmed via grep) — was only
# ever invoked from the now-deleted dq_pipeline_2m.py's main(). send_notification
# itself is still live (the dashboard's manual "Send Reminder" button).
# def run_notification_sweep(categories: dict) -> int:
#     """Auto-notify all institutions with issues due for a reminder. Returns count sent."""
#     issues_by_lb: dict[str, list[dict]] = {}
#     for iss in repo.get_open_issues():
#         issues_by_lb.setdefault(iss["le_book"], []).append(iss)
#
#     sent = 0
#     for lb, issues in issues_by_lb.items():
#         inst = _inst_name(lb, categories)
#         if send_notification(lb, inst, issues, force=False):
#             sent += 1
#     if sent:
#         log.info("Notification sweep: %d email(s) sent", sent)
#     return sent
