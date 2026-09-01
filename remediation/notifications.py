# In-portal notification system — moved from dq_notifications.py.
#
# Triggers:
#   new_cr        — BNR created a CR for this institution
#   cr_approved   — BNR approved a CR
#   cr_rejected   — BNR rejected a CR
#   cr_submitted  — Institution submitted a CR for review (notifies BNR admins)
#   sla_warning   — Issue approaching SLA deadline (7 days)
#
# This is a *separate* notification mechanism from issues/tracker.py's
# urgency-band-driven SMTP emails (send_notification) — this one is in-app
# (SQLite-backed bell icon), triggered by CR/remediation events, not issue
# urgency. The two have never called each other; not unified here.
from __future__ import annotations

import logging
import secrets
from datetime import datetime

from sqlalchemy import text
from storage.postgres.connection import get_engine

log = logging.getLogger("remediation.notifications")

NOTIF_ICONS = {
    "new_cr":       "📋",
    "cr_approved":  "✅",
    "cr_rejected":  "❌",
    "cr_submitted": "📤",
    "sla_warning":  "⚠️",
}


def ensure_table() -> None:
    from storage.postgres.init_tables import init_all
    init_all()


# ── core CRUD ──────────────────────────────────────────────────────────────────

def create_notification(user_id: str, notif_type: str, message: str,
                        cr_id: str | None = None,
                        le_book: str | None = None) -> str:
    ensure_table()
    nid = secrets.token_hex(12)
    now = datetime.utcnow().isoformat(timespec="seconds")
    with get_engine().begin() as con:
        con.execute(
            text("""
            INSERT INTO dq_notifications
                (notif_id, user_id, type, message, cr_id, le_book, is_read, created_at)
            VALUES (:nid,:user_id,:notif_type,:message,:cr_id,:le_book,0,:now)
        """),
            {"nid": nid, "user_id": user_id, "notif_type": notif_type,
             "message": message, "cr_id": cr_id, "le_book": le_book, "now": now},
        )
    return nid


def get_notifications(user_id: str, limit: int = 30) -> list[dict]:
    ensure_table()
    with get_engine().connect() as con:
        rows = con.execute(
            text("""
            SELECT * FROM dq_notifications
            WHERE user_id = :user_id
            ORDER BY created_at DESC
            LIMIT :limit
        """),
            {"user_id": user_id, "limit": limit},
        ).mappings().fetchall()
        return [dict(r) for r in rows]


def get_unread_count(user_id: str) -> int:
    ensure_table()
    with get_engine().connect() as con:
        return con.execute(
            text("SELECT COUNT(*) AS n FROM dq_notifications WHERE user_id=:user_id AND is_read=0"),
            {"user_id": user_id},
        ).mappings().fetchone()["n"]


def mark_read(notif_id: str) -> None:
    with get_engine().begin() as con:
        con.execute(
            text("UPDATE dq_notifications SET is_read=1 WHERE notif_id=:nid"),
            {"nid": notif_id},
        )


def mark_all_read(user_id: str) -> None:
    with get_engine().begin() as con:
        con.execute(
            text("UPDATE dq_notifications SET is_read=1 WHERE user_id=:user_id"),
            {"user_id": user_id},
        )


# ── broadcast helpers ──────────────────────────────────────────────────────────

def notify_le_book(le_book: str, notif_type: str, message: str,
                   cr_id: str | None = None) -> int:
    """Create a notification for every active inst_user linked to le_book. Returns count sent."""
    from auth.users import get_users_by_le_book
    users = get_users_by_le_book(le_book)
    for u in users:
        create_notification(u["user_id"], notif_type, message, cr_id=cr_id, le_book=le_book)
    if users:
        log.info("Notified %d inst_user(s) for le_book %s (%s)", len(users), le_book, notif_type)
    return len(users)


def notify_bnr_admins(notif_type: str, message: str, cr_id: str | None = None) -> int:
    """Create a notification for every active bnr_admin. Returns count sent."""
    from auth.users import list_users
    admins = [u for u in list_users() if u["role"] in ("bnr_admin", "admin") and u["is_active"]]
    for u in admins:
        create_notification(u["user_id"], notif_type, message, cr_id=cr_id)
    if admins:
        log.info("Notified %d BNR admin(s) (%s)", len(admins), notif_type)
    return len(admins)


# ── SLA warning sweep (called from pipeline) ──────────────────────────────────

def send_sla_warnings() -> int:
    """
    Find open issues with exactly 7 days left on their SLA and create
    a notification for each affected institution's inst_users.
    Skips issues already warned today.
    Returns count of notifications created.
    """
    from datetime import date, timedelta

    from issues.repositories import get_open_issues

    today     = date.today()
    low  = (today + timedelta(days=6)).isoformat()
    high = (today + timedelta(days=8)).isoformat()

    ensure_table()
    with get_engine().connect() as con:
        warned_today = {
            r["le_book"] for r in con.execute(
                text("""
                SELECT DISTINCT le_book FROM dq_notifications
                WHERE type='sla_warning' AND DATE(created_at)=:today
            """),
                {"today": today.isoformat()},
            ).mappings().fetchall()
        }

    issues = get_open_issues()
    seen: set[str] = set()
    total = 0
    for iss in issues:
        lb = iss["le_book"]
        if lb in seen or lb in warned_today:
            continue
        if low <= iss.get("sla_deadline", "") <= high:
            inst = (iss.get("institution_name") or lb).title()
            msg  = (f"{inst}: you have open DQ issues with SLA deadlines in 7 days. "
                    f"Log in to the portal to review and submit your Data Correction Requests.")
            total += notify_le_book(lb, "sla_warning", msg)
            seen.add(lb)

    if total:
        log.info("SLA warning notifications sent: %d", total)
    return total
