"""
dq_notifications.py — In-portal notification system.

Notifications are stored per-user in SQLite.
Triggers:
  new_cr        — BNR created a CR for this institution
  cr_approved   — BNR approved a CR
  cr_rejected   — BNR rejected a CR
  cr_submitted  — Institution submitted a CR for review (notifies BNR admins)
  sla_warning   — Issue approaching SLA deadline (7 days)
"""
from __future__ import annotations

import logging
import secrets
import sqlite3
from datetime import datetime
from pathlib import Path

log = logging.getLogger("dq_notifications")

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR / "dq_rules.db"

NOTIF_ICONS = {
    "new_cr":       "📋",
    "cr_approved":  "✅",
    "cr_rejected":  "❌",
    "cr_submitted": "📤",
    "sla_warning":  "⚠️",
}


# ── SQLite ─────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def ensure_table() -> None:
    con = _conn()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_notifications (
                notif_id   TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                type       TEXT NOT NULL,
                message    TEXT NOT NULL,
                cr_id      TEXT,
                le_book    TEXT,
                is_read    INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)
        con.commit()
    finally:
        con.close()


# ── core CRUD ──────────────────────────────────────────────────────────────────

def create_notification(user_id: str, notif_type: str, message: str,
                        cr_id: str | None = None,
                        le_book: str | None = None) -> str:
    ensure_table()
    nid = secrets.token_hex(12)
    now = datetime.utcnow().isoformat(timespec="seconds")
    con = _conn()
    try:
        con.execute("""
            INSERT INTO dq_notifications
                (notif_id, user_id, type, message, cr_id, le_book, is_read, created_at)
            VALUES (?,?,?,?,?,?,0,?)
        """, (nid, user_id, notif_type, message, cr_id, le_book, now))
        con.commit()
    finally:
        con.close()
    return nid


def get_notifications(user_id: str, limit: int = 30) -> list[dict]:
    ensure_table()
    con = _conn()
    try:
        rows = con.execute("""
            SELECT * FROM dq_notifications
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (user_id, limit)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_unread_count(user_id: str) -> int:
    ensure_table()
    con = _conn()
    try:
        return con.execute(
            "SELECT COUNT(*) FROM dq_notifications WHERE user_id=? AND is_read=0",
            (user_id,)
        ).fetchone()[0]
    finally:
        con.close()


def mark_read(notif_id: str) -> None:
    con = _conn()
    try:
        con.execute("UPDATE dq_notifications SET is_read=1 WHERE notif_id=?", (notif_id,))
        con.commit()
    finally:
        con.close()


def mark_all_read(user_id: str) -> None:
    con = _conn()
    try:
        con.execute("UPDATE dq_notifications SET is_read=1 WHERE user_id=?", (user_id,))
        con.commit()
    finally:
        con.close()


# ── broadcast helpers ──────────────────────────────────────────────────────────

def notify_le_book(le_book: str, notif_type: str, message: str,
                   cr_id: str | None = None) -> int:
    """Create a notification for every active inst_user linked to le_book. Returns count sent."""
    from dq_auth import get_users_by_le_book
    users = get_users_by_le_book(le_book)
    for u in users:
        create_notification(u["user_id"], notif_type, message, cr_id=cr_id, le_book=le_book)
    if users:
        log.info("Notified %d inst_user(s) for le_book %s (%s)", len(users), le_book, notif_type)
    return len(users)


def notify_bnr_admins(notif_type: str, message: str, cr_id: str | None = None) -> int:
    """Create a notification for every active bnr_admin. Returns count sent."""
    from dq_auth import list_users
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
    from dq_issue_tracker import get_open_issues
    from datetime import date, timedelta

    today     = date.today()
    warn_date = (today + timedelta(days=7)).isoformat()
    # window: sla_deadline between today+6 and today+8 (±1 day tolerance)
    low  = (today + timedelta(days=6)).isoformat()
    high = (today + timedelta(days=8)).isoformat()

    ensure_table()
    con = _conn()
    try:
        warned_today = {
            r[0] for r in con.execute("""
                SELECT DISTINCT le_book FROM dq_notifications
                WHERE type='sla_warning' AND DATE(created_at)=?
            """, (today.isoformat(),)).fetchall()
        }
    finally:
        con.close()

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
                    f"Log in to the portal to review and submit your Change Requests.")
            total += notify_le_book(lb, "sla_warning", msg)
            seen.add(lb)

    if total:
        log.info("SLA warning notifications sent: %d", total)
    return total
