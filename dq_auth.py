"""
dq_auth.py — User authentication for the BNR DQ Dashboard.

Roles:
  bnr_admin   — full BNR access: create CRs, approve/reject, manage users
  bnr_viewer  — BNR read-only
  admin       — legacy alias for bnr_admin (existing accounts)
  viewer      — legacy alias for bnr_viewer (existing accounts)
  inst_user   — institution focal point: scoped to their le_book(s)

Institution users have no @bnr.rw domain restriction.
Passwords: SHA-256(salt + password), salt is per-user random hex.
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
import sqlite3
from datetime import datetime
from pathlib import Path

log = logging.getLogger("dq_auth")

SCRIPT_DIR     = Path(__file__).parent
DB_PATH        = SCRIPT_DIR / "dq_rules.db"
ALLOWED_DOMAIN = "bnr.rw"

BNR_ROLES  = {"bnr_admin", "bnr_viewer", "admin", "viewer"}
INST_ROLES = {"inst_user"}
ALL_ROLES  = BNR_ROLES | INST_ROLES


# ── SQLite helpers ─────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def ensure_users_table() -> None:
    con = _conn()
    try:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS dq_users (
                user_id       TEXT PRIMARY KEY,
                email         TEXT UNIQUE NOT NULL,
                name          TEXT NOT NULL DEFAULT '',
                salt          TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'viewer',
                is_active     INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT NOT NULL,
                last_login    TEXT
            );

            CREATE TABLE IF NOT EXISTS dq_user_institutions (
                user_id  TEXT NOT NULL,
                le_book  TEXT NOT NULL,
                PRIMARY KEY (user_id, le_book),
                FOREIGN KEY (user_id) REFERENCES dq_users(user_id)
            );
        """)
        con.commit()
    finally:
        con.close()


# ── domain / role validation ───────────────────────────────────────────────────

def is_valid_bnr_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local, _, domain = email.strip().lower().rpartition("@")
    return bool(local) and domain == ALLOWED_DOMAIN

def is_valid_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local, _, domain = email.strip().lower().rpartition("@")
    return bool(local) and bool(domain)


def is_bnr_role(role: str) -> bool:
    return role in BNR_ROLES


def is_admin(role: str) -> bool:
    return role in {"bnr_admin", "admin"}


# ── password helpers ───────────────────────────────────────────────────────────

def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _new_salt() -> str:
    return secrets.token_hex(32)


# ── user CRUD ──────────────────────────────────────────────────────────────────

def create_user(email: str, name: str, password: str,
                role: str = "viewer",
                le_books: list[str] | None = None) -> str:
    """
    Create a new user. Returns the generated user_id.
    BNR roles require @bnr.rw email. inst_user accepts any valid email.
    le_books: list of le_book codes to associate (inst_user only).
    """
    ensure_users_table()
    email = email.strip().lower()

    if role not in ALL_ROLES:
        raise ValueError(f"Invalid role: {role!r}. Must be one of {sorted(ALL_ROLES)}")
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters.")

    if role in BNR_ROLES:
        if not is_valid_bnr_email(email):
            raise ValueError(f"BNR accounts require @{ALLOWED_DOMAIN} email — got: {email!r}")
    else:
        if not is_valid_email(email):
            raise ValueError(f"Invalid email address: {email!r}")

    salt    = _new_salt()
    pw_hash = _hash_password(password, salt)
    user_id = secrets.token_hex(16)
    now     = datetime.now().isoformat(timespec="seconds")

    con = _conn()
    try:
        con.execute("""
            INSERT INTO dq_users
                (user_id, email, name, salt, password_hash, role, is_active, created_at)
            VALUES (?,?,?,?,?,?,1,?)
        """, (user_id, email, name, salt, pw_hash, role, now))

        if le_books:
            con.executemany(
                "INSERT OR IGNORE INTO dq_user_institutions (user_id, le_book) VALUES (?,?)",
                [(user_id, lb) for lb in le_books]
            )
        con.commit()
        log.info("User created: %s (%s)", email, role)
    except sqlite3.IntegrityError:
        raise ValueError(f"User already exists: {email}")
    finally:
        con.close()

    return user_id


def get_user_by_email(email: str) -> dict | None:
    ensure_users_table()
    con = _conn()
    try:
        row = con.execute(
            "SELECT * FROM dq_users WHERE email=? AND is_active=1",
            (email.strip().lower(),)
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def get_user_by_id(user_id: str) -> dict | None:
    ensure_users_table()
    con = _conn()
    try:
        row = con.execute(
            "SELECT * FROM dq_users WHERE user_id=? AND is_active=1", (user_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def verify_credentials(email: str, password: str) -> dict | None:
    """Return user dict on success, None on failure. Updates last_login."""
    if not email or not password:
        return None

    email = email.strip().lower()

    # BNR roles require @bnr.rw; inst_user can have any domain
    user = get_user_by_email(email)
    if not user:
        return None

    # Domain check only for BNR roles
    if user["role"] in BNR_ROLES and not is_valid_bnr_email(email):
        return None

    expected = _hash_password(password, user["salt"])
    if not secrets.compare_digest(expected, user["password_hash"]):
        return None

    con = _conn()
    try:
        con.execute(
            "UPDATE dq_users SET last_login=? WHERE user_id=?",
            (datetime.now().isoformat(timespec="seconds"), user["user_id"])
        )
        con.commit()
    finally:
        con.close()

    return dict(user)


def change_password(email: str, new_password: str) -> None:
    if len(new_password) < 8:
        raise ValueError("Password must be at least 8 characters.")
    salt    = _new_salt()
    pw_hash = _hash_password(new_password, salt)
    con = _conn()
    try:
        con.execute(
            "UPDATE dq_users SET salt=?, password_hash=? WHERE email=?",
            (salt, pw_hash, email.strip().lower())
        )
        con.commit()
    finally:
        con.close()


def force_reset_password(user_id: str) -> str:
    """Generate a temporary password, apply it, and return it (shown to BNR admin)."""
    temp_pw = secrets.token_urlsafe(10)
    salt    = _new_salt()
    pw_hash = _hash_password(temp_pw, salt)
    con = _conn()
    try:
        con.execute(
            "UPDATE dq_users SET salt=?, password_hash=? WHERE user_id=?",
            (salt, pw_hash, user_id)
        )
        con.commit()
    finally:
        con.close()
    return temp_pw


def list_users() -> list[dict]:
    ensure_users_table()
    con = _conn()
    try:
        rows = con.execute(
            "SELECT user_id, email, name, role, is_active, created_at, last_login "
            "FROM dq_users ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def deactivate_user(email: str) -> None:
    con = _conn()
    try:
        con.execute("UPDATE dq_users SET is_active=0 WHERE email=?",
                    (email.strip().lower(),))
        con.commit()
    finally:
        con.close()


# ── institution linking ────────────────────────────────────────────────────────

def get_user_institutions(user_id: str) -> list[str]:
    """Return list of le_book codes linked to this user."""
    ensure_users_table()
    con = _conn()
    try:
        rows = con.execute(
            "SELECT le_book FROM dq_user_institutions WHERE user_id=? ORDER BY le_book",
            (user_id,)
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def get_users_by_le_book(le_book: str) -> list[dict]:
    """Return all active inst_users linked to this le_book."""
    ensure_users_table()
    con = _conn()
    try:
        rows = con.execute("""
            SELECT u.user_id, u.email, u.name, u.role
            FROM dq_users u
            JOIN dq_user_institutions ui ON u.user_id = ui.user_id
            WHERE ui.le_book = ? AND u.is_active = 1 AND u.role = 'inst_user'
        """, (le_book,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def set_user_institutions(user_id: str, le_books: list[str]) -> None:
    """Replace all institution links for this user."""
    con = _conn()
    try:
        con.execute("DELETE FROM dq_user_institutions WHERE user_id=?", (user_id,))
        if le_books:
            con.executemany(
                "INSERT OR IGNORE INTO dq_user_institutions (user_id, le_book) VALUES (?,?)",
                [(user_id, lb) for lb in le_books]
            )
        con.commit()
    finally:
        con.close()
