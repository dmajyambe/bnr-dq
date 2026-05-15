"""
dq_auth.py — User authentication for the BNR DQ Dashboard.

Rules:
  - Email must be @bnr.rw — no external providers accepted.
  - Passwords are stored as SHA-256(salt + password); salt is per-user random hex.
  - Roles: admin (can manage users, approve rules) | viewer (read-only).
  - Users table lives in the same dq_rules.db SQLite used for rules and issues.
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


# ── SQLite helpers ─────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def ensure_users_table() -> None:
    con = _conn()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_users (
                user_id      TEXT PRIMARY KEY,
                email        TEXT UNIQUE NOT NULL,
                name         TEXT NOT NULL DEFAULT '',
                salt         TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role         TEXT NOT NULL DEFAULT 'viewer',
                is_active    INTEGER NOT NULL DEFAULT 1,
                created_at   TEXT NOT NULL,
                last_login   TEXT
            )
        """)
        con.commit()
    finally:
        con.close()


# ── domain validation ──────────────────────────────────────────────────────────

def is_valid_bnr_email(email: str) -> bool:
    """Return True only for non-empty @bnr.rw addresses."""
    if not email or "@" not in email:
        return False
    local, _, domain = email.strip().lower().rpartition("@")
    return bool(local) and domain == ALLOWED_DOMAIN


# ── password helpers ───────────────────────────────────────────────────────────

def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _new_salt() -> str:
    return secrets.token_hex(32)


# ── user CRUD ──────────────────────────────────────────────────────────────────

def create_user(email: str, name: str, password: str,
                role: str = "viewer") -> None:
    """
    Create a new BNR user.  Raises ValueError for invalid email or duplicate.
    """
    ensure_users_table()
    email = email.strip().lower()
    if not is_valid_bnr_email(email):
        raise ValueError(f"Email must be @{ALLOWED_DOMAIN} — got: {email!r}")
    if role not in ("admin", "viewer"):
        raise ValueError(f"Role must be 'admin' or 'viewer' — got: {role!r}")
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters.")

    salt     = _new_salt()
    pw_hash  = _hash_password(password, salt)
    user_id  = secrets.token_hex(16)
    now      = datetime.now().isoformat(timespec="seconds")

    con = _conn()
    try:
        con.execute("""
            INSERT INTO dq_users
                (user_id, email, name, salt, password_hash, role, is_active, created_at)
            VALUES (?,?,?,?,?,?,1,?)
        """, (user_id, email, name, salt, pw_hash, role, now))
        con.commit()
        log.info("User created: %s (%s)", email, role)
    except sqlite3.IntegrityError:
        raise ValueError(f"User already exists: {email}")
    finally:
        con.close()


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


def verify_credentials(email: str, password: str) -> dict | None:
    """
    Return user dict on success, None on failure.
    Updates last_login on success.
    """
    if not email or not password:
        return None
    if not is_valid_bnr_email(email):
        return None

    user = get_user_by_email(email)
    if not user:
        return None

    expected = _hash_password(password, user["salt"])
    if not secrets.compare_digest(expected, user["password_hash"]):
        return None

    # Update last_login
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
