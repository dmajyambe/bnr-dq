# User accounts, roles, and credential verification
from __future__ import annotations
import hashlib
import secrets
from datetime import datetime
from sqlalchemy import text
from storage.postgres.connection import get_engine

ALLOWED_DOMAIN = "bnr.rw"
BNR_ROLES  = {"bnr_admin", "bnr_viewer", "admin", "viewer", "bnr_executive"}
INST_ROLES = {"inst_user", "inst_executive"}
EXEC_ROLES = {"bnr_executive", "inst_executive"}   # tb added to the executive roles
ALL_ROLES  = BNR_ROLES | INST_ROLES


# schema: dq_users
def ensure_users_table() -> None:
    from storage.postgres.init_tables import init_all
    init_all()

# role validation and email domain checks
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


def is_executive(role: str) -> bool:
    return role in EXEC_ROLES


# password helpers

def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _new_salt() -> str:
    return secrets.token_hex(32)


# users

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

    try:
        with get_engine().begin() as con:
            con.execute(
                text("""
                INSERT INTO dq_users
                    (user_id, email, name, salt, password_hash, role, is_active, created_at)
                VALUES (:user_id,:email,:name,:salt,:pw_hash,:role,1,:now)
            """),
                {"user_id": user_id, "email": email, "name": name,
                 "salt": salt, "pw_hash": pw_hash, "role": role, "now": now},
            )
            if le_books:
                con.execute(
                    text("INSERT INTO dq_user_institutions (user_id, le_book) VALUES (:user_id,:lb)"),
                    [{"user_id": user_id, "lb": lb} for lb in le_books],
                )
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise ValueError(f"User already exists: {email}")
        raise

    return user_id


def get_user_by_email(email: str) -> dict | None:
    ensure_users_table()
    with get_engine().connect() as con:
        row = con.execute(
            text("SELECT * FROM dq_users WHERE email=:email AND is_active=1"),
            {"email": email.strip().lower()},
        ).mappings().fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id: str) -> dict | None:
    ensure_users_table()
    with get_engine().connect() as con:
        row = con.execute(
            text("SELECT * FROM dq_users WHERE user_id=:user_id AND is_active=1"),
            {"user_id": user_id},
        ).mappings().fetchone()
        return dict(row) if row else None


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

    with get_engine().begin() as con:
        con.execute(
            text("UPDATE dq_users SET last_login=:last_login WHERE user_id=:user_id"),
            {"last_login": datetime.now().isoformat(timespec="seconds"), "user_id": user["user_id"]},
        )

    return dict(user)

def change_password(email: str, new_password: str) -> None:
    if len(new_password) < 8:
        raise ValueError("Password must be at least 8 characters.")
    salt    = _new_salt()
    pw_hash = _hash_password(new_password, salt)
    with get_engine().begin() as con:
        con.execute(
            text("UPDATE dq_users SET salt=:salt, password_hash=:pw_hash WHERE email=:email"),
            {"salt": salt, "pw_hash": pw_hash, "email": email.strip().lower()},
        )


def force_reset_password(user_id: str) -> str:
    """Generate a temporary password, apply it, and return it (shown to BNR admin)."""
    temp_pw = secrets.token_urlsafe(10)
    salt    = _new_salt()
    pw_hash = _hash_password(temp_pw, salt)
    with get_engine().begin() as con:
        con.execute(
            text("UPDATE dq_users SET salt=:salt, password_hash=:pw_hash WHERE user_id=:user_id"),
            {"salt": salt, "pw_hash": pw_hash, "user_id": user_id},
        )
    return temp_pw


def list_users() -> list[dict]:
    ensure_users_table()
    with get_engine().connect() as con:
        rows = con.execute(
            text(
                "SELECT user_id, email, name, role, is_active, created_at, last_login "
                "FROM dq_users ORDER BY created_at"
            )
        ).mappings().fetchall()
        return [dict(r) for r in rows]


def deactivate_user(email: str) -> None:
    with get_engine().begin() as con:
        con.execute(
            text("UPDATE dq_users SET is_active=0 WHERE email=:email"),
            {"email": email.strip().lower()},
        )


# link institutions to users

def get_user_institutions(user_id: str) -> list[str]:
    """Return list of le_book codes linked to this user."""
    ensure_users_table()
    with get_engine().connect() as con:
        rows = con.execute(
            text("SELECT le_book FROM dq_user_institutions WHERE user_id=:user_id ORDER BY le_book"),
            {"user_id": user_id},
        ).mappings().fetchall()
        return [r["le_book"] for r in rows]


def get_users_by_le_book(le_book: str) -> list[dict]:
    """Return all active inst_users linked to this le_book."""
    ensure_users_table()
    with get_engine().connect() as con:
        rows = con.execute(
            text("""
            SELECT u.user_id, u.email, u.name, u.role
            FROM dq_users u
            JOIN dq_user_institutions ui ON u.user_id = ui.user_id
            WHERE ui.le_book = :lb AND u.is_active = 1 AND u.role = 'inst_user'
        """),
            {"lb": le_book},
        ).mappings().fetchall()
        return [dict(r) for r in rows]


def set_user_institutions(user_id: str, le_books: list[str]) -> None:
    """Replace all institution links for this user."""
    with get_engine().begin() as con:
        con.execute(
            text("DELETE FROM dq_user_institutions WHERE user_id=:user_id"),
            {"user_id": user_id},
        )
        if le_books:
            con.execute(
                text("INSERT INTO dq_user_institutions (user_id, le_book) VALUES (:user_id,:lb)"),
                [{"user_id": user_id, "lb": lb} for lb in le_books],
            )
