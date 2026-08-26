"""Greenplum connection for application tables (dq_open_issues, dq_users, etc.).

Provides a thin wrapper that mimics the SQLite connection interface so all
supersedes storage.sqlite.connection (deleted after Greenplum cutover).

Differences handled by the wrapper:
  - ? placeholders  →  %s
  - executescript() splits on ; and runs each statement individually
  - row[0] positional access: callers that need this must use named columns
  - Rows are RealDictRow objects (subscript by column name, iterate like dict)
"""
from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("storage.postgres.app_db")


def _build_dsn() -> str:
    required = [
        "MY_POSTGRES_USERNAME", "MY_POSTGRES_PASSWORD",
        "MY_POSTGRES_HOST", "MY_POSTGRES_PORT", "MY_POSTGRES_DB",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing env vars for app_db: %s", ", ".join(missing))
        sys.exit(1)
    u, pw, h, p, db = (os.environ[k] for k in required)
    return f"host={h} port={p} dbname={db} user={u} password={pw} connect_timeout=10"


class _Cursor:
    """Wraps a psycopg2 RealDictCursor with a SQLite-compatible interface."""

    def __init__(self, cur):
        self._cur = cur

    @property
    def rowcount(self):
        return self._cur.rowcount

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur)


class _Conn:
    """Wraps a psycopg2 connection with a SQLite-style interface.

    Converts ? → %s in all SQL so callers don't need to change placeholder
    style when migrating from SQLite.  Sets search_path to dqp on open so
    table references don't need schema-qualification.
    """

    def __init__(self, pg_conn):
        import psycopg2.extras
        self._conn = pg_conn
        self._extras = psycopg2.extras
        with self._conn.cursor() as cur:
            cur.execute("SET search_path TO dqp")

    @staticmethod
    def _sql(sql: str) -> str:
        return sql.replace("?", "%s")

    def execute(self, sql: str, params=None):
        cur = self._conn.cursor(cursor_factory=self._extras.RealDictCursor)
        cur.execute(self._sql(sql), params or ())
        return _Cursor(cur)

    def executemany(self, sql: str, param_list):
        cur = self._conn.cursor()
        cur.executemany(self._sql(sql), param_list)
        return _Cursor(cur)

    def executescript(self, sql: str):
        """Run multiple semicolon-separated DDL statements (SQLite compatibility)."""
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s:
                with self._conn.cursor() as cur:
                    cur.execute(s)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self.rollback()
        self.close()


def get_connection() -> _Conn:
    """Return an open _Conn wrapping a psycopg2 connection to dqp."""
    import psycopg2
    pg = psycopg2.connect(_build_dsn())
    return _Conn(pg)
