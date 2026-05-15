"""
db_utils.py — Shared DB connection and institution-scope helpers.

Imported by all DQ engine modules to eliminate duplication.
"""
from __future__ import annotations
import logging
import os
import sys

from sqlalchemy import create_engine, text

log = logging.getLogger("dq_db_utils")

CATEGORY_TYPES = ("MF", "SACCO", "OSACCO", "B")


def build_connection_string() -> str:
    required = [
        "MY_POSTGRES_USERNAME", "MY_POSTGRES_PASSWORD",
        "MY_POSTGRES_HOST", "MY_POSTGRES_PORT", "MY_POSTGRES_DB",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)
    u, pw, h, p, db = (os.environ[k] for k in required)
    return f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}"


def get_engine(conn_str: str):
    try:
        engine = create_engine(
            conn_str, pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except ImportError:
        log.error("sqlalchemy or psycopg2-binary not installed.")
        sys.exit(1)
    except Exception as exc:
        log.error("Cannot connect to database: %s", exc)
        sys.exit(1)


def get_valid_le_books(engine, schema: str) -> frozenset:
    """Return le_book codes whose category_type is in CATEGORY_TYPES."""
    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)
    sql = text(f"""
        SELECT DISTINCT lb.le_book
        FROM "{schema}".le_book lb
        LEFT JOIN (
            SELECT alpha_tab     AS category_type_at,
                   alpha_sub_tab AS category_type
            FROM   "{schema}".alpha_sub_tab
        ) ast ON lb.category_type_at = ast.category_type_at
             AND lb.category_type    = ast.category_type
        WHERE ast.category_type IN ({filter_list})
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        result = frozenset(str(r[0]).strip() for r in rows if r[0] is not None)
        log.info("Category filter %s → %d valid le_books", CATEGORY_TYPES, len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch valid le_books: %s — no filter applied.", exc)
        return frozenset()
