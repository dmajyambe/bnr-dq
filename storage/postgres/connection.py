from __future__ import annotations

import logging
import os
import sys
from threading import Lock
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger("storage.postgres")
SCHEMA = "dqp"

_engine: Engine | None = None
_engine_lock = Lock()


def build_connection_string() -> str:
    """Construct a URL-safe PostgreSQL connection string from environment variables."""
    required = [
        "MY_POSTGRES_USERNAME",
        "MY_POSTGRES_PASSWORD",
        "MY_POSTGRES_HOST",
        "MY_POSTGRES_PORT",
        "MY_POSTGRES_DB",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)

    # Safely URL-encode credentials to support special characters in passwords/usernames
    u = quote_plus(os.environ["MY_POSTGRES_USERNAME"])
    pw = quote_plus(os.environ["MY_POSTGRES_PASSWORD"])
    h = os.environ["MY_POSTGRES_HOST"]
    p = os.environ["MY_POSTGRES_PORT"]
    db = os.environ["MY_POSTGRES_DB"]

    return f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}"


def get_engine(conn_str: str | None = None) -> Engine:
    """Return a global SQLAlchemy engine singleton using double-checked locking."""
    global _engine

    # Fast path for existing engine without lock overhead
    if _engine is not None:
        return _engine

    with _engine_lock:
        # Double-check locking to prevent multiple engines from being created in multi-threaded contexts
        if _engine is not None:
            return _engine

        if conn_str is None:
            conn_str = build_connection_string()

        try:
            engine = create_engine(
                conn_str,
                pool_pre_ping=True,
                connect_args={
                    "connect_timeout": 10,
                    "options": f"-c search_path={SCHEMA}",
                },
            )
            # Test connection before caching global engine reference
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            _engine = engine
            return _engine

        except ImportError:
            log.error("sqlalchemy or psycopg2-binary not installed.")
            sys.exit(1)
        except Exception as exc:
            # Dispose of the engine resources if initialization fails
            if "engine" in locals():
                engine.dispose()
            log.error("Cannot connect to database: %s", exc)
            sys.exit(1)