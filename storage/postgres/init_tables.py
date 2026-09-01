# Creating all the needed  tables(DDL)
from __future__ import annotations
import logging
log = logging.getLogger("storage.postgres.init_tables")

# DDL 
_TABLES = [
    # Active issues only
    """
    CREATE TABLE IF NOT EXISTS dq_open_issues (
        issue_id              TEXT PRIMARY KEY,
        le_book               TEXT NOT NULL,
        institution_name      TEXT,
        table_name            TEXT NOT NULL,
        rule_id               TEXT NOT NULL,
        rule_name             TEXT,
        dimension             TEXT NOT NULL,
        failing_rows          INTEGER NOT NULL DEFAULT 0,
        last_failing_rows     INTEGER,
        original_failing_rows INTEGER,
        detected_at          TEXT NOT NULL,
        sla_deadline          TEXT NOT NULL,
        urgency_band          TEXT NOT NULL DEFAULT 'new',
        assigned_to           TEXT,
        notified_at           TEXT,
        resolution_run_id     TEXT,
        recurrence_count      INTEGER NOT NULL DEFAULT 0,
        status                TEXT NOT NULL DEFAULT 'open'
    ) DISTRIBUTED BY (issue_id)
    """, 

    # Resolved (written once, never updated)
    """
    CREATE TABLE IF NOT EXISTS dq_resolved_issues (
        issue_id           TEXT NOT NULL,
        le_book            TEXT NOT NULL,
        institution_name   TEXT,
        table_name         TEXT NOT NULL,
        rule_id            TEXT NOT NULL,
        rule_name          TEXT,
        dimension          TEXT NOT NULL,
        failing_rows       INTEGER NOT NULL DEFAULT 0,
        last_failing_rows  INTEGER,
        detected_at        TEXT NOT NULL,
        sla_deadline       TEXT NOT NULL,
        resolved_at        TEXT NOT NULL,
        resolution_run_id  TEXT,
        recurrence_count   INTEGER NOT NULL DEFAULT 0,
        on_time            BOOLEAN,
        resolution_type    TEXT NOT NULL DEFAULT 'fixed'
    )
    WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=1)
    DISTRIBUTED BY (issue_id)
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_institution_contacts (
        le_book        TEXT PRIMARY KEY,
        contact_email  TEXT,
        contact_name   TEXT,
        updated_at     TEXT
    ) DISTRIBUTED REPLICATED
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_issue_evidence (
        le_book       TEXT NOT NULL,
        rule_id       TEXT NOT NULL,
        table_name    TEXT NOT NULL,
        run_date      TEXT NOT NULL,
        row_data      TEXT NOT NULL,
        snapshot_type TEXT NOT NULL DEFAULT 'original'
    ) DISTRIBUTED BY (le_book)
    """,

    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = 'dq_issue_evidence'
              AND indexname = 'idx_issue_evidence_lookup'
        ) THEN
            CREATE INDEX idx_issue_evidence_lookup
            ON dq_issue_evidence (le_book, rule_id, table_name);
        END IF;
    END$$;
    """,

    # Per-scan row counts for partial-resolution progress tracking
    """
    CREATE TABLE IF NOT EXISTS dq_issue_progress (
        issue_id     TEXT NOT NULL,
        scan_date    TEXT NOT NULL,
        failing_rows INTEGER NOT NULL,
        PRIMARY KEY (issue_id, scan_date)
    ) DISTRIBUTED BY (issue_id)
    """,

    # Migration: add new columns to pre-existing tables (safe to re-run)
    """
    ALTER TABLE dq_open_issues 
    ADD COLUMN IF NOT EXISTS original_failing_rows INTEGER;
    """,
    """
    ALTER TABLE dq_issue_evidence 
    ADD COLUMN IF NOT EXISTS snapshot_type TEXT NOT NULL DEFAULT 'original';
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_rules (
        rule_id    TEXT PRIMARY KEY,
        dimension  TEXT NOT NULL,
        category   TEXT,
        rule_name  TEXT NOT NULL,
        tables     TEXT NOT NULL,
        fields     TEXT
    ) DISTRIBUTED REPLICATED
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_column_profiles (
        le_book        TEXT NOT NULL,
        table_name     TEXT NOT NULL,
        column_name    TEXT NOT NULL,
        run_date       TEXT NOT NULL,
        row_count      INTEGER,
        null_count     INTEGER,
        null_pct       DOUBLE PRECISION,
        distinct_count INTEGER,
        distinct_pct   DOUBLE PRECISION,
        min_val        TEXT,
        max_val        TEXT,
        top_values     TEXT,
        data_type      TEXT,
        PRIMARY KEY (le_book, table_name, column_name, run_date)
    ) DISTRIBUTED BY (le_book)
    """,

    """
    ALTER TABLE dq_column_profiles 
    ADD COLUMN IF NOT EXISTS data_type TEXT;
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_change_requests (
        cr_id            TEXT PRIMARY KEY,
        title            TEXT NOT NULL,
        description      TEXT,
        issue_ids        TEXT NOT NULL DEFAULT '[]',
        le_book          TEXT NOT NULL,
        institution_name TEXT,
        dimension        TEXT,
        assigned_to      TEXT,
        created_by       TEXT,
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL,
        target_date      TEXT,
        status           TEXT NOT NULL DEFAULT 'open',
        reviewed_by      TEXT,
        reviewed_at      TEXT,
        review_notes     TEXT,
        failing_rows     INTEGER NOT NULL DEFAULT 0,
        tables           TEXT DEFAULT '[]',
        table_approvals  TEXT DEFAULT '{}'
    ) DISTRIBUTED BY (cr_id)
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_notifications (
        notif_id   TEXT PRIMARY KEY,
        user_id    TEXT NOT NULL,
        type       TEXT NOT NULL,
        message    TEXT NOT NULL,
        cr_id      TEXT,
        le_book    TEXT,
        is_read    INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    ) DISTRIBUTED REPLICATED
    """,

    """
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
    ) DISTRIBUTED REPLICATED
    """,

    """
    CREATE TABLE IF NOT EXISTS dq_user_institutions (
        user_id  TEXT NOT NULL,
        le_book  TEXT NOT NULL,
        PRIMARY KEY (user_id, le_book)
    ) DISTRIBUTED REPLICATED
    """,
]

_init_done = False

def init_all() -> None:
    """Create all application tables in dqp."""
    global _init_done
    if _init_done:
        return
    from sqlalchemy import text
    from storage.postgres.connection import get_engine

    with get_engine().begin() as con:
        for ddl in _TABLES:
            s = ddl.strip()
            if s:
                con.execute(text(s))
    log.info("All  tables ensured in dqp")
    _init_done = True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_all()