"""Greenplum persistence for column profiles.

Keeps the last `keep_runs` run dates per (le_book, table_name).
Older runs are pruned automatically after each write.
"""
from __future__ import annotations

import logging

from sqlalchemy import text
from storage.postgres.connection import get_engine

log = logging.getLogger("dq.profiling.storage")

_KEEP_RUNS = 3


def ensure_table() -> None:
    from storage.postgres.init_tables import init_all
    init_all()


def write_profiles(profiles: list[dict], keep_runs: int = _KEEP_RUNS) -> None:
    """Upsert profile records and prune runs beyond keep_runs per (le_book, table)."""
    if not profiles:
        return
    ensure_table()
    with get_engine().begin() as con:
        # Delete any existing rows for the same (le_book, table, column, run_date)
        # before re-inserting — Greenplum does not support ON CONFLICT.
        con.execute(
            text(
                "DELETE FROM dq_column_profiles "
                "WHERE le_book=:le_book AND table_name=:table_name "
                "AND column_name=:column_name AND run_date=:run_date"
            ),
            profiles,
        )
        con.execute(
            text("""
            INSERT INTO dq_column_profiles
                (le_book, table_name, column_name, run_date, row_count,
                 null_count, null_pct, distinct_count, distinct_pct,
                 min_val, max_val, top_values, data_type)
            VALUES
                (:le_book, :table_name, :column_name, :run_date, :row_count,
                 :null_count, :null_pct, :distinct_count, :distinct_pct,
                 :min_val, :max_val, :top_values, :data_type)
        """),
            profiles,
        )

        # Prune old runs — keep only the most recent `keep_runs` per (le_book, table_name)
        pairs = {(p["le_book"], p["table_name"]) for p in profiles}
        for lb, tbl in pairs:
            keep = [r["run_date"] for r in con.execute(
                text(
                    "SELECT DISTINCT run_date FROM dq_column_profiles "
                    "WHERE le_book=:lb AND table_name=:tbl ORDER BY run_date DESC LIMIT :n"
                ),
                {"lb": lb, "tbl": tbl, "n": keep_runs},
            ).mappings().fetchall()]
            if keep:
                from sqlalchemy import bindparam
                con.execute(
                    text(
                        "DELETE FROM dq_column_profiles "
                        "WHERE le_book=:lb AND table_name=:tbl AND run_date NOT IN :keep"
                    ).bindparams(bindparam("keep", expanding=True)),
                    {"lb": lb, "tbl": tbl, "keep": keep},
                )

        log.info("Wrote %d profile records (%d run(s) retained per table/institution)",
                 len(profiles), keep_runs)


def available_run_dates(le_book: str, table: str) -> list[str]:
    """Return up to keep_runs run dates for this institution+table, newest first."""
    ensure_table()
    try:
        with get_engine().connect() as con:
            rows = con.execute(
                text(
                    "SELECT DISTINCT run_date FROM dq_column_profiles "
                    "WHERE le_book=:lb AND table_name=:tbl ORDER BY run_date DESC LIMIT :n"
                ),
                {"lb": le_book, "tbl": table, "n": _KEEP_RUNS},
            ).mappings().fetchall()
            return [r["run_date"] for r in rows]
    except Exception:
        return []


def load_profile(le_book: str, table: str, run_date: str) -> list[dict]:
    """Return column profiles for one institution+table+run, ordered by column name."""
    ensure_table()
    try:
        with get_engine().connect() as con:
            rows = con.execute(
                text(
                    "SELECT * FROM dq_column_profiles "
                    "WHERE le_book=:lb AND table_name=:tbl AND run_date=:run_date "
                    "ORDER BY column_name"
                ),
                {"lb": le_book, "tbl": table, "run_date": run_date},
            ).mappings().fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []
