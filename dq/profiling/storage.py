"""Greenplum persistence for column profiles.

Keeps the last `keep_runs` run dates per (le_book, table_name).
Older runs are pruned automatically after each write.
"""
from __future__ import annotations

import logging

from storage.postgres.app_db import get_connection

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
    con = get_connection()
    try:
        # Delete any existing rows for the same (le_book, table, column, run_date)
        # before re-inserting — Greenplum does not support ON CONFLICT.
        con.executemany(
            "DELETE FROM dq_column_profiles "
            "WHERE le_book=%(le_book)s AND table_name=%(table_name)s "
            "AND column_name=%(column_name)s AND run_date=%(run_date)s",
            profiles,
        )
        con.executemany("""
            INSERT INTO dq_column_profiles
                (le_book, table_name, column_name, run_date, row_count,
                 null_count, null_pct, distinct_count, distinct_pct,
                 min_val, max_val, top_values, data_type)
            VALUES
                (%(le_book)s, %(table_name)s, %(column_name)s, %(run_date)s, %(row_count)s,
                 %(null_count)s, %(null_pct)s, %(distinct_count)s, %(distinct_pct)s,
                 %(min_val)s, %(max_val)s, %(top_values)s, %(data_type)s)
        """, profiles)

        # Prune old runs — keep only the most recent `keep_runs` per (le_book, table_name)
        pairs = {(p["le_book"], p["table_name"]) for p in profiles}
        for lb, tbl in pairs:
            keep = [r["run_date"] for r in con.execute(
                "SELECT DISTINCT run_date FROM dq_column_profiles "
                "WHERE le_book=%s AND table_name=%s ORDER BY run_date DESC LIMIT %s",
                (lb, tbl, keep_runs),
            ).fetchall()]
            if keep:
                ph = ",".join(["%s"] * len(keep))
                con.execute(
                    f"DELETE FROM dq_column_profiles "
                    f"WHERE le_book=%s AND table_name=%s AND run_date NOT IN ({ph})",
                    (lb, tbl, *keep),
                )

        con.commit()
        log.info("Wrote %d profile records (%d run(s) retained per table/institution)",
                 len(profiles), keep_runs)
    finally:
        con.close()


def available_run_dates(le_book: str, table: str) -> list[str]:
    """Return up to keep_runs run dates for this institution+table, newest first."""
    ensure_table()
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT DISTINCT run_date FROM dq_column_profiles "
            "WHERE le_book=%s AND table_name=%s ORDER BY run_date DESC LIMIT %s",
            (le_book, table, _KEEP_RUNS),
        ).fetchall()
        return [r["run_date"] for r in rows]
    except Exception:
        return []
    finally:
        con.close()


def load_profile(le_book: str, table: str, run_date: str) -> list[dict]:
    """Return column profiles for one institution+table+run, ordered by column name."""
    ensure_table()
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT * FROM dq_column_profiles "
            "WHERE le_book=%s AND table_name=%s AND run_date=%s "
            "ORDER BY column_name",
            (le_book, table, run_date),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        con.close()
