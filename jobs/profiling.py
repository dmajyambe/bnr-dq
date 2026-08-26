#job for profiling tables and columns in the DQ schema
from __future__ import annotations

import logging

log = logging.getLogger("jobs.profiling")


def run_profiling(
    engine,
    schema: str,
    run_date: str,
    valid_le_books: frozenset,
    keep_runs: int = 3,
) -> None:
    """Profile every table for every institution and persist results.

    engine       : SQLAlchemy engine connected to Greenplum (dqp schema)
    schema       : Greenplum schema name (e.g. "dqp")
    run_date     : ISO date string for this run (YYYY-MM-DD)
    valid_le_books: institutions to include
    keep_runs    : number of historical runs to retain per (le_book, table)
    """
    from dq.profiling.engine import profile_table
    from dq.profiling.storage import write_profiles
    from dq.rules.completeness import MANDATORY_COLUMNS

    tables = list(MANDATORY_COLUMNS.keys())
    log.info("Profiling %d table(s) for %d institution(s) — run %s",
             len(tables), len(valid_le_books), run_date)

    for table in tables:
        log.info("  → %s", table)
        try:
            with engine.connect() as conn:
                profiles = profile_table(
                    conn, schema, table, valid_le_books, run_date
                )
            if profiles:
                write_profiles(profiles, keep_runs=keep_runs)
                log.info("    %s: %d records written", table, len(profiles))
            else:
                log.warning("    %s: no profiles generated", table)
        except Exception as exc:
            log.error("    %s: profiling failed — %s", table, exc)

    log.info("Profiling complete for run %s", run_date)
