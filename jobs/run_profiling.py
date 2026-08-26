"""Stand-alone profiling pipeline.

Profiles every table for the specified institutions and persists results to
dq_column_profiles (SQLite), retaining the last 3 run dates per
(le_book, table_name).

Usage examples
--------------
# Full run for today's date:
python -m jobs.run_profiling --schema dqp

# Scope to one institution and a specific date (useful for backfilling):
python -m jobs.run_profiling --schema dqp --date 2026-07-01 --le-book 010
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

load_dotenv(SCRIPT_DIR / ".env")

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("jobs.run_profiling")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BNR DQ Column Profiling Pipeline.")
    parser.add_argument("--schema", default=os.environ.get("DQ_SCHEMA", "dqp"),
                        help="Greenplum schema (default: dqp)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        metavar="YYYY-MM-DD",
                        help="Run date label stored with results (default: today)")
    parser.add_argument("--le-book", nargs="+", default=None, metavar="CODE",
                        help="Restrict to these institution codes only (testing)")
    parser.add_argument("--keep-runs", type=int, default=3,
                        help="Historical runs to retain per (le_book, table) (default: 3)")
    args = parser.parse_args()

    from storage.postgres.connection import build_connection_string, get_engine
    from storage.postgres.institutions import get_valid_le_books
    from jobs.profiling import run_profiling

    engine        = get_engine(build_connection_string())
    valid_le_books = (
        frozenset(args.le_book)
        if args.le_book
        else get_valid_le_books(engine, args.schema)
    )

    log.info("=" * 60)
    log.info("Profiling pipeline started  schema=%s  date=%s", args.schema, args.date)
    log.info("  Institutions : %d", len(valid_le_books))
    log.info("  Keep runs    : %d", args.keep_runs)
    log.info("=" * 60)

    run_profiling(
        engine=engine,
        schema=args.schema,
        run_date=args.date,
        valid_le_books=valid_le_books,
        keep_runs=args.keep_runs,
    )
