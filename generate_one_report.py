"""
Generate a DQ issue report for a single institution without running the full pipeline.
Usage:
    python generate_one_report.py --le-book 040
    python generate_one_report.py --le-book 040 --start-date 2026-01-01 --end-date 2026-05-12
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("gen_report")

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

# reuse pipeline helpers — no duplication
from dq_pipeline_2m import (
    _load_env,
    _build_conn_string,
    _get_engine,
    load_all_tables,
    fetch_le_book_categories,
    SCHEMA,
)
import dq_issue_export


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate DQ issue report for one institution")
    parser.add_argument("--le-book",    required=True, help="Institution le_book code, e.g. 040")
    parser.add_argument("--schema",     default=SCHEMA)
    parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--end-date",   default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--limit",      type=int, default=50_000,
                        help="Max rows per table (default 50 000; use 0 for no cap)")
    args = parser.parse_args()

    le_book = str(args.le_book).strip().zfill(3)   # normalise: "40" → "040"

    _load_env()
    engine = _get_engine(_build_conn_string())

    # resolve institution name
    log.info("Loading institution metadata …")
    categories = fetch_le_book_categories(engine, args.schema)
    cat_info   = categories.get(le_book, {})
    inst_name  = cat_info.get("name", le_book).title()

    if not cat_info:
        log.warning("le_book '%s' not found in categories — proceeding anyway.", le_book)

    log.info("Institution : %s  (le_book=%s, type=%s)",
             inst_name, le_book, cat_info.get("category_type", "?"))

    # load only this institution's rows (valid_le_books scoped to just this one)
    valid_le_books = frozenset({le_book})

    log.info("Loading table data …")
    dataframes = load_all_tables(
        engine, args.schema, valid_le_books,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    total_rows = sum(len(df) for df in dataframes.values())
    log.info("Total rows loaded: %d", total_rows)

    if total_rows == 0:
        log.error("No data found for le_book=%s. Check the date window or le_book code.", le_book)
        sys.exit(1)

    log.info("Generating issue report …")
    out_dir = SCRIPT_DIR / "reports"
    dq_issue_export.export_institution_issues(
        dataframes, categories, valid_le_books, out_dir,
    )

    # find the generated file and report its path
    matches = sorted(out_dir.glob(f"{le_book}_*.xlsx"))
    if matches:
        log.info("Done. Report written → %s", matches[0])
    else:
        log.warning("No output file found in %s", out_dir)


if __name__ == "__main__":
    main()
