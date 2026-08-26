#orchestration job for monthly detection and reporting of data quality issues across institutions.
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

from dq.sql.filters import month_filter


load_dotenv()

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("jobs.monthly_detection")

# Tables the monthly job scores and exports ZIPs for.
TABLES = [
    "accounts",
    "customers_expanded",
    "contracts_disburse",
    "contract_loans",
    "contract_schedules",
    "contracts_expanded",
    "loan_applications_2",
    "prev_loan_applications",
]


def run_monthly_detection(engine, schema: str, run_date: str,
                          tables: list[str] | None = None,
                          limit: int = 0,
                          le_books: frozenset | None = None,
                          extra_where: str = "") -> None:
    import dq.engines.accuracy as acc_eng
    import dq.engines.completeness as comp_eng
    import dq.engines.uniqueness as uni_eng
    import dq.engines.validity as val_eng
    import dq.engines.timeliness as time_eng
    from dq.reports.history import build_history_entry
    from issues.tracker import detect_and_update_issues
    from issues import repositories as issue_repo
    from jobs.exports import write_monthly_zips
    from storage.files.history_store import append_history_entry
    from storage.postgres.institutions import (
        customer_dup_counts, get_le_book_categories, get_valid_le_books,
    ) 

    issue_repo.ensure_tables()

    log.info("Fetching institution metadata …")
    valid_le_books = le_books if le_books is not None else get_valid_le_books(engine, schema)
    categories     = get_le_book_categories(engine, schema)
    if le_books is not None:
        log.info("Institution filter: %s", ", ".join(sorted(valid_le_books)))

    if tables:
        log.info("Table filter: %s", ", ".join(tables))
    if limit:
        log.info("Row limit: %d per table (TEST MODE)", limit)

    # dimension scoring
    FULL_SCAN = 0
    wm: dict = {}
    log.info("Running completeness …")
    comp_report = comp_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                             str(SCRIPT_DIR / "dq_completeness_report.json"),
                                             row_limit=limit, tables=tables, extra_where=extra_where)
    log.info("Running accuracy …")
    acc_report  = acc_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                            str(SCRIPT_DIR / "dq_accuracy_report.json"),
                                            row_limit=limit, tables=tables, extra_where=extra_where)
    log.info("Running validity …")
    val_report  = val_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                            str(SCRIPT_DIR / "dq_validity_report.json"),
                                            row_limit=limit, tables=tables, extra_where=extra_where)
    log.info("Running uniqueness …")
    uni_report  = uni_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                            str(SCRIPT_DIR / "dq_uniqueness_report.json"),
                                            row_limit=limit, tables=tables, extra_where=extra_where)
    log.info("Running timeliness …")
    time_report = time_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                             str(SCRIPT_DIR / "dq_timeliness_report.json"),
                                             row_limit=limit, tables=tables, extra_where=extra_where)

    R = {"comp": comp_report, "acc": acc_report, "val": val_report,
         "uni": uni_report, "tim": time_report}

    log.info("Writing issues to tracker …")
    detect_and_update_issues(R, categories, run_date)

    log.info("Computing customer duplicate counts …")
    dup_counts, cat_dup_counts = customer_dup_counts(engine, schema, valid_le_books)
    log.info("  %d institution(s) with duplicate customers", len(dup_counts))

    log.info("Writing history entry …")
    entry = build_history_entry(run_date, R, categories, dup_counts, cat_dup_counts)
    if entry.get("by_institution"):
        append_history_entry(entry)
    else:
        log.warning("No institution data in scope (empty run) — skipping history entry "
                    "for run_date=%s (would otherwise render as a 0%% cliff / blank overview)",
                    run_date)

    # per-institution failing-row ZIPs
    month = run_date[:7] 
    write_monthly_zips(engine, schema, tables or TABLES, valid_le_books, categories,
                       month, limit, extra_where=extra_where)

    # Update pipeline_run.json so the dashboard "Data as of" banner reflects this run.
    import json as _json
    _pipeline_file = SCRIPT_DIR / "pipeline_run.json"
    _pipeline_file.write_text(_json.dumps({
        "run_date": run_date,
        "data_processed": datetime.utcnow().isoformat() + "+00:00",
        "mode": "sql",
        "le_books": sorted(valid_le_books),
    }, indent=2), encoding="utf-8")

    log.info("Monthly detection complete — run_date=%s", run_date)


#cli
if __name__ == "__main__":
    load_dotenv(SCRIPT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="BNR DQ Monthly Detection."
    )
    parser.add_argument("--schema", default=os.environ.get("DQ_SCHEMA", "dqp"),
                        help="PostgreSQL schema (default: dqp)") 
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        metavar="YYYY-MM-DD",
                        help="Run date for the detection pipeline (default: today)")
    parser.add_argument("--tables", nargs="+", default=None,
                        metavar="TABLE",
                        help="Restrict to these tables only (testing)") 
    parser.add_argument("--limit", type=int, default=0,
                        help="Row cap per table, 0 = no limit (testing)")
    parser.add_argument("--le-book", nargs="+", default=None, metavar="CODE",
                        help="Restrict to these institution codes only (testing)")
    parser.add_argument("--month", default=None, metavar="YYYY-MM",
                        help="Scope detection to one reporting month by "
                             "date_creation (e.g. 2026-05). Sets the run date "
                             "to the month-end date.")
    args = parser.parse_args() 

    # --month: restrict data to that calendar month (by date_creation) and
    # date the run at month-end so issues are tagged to the reporting period.
    extra_where = ""
    if args.month:
        extra_where, args.date = month_filter(args.month)

    from storage.postgres.connection import build_connection_string, get_engine
    engine = get_engine(build_connection_string())

    log.info("=" * 60)
    log.info("Monthly detection pipeline started  schema=%s  date=%s",
             args.schema, args.date)
    if args.tables:
        log.info("  Tables : %s", ", ".join(args.tables))
    if args.limit:
        log.info("  Limit  : %d rows/table  [TEST MODE]", args.limit)
    log.info("=" * 60)

    run_monthly_detection(engine, args.schema, args.date,
                          tables=args.tables, limit=args.limit,
                          le_books=frozenset(args.le_book) if args.le_book else None,
                          extra_where=extra_where)

    log.info("=" * 60)
    log.info("Done.")
    log.info("=" * 60)
    sys.exit(0)
