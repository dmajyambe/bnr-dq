"""
dq_monthly_detection.py — Monthly DQ detection pipeline.

Runs a full-table scan (no date window) across all dimensions:
completeness, accuracy, timeliness, validity, uniqueness, relationship.

For every failing (rule, institution, table) combination an issue is written
to dq_open_issues with sla_deadline = today + 30 days.  Issues that were
open and now pass are marked pending_resolution (confirmed by the daily
resolution scanner).

Run this once per month.  The daily resolution pipeline
(dq_resolution_pipeline.py) then re-checks open issues each day and
marks them resolved when the underlying data is fixed, or overdue when the
SLA deadline passes without a fix.
"""
from __future__ import annotations
import argparse
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

SCRIPT_DIR = Path(__file__).parent
ISSUE_REPORTS_DIR = SCRIPT_DIR / "issue_reports"
sys.path.insert(0, str(SCRIPT_DIR))
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_monthly")

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

# db helper functions
def _db_columns(conn, schema: str, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()
    return {r[0].lower() for r in rows}


def _needed_columns(table: str) -> set[str]:
    from dq_rules import MANDATORY_COLUMNS, VALIDITY_COLUMNS
    cols: set[str] = {"le_book"}
    cols.update(MANDATORY_COLUMNS.get(table, []))
   # cols.update(VALIDITY_COLUMNS.get(table, []))
    return cols

# load tables
def load_full_tables(engine, schema: str, valid_le_books: frozenset,
                     tables: list[str] | None = None,
                     limit: int = 0) -> dict:
    """Load every row for each table — no date filter, le_book filter only.
    tables: restrict to this subset (default: all TABLES)
    limit:  row cap per table for testing (0 = no limit)
    """
    import pandas as pd

    target = tables if tables else TABLES
    dataframes: dict = {}
    with engine.connect() as conn:
        conn.execute(text("SET work_mem = '512MB'"))
        for table in target:
            needed   = _needed_columns(table)
            existing = _db_columns(conn, schema, table)
            cols     = sorted(needed & existing) #get needed columns that actually exist in the table

            if not cols:
                log.warning("  %s: no matching columns — skipping", table)
                dataframes[table] = pd.DataFrame()
                continue

            quoted = ", ".join(f'"{c}"' for c in cols)
            lb_sql = ""
            if valid_le_books and "le_book" in cols:
                books  = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
                lb_sql = f' WHERE "le_book" IN ({books})'

            limit_sql = f" LIMIT {limit}" if limit > 0 else ""
            sql = f'SELECT {quoted} FROM "{schema}"."{table}"{lb_sql}{limit_sql}'
            try:
                df = pd.read_sql(text(sql), conn)
                df.columns = [c.lower() for c in df.columns]
                df["data_processed"] = datetime.now().isoformat()
                log.info("  %-30s  %d rows × %d cols%s", table, len(df), len(df.columns),
                         f"  [LIMIT {limit}]" if limit else "")
                dataframes[table] = df
            except Exception as exc:
                log.error("  %s: load failed — %s", table, exc)
                dataframes[table] = pd.DataFrame()

    return dataframes

#commented out for now ( for testing)
# def load_parent_keys(engine, schema: str,
#                      valid_le_books: frozenset | None = None) -> dict:
#     """Load full parent key sets for relationship checks."""
#     import pandas as pd
#     from dq_rules import REL_RULE_META

#     parent_cols: dict[str, set] = {}
#     for meta in REL_RULE_META.values():
#         parent_cols.setdefault(meta["parent_table"], set()).add(meta["parent_col"])

#     result: dict = {}
#     with engine.connect() as conn:
#         for table, cols in sorted(parent_cols.items()):
#             quoted = ", ".join(f'"{c}"' for c in sorted(cols))
#             lb_sql = ""
#             if valid_le_books:
#                 existing = _db_columns(conn, schema, table)
#                 if "le_book" in existing:
#                     books  = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
#                     lb_sql = f' WHERE "le_book" IN ({books})'
#             sql    = f'SELECT DISTINCT {quoted} FROM "{schema}"."{table}"{lb_sql}'
#             try:
#                 df = pd.read_sql(text(sql), conn)
#                 df.columns = [c.lower() for c in df.columns]
#                 log.info("  parent keys %-30s  %d keys", table, len(df))
#                 result[table] = df
#             except Exception as exc:
#                 log.warning("  parent keys %s: failed — %s", table, exc)
#                 result[table] = pd.DataFrame()

#     return result


# issue reports

def _append_table_to_zips(lb_values: list, table: str, df_full,
                           categories: dict, month: str) -> None:
    """
    Write one CSV entry per institution into their on-disk ZIP, immediately
    after each table is processed.  Avoids accumulating all CSV bytes in RAM.
    """
    import zipfile
    from dq_issue_export import _collect_failing_df, _enrich_csv_df, TABLE_CSV_COLS

    ISSUE_REPORTS_DIR.mkdir(exist_ok=True)

    # Trim each failing-row chunk to output columns only — avoids holding
    # wide full-column copies in RAM for tables with many rule/null columns.
    _derived = {"stakeholder_name", "category_type", "issue_type"}
    _template = TABLE_CSV_COLS.get(table, [])
    output_cols = ["le_book"] + [c for c in _template
                                 if c not in _derived and c != "le_book"]

    for lb in lb_values:
        inst_df = df_full[df_full["le_book"].astype(str) == lb].reset_index(drop=True)
        if inst_df.empty:
            continue
        try:
            combined = _collect_failing_df(table=table, df=inst_df,
                                           all_frames={table: inst_df},
                                           parent_frames=None,
                                           categories=categories,
                                           output_cols=output_cols)
            if combined.empty:
                continue
            combined  = _enrich_csv_df(combined, table, categories)
            csv_bytes = combined.to_csv(index=False,
                                        encoding="utf-8-sig").encode("utf-8-sig")
            del combined
            zip_path = ISSUE_REPORTS_DIR / f"{lb}_{month}.zip"
            with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{table}.csv", csv_bytes)
            log.info("  ZIP %-6s  %s.csv  %d bytes", lb, table, len(csv_bytes))
        except Exception as exc:
            log.warning("Issue report collection failed %s/%s: %s", lb, table, exc)


# engine runner

def _run(eng_module, dataframes: dict, valid_le_books: frozenset,
         extra_kwargs: dict | None = None) -> dict:
    """Run evaluate_from_dataframes() for one engine; discard temp JSON output."""
    tmp = tempfile.mkstemp(suffix=".json")
    kwargs = extra_kwargs or {}
    try:
        return eng_module.evaluate_from_dataframes(
            dataframes, valid_le_books, tmp, **kwargs)
    except Exception as exc:
        log.error("  %s engine failed: %s", eng_module.__name__, exc)
        return {}
    finally:
        try:
            Path(tmp).unlink(missing_ok=True)
        except Exception:
            pass


# orchestration

def run_monthly_detection(engine, schema: str, run_date: str,
                          tables: list[str] | None = None,
                          limit: int = 0) -> None:
    """
    Full-table DQ scan across all dimensions.
    Writes results to dq_open_issues via detect_and_update_issues().

    Loads one table at a time to avoid holding all DataFrames in memory
    simultaneously.

    tables: restrict to a subset of tables (testing)
    limit:  row cap per table, 0 = no limit (testing)
    """
    import gc
    import completeness_check as comp_eng
    import validity_check     as val_eng
    import pandas as pd
    from dq_pipeline_2m   import (fetch_valid_le_books, fetch_le_book_categories,
                                   _customer_dup_counts, _build_history_entry,
                                   _append_history)
   # from db_utils import get_valid_le_books, build_connection_string
    from dq_issue_tracker import detect_and_update_issues, ensure_tables

    ensure_tables()

    log.info("Fetching institution metadata …")
    valid_le_books = fetch_valid_le_books(engine, schema)
    categories     = fetch_le_book_categories(engine, schema)

    target = tables or TABLES
    if tables:
        log.info("Table filter: %s", ", ".join(tables))
    if limit:
        log.info("Row limit: %d per table (TEST MODE)", limit)

    def _empty_report() -> dict:
        return {"generated_at": datetime.now().isoformat(timespec="seconds"),
                "tables": {}, "warnings": {}}

    comp_report = _empty_report()
    val_report  = _empty_report()
    rel_report: dict = {"generated_at": datetime.utcnow().isoformat(),
                        "tables": {}, "le_books": [], "warnings": {}}

    # ZIPs are written incrementally per table — no in-memory accumulation.
    month = run_date[:7]
    # Remove any stale ZIPs for this month so a re-run starts clean.
    ISSUE_REPORTS_DIR.mkdir(exist_ok=True)
    for old in ISSUE_REPORTS_DIR.glob(f"*_{month}.zip"):
        old.unlink()
        log.info("Removed stale ZIP: %s", old.name)

    # Parent key sets are key columns only (DISTINCT) — much smaller than full tables.
    #log.info("Loading parent key sets …")
    log.info("skipped loading parent key sets for now (testing)")
    #parent_frames = load_parent_keys(engine, schema, valid_le_books)

    try:
        import relationship_check as rel_eng
        _have_rel = True
    except ImportError:
        _have_rel = False
        log.warning("relationship_check not found — skipping RI checks")

    valid_lb_strs = {str(lb) for lb in valid_le_books} if valid_le_books else set()

    for table in target:
        log.info("Loading table: %s …", table)
        single_df = load_full_tables(engine, schema, valid_le_books,
                                     tables=[table], limit=limit)

        # ── single-table dimension checks ────────────────────────────────────
        for eng, accum in [
            (comp_eng, comp_report),
            (val_eng,  val_report),
        ]:
            partial = _run(eng, single_df, valid_le_books)
            if partial:
                # Only carry forward evaluated tables — skip "no_data" entries so
                # earlier tables aren't overwritten when a later partial re-emits them
                # as no_data (each partial covers all target tables, not just the one loaded).
                for tbl, tdata in partial.get("tables", {}).items():
                    if tdata.get("status") == "evaluated":
                        accum["tables"][tbl] = tdata
                accum["warnings"].update(partial.get("warnings", {}))

        # ── relationship checks for this child table ─────────────────────────
        # if _have_rel:
        #     try:
        #         partial = rel_eng.evaluate_all_from_dataframes(
        #             single_df, valid_le_books, parent_frames)
        #         if partial:
        #             rel_report["tables"].update(partial.get("tables", {}))
        #             rel_report["warnings"].update(partial.get("warnings", {}))
        #             rel_report["le_books"] = list(
        #                 set(rel_report["le_books"]) | set(partial.get("le_books", [])))
        #     except Exception as exc:
        #         log.warning("Relationship check for %s failed: %s", table, exc)

        # ── write failing rows directly into per-institution ZIPs ───────────
        df_full = single_df.get(table, pd.DataFrame())
        if not df_full.empty and "le_book" in df_full.columns:
            lb_values = sorted(df_full["le_book"].dropna().astype(str).unique())
            if valid_lb_strs:
                lb_values = [lb for lb in lb_values if lb in valid_lb_strs]
            _append_table_to_zips(lb_values, table, df_full, categories, month)

        del single_df, df_full
        gc.collect()

    R = {
        "comp": comp_report,
        "val":  val_report,
    }

    log.info("Writing issues to tracker …")
    detect_and_update_issues(R, categories, run_date)

    log.info("Computing customer duplicate counts …")
    dup_counts, cat_dup_counts = _customer_dup_counts(engine, schema, valid_le_books)
    log.info("  %d institution(s) with duplicate customers", len(dup_counts))

    log.info("Writing history entry …")
    entry = _build_history_entry(run_date, R, categories, dup_counts, cat_dup_counts)
    _append_history(entry)

    log.info("Monthly detection complete — run_date=%s", run_date)


# cli

if __name__ == "__main__":
    load_dotenv(SCRIPT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="BNR DQ Monthly Detection."
    )
    parser.add_argument("--schema", default=os.environ.get("DQ_SCHEMA", "dqp"),
                        help="PostgreSQL schema (default: dqp)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        metavar="YYYY-MM-DD",
                        help="Run date used as detected_at (default: today)")
    parser.add_argument("--tables", nargs="+", default=None,
                        metavar="TABLE",
                        help="Restrict to these tables only (testing)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Row cap per table, 0 = no limit (testing)")
    args = parser.parse_args()

    from db_utils import get_engine, build_connection_string
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
                          tables=args.tables, limit=args.limit)

    log.info("=" * 60)
    log.info("Done.")
    log.info("=" * 60)
    sys.exit(0)
