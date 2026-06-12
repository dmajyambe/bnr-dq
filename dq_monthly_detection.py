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
import json
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

from db_utils import CATEGORY_TYPES

SCRIPT_DIR = Path(__file__).parent
ISSUE_REPORTS_DIR = SCRIPT_DIR / "issue_reports"
HISTORY_FILE = SCRIPT_DIR / "dq_history.json"
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


# ── history aggregation (moved here from dq_pipeline_2m to decouple) ───────────
_SCORE_KEYS = {
    "comp": ("completeness", "overall_completeness_score", "completeness_score"),
    "acc":  ("accuracy",     "overall_accuracy_score",     "accuracy_score"),
    "tim":  ("timeliness",   "overall_timeliness_score",   "timeliness_score"),
    "val":  ("validity",     "overall_validity_score",     "validity_score"),
    "rel":  ("_rel",         "overall_ri_score",           "ri_score"),
}

DIMS = ["completeness", "accuracy", "timeliness", "validity"]


def _merge_rel(scores: dict) -> dict:
    """Average RI score into accuracy and drop the temporary _rel key."""
    if "_rel" not in scores:
        return scores
    acc = float(scores.get("accuracy") or 0.0)
    rel = float(scores.pop("_rel"))
    scores["accuracy"] = round((acc + rel) / 2, 2)
    return scores


def _inst_scores_from_report(report: dict, lb_score_key: str) -> dict[str, float]:
    """Average each le_book's per-table scores across all evaluated tables."""
    lb_table_scores: dict[str, list[float]] = {}
    for tbl_data in report.get("tables", {}).values():
        if tbl_data.get("status") != "evaluated":
            continue
        for lb, lb_data in tbl_data.get("le_book_breakdown", {}).items():
            s = lb_data.get(lb_score_key)
            if s is not None:
                lb_table_scores.setdefault(lb, []).append(float(s))
    return {
        lb: round(sum(scores) / len(scores), 2)
        for lb, scores in lb_table_scores.items()
        if scores
    }


def _build_history_entry(run_date: str, R: dict, categories: dict,
                         dup_counts: dict | None = None,
                         cat_dup_counts: dict | None = None) -> dict:
    """
    Aggregate engine results into a single history entry:
      overall        — one score per dimension (4 dims; RI averaged into accuracy)
      by_category    — average scores per category type (B, MF, OSACCO)
      by_institution — per-le_book scores across all 4 dimensions
    """
    # overall scores — extract all engines then merge RI into accuracy
    overall: dict[str, float] = {}
    for rkey, (dim, overall_key, _) in _SCORE_KEYS.items():
        esummary = (R.get(rkey) or {}).get("executive_summary") or {}
        overall[dim] = round(float(esummary.get(overall_key) or 0.0), 2)
    overall = _merge_rel(overall)

    # per-institution scores per dimension (extract then merge RI into accuracy)
    lb_dim_scores: dict[str, dict[str, float]] = {}
    for rkey, (dim, _, lb_score_key) in _SCORE_KEYS.items():
        inst_scores = _inst_scores_from_report(R.get(rkey) or {}, lb_score_key)
        for lb, score in inst_scores.items():
            lb_dim_scores.setdefault(lb, {})[dim] = score
    for lb in lb_dim_scores:
        lb_dim_scores[lb] = _merge_rel(lb_dim_scores[lb])

    # enrich with category metadata; compute per-institution overall (4 dims)
    _dups     = dup_counts or {}
    _cat_dups = cat_dup_counts or {}
    by_institution: dict = {}
    for lb, dim_scores in lb_dim_scores.items():
        cat_info    = categories.get(lb, {})
        inst_scores = [dim_scores[d] for d in DIMS if d in dim_scores]
        by_institution[lb] = {
            "name":               cat_info.get("name", lb),
            "category_type":      cat_info.get("category_type", ""),
            "overall":            round(sum(inst_scores) / len(inst_scores), 2) if inst_scores else 0.0,
            "customer_duplicates": _dups.get(lb, 0),
            **{d: dim_scores.get(d, 0.0) for d in DIMS},
        }

    # Institutions with dup counts but no windowed DQ data still need an entry
    for lb, dup in _dups.items():
        if lb not in by_institution:
            cat_info = categories.get(lb, {})
            by_institution[lb] = {
                "name":               cat_info.get("name", lb),
                "category_type":      cat_info.get("category_type", ""),
                "overall":            0.0,
                "customer_duplicates": dup,
                **{d: 0.0 for d in DIMS},
            }

    # by_category: average institution scores grouped by category_type
    cat_buckets: dict[str, list[dict]] = {}
    for inst_data in by_institution.values():
        ct = inst_data.get("category_type", "")
        if ct in CATEGORY_TYPES:
            cat_buckets.setdefault(ct, []).append(inst_data)

    by_category: dict = {}
    for ct, institutions in cat_buckets.items():
        cat_scores: dict[str, float] = {}
        for dim in DIMS:
            scores = [i[dim] for i in institutions if i.get(dim, 0) > 0]
            cat_scores[dim] = round(sum(scores) / len(scores), 2) if scores else 0.0
        # Use pre-computed DISTINCT count when available to avoid
        # double-counting customers with duplicates at multiple institutions.
        cat_scores["customer_duplicates"] = _cat_dups.get(
            ct, sum(i.get("customer_duplicates", 0) for i in institutions)
        )
        by_category[ct] = cat_scores

    # per-table failing rule counts and null column detail
    table_fail_counts = _table_fail_counts(R)
    table_null_cols   = _table_null_cols((R.get("comp") or {}))

    return {
        "date":              run_date,
        "overall":           overall,
        "by_category":       by_category,
        "by_institution":    by_institution,
        "table_fail_counts": table_fail_counts,
        "table_null_cols":   table_null_cols,
    }


def _table_fail_counts(R: dict) -> dict[str, int]:
    """Count failing rules (invalid > 0) per table across all engines."""
    from dq_rules import REL_RULE_META
    counts: dict[str, int] = {}

    # Completeness: 1 rule per table — fails when any null_cells > 0
    for tbl, tdata in (R.get("comp") or {}).get("tables", {}).items():
        if tdata.get("status") == "evaluated" and tdata.get("null_cells", 0) > 0:
            counts[tbl] = counts.get(tbl, 0) + 1

    # Accuracy, Timeliness, Validity, Uniqueness: rule-level breakdown
    for rkey in ("acc", "tim", "val", "uni"):
        for tbl, tdata in (R.get(rkey) or {}).get("tables", {}).items():
            if tdata.get("status") == "evaluated":
                for rdata in tdata.get("rules", {}).values():
                    if rdata.get("invalid", 0) > 0:
                        counts[tbl] = counts.get(tbl, 0) + 1

    # Relationship: credit child table
    for rid, rdata in (R.get("rel") or {}).get("rules", {}).items():
        if rdata.get("invalid", 0) > 0:
            child_tbl = REL_RULE_META.get(rid, {}).get("child_table")
            if child_tbl:
                counts[child_tbl] = counts.get(child_tbl, 0) + 1

    return counts


def _table_null_cols(comp_report: dict) -> dict[str, dict[str, int]]:
    """Return {table: {col: null_count}} for columns that have nulls."""
    result: dict[str, dict[str, int]] = {}
    for tbl, tdata in comp_report.get("tables", {}).items():
        if tdata.get("status") == "evaluated":
            nulls = {k: v for k, v in tdata.get("null_counts", {}).items() if v > 0}
            if nulls:
                result[tbl] = nulls
    return result


def _append_history(entry: dict) -> None:
    """Append (or replace same-date entry) in the history log."""
    history: list = []
    if HISTORY_FILE.exists():
        try:
            history = json.loads(HISTORY_FILE.read_text())
        except Exception:
            history = []
    # idempotent: replace if same date was already written today
    history = [e for e in history if e.get("date") != entry["date"]]
    history.append(entry)
    history.sort(key=lambda e: e.get("date", ""))
    HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str))
    log.info("History log updated → %s  (%d entries total)", HISTORY_FILE, len(history))


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
    from db_utils import get_valid_le_books, get_le_book_categories, customer_dup_counts
    from dq_issue_tracker import detect_and_update_issues, ensure_tables

    ensure_tables()

    log.info("Fetching institution metadata …")
    valid_le_books = get_valid_le_books(engine, schema)
    categories     = get_le_book_categories(engine, schema)

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
    dup_counts, cat_dup_counts = customer_dup_counts(engine, schema, valid_le_books)
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
