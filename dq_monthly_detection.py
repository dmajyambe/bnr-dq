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
        # Relationship not run (no report) → don't fold a phantom 0 into accuracy
        # (that would halve it via _merge_rel). Skip rel entirely when absent.
        if rkey == "rel" and not esummary:
            continue
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


def _zip_failing_rows_sql(engine, schema: str, table: str,
                          valid_le_books: frozenset, categories: dict,
                          month: str, limit: int = 0) -> None:
    """Stream FAILING rows across all dimensions (completeness/accuracy/validity/
    uniqueness) for `table` from SQL, grouped by institution, and append a
    per-institution CSV into their monthly ZIP.

    Memory-safe: server-side filter + streamed result written straight to a temp
    file per institution (no full-table load, no large in-RAM buffer).

    CSV columns: identifiers (left) → issue_type → failing rule columns (rightmost),
    with stakeholder_name/category_type inserted after le_book.
    """
    import csv
    import os
    import tempfile
    import zipfile
    from failing_rows_sql import build_failing_union

    with engine.connect() as conn:
        existing = _db_columns(conn, schema, table)
        built = build_failing_union(schema, table, existing, valid_le_books, limit)
        if not built:
            return
        sql, out_cols = built

        # header: le_book, stakeholder_name, category_type, <other ids>, issue_type, <fail cols>
        header: list[str] = []
        for c in out_cols:
            header.append(c)
            if c == "le_book":
                header += ["stakeholder_name", "category_type"]

        def _row_values(m: dict, name: str, ctype: str) -> list:
            vals: list = []
            for c in out_cols:
                vals.append(m.get(c))
                if c == "le_book":
                    vals += [name, ctype]
            return vals

        result = conn.execution_options(stream_results=True).execute(text(sql))

        state = {"path": None, "fh": None, "writer": None, "n": 0}

        def _open() -> None:
            fd, path = tempfile.mkstemp(suffix=".csv")
            fh = os.fdopen(fd, "w", newline="", encoding="utf-8-sig")
            w = csv.writer(fh)
            w.writerow(header)
            state.update(path=path, fh=fh, writer=w, n=0)

        def _close(lb: str) -> None:
            if state["fh"] is None:
                return
            state["fh"].close()
            if state["n"] > 0:
                zip_path = ISSUE_REPORTS_DIR / f"{lb}_{month}.zip"
                with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(state["path"], arcname=f"{table}.csv")
                log.info("  ZIP %-6s  %s.csv  %d rows", lb, table, state["n"])
            os.unlink(state["path"])
            state.update(path=None, fh=None, writer=None, n=0)

        cur_lb: str | None = None
        for row in result:
            m  = dict(row._mapping)
            lb = str(m["le_book"]).strip() if m.get("le_book") is not None else None
            if lb is None:
                continue
            if lb != cur_lb:
                if cur_lb is not None:
                    _close(cur_lb)
                cur_lb = lb
                _open()
            info  = categories.get(lb, {})
            name  = info.get("name") or lb
            name  = name.title() if isinstance(name, str) else name
            ctype = info.get("category_type") or ""
            state["writer"].writerow(_row_values(m, name, ctype))
            state["n"] += 1
        if cur_lb is not None:
            _close(cur_lb)


# orchestration

def run_monthly_detection(engine, schema: str, run_date: str,
                          tables: list[str] | None = None,
                          limit: int = 0) -> None:
    """
    Full-table DQ scan across all dimensions, run entirely in SQL (memory-safe).

    Pipeline:
      1. comp/acc/val/uni evaluate_from_sql with window_days=0 (full-table scan) → R
      2. detect_and_update_issues(R)  → dq_open_issues
      3. _build_history_entry / _append_history  → dq_history.json (dashboard trends)
      4. per-institution failing-row ZIPs streamed from SQL → issue_reports/

    tables: restrict to a subset of tables (testing)
    limit:  row cap per table, 0 = no limit (testing)
    """
    import completeness_check as comp_eng
    import accuracy_check     as acc_eng
    import validity_check     as val_eng
    import uniqueness_check    as uni_eng
    from db_utils import get_valid_le_books, get_le_book_categories, customer_dup_counts
    from dq_issue_tracker import detect_and_update_issues, ensure_tables

    ensure_tables()

    log.info("Fetching institution metadata …")
    valid_le_books = get_valid_le_books(engine, schema)
    categories     = get_le_book_categories(engine, schema)

    if tables:
        log.info("Table filter: %s", ", ".join(tables))
    if limit:
        log.info("Row limit: %d per table (TEST MODE)", limit)

    # ── dimension scoring: pure-SQL engines, full-table scan (window_days=0) ───
    FULL_SCAN = 0
    wm: dict = {}
    log.info("Running completeness …")
    comp_report = comp_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                             str(SCRIPT_DIR / "dq_report.json"),
                                             row_limit=limit, tables=tables)
    log.info("Running accuracy …")
    acc_report  = acc_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                            str(SCRIPT_DIR / "dq_accuracy_report.json"),
                                            row_limit=limit, tables=tables)
    log.info("Running validity …")
    val_report  = val_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                            str(SCRIPT_DIR / "dq_validity_report.json"),
                                            row_limit=limit, tables=tables)
    log.info("Running uniqueness …")
    uni_report  = uni_eng.evaluate_from_sql(engine, schema, valid_le_books, FULL_SCAN, wm,
                                            str(SCRIPT_DIR / "dq_uniqueness_report.json"),
                                            row_limit=limit, tables=tables)

    R = {"comp": comp_report, "acc": acc_report, "val": val_report, "uni": uni_report}

    log.info("Writing issues to tracker …")
    detect_and_update_issues(R, categories, run_date)

    log.info("Computing customer duplicate counts …")
    dup_counts, cat_dup_counts = customer_dup_counts(engine, schema, valid_le_books)
    log.info("  %d institution(s) with duplicate customers", len(dup_counts))

    log.info("Writing history entry …")
    entry = _build_history_entry(run_date, R, categories, dup_counts, cat_dup_counts)
    _append_history(entry)

    # ── per-institution failing-row ZIPs: streamed from SQL (memory-safe) ──────
    month = run_date[:7]
    ISSUE_REPORTS_DIR.mkdir(exist_ok=True)
    for old in ISSUE_REPORTS_DIR.glob(f"*_{month}.zip"):
        old.unlink()
        log.info("Removed stale ZIP: %s", old.name)
    log.info("Streaming per-institution failing-row ZIPs …")
    for table in (tables or TABLES):
        _zip_failing_rows_sql(engine, schema, table, valid_le_books, categories, month, limit)

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
