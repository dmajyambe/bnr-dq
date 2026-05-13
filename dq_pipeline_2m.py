from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_pipeline")

SCRIPT_DIR = Path(__file__).parent
SCHEMA     = "dqp"

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

DATE_COLUMN    = "date_creation"
WINDOW_DAYS    = 7
WINDOW_DESC    = f"date_creation OR date_last_modified within last {WINDOW_DAYS} days"
WATERMARK_FILE = SCRIPT_DIR / "watermark.json"
HISTORY_FILE   = SCRIPT_DIR / "dq_history.json"
CATEGORY_TYPES = ("MF", "SACCO", "OSACCO", "B")


#watermark helpers 
def _load_watermarks() -> dict:
    if WATERMARK_FILE.exists():
        return json.loads(WATERMARK_FILE.read_text())
    return {}

def _save_watermarks(marks: dict) -> None:
    WATERMARK_FILE.write_text(json.dumps(marks, indent=2, default=str))


#connection helpers
def _load_env() -> None:
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)


def _build_conn_string() -> str:
    required = [
        "MY_POSTGRES_USERNAME", "MY_POSTGRES_PASSWORD",
        "MY_POSTGRES_HOST", "MY_POSTGRES_PORT", "MY_POSTGRES_DB",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)
    u, pw, h, p, db = (os.environ[k] for k in required)
    return f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}"


def _get_engine(conn_str: str):
    try:
        engine = create_engine(conn_str, pool_pre_ping=True,
                               connect_args={"connect_timeout": 10})
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        log.error("DB connection failed: %s", exc)
        sys.exit(1)


def _has_column(conn, schema: str, table: str, column: str) -> bool:
    row = conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :column
        LIMIT 1
    """), {"schema": schema, "table": table, "column": column}).fetchone()
    return row is not None


def _db_columns(conn, schema: str, table: str) -> set:
    rows = conn.execute(text("""
        SELECT column_name
        FROM   information_schema.columns
        WHERE  table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()
    return {r[0].lower() for r in rows}


#date filter builder 

def _build_date_filter(conn, schema: str, table: str,
                       watermarks: dict = None,
                       start_date: str = None,
                       end_date: str = None):
    """
    Build a WHERE clause covering date_creation and/or date_last_modified.
    When start_date/end_date are given (e.g. --start-date / --end-date testing
    flags), use a fixed BETWEEN range.  Otherwise use a rolling WINDOW_DAYS
    window for date_creation and the watermark (incremental) for date_last_modified.
    """
    has_created  = _has_column(conn, schema, table, "date_creation")
    has_modified = _has_column(conn, schema, table, "date_last_modified")

    parts, labels = [], [] 

    if start_date and end_date:
        range_clause = f"BETWEEN '{start_date}' AND '{end_date}'"
        if has_created:
            parts.append(f'"date_creation" {range_clause}')
            labels.append("created")
        if has_modified:
            parts.append(f'"date_last_modified" {range_clause}')
            labels.append("modified")
    else:
        if has_created:
            parts.append(
                f'"date_creation" BETWEEN CURRENT_DATE - INTERVAL \'{WINDOW_DAYS} days\' AND CURRENT_DATE'
            )
            labels.append("created")
        if has_modified:
            hwm = (watermarks or {}).get(table)
            if hwm:
                parts.append(f'"date_last_modified" > \'{hwm}\'')
                labels.append(f"modified>{hwm[:10]}")
            else:
                parts.append(
                    f'"date_last_modified" BETWEEN CURRENT_DATE - INTERVAL \'{WINDOW_DAYS} days\' AND CURRENT_DATE'
                )
                labels.append("modified(init)")

    if not parts:
        return None, None

    clause = " OR ".join(parts)
    if len(parts) > 1:
        clause = f"({clause})"
    return clause, "+".join(labels)


# institution metadata 

def fetch_valid_le_books(engine, schema: str) -> frozenset:
    """Return le_book codes whose category_type is in CATEGORY_TYPES."""
    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)
    sql = text(f"""
        SELECT DISTINCT lb.le_book
        FROM "{schema}".le_book lb
        LEFT JOIN (
            SELECT alpha_tab     AS category_type_at,
                   alpha_sub_tab AS category_type
            FROM   "{schema}".alpha_sub_tab
        ) ast ON lb.category_type_at = ast.category_type_at
             AND lb.category_type    = ast.category_type
        WHERE ast.category_type IN ({filter_list})
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        result = frozenset(str(r[0]).strip() for r in rows if r[0] is not None)
        log.info("valid le_books: %d institutions loaded", len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch valid le_books: %s — no filter applied.", exc)
        return frozenset()


def fetch_le_book_categories(engine, schema: str) -> dict:
    """
    Return {le_book: {"name": ..., "category_type": ...}} for all in-scope
    institutions.
    """
    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)
    sql = text(f"""
        SELECT lb.le_book,
               LOWER(lb.leb_description)  AS le_book_name,
               ast.category_type
        FROM "{schema}".le_book lb
        LEFT JOIN (
            SELECT alpha_tab     AS category_type_at,
                   alpha_sub_tab AS category_type
            FROM   "{schema}".alpha_sub_tab
        ) ast ON lb.category_type_at = ast.category_type_at
             AND lb.category_type    = ast.category_type
        WHERE ast.category_type IN ({filter_list})
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        result = {}
        for r in rows:
            lb = str(r[0]).strip() if r[0] else None
            if not lb:
                continue
            name = str(r[1]).strip() if r[1] and str(r[1]).strip() not in ("", "none", "nan") else lb
            ct   = str(r[2]).strip() if r[2] else ""
            result[lb] = {"name": name, "category_type": ct}
        log.info("le_book categories: %d institutions loaded", len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch le_book categories: %s", exc)
        return {}


# ── column selection ──────────────────────────────────────────────────────────

def _needed_columns(table: str) -> set:
    """Union of all columns needed by every DQ engine for *table*."""
    from dq_rules import (
        MANDATORY_COLUMNS, ACCURACY_COLUMNS,
        TIMELINESS_COLUMNS, VALIDITY_COLUMNS, REL_RULE_META,
    )
    cols = {"le_book", "date_creation", "date_last_modified"}
    cols.update(MANDATORY_COLUMNS.get(table, []))
    cols.update(ACCURACY_COLUMNS.get(table, []))
    cols.update(TIMELINESS_COLUMNS.get(table, []))
    cols.update(VALIDITY_COLUMNS.get(table, []))
    for rule in REL_RULE_META.values():
        if rule["child_table"]  == table: cols.add(rule["child_col"])
        if rule["parent_table"] == table: cols.add(rule["parent_col"])
    return cols


#table loader

def load_all_tables(engine, schema: str, valid_le_books: frozenset,
                    start_date: str = None, end_date: str = None) -> dict:
    """
    Fetch all dimension tables within the date window into DataFrames.
    Only columns required by the DQ engines are selected.
    le_book filter is applied in-memory after loading.
    """
    dataframes   = {}
    processed_at = datetime.now(timezone.utc)
    watermarks   = _load_watermarks()

    with engine.connect() as conn:
        for table in TABLES:
            sq_tbl = f'"{schema}"."{table}"'
            clause, filter_type = _build_date_filter(
                conn, schema, table, watermarks, start_date, end_date)

            needed   = _needed_columns(table)
            existing = _db_columns(conn, schema, table)
            cols     = sorted(needed & existing)

            if not cols:
                log.warning("  %s: no matching columns found — skipping.", table)
                dataframes[table] = pd.DataFrame()
                continue

            quoted = ", ".join(f'"{c}"' for c in cols)

            # Push le_book filter into SQL so the DB only sends the rows we need.
            # Without this, every institution's rows would be fetched and discarded
            # in Python — fine for the full pipeline, very slow for single-institution reports.
            le_sql = ""
            if valid_le_books and "le_book" in cols:
                books  = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
                le_sql = f' AND "le_book" IN ({books})'

            if clause:
                sql = f"SELECT {quoted} FROM {sq_tbl} WHERE {clause}{le_sql}"
            elif le_sql:
                sql = f"SELECT {quoted} FROM {sq_tbl} WHERE 1=1{le_sql}"
                log.warning("  %s: no date columns — restricted by le_book filter only.", table)
            else:
                sql = f"SELECT {quoted} FROM {sq_tbl}"
                log.warning("  %s: no date columns and no le_book filter — loading all rows.", table)

            try:
                df = pd.read_sql(text(sql), conn)
                df.columns = [c.lower() for c in df.columns]
                if valid_le_books and "le_book" in df.columns:
                    before = len(df)
                    df = df[df["le_book"].isin(valid_le_books)].reset_index(drop=True)
                    log.info("  %-30s %8d rows × %d cols  [%s]  (le_book: %d→%d)",
                             table, len(df), len(df.columns),
                             filter_type or "unfiltered", before, len(df))
                else:
                    log.info("  %-30s %8d rows × %d cols  [%s]",
                             table, len(df), len(df.columns), filter_type or "unfiltered")
                df["data_processed"] = processed_at
                if "date_last_modified" in df.columns and not df.empty:
                    new_max = df["date_last_modified"].max()
                    if pd.notna(new_max):
                        watermarks[table] = str(new_max)
                dataframes[table] = df
            except Exception as exc:
                log.error("  Failed to load %s: %s", table, exc)
                dataframes[table] = pd.DataFrame()

    _save_watermarks(watermarks)
    log.info("Watermarks saved → %s", WATERMARK_FILE)
    return dataframes


#parallel runner

def _run_parallel(tasks: dict, max_workers: int = 8) -> dict:
    """Run {name: callable} concurrently. Returns {name: result}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results: dict = {}
    n = min(len(tasks), max_workers)
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
                log.info("  ✓ %s", name)
            except Exception as exc:
                log.error("  ✗ %s — %s", name, exc)
                results[name] = {}
    return results


# ── history log helpers ───────────────────────────────────────────────────────

# Referential-integrity results are extracted separately ("_rel") then averaged
# into the accuracy score — relationship is no longer a standalone dimension.
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


def _customer_dup_counts(engine, schema: str, valid_le_books: frozenset) -> dict[str, int]:
    """Count distinct customer_ids with duplicates per le_book across the full table (no date filter)."""
    from sqlalchemy import text as _text
    lb_filter = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_filter = f"WHERE le_book IN ({codes})"
    sql = _text(f"""
        SELECT le_book, COUNT(*) AS dup_customers
        FROM (
            SELECT le_book, customer_id
            FROM "{schema}".customers_expanded
            {lb_filter}
            GROUP BY le_book, customer_id
            HAVING COUNT(*) > 1
        ) sub
        GROUP BY le_book
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        return {str(r[0]).strip(): int(r[1]) for r in rows if r[0] is not None}
    except Exception as exc:
        log.warning("Could not compute customer duplicate counts: %s", exc)
        return {}


def _build_history_entry(run_date: str, R: dict, categories: dict,
                         dup_counts: dict | None = None) -> dict:
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
    _dups = dup_counts or {}
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
        cat_scores["customer_duplicates"] = sum(i.get("customer_duplicates", 0) for i in institutions)
        by_category[ct] = cat_scores

    return {
        "date":           run_date,
        "overall":        overall,
        "by_category":    by_category,
        "by_institution": by_institution,
    }


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
    HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str))
    log.info("History log updated → %s  (%d entries total)", HISTORY_FILE, len(history))


# ── window verification (debug / --verify-only mode) ─────────────────────────

def verify_window(engine, schema: str,
                  start_date: str = None, end_date: str = None) -> list:
    results    = []
    watermarks = _load_watermarks()

    with engine.connect() as conn:
        for table in TABLES:
            sq_tbl = f'"{schema}"."{table}"'
            entry  = {"table": table, "filter_type": None}
            clause, filter_type = _build_date_filter(
                conn, schema, table, watermarks, start_date, end_date)

            if clause is None:
                log.warning("  %-30s — no date columns, skipped", table)
                results.append(entry)
                continue
            entry["filter_type"] = filter_type
            try:
                total    = conn.execute(text(f"SELECT COUNT(*) FROM {sq_tbl}")).scalar() or 0
                windowed = conn.execute(
                    text(f"SELECT COUNT(*) FROM {sq_tbl} WHERE {clause}")
                ).scalar() or 0
                mm = conn.execute(text(
                    f'SELECT MIN("{DATE_COLUMN}")::TEXT, MAX("{DATE_COLUMN}")::TEXT '
                    f'FROM {sq_tbl} WHERE {clause}'
                )).fetchone()
                entry.update({
                    "total_rows":  total,
                    "window_rows": windowed,
                    "window_pct":  round(windowed / total * 100, 1) if total else 0.0,
                    "min_date":    mm[0] if mm else None,
                    "max_date":    mm[1] if mm else None,
                })
            except Exception as exc:
                log.error("  %s: query failed — %s", table, exc)
                entry["error"] = str(exc)
            results.append(entry)
    return results


def _print_report(results: list) -> None:
    header = (
        f"{'Table':<30} {'Filter':<22} {'Total':>10} "
        f"{'Window':>10} {'Pct':>7}  {'Min date':<12}  {'Max date':<12}"
    )
    print()
    print("=" * len(header))
    print(f"  {WINDOW_DAYS}-DAY WINDOW VERIFICATION  ({WINDOW_DESC})")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        ft = r.get("filter_type") or "—"
        if ft == "—":
            print(f"  {r['table']:<28}  {'no date cols':<22}  {'—':>10}  {'—':>10}  {'—':>7}")
            continue
        if "error" in r:
            print(f"  {r['table']:<28}  {ft:<22}  {'ERROR':>10}")
            continue
        print(
            f"  {r['table']:<28}  {ft:<22}  "
            f"{r['total_rows']:>10,}  {r['window_rows']:>10,}  "
            f"{r['window_pct']:>6.1f}%  "
            f"{str(r.get('min_date','')):<12}  {str(r.get('max_date','')):<12}"
        )
    print("=" * len(header))
    print()


#entry point 
def main() -> None:
    _load_env()

    parser = argparse.ArgumentParser(
        description=f"BNR DQ daily pipeline — {WINDOW_DAYS}-day rolling window"
    )
    parser.add_argument("--schema", default=SCHEMA,
                        help=f"Source schema (default: {SCHEMA})")
    parser.add_argument("--load", action="store_true",
                        help="Load data and run all DQ engines")
    parser.add_argument("--verify-only", action="store_true",
                        help="Print window row counts and exit (no engine runs)")
    parser.add_argument("--output", default=None,
                        help="Write verification JSON to path (verify mode only)")
    parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD",
                        help="Override window start date (e.g. 2026-01-01)")
    parser.add_argument("--end-date", default=None, metavar="YYYY-MM-DD",
                        help="Override window end date   (e.g. 2026-03-31)")
    args = parser.parse_args()

    start_date = args.start_date
    end_date   = args.end_date
    run_date   = end_date or datetime.now().strftime("%Y-%m-%d")

    if start_date:
        log.info("Fixed date range: %s → %s", start_date, end_date)

    log.info("Connecting to database …")
    engine = _get_engine(_build_conn_string())

    # ── verify-only mode
    if not args.load:
        log.info("Window verification across %d tables …", len(TABLES))
        results = verify_window(engine, args.schema, start_date, end_date)
        _print_report(results)
        if args.output:
            out = {
                "generated_at": datetime.utcnow().isoformat(),
                "schema":       args.schema,
                "window":       f"{start_date}:{end_date}" if start_date else WINDOW_DESC,
                "tables":       results,
            }
            Path(args.output).write_text(json.dumps(out, indent=2, default=str))
            log.info("Verification report written → %s", args.output)
        log.info("Done.")
        return

    # ── full run ──────────────────────────────────────────────────────────────
    log.info("Fetching institution metadata …")
    valid_le_books = fetch_valid_le_books(engine, args.schema)
    categories     = fetch_le_book_categories(engine, args.schema)
    (SCRIPT_DIR / "le_book_categories.json").write_text(
        json.dumps(categories, indent=2, ensure_ascii=False))
    log.info("le_book_categories.json written (%d institutions)", len(categories))

    log.info("Loading %d-day window …", WINDOW_DAYS)
    dataframes = load_all_tables(engine, args.schema, valid_le_books, start_date, end_date)
    log.info("All tables loaded — DB connection no longer needed.")

    total_rows = sum(len(df) for df in dataframes.values())
    log.info("Total rows loaded: %d", total_rows)

    # write manifest immediately so dashboard shows a fresh timestamp
    processed_at = datetime.now(timezone.utc)
    run_manifest = {
        "data_processed": str(processed_at),
        "run_date":       run_date,
        "window":         f"{start_date}:{end_date}" if start_date else WINDOW_DESC,
        "window_days":    WINDOW_DAYS,
        "tables_loaded":  {t: len(dataframes.get(t, pd.DataFrame())) for t in TABLES},
    }
    (SCRIPT_DIR / "pipeline_run.json").write_text(
        json.dumps(run_manifest, indent=2, default=str))
    log.info("Pipeline manifest written.")

    # ── import engines ────────────────────────────────────────────────────────
    import completeness_check as comp_eng
    import accuracy_check     as acc_eng
    import timeliness_check   as tim_eng
    import validity_check     as val_eng
    import relationship_check as rel_eng
    import dq_profiler_engine as prof_eng
    import dq_issue_export    as issue_eng

    def _rel():
        return rel_eng.evaluate_all_from_dataframes(dataframes, valid_le_books)

    def _prof():
        r = prof_eng.profile_all_from_dataframes(dataframes, args.schema)
        (SCRIPT_DIR / "dq_profile_report.json").write_text(
            json.dumps(r, indent=2, default=str))
        return r

    tasks = {
        "comp": partial(comp_eng.evaluate_from_dataframes, dataframes, valid_le_books,
                        str(SCRIPT_DIR / "dq_report.json")),
        "acc":  partial(acc_eng.evaluate_from_dataframes,  dataframes, valid_le_books,
                        str(SCRIPT_DIR / "dq_accuracy_report.json")),
        "tim":  partial(tim_eng.evaluate_from_dataframes,  dataframes, valid_le_books,
                        str(SCRIPT_DIR / "dq_timeliness_report.json")),
        "val":  partial(val_eng.evaluate_from_dataframes,  dataframes, valid_le_books,
                        str(SCRIPT_DIR / "dq_validity_report.json")),
        "rel":  _rel,
        "prof": _prof,
    }

    log.info("Running %d engine tasks in parallel …", len(tasks))
    t0 = time.perf_counter()
    R  = _run_parallel(tasks)
    log.info("All engines finished in %.1fs", time.perf_counter() - t0)

    # relationship engine returns the dict; pipeline writes the file
    (SCRIPT_DIR / "dq_relationship_report.json").write_text(
        json.dumps(R.get("rel") or {}, indent=2, default=str))

    # ── user-defined rules ────────────────────────────────────────────────────
    import dq_user_rule_executor as usr_eng
    log.info("Running user-defined rules …")
    usr_eng.run_all_user_rules(dataframes, valid_le_books)

    # ── per-institution row-level issue reports ───────────────────────────────
    log.info("Generating per-institution issue reports …")
    issue_eng.export_institution_issues(
        dataframes, categories, valid_le_books, SCRIPT_DIR / "reports"
    )

    # ── build and append history entry ────────────────────────────────────────
    log.info("Building history entry for %s …", run_date)
    log.info("Counting customer duplicates across full table …")
    dup_counts = _customer_dup_counts(engine, args.schema, valid_le_books)
    log.info("Customer duplicate counts: %d institution(s) with duplicates", len(dup_counts))
    entry = _build_history_entry(run_date, R, categories, dup_counts)
    _append_history(entry)

    # ── summary ───────────────────────────────────────────────────────────────
    log.info("%-14s  %6s  %s", "Dimension", "Score", "Category breakdown")
    for dim in DIMS:
        overall_score = entry["overall"].get(dim, 0.0)
        cat_detail    = "  ".join(
            f"{ct}={entry['by_category'].get(ct, {}).get(dim, 0.0):.1f}%"
            for ct in CATEGORY_TYPES
            if ct in entry["by_category"]
        )
        log.info("  %-14s  %5.2f%%  %s", dim, overall_score, cat_detail)

    log.info("Institutions scored: %d", len(entry["by_institution"]))
    log.info("All done.")


if __name__ == "__main__":
    main()
