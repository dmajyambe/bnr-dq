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
WINDOW_DAYS    = 30
WINDOW_DESC    = f"date_creation OR date_last_modified within last {WINDOW_DAYS} days"
WATERMARK_FILE = SCRIPT_DIR / "watermark.json"
HISTORY_FILE   = SCRIPT_DIR / "dq_history.json"
ACTIVITY_FILE  = SCRIPT_DIR / "institution_activity.json"
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
        hwm    = (watermarks or {}).get(table)
        anchor = f"'{hwm[:10]}'::date" if hwm else "CURRENT_DATE"
        if has_created:
            parts.append(
                f'"date_creation" BETWEEN {anchor} - INTERVAL \'{WINDOW_DAYS} days\' AND {anchor}'
            )
            labels.append(f"created≤{hwm[:10]}" if hwm else "created")
        if has_modified:
            if hwm:
                parts.append(f'"date_last_modified" > \'{hwm}\'')
                labels.append(f"modified>{hwm[:10]}")
            else:
                parts.append(
                    f'"date_last_modified" BETWEEN {anchor} - INTERVAL \'{WINDOW_DAYS} days\' AND {anchor}'
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
                    watermarks: dict | None = None,
                    start_date: str = None, end_date: str = None,
                    row_limit: int = 0) -> tuple[dict, dict]:
    """
    Fetch all dimension tables within the date window into DataFrames.
    Only columns required by the DQ engines are selected.
    le_book filter is applied in-memory after loading.

    Returns (dataframes, updated_watermarks). Does NOT save watermarks —
    the caller is responsible for persisting them.
    """
    dataframes   = {}
    processed_at = datetime.now(timezone.utc)
    wm           = dict(watermarks) if watermarks else {}

    with engine.connect() as conn:
        for table in TABLES:
            sq_tbl = f'"{schema}"."{table}"'
            clause, filter_type = _build_date_filter(
                conn, schema, table, wm, start_date, end_date)

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
            if row_limit > 0:
                sql += f" LIMIT {row_limit}"

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
                    if pd.notna(new_max) and str(new_max) > wm.get(table, ""):
                        wm[table] = str(new_max)
                dataframes[table] = df
            except Exception as exc:
                log.error("  Failed to load %s: %s", table, exc)
                dataframes[table] = pd.DataFrame()

    return dataframes, wm


def load_parent_keys(engine, schema: str,
                     valid_le_books: frozenset | None = None) -> dict:
    """
    Load only the FK key column(s) for each parent table in REL_RULE_META,
    with no date filter.  This gives the relationship engine a complete key
    space so that parents created before the current 7-day window are not
    mistaken for missing references.
    """
    from dq_rules import REL_RULE_META

    parent_cols: dict[str, set] = {}
    for meta in REL_RULE_META.values():
        parent_cols.setdefault(meta["parent_table"], set()).add(meta["parent_col"])

    result = {}
    with engine.connect() as conn:
        for table, cols in sorted(parent_cols.items()):
            quoted = ", ".join(f'"{c}"' for c in sorted(cols))
            sq_tbl = f'"{schema}"."{table}"'
            lb_sql = ""
            if valid_le_books and _has_column(conn, schema, table, "le_book"):
                books  = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
                lb_sql = f" WHERE le_book IN ({books})"
            sql    = f"SELECT DISTINCT {quoted} FROM {sq_tbl}{lb_sql}"
            try:
                df = pd.read_sql(text(sql), conn)
                df.columns = [c.lower() for c in df.columns]
                log.info("  parent keys %-30s %8d distinct keys", table, len(df))
                result[table] = df
            except Exception as exc:
                log.warning("  Could not load parent keys for %s: %s — RI will use windowed data.", table, exc)
                result[table] = pd.DataFrame()
    return result


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


def _customer_dup_counts(
    engine, schema: str, valid_le_books: frozenset
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Returns (per_lb, per_cat) where:
      per_lb  — {le_book: distinct customers with dups in that institution}
      per_cat — {category_type: DISTINCT customers with dups in that category}

    per_cat uses COUNT(DISTINCT) so a customer with duplicates at multiple
    institutions in the same category is counted only once.
    """
    from sqlalchemy import text as _text
    lb_filter = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_filter = f"WHERE ce.le_book IN ({codes})"

    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)

    sql_lb = _text(f"""
        SELECT le_book, COUNT(*) AS dup_customers
        FROM (
            SELECT le_book, customer_id
            FROM "{schema}".customers_expanded
            {lb_filter.replace("ce.", "")}
            GROUP BY le_book, customer_id
            HAVING COUNT(*) > 1
        ) sub
        GROUP BY le_book
    """)

    sql_cat = _text(f"""
        SELECT ast.category_type,
               COUNT(DISTINCT dc.customer_id) AS dup_customers
        FROM (
            SELECT ce.le_book, ce.customer_id
            FROM "{schema}".customers_expanded ce
            {lb_filter}
            GROUP BY ce.le_book, ce.customer_id
            HAVING COUNT(*) > 1
        ) dc
        JOIN "{schema}".le_book lb
            ON dc.le_book = lb.le_book
        JOIN (
            SELECT alpha_tab     AS category_type_at,
                   alpha_sub_tab AS category_type
            FROM   "{schema}".alpha_sub_tab
        ) ast
            ON lb.category_type_at = ast.category_type_at
           AND lb.category_type    = ast.category_type
        WHERE ast.category_type IN ({filter_list})
        GROUP BY ast.category_type
    """)

    try:
        with engine.connect() as conn:
            per_lb  = {str(r[0]).strip(): int(r[1])
                       for r in conn.execute(sql_lb).fetchall() if r[0] is not None}
            per_cat = {str(r[0]).strip(): int(r[1])
                       for r in conn.execute(sql_cat).fetchall() if r[0] is not None}
        return per_lb, per_cat
    except Exception as exc:
        log.warning("Could not compute customer duplicate counts: %s", exc)
        return {}, {}


def _compute_institution_activity(engine, schema: str, valid_le_books: frozenset) -> dict:
    """Return {le_book: {last_modified: "YYYY-MM-DD", last_created: "YYYY-MM-DD"}}
    by querying max date columns across key tables (full scan, no watermark filter).
    Used by the dashboard to surface when unscored institutions last had data changes."""
    lb_filter = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_filter = f"AND le_book IN ({codes})"

    results: dict[str, dict] = {}

    for table in ("accounts", "customers_expanded", "contracts_expanded"):
        try:
            with engine.connect() as conn:
                has_mod = _has_column(conn, schema, table, "date_last_modified")
                has_cre = _has_column(conn, schema, table, "date_creation")
                if not has_mod and not has_cre:
                    continue
                mod_expr = "MAX(date_last_modified)" if has_mod else "NULL"
                cre_expr = "MAX(date_creation)"      if has_cre else "NULL"
                sql = text(f"""
                    SELECT le_book,
                           {mod_expr} AS last_modified,
                           {cre_expr} AS last_created
                    FROM "{schema}".{table}
                    WHERE le_book IS NOT NULL {lb_filter}
                    GROUP BY le_book
                """)
                for row in conn.execute(sql).fetchall():
                    lb = str(row[0]).strip() if row[0] else None
                    if not lb:
                        continue
                    prev = results.get(lb, {})
                    mod  = str(row[1])[:10] if row[1] else None
                    cre  = str(row[2])[:10] if row[2] else None
                    results[lb] = {
                        "last_modified": max(
                            (v for v in [prev.get("last_modified"), mod] if v),
                            default=None,
                        ),
                        "last_created": max(
                            (v for v in [prev.get("last_created"), cre] if v),
                            default=None,
                        ),
                    }
        except Exception as exc:
            log.warning("Activity query failed for %s: %s", table, exc)

    return results


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
    parser.add_argument("--test", action="store_true",
                        help="Test mode: limit 1000 rows/table, skip relationship checks")
    parser.add_argument("--reports", action="store_true",
                        help="Reports-only stage: load frames, run RI, write XLSX (no issue tracker)")
    args = parser.parse_args()

    start_date = args.start_date
    end_date   = args.end_date
    run_date   = end_date or datetime.now().strftime("%Y-%m-%d")
    test_limit = 1000 if args.test else 0

    if start_date:
        log.info("Fixed date range: %s → %s", start_date, end_date)
    if args.test:
        log.info("TEST MODE — row limit %d per table, relationship checks skipped", test_limit)

    log.info("Connecting to database …")
    engine = _get_engine(_build_conn_string())

    # ── verify-only mode
    if not args.load and not args.reports:
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

    # write manifest immediately so dashboard shows a fresh timestamp
    processed_at = datetime.now(timezone.utc)
    run_manifest = {
        "data_processed": str(processed_at),
        "run_date":       run_date,
        "window":         f"{start_date}:{end_date}" if start_date else WINDOW_DESC,
        "window_days":    WINDOW_DAYS,
        "mode":           "sql",
    }
    (SCRIPT_DIR / "pipeline_run.json").write_text(
        json.dumps(run_manifest, indent=2, default=str))
    log.info("Pipeline manifest written.")

    # ── import engines ────────────────────────────────────────────────────────
    import completeness_check as comp_eng
    import accuracy_check     as acc_eng
    # import timeliness_check   as tim_eng
    # import validity_check     as val_eng
    import dq_issue_export    as issue_eng

    watermarks = _load_watermarks()

    # ══════════════════════════════════════════════════════════════════════════
    # STAGE: --reports  (load frames → RI → XLSX only, no issue tracker)
    # Run separately after --load has completed to avoid OOM during core run.
    # ══════════════════════════════════════════════════════════════════════════
    if args.reports:
        import gc
        import relationship_check as rel_eng

        log.info("REPORTS STAGE — loading frames for RI + XLSX export …")
        dataframes, updated_wm = load_all_tables(engine, args.schema, valid_le_books,
                                                  watermarks, start_date, end_date)
        _save_watermarks(updated_wm)
        log.info("Watermarks saved → %s", WATERMARK_FILE)

        log.info("Loading full parent key tables …")
        parent_dataframes = load_parent_keys(engine, args.schema, valid_le_books)

        log.info("Running referential integrity checks …")
        rel_report = rel_eng.evaluate_all_from_dataframes(dataframes, valid_le_books, parent_dataframes)
        (SCRIPT_DIR / "dq_relationship_report.json").write_text(
            json.dumps(rel_report, indent=2, default=str))

        log.info("Pre-computing rule masks …")
        mask_caches = issue_eng.build_mask_caches(dataframes, valid_le_books)

        log.info("Generating per-institution CSV reports …")
        issue_eng.export_csv_reports(
            dataframes, categories, valid_le_books, SCRIPT_DIR / "reports",
            parent_dataframes=parent_dataframes,
            mask_caches=mask_caches,
        )

        # Free parent frames — no longer needed after CSV export
        del parent_dataframes
        gc.collect()
        log.info("Parent frames freed.")

        # Free all frames
        del dataframes
        gc.collect()
        log.info("Reports stage complete.")
        return

    # ══════════════════════════════════════════════════════════════════════════
    # STAGE: --load  (SQL engines only — no DataFrames, low memory)
    # ══════════════════════════════════════════════════════════════════════════
    tasks = {
        "comp": partial(comp_eng.evaluate_from_sql, engine, args.schema, valid_le_books,
                        WINDOW_DAYS, watermarks, str(SCRIPT_DIR / "dq_report.json"),
                        row_limit=test_limit),
        # "acc":  PARKED — accuracy_check.py was rewritten to a pandas API and has no
        #         evaluate_from_sql (it was commented out even at 16a9001). Rebuild the
        #         accuracy SQL engine to the standard contract, then re-enable here.
        # "acc":  partial(acc_eng.evaluate_from_sql,  engine, args.schema, valid_le_books,
        #                 WINDOW_DAYS, watermarks, str(SCRIPT_DIR / "dq_accuracy_report.json"),
        #                 row_limit=test_limit),
        # "tim":  partial(tim_eng.evaluate_from_sql,  engine, args.schema, valid_le_books,
        #                 WINDOW_DAYS, watermarks, str(SCRIPT_DIR / "dq_timeliness_report.json"),
        #                 row_limit=test_limit),
        # "val":  partial(val_eng.evaluate_from_sql,  engine, args.schema, valid_le_books,
        #                 WINDOW_DAYS, watermarks, str(SCRIPT_DIR / "dq_validity_report.json"),
        #                 row_limit=test_limit),
    }

    if args.test:
        log.info("TEST MODE — row limit %d, skipping frames/RI/reports", test_limit)
        R = _run_parallel(tasks)
        R["rel"] = {}
    else:
        import gc

        # ── Phase A: SQL engines only ─────────────────────────────────────────
        log.info("Running %d SQL engine tasks in parallel …", len(tasks))
        t0 = time.perf_counter()
        R  = _run_parallel(tasks)
        log.info("SQL engines finished in %.1fs", time.perf_counter() - t0)

        # ── Phase B: activity dates (lightweight, single query per table) ─────
        log.info("Computing institution activity dates …")
        activity_data = _compute_institution_activity(engine, args.schema, valid_le_books)
        ACTIVITY_FILE.write_text(json.dumps(activity_data, indent=2, default=str))
        log.info("Institution activity → %s  (%d institutions)",
                 ACTIVITY_FILE.name, len(activity_data))

        # User-defined rules are disabled — skip frame load entirely (~4–6 GB saved).
        _save_watermarks(watermarks)
        log.info("Watermarks saved → %s", WATERMARK_FILE)

        # ── Phase D: RI results from file (written by --reports stage) ────────
        ri_path = SCRIPT_DIR / "dq_relationship_report.json"
        if ri_path.exists():
            try:
                R["rel"] = json.loads(ri_path.read_text())
                log.info("RI results loaded from dq_relationship_report.json")
            except Exception:
                R["rel"] = {}
        else:
            R["rel"] = {}
            log.info("No RI report found — run --reports to generate it.")

    # ── issue tracking: detect, update urgency, apply penalties, notify ──────
    import dq_issue_tracker as tracker
    log.info("Updating issue tracker …")
    tracker.detect_and_update_issues(R, categories, run_date)

    # PLACEHOLDER: uncomment once issues.* schema is ready (run_date + dimension columns confirmed)
    # import dq_issues_ingest as ext_ingest
    # updated_wm = ext_ingest.ingest_from_issues_schema(engine, TABLES, watermarks, run_date)
    # _save_watermarks(updated_wm)
    log.info("Running notification sweep …")
    tracker.run_notification_sweep(categories)

    # Portal SLA warnings (7-day ahead alert for inst_users)
    try:
        from dq_notifications import send_sla_warnings
        warned = send_sla_warnings()
        if warned:
            log.info("SLA warning portal notifications sent: %d", warned)
    except Exception as exc:
        log.warning("SLA warning notifications failed: %s", exc)

    log.info("All done. (history is written by the monthly detection pipeline)")


if __name__ == "__main__":
    main()
