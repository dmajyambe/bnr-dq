#utility module
from __future__ import annotations
import logging
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
log = logging.getLogger("dq_db_utils")

CATEGORY_TYPES = ("MF", "SACCO", "OSACCO", "B")
SCRIPT_DIR = Path(__file__).parent
SCHEMA     = "dqp"

def build_connection_string() -> str:
    required = [
        "MY_POSTGRES_USERNAME", "MY_POSTGRES_PASSWORD",
        "MY_POSTGRES_HOST", "MY_POSTGRES_PORT", "MY_POSTGRES_DB",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)
    u, pw, h, p, db = (os.environ[k] for k in required)
    return f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}"

def get_engine(conn_str: str):
    try:
        engine = create_engine(
            conn_str, pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except ImportError:
        log.error("sqlalchemy or psycopg2-binary not installed.")
        sys.exit(1)
    except Exception as exc:
        log.error("Cannot connect to database: %s", exc)
        sys.exit(1)


def get_valid_le_books(engine, schema: str) -> frozenset:
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
        ;
    """) 


    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        result = frozenset(str(r[0]).strip() for r in rows if r[0] is not None)
        log.info("Category filter %s → %d valid le_books", CATEGORY_TYPES, len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch valid le_books: %s — no filter applied.", exc)
        return frozenset()


def get_le_book_categories(engine, schema: str) -> dict:
    """Return {le_book: {"name": ..., "category_type": ...}} for all in-scope institutions."""
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


def customer_dup_counts(
    engine, schema: str, valid_le_books: frozenset
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Returns (per_lb, per_cat) where:
      per_lb  — {le_book: distinct customers with dups in that institution}
      per_cat — {category_type: DISTINCT customers with dups in that category}

    per_cat uses COUNT(DISTINCT) so a customer with duplicates at multiple
    institutions in the same category is counted only once.
    """
    lb_filter = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_filter = f"WHERE ce.le_book IN ({codes})"

    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)

    sql_lb = text(f"""
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

    sql_cat = text(f"""
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


# def _customer_dup_counts(
#     engine, schema: str, valid_le_books: frozenset
# ) -> tuple[dict[str, int], dict[str, int]]:
#     """
#     Returns (per_lb, per_cat) where:
#       per_lb  — {le_book: distinct customers with dups in that institution}
#       per_cat — {category_type: DISTINCT customers with dups in that category}

#     per_cat uses COUNT(DISTINCT) so a customer with duplicates at multiple
#     institutions in the same category is counted only once.
#     """
#     from sqlalchemy import text as _text
#     lb_filter = ""
#     if valid_le_books:
#         codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
#         lb_filter = f"WHERE ce.le_book IN ({codes})"

#     filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)

#     sql_lb = _text(f"""
#         SELECT le_book, COUNT(*) AS dup_customers
#         FROM (
#             SELECT le_book, customer_id
#             FROM "{schema}".customers_expanded
#             {lb_filter.replace("ce.", "")}
#             GROUP BY le_book, customer_id
#             HAVING COUNT(*) > 1
#         ) sub
#         GROUP BY le_book
#     """)

#     sql_cat = _text(f"""
#         SELECT ast.category_type,
#                COUNT(DISTINCT dc.customer_id) AS dup_customers
#         FROM (
#             SELECT ce.le_book, ce.customer_id
#             FROM "{schema}".customers_expanded ce
#             {lb_filter}
#             GROUP BY ce.le_book, ce.customer_id
#             HAVING COUNT(*) > 1
#         ) dc
#         JOIN "{schema}".le_book lb
#             ON dc.le_book = lb.le_book
#         JOIN (
#             SELECT alpha_tab     AS category_type_at,
#                    alpha_sub_tab AS category_type
#             FROM   "{schema}".alpha_sub_tab
#         ) ast
#             ON lb.category_type_at = ast.category_type_at
#            AND lb.category_type    = ast.category_type
#         WHERE ast.category_type IN ({filter_list})
#         GROUP BY ast.category_type
#     """)

#     try:
#         with engine.connect() as conn:
#             per_lb  = {str(r[0]).strip(): int(r[1])
#                        for r in conn.execute(sql_lb).fetchall() if r[0] is not None}
#             per_cat = {str(r[0]).strip(): int(r[1])
#                        for r in conn.execute(sql_cat).fetchall() if r[0] is not None}
#         return per_lb, per_cat
#     except Exception as exc:
#         log.warning("Could not compute customer duplicate counts: %s", exc)
#         return {}, {}


# def _compute_institution_activity(engine, schema: str, valid_le_books: frozenset) -> dict:
#     """Return {le_book: {last_modified: "YYYY-MM-DD", last_created: "YYYY-MM-DD"}}
#     by querying max date columns across key tables (full scan, no watermark filter).
#     Used by the dashboard to surface when unscored institutions last had data changes."""
#     lb_filter = ""
#     if valid_le_books:
#         codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
#         lb_filter = f"AND le_book IN ({codes})"

#     results: dict[str, dict] = {}

#     for table in ("accounts", "customers_expanded", "contracts_expanded"):
#         try:
#             with engine.connect() as conn:
#                 has_mod = _has_column(conn, schema, table, "date_last_modified")
#                 has_cre = _has_column(conn, schema, table, "date_creation")
#                 if not has_mod and not has_cre:
#                     continue
#                 mod_expr = "MAX(date_last_modified)" if has_mod else "NULL"
#                 cre_expr = "MAX(date_creation)"      if has_cre else "NULL"
#                 sql = text(f"""
#                     SELECT le_book,
#                            {mod_expr} AS last_modified,
#                            {cre_expr} AS last_created
#                     FROM "{schema}".{table}
#                     WHERE le_book IS NOT NULL {lb_filter}
#                     GROUP BY le_book
#                 """)
#                 for row in conn.execute(sql).fetchall():
#                     lb = str(row[0]).strip() if row[0] else None
#                     if not lb:
#                         continue
#                     prev = results.get(lb, {})
#                     mod  = str(row[1])[:10] if row[1] else None
#                     cre  = str(row[2])[:10] if row[2] else None
#                     results[lb] = {
#                         "last_modified": max(
#                             (v for v in [prev.get("last_modified"), mod] if v),
#                             default=None,
#                         ),
#                         "last_created": max(
#                             (v for v in [prev.get("last_created"), cre] if v),
#                             default=None,
#                         ),
#                     }
#         except Exception as exc:
#             log.warning("Activity query failed for %s: %s", table, exc)

#     return results

# def fetch_le_book_categories(engine, schema: str) -> dict:
#     """
#     Return {le_book: {"name": ..., "category_type": ...}} for all in-scope
#     institutions.
#     """
#     filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)
#     sql = text(f"""
#         SELECT lb.le_book,
#                LOWER(lb.leb_description)  AS le_book_name,
#                ast.category_type
#         FROM "{schema}".le_book lb
#         LEFT JOIN (
#             SELECT alpha_tab     AS category_type_at,
#                    alpha_sub_tab AS category_type
#             FROM   "{schema}".alpha_sub_tab
#         ) ast ON lb.category_type_at = ast.category_type_at
#              AND lb.category_type    = ast.category_type
#         WHERE ast.category_type IN ({filter_list})
#     """)
#     try:
#         with engine.connect() as conn:
#             rows = conn.execute(sql).fetchall()
#         result = {}
#         for r in rows:
#             lb = str(r[0]).strip() if r[0] else None
#             if not lb:
#                 continue
#             name = str(r[1]).strip() if r[1] and str(r[1]).strip() not in ("", "none", "nan") else lb
#             ct   = str(r[2]).strip() if r[2] else ""
#             result[lb] = {"name": name, "category_type": ct}
#         log.info("le_book categories: %d institutions loaded", len(result))
#         return result
#     except Exception as exc:
#         log.warning("Could not fetch le_book categories: %s", exc)
#         return {}


# # # ── column selection ──────────────────────────────────────────────────────────

# # def _needed_columns(table: str) -> set:
# #     """Union of all columns needed by every DQ engine for *table*."""
# #     from dq_rules import (
# #         MANDATORY_COLUMNS, ACCURACY_COLUMNS,
# #         TIMELINESS_COLUMNS, VALIDITY_COLUMNS, REL_RULE_META,
# #     )
# #     cols = {"le_book", "date_creation", "date_last_modified"}
# #     cols.update(MANDATORY_COLUMNS.get(table, []))
# #     cols.update(ACCURACY_COLUMNS.get(table, []))
# #     cols.update(TIMELINESS_COLUMNS.get(table, []))
# #     cols.update(VALIDITY_COLUMNS.get(table, []))
# #     for rule in REL_RULE_META.values():
# #         if rule["child_table"]  == table: cols.add(rule["child_col"])
# #         if rule["parent_table"] == table: cols.add(rule["parent_col"])
# #     return cols
# HISTORY_FILE   = SCRIPT_DIR / "dq_history.json"

# def _append_history(entry: dict) -> None:
#     """Append (or replace same-date entry) in the history log."""
#     history: list = []
#     if HISTORY_FILE.exists():
#         try:
#             history = json.loads(HISTORY_FILE.read_text())
#         except Exception:
#             history = []
#     # idempotent: replace if same date was already written today
#     history = [e for e in history if e.get("date") != entry["date"]]
#     history.append(entry)
#     history.sort(key=lambda e: e.get("date", ""))
#     HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str))
#     log.info("History log updated → %s  (%d entries total)", HISTORY_FILE, len(history))


# # def _build_history_entry(run_date: str, R: dict, categories: dict,
# #                          dup_counts: dict | None = None,
# #                          cat_dup_counts: dict | None = None) -> dict:
# #     """
# #     Aggregate engine results into a single history entry:
# #       overall        — one score per dimension (4 dims; RI averaged into accuracy)
# #       by_category    — average scores per category type (B, MF, OSACCO)
# #       by_institution — per-le_book scores across all 4 dimensions
# #     """
# #     # overall scores — extract all engines then merge RI into accuracy
# #     overall: dict[str, float] = {}
# #     for rkey, (dim, overall_key, _) in _SCORE_KEYS.items():
# #         esummary = (R.get(rkey) or {}).get("executive_summary") or {}
# #         overall[dim] = round(float(esummary.get(overall_key) or 0.0), 2)
# #     overall = _merge_rel(overall)

# #     # per-institution scores per dimension (extract then merge RI into accuracy)
# #     lb_dim_scores: dict[str, dict[str, float]] = {}
# #     for rkey, (dim, _, lb_score_key) in _SCORE_KEYS.items():
# #         inst_scores = _inst_scores_from_report(R.get(rkey) or {}, lb_score_key)
# #         for lb, score in inst_scores.items():
# #             lb_dim_scores.setdefault(lb, {})[dim] = score
# #     for lb in lb_dim_scores:
# #         lb_dim_scores[lb] = _merge_rel(lb_dim_scores[lb])

# #     # enrich with category metadata; compute per-institution overall (4 dims)
# #     _dups     = dup_counts or {}
# #     _cat_dups = cat_dup_counts or {}
# #     by_institution: dict = {}
# #     for lb, dim_scores in lb_dim_scores.items():
# #         cat_info    = categories.get(lb, {})
# #         inst_scores = [dim_scores[d] for d in DIMS if d in dim_scores]
# #         by_institution[lb] = {
# #             "name":               cat_info.get("name", lb),
# #             "category_type":      cat_info.get("category_type", ""),
# #             "overall":            round(sum(inst_scores) / len(inst_scores), 2) if inst_scores else 0.0,
# #             "customer_duplicates": _dups.get(lb, 0),
# #             **{d: dim_scores.get(d, 0.0) for d in DIMS},
# #         }

# #     # Institutions with dup counts but no windowed DQ data still need an entry
# #     for lb, dup in _dups.items():
# #         if lb not in by_institution:
# #             cat_info = categories.get(lb, {})
# #             by_institution[lb] = {
# #                 "name":               cat_info.get("name", lb),
# #                 "category_type":      cat_info.get("category_type", ""),
# #                 "overall":            0.0,
# #                 "customer_duplicates": dup,
# #                 **{d: 0.0 for d in DIMS},
# #             }

# #     # by_category: average institution scores grouped by category_type
# #     cat_buckets: dict[str, list[dict]] = {}
# #     for inst_data in by_institution.values():
# #         ct = inst_data.get("category_type", "")
# #         if ct in CATEGORY_TYPES:
# #             cat_buckets.setdefault(ct, []).append(inst_data)

# #     by_category: dict = {}
# #     for ct, institutions in cat_buckets.items():
# #         cat_scores: dict[str, float] = {}
# #         for dim in DIMS:
# #             scores = [i[dim] for i in institutions if i.get(dim, 0) > 0]
# #             cat_scores[dim] = round(sum(scores) / len(scores), 2) if scores else 0.0
# #         # Use pre-computed DISTINCT count when available to avoid
# #         # double-counting customers with duplicates at multiple institutions.
# #         cat_scores["customer_duplicates"] = _cat_dups.get(
# #             ct, sum(i.get("customer_duplicates", 0) for i in institutions)
# #         )
# #         by_category[ct] = cat_scores

# #     # per-table failing rule counts and null column detail
# #     table_fail_counts = _table_fail_counts(R)
# #     table_null_cols   = _table_null_cols((R.get("comp") or {}))

# #     return {
# #         "date":              run_date,
# #         "overall":           overall,
# #         "by_category":       by_category,
# #         "by_institution":    by_institution,
# #         "table_fail_counts": table_fail_counts,
# #         "table_null_cols":   table_null_cols,
# #     }


# def fetch_le_book_categories(engine, schema: str) -> dict:
#     """
#     Return {le_book: {"name": ..., "category_type": ...}} for all in-scope
#     institutions.
#     """
#     filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)
#     sql = text(f"""
#         SELECT lb.le_book,
#                LOWER(lb.leb_description)  AS le_book_name,
#                ast.category_type
#         FROM "{schema}".le_book lb
#         LEFT JOIN (
#             SELECT alpha_tab     AS category_type_at,
#                    alpha_sub_tab AS category_type
#             FROM   "{schema}".alpha_sub_tab
#         ) ast ON lb.category_type_at = ast.category_type_at
#              AND lb.category_type    = ast.category_type
#         WHERE ast.category_type IN ({filter_list})
#     """)
#     try:
#         with engine.connect() as conn:
#             rows = conn.execute(sql).fetchall()
#         result = {}
#         for r in rows:
#             lb = str(r[0]).strip() if r[0] else None
#             if not lb:
#                 continue
#             name = str(r[1]).strip() if r[1] and str(r[1]).strip() not in ("", "none", "nan") else lb
#             ct   = str(r[2]).strip() if r[2] else ""
#             result[lb] = {"name": name, "category_type": ct}
#         log.info("le_book categories: %d institutions loaded", len(result))
#         return result
#     except Exception as exc:
#         log.warning("Could not fetch le_book categories: %s", exc)
#         return {}
    
