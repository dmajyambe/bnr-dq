# Completeness engine — null-counts per mandatory column, pure SQL, no DataFrames.
from __future__ import annotations
import logging
from dq.rules.completeness import MANDATORY_COLUMNS
from dq.sql.builders import new_report, finalize_report
from dq.sql.filters import date_window_clause, le_book_clause
from dq.sql.metadata import existing_columns
from sqlalchemy import text

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq.engines.completeness")

TARGET_TABLES = list(MANDATORY_COLUMNS.keys())

def _le_book_breakdown(rows, found_cols: list[str]) -> tuple[dict, set]:
    """Per-le_book completeness scores from the grouped SQL rows."""
    breakdown:    dict = {}
    all_le_books: set  = set()
    for r in rows:
        lb             = str(r["le_book"])
        all_le_books.add(lb)
        lb_total       = int(r["total_rows"])
        lb_nulls       = {c: int(r.get(f"null_{c}") or 0) for c in found_cols}
        lb_null_cells  = sum(lb_nulls.values())
        lb_total_cells = lb_total * len(found_cols)
        breakdown[lb]  = {
            "row_count":          lb_total,
            "completeness_score": round((1 - lb_null_cells / lb_total_cells) * 100, 2)
                                  if lb_total_cells else 100.0,
            "null_counts":        lb_nulls,
            "null_cells":         lb_null_cells,
            "total_cells":        lb_total_cells,
        }
    return breakdown, all_le_books


def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
                       window_days: int, watermarks: dict, output_path: str,
                       row_limit: int = 0,
                       tables: list[str] | None = None,
                       extra_where: str = "") -> dict:
    """Run completeness checks in pure SQL — one query per table, no DataFrames."""
    report = new_report()
    all_scores:   list[float] = []
    all_le_books: set         = set()
    target = tables if tables is not None else TARGET_TABLES
    lb_clause    = le_book_clause(valid_le_books)
    extra_clause = f"AND ({extra_where})" if extra_where else ""

    with engine.connect() as conn:
        for table in target:
            log.info("━━  %s", table)
            mandatory = MANDATORY_COLUMNS.get(table, [])
            if not mandatory:
                continue

            sq       = f'"{schema}"."{table}"'
            wanted   = set(mandatory) | {"le_book", "date_creation", "date_last_modified"}
            existing = existing_columns(conn, schema, table, wanted)

            found_cols   = [c for c in mandatory if c in existing]
            missing_cols = [c for c in mandatory if c not in existing]
            if not found_cols:
                report["tables"][table]   = {"status": "not_found"}
                report["warnings"][table] = "No mandatory columns found in DB."
                continue

            date_clause = date_window_clause(existing, watermarks.get(table), window_days)

            null_exprs = ",\n        ".join(
                f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "null_{c}"'
                for c in found_cols
            )
            has_lb     = "le_book" in existing
            lb_select  = '"le_book", ' if has_lb else ""
            group_by   = 'GROUP BY "le_book" ORDER BY "le_book"' if has_lb else ""
            limit_sql  = f"LIMIT {row_limit}" if row_limit > 0 else ""
            scope_cols = sorted(({"le_book"} if has_lb else set()) | set(found_cols))

            sql = f"""
                WITH scope AS (
                    SELECT {", ".join(f'"{c}"' for c in scope_cols)}
                    FROM   {sq}
                    WHERE  {date_clause}
                    {lb_clause}
                    {extra_clause}
                    {limit_sql}
                )
                SELECT {lb_select}COUNT(*) AS total_rows,
                       {null_exprs}
                FROM scope
                {group_by}
            """

            try:
                rows = conn.execute(text(sql)).mappings().fetchall()
            except Exception as exc:
                log.error("  %s: query failed — %s", table, exc)
                conn.rollback()
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = str(exc)
                continue

            if not rows:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = "No rows in window."
                continue

            total_rows  = sum(int(r["total_rows"]) for r in rows)
            null_counts = {c: sum(int(r.get(f"null_{c}") or 0) for r in rows) for c in found_cols}
            null_cells  = sum(null_counts.values())
            total_cells = total_rows * len(found_cols)
            score       = round((1 - null_cells / total_cells) * 100, 2) if total_cells else 100.0
            all_scores.append(score)

            lb_breakdown: dict = {}
            if has_lb:
                lb_breakdown, lb_books = _le_book_breakdown(rows, found_cols)
                all_le_books.update(lb_books)

            report["tables"][table] = {
                "status":             "evaluated",
                "row_count":          total_rows,
                "mandatory_count":    len(mandatory),
                "found_in_db":        len(found_cols),
                "missing_from_db":    missing_cols,
                "completeness_score": score,
                "null_counts":        null_counts,
                "null_cells":         null_cells,
                "total_cells":        total_cells,
                "le_book_breakdown":  lb_breakdown,
            }
            log.info("  %-30s  score=%.2f%%  null=%d/%d cells",
                     table, score, null_cells, total_cells)

    return finalize_report(report, all_scores, all_le_books, output_path,
                            overall_key="overall_completeness_score",
                            schema=schema, row_limit=row_limit)
