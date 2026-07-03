# Relationship (referential-integrity) engine 
from __future__ import annotations
import logging

from sqlalchemy import text

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq.engines.relationship")


#RI rule execution
def run_rule(
    rule_id: str,
    meta: dict,
    conn,
    schema: str,
    valid_le_books: frozenset,
    sample: int,
    extra_where: str = "",
) -> dict | None:
    """
    Execute one RI rule entirely in SQL.

    Returns a dict with aggregated totals and a per-le_book breakdown, or None
    on error.

    Design notes
    ─────────────
    • Both mandatory and optional FK rules exclude NULL child-column values
      from the denominator.  For mandatory FKs a NULL is a completeness
      violation tracked by dq.engines.completeness.  For optional FKs a NULL is
      intentional.  In both cases, checking a NULL against the parent table
      produces no meaningful signal, so we skip it.

    • The child table is filtered to valid le_books; the parent table is NOT
      filtered.  A parent row may legitimately have a different (or no)
      le_book, and filtering the parent would produce false orphans.

    • The GROUP BY le_book in the child CTE gives us institution-level orphan
      counts in a single query per rule.

    • The parent side is joined against SELECT DISTINCT, not the raw table:
      parent_col is not unique on these tables (e.g. customers_expanded has
      ~3.3M duplicate customer_id rows — multiple snapshots per customer).
      An unfiltered LEFT JOIN fans out on every duplicate, inflating
      total_rows/valid for matched child rows while leaving orphan_rows
      untouched (an orphan join produces exactly one row), which silently
      pulled ri_score toward 100% — found by comparing this rule's score
      against an EXPLAIN ANALYZE of the join during ACC-013..020 activation.
    """
    child_t  = meta["child_table"]
    child_c  = meta["child_col"]
    parent_t = meta["parent_table"]
    parent_c = meta["parent_col"]
    lb_filter    = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_filter = f'AND c.le_book IN ({codes})'

    # extra_where (e.g. month filter on date_last_modified) scopes the CHILD rows;
    # unqualified column names resolve to the single child table `c`.
    extra_clause = f"AND ({extra_where})" if extra_where else ""

    limit_clause = f"LIMIT {sample}" if sample > 0 else ""

    sql = text(f"""
        WITH child AS (
            SELECT c.le_book,
                   c."{child_c}"
            FROM   "{schema}"."{child_t}" c
            WHERE  c."{child_c}" IS NOT NULL
                   {lb_filter}
                   {extra_clause}
            {limit_clause}
        )
        SELECT
            c.le_book,
            COUNT(*)                                                        AS total_rows,
            COUNT(DISTINCT c."{child_c}")                                   AS distinct_child_keys,
            SUM(CASE WHEN p."{parent_c}" IS NULL THEN 1 ELSE 0 END)        AS orphan_rows,
            COUNT(DISTINCT
                CASE WHEN p."{parent_c}" IS NULL THEN c."{child_c}" END
            )                                                               AS orphan_keys
        FROM  child c
        LEFT  JOIN (
            SELECT DISTINCT "{parent_c}" FROM "{schema}"."{parent_t}"
        ) p
            ON c."{child_c}" = p."{parent_c}"
        GROUP BY c.le_book
    """)

    try:
        rows = conn.execute(sql).fetchall()
    except Exception as exc:
        log.warning("  %s query failed: %s", rule_id, exc)
        return None

    if not rows:
        return None

    # aggregate totals across all le_books
    total       = sum(int(r[1]) for r in rows)
    orphan_rows = sum(int(r[3]) for r in rows)
    orphan_keys = sum(int(r[4]) for r in rows)
    valid       = total - orphan_rows
    score       = round(valid / total * 100, 2) if total else 100.0

    # per-le_book breakdown
    lb_breakdown: dict = {}
    for r in rows:
        lb_code    = str(r[0]).strip() if r[0] else "unknown"
        lb_total   = int(r[1])
        lb_orphans = int(r[3])
        lb_valid   = lb_total - lb_orphans
        lb_breakdown[lb_code] = {
            "row_count":  lb_total,
            "valid":      lb_valid,
            "invalid":    lb_orphans,
            "total":      lb_total,
            "ri_score":   round(lb_valid / lb_total * 100, 2) if lb_total else 100.0,
        }
    return {
        "valid":             valid,
        "invalid":           orphan_rows,
        "total":             total,
        "orphan_keys":       orphan_keys,
        "ri_score":          score,
        "le_book_breakdown": lb_breakdown,
    }


