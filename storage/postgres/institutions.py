# Utility module for Institution-scope lookups against the dqp schema.
from __future__ import annotations
import logging
from sqlalchemy import text

log = logging.getLogger("storage.postgres.institutions")

CATEGORY_TYPES = ("MF", "SACCO", "OSACCO", "B")


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

#check duplicate customer counts
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
