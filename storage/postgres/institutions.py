# Utility module for Institutions(le-books and categories)
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
    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)#MERGE THE CATEGORIES INTO A COMMA SEP LIST
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
        log.info(f"le_book categories: {len(result)} institutions loaded")
        return result
    except Exception as exc:
        log.warning("Could not fetch le_book categories: %s", exc)
        return {}

