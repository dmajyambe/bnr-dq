# Generates the per-institution failing-row ZIPs for a reporting month.
from __future__ import annotations

import logging

from dq.exports.failing_rows import ISSUE_REPORTS_DIR, write_institution_zips
from dq.sql.filters import month_filter as _month_filter

log = logging.getLogger("jobs.exports")


def write_monthly_zips(engine, schema: str, tables: list[str],
                       valid_le_books: frozenset, categories: dict,
                       month: str, limit: int = 0,
                       extra_where: str = "") -> None:
    """Remove stale ZIPs for this month, then stream a fresh {table}.xlsx into
    each institution's issue_reports/{le_book}_{month}.zip for every table.

    Always scopes rows to `month` via date_last_modified so the ZIP content
    matches its label, even when the caller doesn't pass an explicit extra_where.
    """
    if not extra_where:
        extra_where, _ = _month_filter(month)
        log.info("ZIP export date filter (derived from month=%s): %s", month, extra_where)

    ISSUE_REPORTS_DIR.mkdir(exist_ok=True)
    for old in ISSUE_REPORTS_DIR.glob(f"*_{month}.zip"):
        old.unlink()
        log.info("Removed stale ZIP: %s", old.name)

    log.info("Streaming per-institution failing-row ZIPs …")
    for table in tables:
        write_institution_zips(engine, schema, table, valid_le_books, categories,
                               month, limit, extra_where=extra_where)
