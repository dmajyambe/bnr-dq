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

    # Only back up ZIPs for institutions we are about to rebuild — scoping by
    # valid_le_books prevents a filtered call (e.g. two institutions) from
    # backing up and then deleting all other institutions' ZIPs.
    stale = [
        ISSUE_REPORTS_DIR / f"{lb}_{month}.zip"
        for lb in sorted(valid_le_books)
        if (ISSUE_REPORTS_DIR / f"{lb}_{month}.zip").exists()
    ]
    for old in stale:
        old.rename(old.with_suffix(".zip.bak"))
        log.info("Backed up stale ZIP: %s -> %s", old.name, old.with_suffix(".zip.bak").name)

    backed_up = {old.with_suffix(".zip.bak") for old in stale}

    try:
        log.info("Streaming per-institution failing-row ZIPs …")
        for table in tables:
            write_institution_zips(engine, schema, table, valid_le_books, categories,
                                   month, limit, extra_where=extra_where)
    except Exception:
        # Restore only the backups we created
        for bak in backed_up:
            if bak.exists():
                bak.rename(bak.with_suffix("").with_suffix(".zip"))
                log.warning("Restored backup ZIP: %s", bak.name)
        raise

    # New ZIPs written successfully — remove only our backups
    for bak in backed_up:
        if bak.exists():
            bak.unlink()
            log.info("Removed backup ZIP: %s", bak.name)


def write_resolved_zips(resolved_issues: list[dict]) -> None:
    """Group newly-resolved issues by (le_book, month) and write pre-built resolved ZIPs."""
    from collections import defaultdict
    from dq.exports.failing_rows import write_resolved_institution_zip

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for iss in resolved_issues:
        month = (iss.get("detected_at") or "")[:7]
        if month:
            groups[(str(iss["le_book"]), month)].append(iss)

    for (le_book, month), issues in sorted(groups.items()):
        write_resolved_institution_zip(le_book, month, issues)
