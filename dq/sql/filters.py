#shared SQL filtering utilities for all engines (accuracy, timeliness, relationship, uniqueness)
from __future__ import annotations

#date window to filter on, or None for no date window (e.g. for a full-table scan)
def date_window_clause(existing: set[str], wm: str | None, window_days: int) -> str:

    if not window_days:
        return "TRUE"
    anchor = f"'{wm[:10]}'::date" if wm else "CURRENT_DATE"
    parts = []
    if "date_creation" in existing:
        parts.append(f'"date_creation" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}')
    if "date_last_modified" in existing:
        parts.append(
            f'"date_last_modified" > \'{wm}\'' if wm else
            f'"date_last_modified" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}')
    return "(" + " OR ".join(parts) + ")" if parts else "TRUE"

#institution filter for the in-scope LE books, or empty string if no filter (e.g. for a full-table scan)
def le_book_clause(valid_le_books: frozenset) -> str:
    """AND-able SQL fragment restricting rows to the in-scope institutions."""
    return (
        'AND "le_book" IN (' + ", ".join(f"'{lb}'" for lb in sorted(valid_le_books)) + ")"
        if valid_le_books else ""
    )

#month filter for a YYYY-MM reporting month, or empty string if no filter (e.g. for a full-table scan)
def month_filter(month: str, cutoff_date: str | None = None) -> tuple[str, str]:
    """Return (extra_where, last_day) for the reporting month.
    Cutoff_date (YYYY-MM-DD): caps date_creation at the pipeline run date so
    records created *after* detection are not pulled into the same issue report.
    Defaults to end-of-month if not supplied.
    """
    from calendar import monthrange
    y, m   = (int(p) for p in month.split("-")[:2]) #year,month
    last   = monthrange(y, m)[1] #last day
    ny, nm = (y + (m == 12), (m % 12) + 1) #year, next month
    op  = "<=" if cutoff_date else "<"
    rhs = f"'{cutoff_date}'" if cutoff_date else f"'{ny:04d}-{nm:02d}-01'"
    extra_where = (f"\"date_creation\" >= '{y:04d}-{m:02d}-01' "
                   f"AND \"date_creation\" {op} {rhs}")
    return extra_where, f"{y:04d}-{m:02d}-{last:02d}"
