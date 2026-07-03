# Re-evaluates every active issue against fresh engine results and
# resolves/parks/reopens each one accordingly.
#
# This is the one place outside dq/ that's allowed to import dq.engines
# directly — jobs/resolution_scan.py (the cron entry point) only calls
# run_resolution_scan() below; it never touches dq.engines itself. Keeping
# that boundary means the orchestration layer (jobs/) stays thin and the
# "how do I re-check an issue" logic lives in exactly one place.
from __future__ import annotations

import logging
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime

from dq.sql.filters import month_filter
from issues import repositories as repo
from issues.state_machine import OPEN_STATUSES
from issues.tracker import COMP_TABLE_RULE

log = logging.getLogger("issues.resolution")


def fetch_active_issues() -> list[dict]:
    """All open/penalized/pending_resolution issues, no duplicates.

    (The previous version of this loop called get_issues() once per status in
    OPEN_STATUSES, but get_issues(status="open") already expands to the full
    open+penalized+pending_resolution bucket — so penalized/pending_resolution
    issues were being fetched twice and double-counted in run_resolution_scan's
    stats. get_open_issues() returns that same bucket in one call.)
    """
    return repo.get_open_issues()


def _record(issue: dict, invalid: int, run_id: str, stats: dict) -> None:
    tid = issue["issue_id"]
    if invalid == 0:
        if issue.get("status") == "pending_resolution":   # two clean scans in a row
            repo.resolve_issue(tid, run_id)
            stats["resolved"] += 1
        else:                                              # first clean scan → park it
            repo.mark_pending_resolution(tid)
            stats["pending"] += 1
    else:
        repo.update_issue_count(tid, invalid)              # still failing — un-parks if pending
        stats["unchanged"] += 1


def _collect_counts(reports: dict) -> dict[tuple, int]:
    """Flatten engine reports → {(le_book, rule_id): current_failing_count}.

    Reads the same report shape issues.tracker.detect_and_update_issues consumes:
    completeness contributes null_cells per (le_book, COMP rule); acc/val/uni
    contribute rules[rid].invalid per le_book. Only 'evaluated' tables are trusted.
    """
    counts: dict[tuple, int] = {}
    for table, tdata in (reports.get("comp") or {}).get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        rid = COMP_TABLE_RULE.get(table)
        if not rid:
            continue
        for lb, b in tdata.get("le_book_breakdown", {}).items():
            counts[(str(lb), rid)] = int(b.get("null_cells") or 0)
    for key in ("acc", "val", "uni"):
        for table, tdata in (reports.get(key) or {}).get("tables", {}).items():
            if tdata.get("status") != "evaluated":
                continue
            for lb, b in tdata.get("le_book_breakdown", {}).items():
                for rid, rd in b.get("rules", {}).items():
                    counts[(str(lb), rid)] = int(rd.get("invalid") or 0)
    return counts


def run_resolution_scan(engine, schema: str, run_id: str) -> dict:
    """Re-evaluate every active issue and update status/count accordingly."""
    import dq.engines.accuracy as acc_eng
    import dq.engines.completeness as comp_eng
    import dq.engines.uniqueness as uni_eng
    import dq.engines.validity as val_eng

    stats = {"resolved": 0, "pending": 0, "unchanged": 0, "reopened": 0, "skipped": 0, "errors": 0}

    log.info("Resolution scan run_id=%s", run_id)

    issues = fetch_active_issues()
    if not issues:
        log.info("No active issues.")
        return stats
    log.info("Active issues: %d", len(issues))

    # Group by reporting month (inferred from detected_at) so each group is
    # re-evaluated against the SAME date_last_modified slice detection used.
    # A non-YYYY-MM month (e.g. a full-scan-detected issue) falls back to a
    # full-table scan — never month-scope an issue that wasn't month-detected.
    by_month: dict[str, list] = defaultdict(list)
    for i in issues:
        by_month[(i.get("detected_at") or "")[:7]].append(i)

    def _run(eng, le_books, tables, extra_where):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            return eng.evaluate_from_sql(engine, schema, le_books, 0, {}, path,
                                         tables=tables, extra_where=extra_where)
        except Exception as exc:
            log.warning("  %s re-eval failed: %s", eng.__name__, exc)
            return {}
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    for month, m_issues in sorted(by_month.items()):
        tables   = sorted({i["table_name"] for i in m_issues})
        le_books = frozenset(str(i["le_book"]) for i in m_issues)
        if re.fullmatch(r"\d{4}-\d{2}", month or ""):
            extra_where, _ = month_filter(month)
            scope = f"month {month}"
        else:
            extra_where, scope = "", "full-scan (no reporting month)"
        log.info("Re-evaluating %s: %d issue(s), %d table(s), %d institution(s) …",
                 scope, len(m_issues), len(tables), len(le_books))

        reports = {"comp": _run(comp_eng, le_books, tables, extra_where),
                   "acc":  _run(acc_eng,  le_books, tables, extra_where),
                   "val":  _run(val_eng,  le_books, tables, extra_where),
                   "uni":  _run(uni_eng,  le_books, tables, extra_where)}
        counts = _collect_counts(reports)

        # re-record this month's issues (default 0 = no current failure → resolve)
        for iss in m_issues:
            try:
                cnt = counts.get((str(iss["le_book"]), iss["rule_id"]), 0)
                _record(iss, cnt, run_id, stats)
            except Exception as exc:
                log.warning("  record failed for %s: %s", iss.get("issue_id"), exc)
                stats["errors"] += 1

    log.info("DONE %s", stats)
    return stats
