from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_resolution")

# status constants — MUST match the lowercase values dq_issue_tracker stores
# (get_issues does a case-sensitive `status=?` match). The tracker has no
# 'reopened' status: a recurrence is recorded as status='open' + recurrence_count.
STATUS_OPEN      = "open"
STATUS_PENALIZED = "penalized"
STATUS_PENDING   = "pending_resolution"
STATUS_RESOLVED  = "resolved"

# every status that counts as an active (unresolved) issue to re-evaluate.
# Mirrors resolve_issue's `WHERE status IN ('open','penalized','pending_resolution')`.
ACTIVE_STATUSES  = (STATUS_OPEN, STATUS_PENALIZED, STATUS_PENDING)

# ISSUE FETCH
def fetch_active_issues():
    """Return every issue currently in an active (unresolved) state — the full,
    deterministic candidate set for re-evaluation.

    No incremental/timestamp filtering: the same active set is scanned on every
    run, so a fix is always picked up regardless of when the issue was detected.
    """
    from dq_issue_tracker import get_issues
    issues: list[dict] = []
    for st in ACTIVE_STATUSES:
        issues += get_issues(status=st)
    return issues


#rule engines

def _record(issue, invalid, run_id, stats):
    """Apply a re-evaluation result to one active issue, with a two-scan debounce
    so a single transient 0 (e.g. reading the warehouse mid-reload) can't
    permanently false-resolve a still-failing issue:

      invalid == 0, first clean scan   → park in pending_resolution (NOT resolved)
      invalid == 0, already pending     → resolve (confirmed clean on two scans)
      invalid  > 0                      → update count (also flips pending → open)

    pending_resolution still counts as open everywhere (see OPEN_STATUSES), so a
    parked issue stays visible and is re-fetched on the next scan.
    """
    from dq_issue_tracker import (resolve_issue, update_issue_count,
                                  mark_pending_resolution)

    tid = issue["issue_id"]
    if invalid == 0:
        if issue.get("status") == STATUS_PENDING:   # two clean scans in a row
            resolve_issue(tid, run_id)
            stats["resolved"] += 1
        else:                                        # first clean scan → park it
            mark_pending_resolution(tid)
            stats["pending"] += 1
    else:
        update_issue_count(tid, invalid)             # still failing — un-parks if pending
        stats["unchanged"] += 1


def _collect_counts(reports: dict, comp_table_rule: dict) -> dict[tuple, int]:
    """Flatten engine reports → {(le_book, rule_id): current_failing_count}.

    Reads the same report shape detect_and_update_issues consumes: completeness
    contributes null_cells per (le_book, COMP rule); acc/val/uni contribute
    rules[rid].invalid per le_book. Only 'evaluated' tables are trusted.
    """
    counts: dict[tuple, int] = {}
    for table, tdata in (reports.get("comp") or {}).get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        rid = comp_table_rule.get(table)
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


#re-evaluate every active issue and update status/count accordingly

def run_resolution_scan(engine, schema: str, run_id: str):
    import os
    import re
    import tempfile
    from collections import defaultdict
    import completeness_check as comp_eng
    import accuracy_check     as acc_eng
    import validity_check     as val_eng
    import uniqueness_check   as uni_eng
    from dq_issue_tracker import _COMP_TABLE_RULE
    from dq_monthly_detection import month_filter

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
        counts = _collect_counts(reports, _COMP_TABLE_RULE)

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


#cli
if __name__ == "__main__":

    load_dotenv(SCRIPT_DIR / ".env")

    import argparse
    from db_utils import get_engine, build_connection_string
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=os.environ.get("DQ_SCHEMA", "dqp"))
    args = parser.parse_args()
    engine = get_engine(build_connection_string())
    run_id = datetime.now().strftime("RES-%Y%m%d-%H%M%S")

    run_resolution_scan(engine, args.schema, run_id)