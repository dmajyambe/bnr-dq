from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

log = logging.getLogger("dq_resolution")


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

STATUS_OPEN     = "OPEN"
STATUS_REOPENED = "REOPENED"
STATUS_RESOLVED = "RESOLVED"


# ─────────────────────────────────────────────────────────────
# INCREMENTAL SOURCE OF TRUTH
# ─────────────────────────────────────────────────────────────

def get_last_detection_time(conn, schema: str):
    """
    Pull last detection timestamp from dq_issue_snapshot
    (THIS replaces dq_pipeline_runs entirely)
    """
    try:
        row = conn.execute(text("""
            SELECT MAX(detected_at)
            FROM dq_issue_snapshot
            WHERE schema_name = :sc
        """), {"sc": schema}).fetchone()

        

        return row[0] if row and row[0] else None

    except Exception as exc:
        log.warning("Could not fetch last_detection_time: %s", exc)
        return None

# ISSUE FETCH

def fetch_active_issues(conn, last_time):
    """
    Pull OPEN + REOPENED issues from tracker
    then optionally filter by detected_at (incremental mode)
    """
    from dq_issue_tracker import get_issues

    issues = (
        get_issues(status=STATUS_OPEN) +
        get_issues(status=STATUS_REOPENED)
    )

    if not last_time:
        return issues

    filtered = [
        i for i in issues
        if i.get("detected_at") is None
        or i["detected_at"] >= last_time
    ]

    log.info("Incremental filter applied: %d → %d issues",
             len(issues), len(filtered))

    return filtered


# ─────────────────────────────────────────────────────────────
# CORE RULE ENGINE HOOKS (unchanged)
# ─────────────────────────────────────────────────────────────

def _record(issue, invalid, run_id, stats):
    """Apply a re-evaluation result to one issue: resolve / reopen / update count."""
    from dq_issue_tracker import resolve_issue, update_issue_count, reopen_issue

    tid = issue["issue_id"]
    cur = issue.get("status", STATUS_OPEN)

    if invalid == 0:
        if cur != STATUS_RESOLVED:
            resolve_issue(tid, run_id)
            stats["resolved"] += 1
    else:
        if cur == STATUS_RESOLVED:
            reopen_issue(tid)
            stats["reopened"] += 1
        else:
            update_issue_count(tid, invalid)
            stats["unchanged"] += 1


# ─────────────────────────────────────────────────────────────
# MAIN RESOLUTION ENGINE — re-evaluate open issues via the SQL engines
# ─────────────────────────────────────────────────────────────

def run_resolution_scan(engine, schema: str, run_id: str):
    """Re-check every active (OPEN/REOPENED) issue against current data and
    resolve / reopen / update it.

    Re-evaluation reuses the detection engines' evaluate_from_sql (full-table scan,
    memory-safe) restricted to the tables + institutions that actually have open
    issues, then matches each issue to its current failing count:
      completeness → null_cells   |   accuracy/validity/uniqueness → rule 'invalid'
    Issues whose rule no longer exists simply get a count of 0 → resolved.
    """
    import os
    import tempfile
    import completeness_check as comp_eng
    import accuracy_check     as acc_eng
    import validity_check     as val_eng
    import uniqueness_check   as uni_eng
    from dq_issue_tracker import _COMP_TABLE_RULE

    stats = {"resolved": 0, "unchanged": 0, "reopened": 0, "skipped": 0, "errors": 0}

    with engine.connect() as conn:
        last_time = get_last_detection_time(conn, schema)
    log.info("Resolution scan run_id=%s  (incremental baseline=%s)", run_id, last_time)

    issues = fetch_active_issues(None, last_time)
    if not issues:
        log.info("No active issues.")
        return stats
    log.info("Active issues: %d", len(issues))

    tables   = sorted({i["table_name"] for i in issues})
    le_books = frozenset(str(i["le_book"]) for i in issues)
    log.info("Re-evaluating %d table(s) for %d institution(s) …", len(tables), len(le_books))

    def _run(eng):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            return eng.evaluate_from_sql(engine, schema, le_books, 0, {}, path, tables=tables)
        except Exception as exc:
            log.warning("  %s re-eval failed: %s", eng.__name__, exc)
            return {}
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    reports = {"comp": _run(comp_eng), "acc": _run(acc_eng),
               "val": _run(val_eng),  "uni": _run(uni_eng)}

    # build (le_book, rule_id) -> current failing count
    counts: dict[tuple, int] = {}
    for table, tdata in (reports["comp"] or {}).get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        rid = _COMP_TABLE_RULE.get(table)
        if not rid:
            continue
        for lb, b in tdata.get("le_book_breakdown", {}).items():
            counts[(str(lb), rid)] = int(b.get("null_cells") or 0)
    for key in ("acc", "val", "uni"):
        for table, tdata in (reports[key] or {}).get("tables", {}).items():
            if tdata.get("status") != "evaluated":
                continue
            for lb, b in tdata.get("le_book_breakdown", {}).items():
                for rid, rd in b.get("rules", {}).items():
                    counts[(str(lb), rid)] = int(rd.get("invalid") or 0)

    # re-record every active issue (default 0 = no current failure → resolve)
    for iss in issues:
        try:
            cnt = counts.get((str(iss["le_book"]), iss["rule_id"]), 0)
            _record(iss, cnt, run_id, stats)
        except Exception as exc:
            log.warning("  record failed for %s: %s", iss.get("issue_id"), exc)
            stats["errors"] += 1

    log.info("DONE %s", stats)
    return stats


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

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