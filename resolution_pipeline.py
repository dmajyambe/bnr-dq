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


# ─────────────────────────────────────────────────────────────
# ISSUE FETCH
# ─────────────────────────────────────────────────────────────

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

def _build_dispatch():
    import accuracy_check
    import timeliness_check
    import validity_check

    from accuracy_check   import RULE_META as ACC_META
    from timeliness_check import RULE_META as TIM_META
    from validity_check   import RULE_META as VAL_META

    dispatch = {}

    for rid in ACC_META:
        dispatch[rid] = accuracy_check
    for rid in TIM_META:
        dispatch[rid] = timeliness_check
    for rid in VAL_META:
        dispatch[rid] = validity_check

    return dispatch


def _db_columns(conn, schema: str, table: str):
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()

    return {r[0].lower() for r in rows}


# ─────────────────────────────────────────────────────────────
# CORE EVALUATION (UNCHANGED LOGIC)
# ─────────────────────────────────────────────────────────────

def _record(conn, issue, invalid, run_id, stats):
    from dq_issue_tracker import (
        resolve_issue,
        update_issue_count,
        reopen_issue,
    )

    from dq_resolution_history import append_event

    tid = issue["issue_id"]
    cur = issue.get("status", STATUS_OPEN)

    if invalid == 0:
        if cur != STATUS_RESOLVED:
            resolve_issue(tid, run_id)
            append_event(conn, tid, "RESOLVED", cur, STATUS_RESOLVED, run_id)
            stats["resolved"] += 1

    else:
        if cur == STATUS_RESOLVED:
            reopen_issue(tid, run_id)
            append_event(conn, tid, "REOPENED", STATUS_RESOLVED, STATUS_REOPENED, run_id)
            stats["reopened"] += 1
        else:
            update_issue_count(tid, invalid)
            append_event(conn, tid, "STATUS_CHECKED", cur, cur, run_id)
            stats["unchanged"] += 1


# ─────────────────────────────────────────────────────────────
# MAIN RESOLUTION ENGINE
# ─────────────────────────────────────────────────────────────

def run_resolution_scan(engine, schema: str, run_id: str):

    from dq_issue_tracker import get_issues, _conn

    stats = {
        "resolved": 0,
        "unchanged": 0,
        "reopened": 0,
        "errors": 0,
    }

    with engine.connect() as conn:

        log.info("Starting resolution scan run_id=%s", run_id)

        # ─────────────────────────────────────────────
        # INCREMENTAL BASELINE (NEW CORE FIX)
        # ─────────────────────────────────────────────
        last_time = get_last_detection_time(conn, schema)

        log.info("Incremental mode: last_detection_time=%s", last_time)

        # ─────────────────────────────────────────────
        # FETCH ACTIVE ISSUES
        # ─────────────────────────────────────────────
        issues = fetch_active_issues(conn, last_time)

        if not issues:
            log.info("No active issues.")
            return stats

        log.info("Active issues: %d", len(issues))

        dispatch = _build_dispatch()

        # group by table/le_book (optimization from detection design)
        groups = {}
        for i in issues:
            groups.setdefault((i["table_name"], i["le_book"]), []).append(i)

        # ─────────────────────────────────────────────
        # PROCESS GROUPS
        # ─────────────────────────────────────────────
        for (table, le_book), group in groups.items():

            log.info("Processing table=%s le_book=%s issues=%d",
                     table, le_book, len(group))

            with engine.connect() as conn:

                conn.execute(text("SET work_mem = '256MB'"))

                for iss in group:

                    rule_id = iss["rule_id"]
                    mod = dispatch.get(rule_id)

                    try:
                        if not mod:
                            stats["errors"] += 1
                            continue

                        # load minimal required data
                        df = mod.load_data(engine, schema, table, le_book)

                        result = mod.run_rule(rule_id, df)

                        if result is None:
                            stats["errors"] += 1
                            continue

                        _, invalid, _ = result

                        _record(conn, iss, invalid, run_id, stats)

                    except Exception as exc:
                        log.warning("Rule failed %s: %s", rule_id, exc)
                        stats["errors"] += 1

                conn.commit()

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