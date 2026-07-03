#issue lifecycle tracker for the dashboard
from __future__ import annotations
import logging
from datetime import date
from issues import repositories as repo
from issues.state_machine import OPEN_SQL, urgency_band
from storage.sqlite.connection import get_connection

log = logging.getLogger("issues.tracker")

# table -> completeness rule_id, derived from the registry (was previously a
# hand-maintained dict here, independently duplicating the inverse mapping
# dq.rules.completeness._COMP_TABLE_NAMES already defines — removed the
# duplicate rather than keeping two registries in sync by hand).
from dq.rules.completeness import _COMP_TABLE_NAMES
COMP_TABLE_RULE: dict[str, str] = {table: rid for rid, table in _COMP_TABLE_NAMES.items()}


def _lookup_rule_name(rule_id: str) -> str:
    """Return the human-readable rule name from the registry, or the rule_id itself."""
    try:
        from dq.rules.accuracy import ACC_RULE_META
        from dq.rules.completeness import COMP_RULE_META
        from dq.rules.timeliness import TIM_RULE_META
        from dq.rules.uniqueness import UNI_RULE_META
        from dq.rules.validity import VAL_RULE_META
        all_meta = {**COMP_RULE_META, **ACC_RULE_META, **TIM_RULE_META,
                    **VAL_RULE_META, **UNI_RULE_META}
        return all_meta.get(rule_id, {}).get("name") or rule_id
    except Exception:
        return rule_id


def _inst_name(lb: str, categories: dict) -> str:
    return (categories.get(lb, {}).get("name") or lb).title()


def _refresh_urgency_bands(con) -> None:
    """Recompute urgency_band for all open issues (picks up overdue once SLA passes)."""
    rows = con.execute(
        f"SELECT issue_id, detected_at, sla_deadline FROM dq_open_issues WHERE status IN {OPEN_SQL}"
    ).fetchall()
    for row in rows:
        band = urgency_band(row["detected_at"], row["sla_deadline"])
        con.execute("UPDATE dq_open_issues SET urgency_band=? WHERE issue_id=?",
                    (band, row["issue_id"]))


# main detection for issues
def detect_and_update_issues(R: dict, categories: dict, run_date: str) -> None:
    """
    Parse engine results (R) and write/update dq_open_issues.

    Any rule with invalid > 0 for a given le_book immediately becomes an open issue.
    Issues where invalid == 0 are marked resolved.
    No score threshold — one failing row is enough.
    """
    repo.ensure_tables()
    con = get_connection()
    try:
        _process_completeness(con, R.get("comp") or {}, categories, run_date)
        _process_rule_dimension(con, R.get("acc") or {}, "accuracy",   "accuracy_score",   categories, run_date)
        _process_rule_dimension(con, R.get("val") or {}, "validity",   "validity_score",   categories, run_date)
        _process_rule_dimension(con, R.get("uni") or {}, "uniqueness", "uniqueness_score", categories, run_date)
        _process_relationship(con, R.get("rel") or {}, categories, run_date)
        _refresh_urgency_bands(con)
        con.commit()
    finally:
        con.close()

    total = repo.count_open()
    log.info("Issue tracker: %d open issue(s) after run %s", total, run_date)
    sync_cr_failing_rows()


def ingest_issues(issues: list[dict], run_date: str) -> None:
    """
    Accept a flat list of pre-detected issues from an external pipeline.

    Each dict must have: le_book, table, rule_id, dimension, failing_rows, score.
    Optional: institution_name.
    """
    repo.ensure_tables()
    con = get_connection()
    try:
        for item in issues:
            lb      = item["le_book"]
            table   = item["table"]
            rule_id = item["rule_id"]
            failing = int(item.get("failing_rows", 0))
            inst    = item.get("institution_name") or lb.title()

            if failing > 0:
                repo.upsert_issue(con, lb, inst, table, rule_id,
                                  item["dimension"], failing, run_date, _lookup_rule_name(rule_id))
            else:
                repo.maybe_resolve(con, lb, table, rule_id)
        _refresh_urgency_bands(con)
        con.commit()
    finally:
        con.close()

    log.info("ingest_issues: processed %d item(s) for %s", len(issues), run_date)


def _process_completeness(con, report: dict, categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        rule_id = COMP_TABLE_RULE.get(table)
        if not rule_id:
            continue
        for lb, lb_data in tdata.get("le_book_breakdown", {}).items():
            failing_rows = int(lb_data.get("null_cells") or 0)
            if failing_rows > 0:
                repo.upsert_issue(con, lb, _inst_name(lb, categories), table,
                                  rule_id, "completeness", failing_rows, run_date,
                                  _lookup_rule_name(rule_id))
            else:
                repo.maybe_resolve(con, lb, table, rule_id)


def _process_rule_dimension(con, report: dict, dimension: str, score_key: str,
                             categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        for lb, lb_data in tdata.get("le_book_breakdown", {}).items():
            inst  = _inst_name(lb, categories)
            rules = lb_data.get("rules", {})
            if not rules:
                failing = int(lb_data.get("invalid") or lb_data.get("null_cells") or 0)
                rule_id = f"{dimension[:3].upper()}-ALL"
                if failing > 0:
                    repo.upsert_issue(con, lb, inst, table, rule_id, dimension, failing,
                                      run_date, _lookup_rule_name(rule_id))
                else:
                    repo.maybe_resolve(con, lb, table, rule_id)
                continue
            for rule_id, rule_data in rules.items():
                failing = int(rule_data.get("invalid") or 0)
                if failing > 0:
                    repo.upsert_issue(con, lb, inst, table, rule_id, dimension, failing,
                                      run_date, _lookup_rule_name(rule_id))
                else:
                    repo.maybe_resolve(con, lb, table, rule_id)


def _process_relationship(con, report: dict, categories: dict, run_date: str) -> None:
    for table, tdata in report.get("tables", {}).items():
        if tdata.get("status") != "evaluated":
            continue
        for rule_id, rule_data in tdata.get("rules", {}).items():
            for lb, lb_data in rule_data.get("le_book_breakdown", {}).items():
                failing = int(lb_data.get("invalid") or 0)
                if failing > 0:
                    repo.upsert_issue(con, lb, _inst_name(lb, categories),
                                      table, rule_id, "relationship", failing, run_date,
                                      _lookup_rule_name(rule_id))
                else:
                    repo.maybe_resolve(con, lb, table, rule_id)


def sync_cr_failing_rows() -> None:
    """
    Recompute each active CR's failing_rows as the sum of its linked issues'
    current counts. Called at the end of each detection run so the CR list
    always reflects the latest pipeline numbers.
    """
    import json as _json
    try:
        import remediation.change_requests as _cr
        active_crs = [c for c in _cr.get_crs()
                      if c["status"] not in ("closed",)]
        if not active_crs:
            return
        con = get_connection()
        try:
            for cr in active_crs:
                iids = _json.loads(cr.get("issue_ids") or "[]")
                if not iids:
                    continue
                placeholders = ",".join("?" * len(iids))
                row = con.execute(
                    f"SELECT COALESCE(SUM(failing_rows),0) FROM dq_open_issues"
                    f" WHERE issue_id IN ({placeholders})", iids
                ).fetchone()
                total = int(row[0])
                con.execute(
                    "UPDATE dq_change_requests SET failing_rows=?, updated_at=?"
                    " WHERE cr_id=?",
                    (total, date.today().isoformat(), cr["cr_id"]),
                )
            con.commit()
        finally:
            con.close()
    except Exception as exc:
        log.warning("sync_cr_failing_rows failed: %s", exc)
