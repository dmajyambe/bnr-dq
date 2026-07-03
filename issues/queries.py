# Dashboard-facing read aggregations over dq_open_issues — split out of
# issues/tracker.py (which is now lifecycle-only) per review feedback.
# These are query/view-shaping functions: they read issue rows and annotate
# them with urgency + rule metadata for display, but never write state.
from __future__ import annotations

from datetime import date

from issues import repositories as repo
from issues.state_machine import OPEN_SQL, urgency_band
from storage.sqlite.connection import get_connection


def get_institution_issue_summary() -> dict[str, dict]:
    """
    Return {le_book: {worst_urgency, total, new, attention, urgent, critical, overdue}}
    for all institutions with open issues.
    """
    repo.ensure_tables()
    con = get_connection()
    try:
        rows = con.execute(
            f"SELECT le_book, detected_at, sla_deadline FROM dq_open_issues WHERE status IN {OPEN_SQL}"
        ).fetchall()
    finally:
        con.close()

    summary: dict[str, dict] = {}
    _order = ["new", "attention", "urgent", "critical", "overdue"]

    for row in rows:
        lb   = row["le_book"]
        band = urgency_band(row["detected_at"], row["sla_deadline"])

        if lb not in summary:
            summary[lb] = {"total": 0, "new": 0, "attention": 0,
                           "urgent": 0, "critical": 0, "overdue": 0,
                           "worst_urgency": "new"}
        s = summary[lb]
        s["total"] += 1
        s[band]    += 1
        if _order.index(band) > _order.index(s["worst_urgency"]):
            s["worst_urgency"] = band

    return summary


def get_issues_by_table(
    status: str = "open",
    le_book: str | None = None,
    include_pending: bool = True,
) -> dict[str, list[dict]]:
    """
    Return open (+ pending_resolution) issues grouped by table -> rule_id.

    inst_rec fields include: le_book, institution_name, failing_rows,
    urgency_band, detected_at, sla_deadline, days_left, issue_id,
    recurrence_count, rule_name, pending (bool — True if pending_resolution).
    """
    repo.ensure_tables()
    statuses = (f"('{status}','pending_resolution')" if include_pending
                else f"('{status}')")
    con = get_connection()
    try:
        clauses = [f"status IN {statuses}"]
        params: list = []
        if le_book:
            clauses.append("le_book=?")
            params.append(le_book)
        where = "WHERE " + " AND ".join(clauses)
        rows  = con.execute(
            f"SELECT * FROM dq_open_issues {where} ORDER BY sla_deadline", params
        ).fetchall()
    finally:
        con.close()

    today = date.today()

    try:
        from dq.rules.accuracy import ACC_RULE_META
        from dq.rules.completeness import COMP_RULE_META
        from dq.rules.timeliness import TIM_RULE_META
        from dq.rules.uniqueness import UNI_RULE_META
        from dq.rules.validity import VAL_RULE_META
        _all_meta = {**COMP_RULE_META, **ACC_RULE_META, **TIM_RULE_META,
                     **VAL_RULE_META, **UNI_RULE_META}
    except Exception:
        _all_meta = {}

    _band_order = ["new", "attention", "urgent", "critical", "overdue"]

    table_rule: dict[str, dict[str, list]] = {}
    for row in rows:
        tbl  = row["table_name"]
        rid  = row["rule_id"]
        band = urgency_band(row["detected_at"], row["sla_deadline"])
        try:
            days_left = (date.fromisoformat(row["sla_deadline"]) - today).days
        except Exception:
            days_left = 0

        # rule_name: stored on the row (from our schema), fall back to registry
        rname = (row["rule_name"] if row["rule_name"]
                 else _all_meta.get(rid, {}).get("name", rid))

        inst_rec = {
            "le_book":          row["le_book"],
            "institution_name": (row["institution_name"] or row["le_book"]).title(),
            "failing_rows":     row["failing_rows"],
            "urgency_band":     band,
            "detected_at":      row["detected_at"],
            "sla_deadline":     row["sla_deadline"],
            "days_left":        days_left,
            "issue_id":         row["issue_id"],
            "recurrence_count": int(row["recurrence_count"] or 0),
            "rule_name":        rname,
            "pending":          row["status"] == "pending_resolution",
        }
        table_rule.setdefault(tbl, {}).setdefault(rid, []).append(inst_rec)

    # Build final structure
    result: dict[str, list[dict]] = {}
    for tbl, rules in table_rule.items():
        rule_list = []
        for rid, inst_list in rules.items():
            meta       = _all_meta.get(rid, {})
            all_bands  = [i["urgency_band"] for i in inst_list]
            worst      = max(all_bands, key=lambda b: _band_order.index(b)
                             if b in _band_order else 0)
            days_vals  = [i["days_left"] for i in inst_list]
            rule_list.append({
                "rule_id":            rid,
                "rule_name":          inst_list[0].get("rule_name") or meta.get("name", rid),
                "dimension":          inst_list[0].get("dimension",
                                       rows[0]["dimension"] if rows else ""),
                "worst_urgency":      worst,
                "institution_count":  len(inst_list),
                "total_failing_rows": sum(i["failing_rows"] for i in inst_list),
                "min_sla_days":       min(days_vals),
                "sla_overdue":        any(i["urgency_band"] == "overdue" for i in inst_list),
                "any_pending":        any(i.get("pending") for i in inst_list),
                "institutions":       sorted(inst_list,
                                             key=lambda i: _band_order.index(i["urgency_band"])
                                             if i["urgency_band"] in _band_order else 0,
                                             reverse=True),
            })

        # Sort rules: worst urgency first, then most failing rows
        rule_list.sort(
            key=lambda r: (_band_order.index(r["worst_urgency"])
                           if r["worst_urgency"] in _band_order else 0,
                           r["total_failing_rows"]),
            reverse=True,
        )
        result[tbl] = rule_list

    # Sort tables: most rules first
    return dict(sorted(result.items(), key=lambda kv: len(kv[1]), reverse=True))
