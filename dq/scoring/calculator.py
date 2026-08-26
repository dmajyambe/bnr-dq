# Pure scoring/aggregation math — no DB, no file I/O. This is exactly the
# slice tests/test_pipeline_unit.py already exercises in isolation, which is
# what makes this a natural module boundary rather than an invented one.
from __future__ import annotations

# rkey -> (dim label, executive_summary overall key, per-le_book score key)
_SCORE_KEYS = {
    "comp": ("completeness", "overall_completeness_score", "completeness_score"),
    "acc":  ("accuracy",     "overall_accuracy_score",     "accuracy_score"),
    "tim":  ("timeliness",   "overall_timeliness_score",   "timeliness_score"),
    "val":  ("validity",     "overall_validity_score",     "validity_score"),
    "uni":  ("uniqueness",   "overall_uniqueness_score",   "uniqueness_score"),
    "rel":  ("_rel",         "overall_ri_score",           "ri_score"),
}

DIMS = ["completeness", "accuracy", "timeliness", "validity", "uniqueness"]


def merge_rel(scores: dict) -> dict:
    """Fold the relationship (referential-integrity) score into accuracy,
    since relationship isn't one of the 4 scored DIMS on its own. No-op when
    "_rel" isn't present — callers must not inject a phantom 0 for a
    dimension that wasn't run (that would silently halve accuracy)."""
    if "_rel" not in scores:
        return scores
    acc = float(scores.get("accuracy") or 0.0)
    rel = float(scores.pop("_rel"))
    scores["accuracy"] = round((acc + rel) / 2, 2)
    return scores


def inst_scores_from_report(report: dict, lb_score_key: str) -> dict[str, float]:
    """Average each le_book's per-table scores across all evaluated tables."""
    lb_table_scores: dict[str, list[float]] = {}
    for tbl_data in report.get("tables", {}).values():
        if tbl_data.get("status") != "evaluated":
            continue
        for lb, lb_data in tbl_data.get("le_book_breakdown", {}).items():
            s = lb_data.get(lb_score_key)
            if s is not None:
                lb_table_scores.setdefault(lb, []).append(float(s))
    return {
        lb: round(sum(scores) / len(scores), 2)
        for lb, scores in lb_table_scores.items()
        if scores
    }


def inst_table_scores_from_report(report: dict, lb_score_key: str) -> dict[str, dict[str, float]]:
    """Return {le_book: {table_name: score}} — per-table breakdown, not averaged."""
    result: dict[str, dict[str, float]] = {}
    for table, tbl_data in report.get("tables", {}).items():
        if tbl_data.get("status") != "evaluated":
            continue
        for lb, lb_data in tbl_data.get("le_book_breakdown", {}).items():
            s = lb_data.get(lb_score_key)
            if s is not None:
                result.setdefault(lb, {})[table] = round(float(s), 1)
    return result
