# Shared SQL engine for rule-based DQ dimensions (accuracy, validity, …).
#
# Rule-based dimensions all evaluate the same way: each rule is a
# (total_expr, valid_expr) pair of SUM(CASE...) aggregates, run in one
# grouped-by-le_book query per table over a date-windowed scope. invalid =
# total - valid; score = valid/total*100. Only the rule→SQL mapping and the
# dimension's score-key names differ between engines, so that orchestration
# lives here once and each engine supplies its specifics.
#
# Emits the rule-dimension report shape the downstream consumes:
#   tables[t].le_book_breakdown[lb] = {row_count, <score_key>, rules:{rid:{invalid,...}}}
#   executive_summary.<overall_key>
# (see dq_issue_tracker._process_rule_dimension and dq_pipeline_2m._SCORE_KEYS)
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Callable

from sqlalchemy import text

log = logging.getLogger("dq_rule_dimension")


def pct(valid: int, total: int) -> float:
    return round((valid / total) * 100, 2) if total else 100.0


def run_rule_dimension_sql(
    engine, schema: str, valid_le_books: frozenset,
    window_days: int, watermarks: dict, output_path: str,
    row_limit: int = 0,
    tables: list[str] | None = None,
    *,
    table_rules: dict[str, list[str]],
    rule_meta: dict[str, dict],
    rule_sql_fn: Callable[[str, set], "tuple[str, str] | None"],
    score_key: str,
    overall_key: str,
    dim_label: str = "rule",
) -> dict:
    """Run a rule-based dimension in pure SQL. See module docstring for the shape.

    table_rules / rule_meta : the dimension's registry (e.g. ACC_TABLE_RULES / ACC_RULE_META)
    rule_sql_fn(rule_id, existing) -> (total_expr, valid_expr) | None
    score_key / overall_key : e.g. "accuracy_score" / "overall_accuracy_score"
    """
    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables":       {},
        "warnings":     {},
    }
    all_scores:   list[float] = []
    all_le_books: set         = set()
    target = tables if tables is not None else [t for t in table_rules if table_rules.get(t)]

    lb_clause = (
        'AND "le_book" IN (' + ", ".join(f"'{lb}'" for lb in sorted(valid_le_books)) + ")"
        if valid_le_books else ""
    )

    with engine.connect() as conn:
        for table in target:
            log.info("━━  %s", table)
            rule_ids = table_rules.get(table, [])
            if not rule_ids:
                continue

            rule_cols: set = set()
            for rid in rule_ids:
                rule_cols.update(rule_meta.get(rid, {}).get("fields", []))

            sq     = f'"{schema}"."{table}"'
            wanted = list(rule_cols | {"le_book", "date_creation", "date_last_modified"})
            existing = {
                r[0] for r in conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = :s AND table_name = :t AND column_name = ANY(:cols)
                """), {"s": schema, "t": table, "cols": wanted}).fetchall()
            }

            rule_exprs: dict[str, tuple[str, str]] = {}
            for rid in rule_ids:
                ex = rule_sql_fn(rid, existing)
                if ex:
                    rule_exprs[rid] = ex
            if not rule_exprs:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = f"No applicable {dim_label} columns found."
                continue

            # Date window — same convention as completeness_check.
            # window_days falsy (0/None) ⇒ full-table scan (no date filter), used by
            # the monthly full-scan pipeline.
            if not window_days:
                date_clause = "TRUE"
            else:
                wm     = watermarks.get(table)
                anchor = f"'{wm[:10]}'::date" if wm else "CURRENT_DATE"
                date_parts = []
                if "date_creation" in existing:
                    date_parts.append(
                        f'"date_creation" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}')
                if "date_last_modified" in existing:
                    date_parts.append(
                        f'"date_last_modified" > \'{wm}\'' if wm else
                        f'"date_last_modified" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}')
                date_clause = "(" + " OR ".join(date_parts) + ")" if date_parts else "TRUE"

            has_lb     = "le_book" in existing
            scope_cols = sorted(({"le_book"} if has_lb else set()) | (rule_cols & existing))
            lb_select  = '"le_book", ' if has_lb else ""
            group_by   = 'GROUP BY "le_book" ORDER BY "le_book"' if has_lb else ""
            limit_sql  = f"LIMIT {row_limit}" if row_limit > 0 else ""

            rule_selects = []
            for rid, (tot_expr, val_expr) in rule_exprs.items():
                k = rid.lower().replace("-", "")
                rule_selects.append(f'({tot_expr}) AS {k}_total, ({val_expr}) AS {k}_valid')

            sql = f"""
                WITH scope AS (
                    SELECT {", ".join(f'"{c}"' for c in scope_cols)}
                    FROM   {sq}
                    WHERE  {date_clause}
                    {lb_clause}
                    {limit_sql}
                )
                SELECT {lb_select}COUNT(*) AS total_rows,
                       {", ".join(rule_selects)}
                FROM scope
                {group_by}
            """

            try:
                rows = conn.execute(text(sql)).mappings().fetchall()
            except Exception as exc:
                log.error("  %s: query failed — %s", table, exc)
                conn.rollback()
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = str(exc)
                continue

            if not rows:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = "No rows in window."
                continue

            total_rows = sum(int(r["total_rows"]) for r in rows)

            lb_breakdown:    dict        = {}
            table_lb_scores: list[float] = []
            for r in rows:
                if not has_lb:
                    break
                lb = str(r["le_book"])
                all_le_books.add(lb)
                lb_rules:  dict        = {}
                lb_scores: list[float] = []
                for rid in rule_exprs:
                    k   = rid.lower().replace("-", "")
                    tot = int(r.get(f"{k}_total") or 0)
                    val = int(r.get(f"{k}_valid") or 0)
                    if tot == 0:
                        continue
                    score = pct(val, tot)
                    lb_scores.append(score)
                    meta = rule_meta.get(rid, {})
                    lb_rules[rid] = {
                        "rule_name": meta.get("name", rid),
                        "category":  meta.get("category", ""),
                        "fields":    meta.get("fields", []),
                        "valid":     val,
                        "invalid":   tot - val,
                        "total":     tot,
                        score_key:   score,
                    }
                if not lb_rules:
                    continue
                lb_score = round(sum(lb_scores) / len(lb_scores), 2)
                table_lb_scores.append(lb_score)
                lb_breakdown[lb] = {
                    "row_count": int(r["total_rows"]),
                    score_key:   lb_score,
                    "rules":     lb_rules,
                }

            if lb_breakdown:
                table_score = round(sum(table_lb_scores) / len(table_lb_scores), 2)
            else:
                flat_scores = []
                for rid in rule_exprs:
                    k   = rid.lower().replace("-", "")
                    tot = sum(int(r.get(f"{k}_total") or 0) for r in rows)
                    val = sum(int(r.get(f"{k}_valid") or 0) for r in rows)
                    if tot:
                        flat_scores.append(pct(val, tot))
                if not flat_scores:
                    report["tables"][table] = {"status": "no_data", "row_count": total_rows}
                    continue
                table_score = round(sum(flat_scores) / len(flat_scores), 2)

            all_scores.append(table_score)
            report["tables"][table] = {
                "status":            "evaluated",
                "row_count":         total_rows,
                "rules_evaluated":   list(rule_exprs.keys()),
                score_key:           table_score,
                "le_book_breakdown": lb_breakdown,
            }
            log.info("  %-30s  score=%.2f%%  (%d rules, %d le_books)",
                     table, table_score, len(rule_exprs), len(lb_breakdown))

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    report["le_books"]          = sorted(all_le_books)
    report["executive_summary"] = {
        overall_key:        overall,
        "total_tables":     len(report["tables"]),
        "evaluated_tables": sum(1 for v in report["tables"].values()
                                if v.get("status") == "evaluated"),
        "schema":           schema,
        "row_limit":        row_limit,
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("Report written → %s  (overall %.2f%%)", output_path, overall)
    return report
