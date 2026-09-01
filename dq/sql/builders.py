# Shared SQL query-building utilities for all engines (accuracy, timeliness, relationship, uniqueness)
from __future__ import annotations
import json
import logging
from datetime import datetime
from typing import Callable
from sqlalchemy import text
from dq.sql.filters import date_window_clause, le_book_clause
from dq.sql.metadata import existing_columns
log = logging.getLogger("dq.sql.builders")


def pct(valid: int, total: int) -> float:
    return round((valid / total) * 100, 2) if total else 100.0


def new_report() -> dict:
    """Empty report skeleton shared by every engine."""
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables":       {},
        "warnings":     {},
    }


def finalize_report(report: dict, all_scores: list[float], all_le_books: set,
                     output_path: str, overall_key: str, **extra_summary) -> dict:
    """Attach le_books + executive_summary, write the JSON report, return it."""
    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    evaluated = sum(1 for v in report["tables"].values() if v.get("status") == "evaluated")

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        overall_key:         overall,
        "total_tables":      len(report["tables"]),
        "evaluated_tables":  evaluated,
        **extra_summary,                          # e.g. schema, row_limit
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    log.info("Report written → %s  (overall %.2f%%)", output_path, overall)
    return report


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
    extra_where: str = "",
) -> dict:
    """Run a rule-based dimension engine against a database schema, returning the report dict."""
    
    report       = new_report()
    all_scores:   list[float] = []
    all_le_books: set         = set()
    target = tables if tables is not None else [t for t in table_rules if table_rules.get(t)]
    lb_clause = le_book_clause(valid_le_books)

    with engine.connect() as conn:
        for table in target:
            log.info("━━  %s", table)
            rule_ids = table_rules.get(table, [])
            if not rule_ids:
                continue

            rule_cols: set = set()
            for rid in rule_ids:
                rule_cols.update(rule_meta.get(rid, {}).get("fields", []))

            sq       = f'"{schema}"."{table}"'
            wanted   = rule_cols | {"le_book", "date_creation", "date_last_modified"}
            existing = existing_columns(conn, schema, table, wanted)

            rule_exprs: dict[str, tuple[str, str]] = {}
            for rid in rule_ids:
                ex = rule_sql_fn(rid, existing)
                if ex:
                    rule_exprs[rid] = ex
            if not rule_exprs:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = f"No applicable {dim_label} columns found."
                continue

            date_clause = date_window_clause(existing, watermarks.get(table), window_days)
            has_lb     = "le_book" in existing
            scope_cols = sorted(({"le_book"} if has_lb else set()) | (rule_cols & existing))
            lb_select  = '"le_book", ' if has_lb else ""
            group_by   = 'GROUP BY "le_book" ORDER BY "le_book"' if has_lb else ""
            limit_sql  = f"LIMIT {row_limit}" if row_limit > 0 else ""
            extra_clause = f"AND ({extra_where})" if extra_where else ""

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
                    {extra_clause}
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

    return finalize_report(report, all_scores, all_le_books, output_path, overall_key,
                            schema=schema, row_limit=row_limit)
