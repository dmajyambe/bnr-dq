# Uniqueness engine — within-window duplicate detection (pure SQL, window functions).
#
# A row is a duplicate when its full key tuple (UNI_RULE_META[rid]["fields"])
# repeats for the same le_book inside the rolling window. Detected server-side
# with ROW_NUMBER() OVER (PARTITION BY le_book, <keys>) — rank > 1 ⇒ duplicate —
# so it stays memory-safe (no DataFrames, no full-table client loads).
#
# Emits the rule-dimension report shape the downstream consumes:
#   tables[t].le_book_breakdown[lb] = {row_count, uniqueness_score, rules:{rid:{invalid,...}}}
#   executive_summary.overall_uniqueness_score
# (see issues/tracker.py:_process_rule_dimension and jobs/monthly_detection.py)
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

from dq.rules.uniqueness import UNI_RULE_META, UNI_TABLE_RULES
from dq.sql.builders import pct, new_report, finalize_report
from dq.sql.filters import date_window_clause, le_book_clause
from dq.sql.metadata import existing_columns
from storage.postgres.connection import build_connection_string, get_engine
from storage.postgres.institutions import get_valid_le_books

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq.engines.uniqueness")



_SQL_TARGET_TABLES = [t for t in UNI_TABLE_RULES if UNI_TABLE_RULES.get(t)]


def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
                      window_days: int, watermarks: dict, output_path: str,
                      row_limit: int = 0,
                      tables: list[str] | None = None,
                      extra_where: str = "") -> dict:
    """Run uniqueness (duplicate-detection) checks in pure SQL — one window query
    per (table, rule), grouped by le_book."""
    report       = new_report()
    all_scores:   list[float] = []
    all_le_books: set         = set()
    target = tables if tables is not None else _SQL_TARGET_TABLES
    lb_clause    = le_book_clause(valid_le_books)
    extra_clause = f"AND ({extra_where})" if extra_where else ""

    with engine.connect() as conn:
        for table in target:
            log.info("━━  %s", table)
            rule_ids = UNI_TABLE_RULES.get(table, [])
            if not rule_ids:
                continue

            wanted: set = {"le_book", "date_creation", "date_last_modified"}
            for rid in rule_ids:
                wanted |= set(UNI_RULE_META[rid]["fields"])
            existing = existing_columns(conn, schema, table, wanted)

            has_lb      = "le_book" in existing
            date_clause = date_window_clause(existing, watermarks.get(table), window_days)
            limit_sql   = f"LIMIT {row_limit}" if row_limit > 0 else ""

            # per-le_book accumulation across this table's rules
            lb_rules_acc:  dict = {}   # lb -> {rid: {...}}
            lb_score_acc:  dict = {}   # lb -> [scores]
            applied_rules: list = []

            for rid in rule_ids:
                fields = UNI_RULE_META[rid]["fields"]
                keys   = [c for c in fields if c in existing]
                anchor = fields[0]
                if anchor not in existing or not keys:
                    continue  # rule not applicable to this table's columns

                part_cols  = (["le_book"] if has_lb else []) + keys
                part_sql   = ", ".join(f'"{c}"' for c in part_cols)
                scope_cols = ", ".join(f'"{c}"' for c in part_cols)
                lb_select  = '"le_book", ' if has_lb else ""
                group_by   = 'GROUP BY "le_book" ORDER BY "le_book"' if has_lb else ""

                sql = f"""
                    WITH scope AS (
                        SELECT {scope_cols}
                        FROM   "{schema}"."{table}"
                        WHERE  {date_clause} AND "{anchor}" IS NOT NULL
                        {lb_clause}
                        {extra_clause}
                        {limit_sql}
                    ),
                    flagged AS (
                        SELECT {lb_select}
                               CASE WHEN ROW_NUMBER() OVER (PARTITION BY {part_sql} ORDER BY {part_sql}) > 1
                                    THEN 1 ELSE 0 END AS is_dup
                        FROM scope
                    )
                    SELECT {lb_select}COUNT(*) AS total_rows, COALESCE(SUM(is_dup), 0) AS invalid
                    FROM flagged
                    {group_by}
                """

                try:
                    rows = conn.execute(text(sql)).mappings().fetchall()
                except Exception as exc:
                    log.error("  %s/%s: query failed — %s", table, rid, exc)
                    conn.rollback()
                    continue

                if not rows:
                    continue
                applied_rules.append(rid)
                meta = UNI_RULE_META[rid]
                for r in rows:
                    if not has_lb:
                        break
                    lb  = str(r["le_book"])
                    tot = int(r["total_rows"])
                    if tot == 0:
                        continue
                    all_le_books.add(lb)
                    inv   = int(r["invalid"] or 0)
                    val   = tot - inv
                    score = pct(val, tot)
                    lb_rules_acc.setdefault(lb, {})[rid] = {
                        "rule_name":        meta["name"],
                        "category":         meta["category"],
                        "fields":           keys,
                        "valid":            val,
                        "invalid":          inv,
                        "total":            tot,
                        "uniqueness_score": score,
                    }
                    lb_score_acc.setdefault(lb, []).append(score)

            if not lb_rules_acc:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = (
                    "No rows in window." if applied_rules else
                    "No applicable uniqueness columns found.")
                continue

            lb_breakdown:    dict        = {}
            table_lb_scores: list[float] = []
            for lb, rules in lb_rules_acc.items():
                lb_score = round(sum(lb_score_acc[lb]) / len(lb_score_acc[lb]), 2)
                table_lb_scores.append(lb_score)
                lb_breakdown[lb] = {
                    "row_count":        max(rd["total"] for rd in rules.values()),
                    "uniqueness_score": lb_score,
                    "rules":            rules,
                }

            table_score = round(sum(table_lb_scores) / len(table_lb_scores), 2)
            total_rows  = sum(b["row_count"] for b in lb_breakdown.values())
            all_scores.append(table_score)
            report["tables"][table] = {
                "status":            "evaluated",
                "row_count":         total_rows,
                "rules_evaluated":   applied_rules,
                "uniqueness_score":  table_score,
                "le_book_breakdown": lb_breakdown,
            }
            log.info("  %-30s  score=%.2f%%  (%d rules, %d le_books)",
                     table, table_score, len(applied_rules), len(lb_breakdown))

    return finalize_report(report, all_scores, all_le_books, output_path,
                            overall_key="overall_uniqueness_score",
                            schema=schema, row_limit=row_limit)


def main():
    parser = argparse.ArgumentParser(description="DQ Engine — Uniqueness (pure SQL)")
    parser.add_argument("--schema", default="dqp")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Row cap per table (0 = full table)")
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--output", default="dq_uniqueness_report.json")
    parser.add_argument("--env", default=".env")
    parser.add_argument("--tables", nargs="+", default=None)
    args = parser.parse_args()

    if Path(args.env).exists():
        load_dotenv(args.env)

    engine = get_engine(build_connection_string())
    vlb    = get_valid_le_books(engine, args.schema)
    report = evaluate_from_sql(engine, args.schema, vlb, args.window_days, {},
                               args.output, row_limit=args.limit, tables=args.tables)
    log.info("DONE → overall uniqueness: %.2f%%",
             report["executive_summary"]["overall_uniqueness_score"])


if __name__ == "__main__":
    main()
