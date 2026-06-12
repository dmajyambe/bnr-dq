# uniqueness check — within-window duplicate detection (pure SQL, window functions)
#
# A row is a duplicate when its full key tuple (UNI_RULE_META[rid]["fields"])
# repeats for the same le_book inside the rolling window. Detected server-side
# with ROW_NUMBER() OVER (PARTITION BY le_book, <keys>) — rank > 1 ⇒ duplicate —
# so it stays memory-safe (no DataFrames, no full-table client loads).
#
# Emits the rule-dimension report shape the downstream consumes:
#   tables[t].le_book_breakdown[lb] = {row_count, uniqueness_score, rules:{rid:{invalid,...}}}
#   executive_summary.overall_uniqueness_score
# (see dq_issue_tracker._process_rule_dimension and dq_pipeline_2m._SCORE_KEYS)
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

from db_utils import build_connection_string, get_engine, get_valid_le_books
from dq_rules import UNI_RULE_META, UNI_TABLE_RULES
from rule_dimension_sql import pct

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_uniqueness")

# Backwards-compat aliases (parity with other engines).
RULE_META   = UNI_RULE_META
TABLE_RULES = UNI_TABLE_RULES

_SQL_TARGET_TABLES = [t for t in UNI_TABLE_RULES if UNI_TABLE_RULES.get(t)]


def _existing_columns(conn, schema: str, table: str, wanted: set) -> set:
    return {
        r[0] for r in conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t AND column_name = ANY(:cols)
        """), {"s": schema, "t": table, "cols": list(wanted)}).fetchall()
    }


def _date_clause(existing: set, wm: str | None, window_days: int) -> str:
    if not window_days:          # 0/None ⇒ full-table scan (monthly pipeline)
        return "TRUE"
    anchor = f"'{wm[:10]}'::date" if wm else "CURRENT_DATE"
    parts = []
    if "date_creation" in existing:
        parts.append(f'"date_creation" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}')
    if "date_last_modified" in existing:
        parts.append(
            f'"date_last_modified" > \'{wm}\'' if wm else
            f'"date_last_modified" BETWEEN {anchor} - INTERVAL \'{window_days} days\' AND {anchor}')
    return "(" + " OR ".join(parts) + ")" if parts else "TRUE"


def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
                      window_days: int, watermarks: dict, output_path: str,
                      row_limit: int = 0,
                      tables: list[str] | None = None) -> dict:
    """Run uniqueness (duplicate-detection) checks in pure SQL — one window query
    per (table, rule), grouped by le_book."""
    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables":       {},
        "warnings":     {},
    }
    all_scores:   list[float] = []
    all_le_books: set         = set()
    target = tables if tables is not None else _SQL_TARGET_TABLES

    lb_clause = (
        'AND "le_book" IN (' + ", ".join(f"'{lb}'" for lb in sorted(valid_le_books)) + ")"
        if valid_le_books else ""
    )

    with engine.connect() as conn:
        for table in target:
            log.info("━━  %s", table)
            rule_ids = UNI_TABLE_RULES.get(table, [])
            if not rule_ids:
                continue

            wanted: set = {"le_book", "date_creation", "date_last_modified"}
            for rid in rule_ids:
                wanted |= set(UNI_RULE_META[rid]["fields"])
            existing = _existing_columns(conn, schema, table, wanted)

            has_lb      = "le_book" in existing
            date_clause = _date_clause(existing, watermarks.get(table), window_days)
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

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    report["le_books"]          = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_uniqueness_score": overall,
        "total_tables":             len(report["tables"]),
        "evaluated_tables":         sum(1 for v in report["tables"].values()
                                        if v.get("status") == "evaluated"),
        "schema":                   schema,
        "row_limit":                row_limit,
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("Report written → %s  (overall %.2f%%)", output_path, overall)
    return report


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
