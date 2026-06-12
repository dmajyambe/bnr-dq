# validity check — rule-based format / range / cross-field validation (pure SQL)
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

from db_utils import build_connection_string, get_engine, get_valid_le_books
from dq_rules import (
    VAL_RULE_META,
    VAL_TABLE_RULES,
    VALIDITY_COLUMNS,           # noqa: F401 — kept for external callers / parity
    MIN_PHONE_DIGITS,
    MIN_NATIONAL_ID,
    INTEREST_RATE_MAX,
    MIN_AGE_AT_OPEN,
)

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_validity")

# Backwards-compat aliases — resolution_pipeline._build_dispatch imports these.
RULE_META   = VAL_RULE_META
TABLE_RULES = VAL_TABLE_RULES

# Tables that actually have validity rules mapped.
_SQL_TARGET_TABLES = [t for t in VAL_TABLE_RULES if VAL_TABLE_RULES.get(t)]


def pct(valid: int, total: int) -> float:
    return round((valid / total) * 100, 2) if total else 100.0


# ============================================================================
# Per-rule SQL — each rule is a (total_expr, valid_expr) pair of SUM(CASE...)
# aggregates. invalid = total - valid; score = valid/total*100. Multi-column
# rules sum across the columns that exist (cell-level totals).
# ============================================================================
def _val_rule_sql(rule_id: str, existing: set) -> tuple[str, str] | None:
    """Return (total_expr, valid_expr) SQL for a validity rule, or None when its
    columns are unavailable (rule is then skipped)."""
    def has(*cols: str) -> bool:
        return all(c in existing for c in cols)

    if rule_id == "VAL-001":            # email format
        if not has("email_id"):
            return None
        return (
            'SUM(CASE WHEN "email_id" IS NOT NULL THEN 1 ELSE 0 END)',
            r"""SUM(CASE WHEN "email_id" IS NOT NULL AND "email_id"::TEXT ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$' THEN 1 ELSE 0 END)""",
        )

    if rule_id == "VAL-002":            # phone has >= MIN_PHONE_DIGITS digits
        cols = [c for c in ("work_telephone", "home_telephone") if c in existing]
        if not cols:
            return None
        total = " + ".join(
            f'SUM(CASE WHEN "{c}" IS NOT NULL AND TRIM("{c}"::TEXT) != \'\' THEN 1 ELSE 0 END)'
            for c in cols
        )
        valid = " + ".join(
            f'SUM(CASE WHEN "{c}" IS NOT NULL AND TRIM("{c}"::TEXT) != \'\' '
            f'AND LENGTH(REGEXP_REPLACE("{c}"::TEXT, \'[^0-9]\', \'\', \'g\')) >= {MIN_PHONE_DIGITS} THEN 1 ELSE 0 END)'
            for c in cols
        )
        return (total, valid)

    if rule_id == "VAL-003":            # currency = 3-letter uppercase ISO
        cols = [c for c in ("currency", "mis_currency") if c in existing]
        if not cols:
            return None
        total = " + ".join(
            f'SUM(CASE WHEN "{c}" IS NOT NULL AND TRIM("{c}"::TEXT) != \'\' THEN 1 ELSE 0 END)'
            for c in cols
        )
        valid = " + ".join(
            f'SUM(CASE WHEN "{c}" IS NOT NULL AND TRIM("{c}"::TEXT) != \'\' '
            f'AND TRIM("{c}"::TEXT) ~ \'^[A-Z]{{3}}$\' THEN 1 ELSE 0 END)'
            for c in cols
        )
        return (total, valid)

    if rule_id == "VAL-004":            # national id number length >= MIN_NATIONAL_ID
        if not has("national_id_type", "national_id_number"):
            return None
        return (
            'SUM(CASE WHEN "national_id_type" IS NOT NULL AND TRIM("national_id_type"::TEXT) != \'\' THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "national_id_type" IS NOT NULL AND TRIM("national_id_type"::TEXT) != \'\' '
            f'AND LENGTH(TRIM(COALESCE("national_id_number"::TEXT, \'\'))) >= {MIN_NATIONAL_ID} THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-010":            # debit interest rate in [0, MAX]
        if not has("interest_rate_dr"):
            return None
        return (
            'SUM(CASE WHEN "interest_rate_dr" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "interest_rate_dr" IS NOT NULL AND "interest_rate_dr"::NUMERIC >= 0 '
            f'AND "interest_rate_dr"::NUMERIC <= {INTEREST_RATE_MAX} THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-011":            # credit interest rate in [0, MAX]
        if not has("interest_rate_cr"):
            return None
        return (
            'SUM(CASE WHEN "interest_rate_cr" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "interest_rate_cr" IS NOT NULL AND "interest_rate_cr"::NUMERIC >= 0 '
            f'AND "interest_rate_cr"::NUMERIC <= {INTEREST_RATE_MAX} THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-012":            # disbursement amounts non-negative
        cols = [c for c in ("current_disbursed_amt", "previous_disbursed_amt") if c in existing]
        if not cols:
            return None
        return (
            " + ".join(f'SUM(CASE WHEN "{c}" IS NOT NULL THEN 1 ELSE 0 END)' for c in cols),
            " + ".join(f'SUM(CASE WHEN "{c}" IS NOT NULL AND "{c}"::NUMERIC >= 0 THEN 1 ELSE 0 END)' for c in cols),
        )

    if rule_id == "VAL-013":            # EMI amount > 0
        if not has("emi_amount"):
            return None
        return (
            'SUM(CASE WHEN "emi_amount" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "emi_amount" IS NOT NULL AND "emi_amount"::NUMERIC > 0 THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-014":            # outstanding / due amounts non-negative
        cols = [c for c in ("outstanding_amount_lcy", "outstanding_amount",
                            "principal_amount_due", "int_amount_due",
                            "due_amount", "principal_amount_lcy") if c in existing]
        if not cols:
            return None
        return (
            " + ".join(f'SUM(CASE WHEN "{c}" IS NOT NULL THEN 1 ELSE 0 END)' for c in cols),
            " + ".join(f'SUM(CASE WHEN "{c}" IS NOT NULL AND "{c}"::NUMERIC >= 0 THEN 1 ELSE 0 END)' for c in cols),
        )

    if rule_id == "VAL-015":            # applied loan amount > 0
        if not has("applied_amount_lcy"):
            return None
        return (
            'SUM(CASE WHEN "applied_amount_lcy" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "applied_amount_lcy" IS NOT NULL AND "applied_amount_lcy"::NUMERIC > 0 THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-016":            # number of instalments >= 1
        if not has("num_of_instalments"):
            return None
        return (
            'SUM(CASE WHEN "num_of_instalments" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "num_of_instalments" IS NOT NULL AND "num_of_instalments"::NUMERIC >= 1 THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-020":            # instalments paid <= total instalments
        if not has("num_instalments_paid", "num_of_instalments"):
            return None
        return (
            'SUM(CASE WHEN "num_instalments_paid" IS NOT NULL AND "num_of_instalments" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "num_instalments_paid" IS NOT NULL AND "num_of_instalments" IS NOT NULL '
            'AND "num_instalments_paid"::NUMERIC <= "num_of_instalments"::NUMERIC THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-021":            # approved amount <= applied amount
        if not has("approved_amount_lcy", "applied_amount_lcy"):
            return None
        return (
            'SUM(CASE WHEN "approved_amount_lcy" IS NOT NULL AND "applied_amount_lcy" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "approved_amount_lcy" IS NOT NULL AND "applied_amount_lcy" IS NOT NULL '
            'AND "approved_amount_lcy"::NUMERIC <= "applied_amount_lcy"::NUMERIC THEN 1 ELSE 0 END)',
        )

    if rule_id == "VAL-022":            # customer >= MIN_AGE_AT_OPEN years at account open
        if not has("date_of_birth", "customer_open_date"):
            return None
        return (
            'SUM(CASE WHEN "date_of_birth" IS NOT NULL AND "customer_open_date" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "date_of_birth" IS NOT NULL AND "customer_open_date" IS NOT NULL '
            f'AND ("customer_open_date"::DATE - "date_of_birth"::DATE) >= {MIN_AGE_AT_OPEN * 365} THEN 1 ELSE 0 END)',
        )

    return None


# ============================================================================
# SQL ENGINE — pipeline contract (memory-safe, no DataFrames)
# Emits the rule-dimension report shape consumed downstream:
#   tables[t].le_book_breakdown[lb] = {row_count, validity_score, rules:{rid:{invalid,...}}}
#   executive_summary.overall_validity_score
# (see dq_issue_tracker._process_rule_dimension and dq_pipeline_2m._SCORE_KEYS)
# ============================================================================
def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
                      window_days: int, watermarks: dict, output_path: str,
                      row_limit: int = 0,
                      tables: list[str] | None = None) -> dict:
    """Run validity checks in pure SQL — one grouped query per table, no DataFrames."""
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
            rule_ids = VAL_TABLE_RULES.get(table, [])
            if not rule_ids:
                continue

            rule_cols: set = set()
            for rid in rule_ids:
                rule_cols.update(VAL_RULE_META.get(rid, {}).get("fields", []))

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
                ex = _val_rule_sql(rid, existing)
                if ex:
                    rule_exprs[rid] = ex
            if not rule_exprs:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = "No applicable validity columns found."
                continue

            # Date window — identical convention to completeness_check
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
                    meta = VAL_RULE_META.get(rid, {})
                    lb_rules[rid] = {
                        "rule_name":      meta.get("name", rid),
                        "category":       meta.get("category", ""),
                        "fields":         meta.get("fields", []),
                        "valid":          val,
                        "invalid":        tot - val,
                        "total":          tot,
                        "validity_score": score,
                    }
                if not lb_rules:
                    continue
                lb_score = round(sum(lb_scores) / len(lb_scores), 2)
                table_lb_scores.append(lb_score)
                lb_breakdown[lb] = {
                    "row_count":      int(r["total_rows"]),
                    "validity_score": lb_score,
                    "rules":          lb_rules,
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
                "validity_score":    table_score,
                "le_book_breakdown": lb_breakdown,
            }
            log.info("  %-30s  score=%.2f%%  (%d rules, %d le_books)",
                     table, table_score, len(rule_exprs), len(lb_breakdown))

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    report["le_books"]          = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_validity_score": overall,
        "total_tables":           len(report["tables"]),
        "evaluated_tables":       sum(1 for v in report["tables"].values()
                                      if v.get("status") == "evaluated"),
        "schema":                 schema,
        "row_limit":              row_limit,
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("Report written → %s  (overall %.2f%%)", output_path, overall)
    return report


def main():
    parser = argparse.ArgumentParser(description="DQ Engine — Validity (pure SQL)")
    parser.add_argument("--schema", default="dqp")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Row cap per table (0 = full table)")
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--output", default="dq_validity_report.json")
    parser.add_argument("--env", default=".env")
    parser.add_argument("--tables", nargs="+", default=None)
    args = parser.parse_args()

    if Path(args.env).exists():
        load_dotenv(args.env)

    engine = get_engine(build_connection_string())
    vlb    = get_valid_le_books(engine, args.schema)
    report = evaluate_from_sql(engine, args.schema, vlb, args.window_days, {},
                               args.output, row_limit=args.limit, tables=args.tables)
    log.info("DONE → overall validity: %.2f%%",
             report["executive_summary"]["overall_validity_score"])


if __name__ == "__main__":
    main()
