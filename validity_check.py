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
from rule_dimension_sql import run_rule_dimension_sql
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
                      tables: list[str] | None = None,
                      extra_where: str = "") -> dict:
    """Run validity checks in pure SQL via the shared rule-dimension runner."""
    return run_rule_dimension_sql(
        engine, schema, valid_le_books, window_days, watermarks, output_path,
        row_limit=row_limit, tables=tables,
        table_rules=VAL_TABLE_RULES, rule_meta=VAL_RULE_META,
        rule_sql_fn=_val_rule_sql,
        score_key="validity_score", overall_key="overall_validity_score",
        dim_label="validity", extra_where=extra_where,
    )


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
