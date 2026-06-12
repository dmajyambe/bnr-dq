from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple, Callable

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

# =========================
# Missing imports (EXPECTED from dq_rules or future extensions)
# =========================
# from dq_rules import PENSION_ACCOUNT_TYPES
# from dq_rules import VALID_LOAN_DEAL_SUB_TYPES
# from dq_rules import VALID_DEP_DEAL_SUB_TYPES
# from dq_rules import VALID_CORP_VISION_SBU
# from dq_rules import VALID_GI_DEAL_SUB_TYPES
# from dq_rules import VALID_LI_DEAL_SUB_TYPES
# from dq_rules import INSURANCE_LE_BOOKS_GI, INSURANCE_LE_BOOKS_LI
# from dq_rules import VALID_INS_CONTRACT_STATUS

from dq_rules import (
    VALID_ACCOUNT_STATUS,
    VALID_PERFORMANCE_CLASS,
    VALID_GENDER,
    VALID_ACCOUNT_TYPE,
    CORPORATE_LEGAL_STATUS,
    ACC_RULE_META,
    ACC_TABLE_RULES,
    ACCURACY_COLUMNS,
)

from db_utils import build_connection_string, get_engine, get_valid_le_books
from rule_dimension_sql import run_rule_dimension_sql


# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_accuracy")


# =========================
# Helpers (reduce duplication)
# =========================
def norm_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()


def norm_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def pct(valid: int, total: int) -> float:
    return round((valid / total) * 100, 2) if total else 100.0


def safe_col(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
    return df[col].dropna() if col in df.columns else None


# =========================
# Rule Engine
# =========================
RuleFn = Callable[[pd.DataFrame], Optional[Tuple[int, int, int]]]


class Rule:
    def __init__(self, rule_id: str, fn: RuleFn):
        self.rule_id = rule_id
        self.fn = fn
        self.meta = ACC_RULE_META.get(rule_id, {})

    def run(self, df: pd.DataFrame):
        return self.fn(df)


# =========================
# RULE IMPLEMENTATIONS
# =========================

def acc_002(df):
    s = safe_col(df, "account_status")
    if s is None:
        return None
    s = norm_num(s)
    total = len(s)
    valid = int(s.isin(VALID_ACCOUNT_STATUS).sum())
    return valid, total - valid, total


def acc_003(df):
    s = safe_col(df, "performance_class")
    if s is None:
        return None
    s = norm_str(s)
    total = len(s)
    valid = int(s.isin(VALID_PERFORMANCE_CLASS).sum())
    return valid, total - valid, total


def acc_004(df):
    s = safe_col(df, "customer_gender")
    if s is None:
        return None
    s = norm_str(s)
    total = len(s)
    valid = int(s.isin(VALID_GENDER).sum())
    return valid, total - valid, total


def acc_005(df):
    s = safe_col(df, "account_type")
    if s is None:
        return None
    s = norm_str(s)
    total = len(s)
    valid = int(s.isin(VALID_ACCOUNT_TYPE).sum())
    return valid, total - valid, total


def acc_010(df):
    needed = {"customer_gender", "legal_status"}
    if not needed.issubset(df.columns):
        return None

    sub = df[list(needed)].dropna()
    if sub.empty:
        return None

    gender = norm_str(sub["customer_gender"])
    legal = norm_num(sub["legal_status"])

    is_corp = legal.isin(CORPORATE_LEGAL_STATUS)

    total = len(sub)
    valid = int((~(is_corp & (gender != "C"))).sum())
    return valid, total - valid, total


def acc_012(df):
    needed = {"customer_gender", "marital_status"}
    if not needed.issubset(df.columns):
        return None

    sub = df[list(needed)].dropna()
    if sub.empty:
        return None

    gender = norm_str(sub["customer_gender"])
    ms = norm_str(sub["marital_status"])

    is_corp = gender == "C"

    total = len(sub)
    valid = int((~(is_corp & (ms != "NA"))).sum())
    return valid, total - valid, total


# =========================
# RULE REGISTRY (NO IF-ELSE CHAINS)
# =========================
RULES: Dict[str, Rule] = {
    "ACC-002": Rule("ACC-002", acc_002),
    "ACC-003": Rule("ACC-003", acc_003),
    "ACC-004": Rule("ACC-004", acc_004),
    "ACC-005": Rule("ACC-005", acc_005),
    "ACC-010": Rule("ACC-010", acc_010),
    "ACC-012": Rule("ACC-012", acc_012),
}


# =========================
# Execution Engine
# =========================
def run_rules(df: pd.DataFrame, rule_ids: list[str]):
    results = {}

    for rid in rule_ids:
        rule = RULES.get(rid)
        if not rule:
            continue

        res = rule.run(df)
        if res is None:
            continue

        valid, invalid, total = res
        results[rid] = {
            "meta": rule.meta,
            "valid": valid,
            "invalid": invalid,
            "total": total,
            "accuracy_score": pct(valid, total),
        }

        log.info(
            "  %s score=%.2f%% invalid=%d/%d",
            rid,
            pct(valid, total),
            invalid,
            total,
        )

    return results


def evaluate_table(df: pd.DataFrame, table_name: str):
    rule_ids = ACC_TABLE_RULES.get(table_name, [])
    rules_out = run_rules(df, rule_ids)

    scores = [r["accuracy_score"] for r in rules_out.values()]
    overall = round(sum(scores) / len(scores), 2) if scores else 0.0

    return {
        "table": table_name,
        "row_count": len(df),
        "accuracy_score": overall,
        "rules": rules_out,
    }


# =========================
# DB FETCH (simplified version of your original)
# =========================
def fetch_table(engine, table: str, schema: str, columns: list[str], limit: int):
    quoted = ", ".join(f'"{c}"' for c in columns)

    sql = f'SELECT {quoted} FROM "{schema}"."{table}"'
    if limit > 0:
        sql += " LIMIT :lim"

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params={"lim": limit} if limit else {})
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        log.error("Fetch failed for %s: %s", table, e)
        return pd.DataFrame()


# =========================
# ORCHESTRATOR
# =========================
def evaluate(engine, tables, schema, limit, output):
    valid_le_books = get_valid_le_books(engine, schema)

    report = {
        "generated_at": datetime.now().isoformat(),
        "schema": schema,
        "tables": {},
        "warnings": {},
    }

    all_scores = []

    for table in tables:
        log.info("━━ Table: %s", table)

        cols = ACCURACY_COLUMNS.get(table, [])
        if not cols:
            continue

        df = fetch_table(engine, table, schema, cols, limit)

        if df.empty:
            report["tables"][table] = {"status": "no_data"}
            continue

        res = evaluate_table(df, table)
        report["tables"][table] = res
        all_scores.append(res["accuracy_score"])

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["executive_summary"] = {
        "overall_accuracy_score": overall,
        "tables": len(tables),
    }

    with open(output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report

# ============================================================================
# SQL ENGINE — pipeline contract (memory-safe, no DataFrames)
# ----------------------------------------------------------------------------
# Mirrors completeness_check.evaluate_from_sql: one grouped-by-le_book query per
# table, all rule logic pushed into SUM(CASE ...) aggregates. Each rule yields a
# (total_expr, valid_expr) pair → invalid = total - valid, score = valid/total*100.
# Emits the rule-dimension report shape the downstream consumes:
#   tables[t].le_book_breakdown[lb] = {row_count, accuracy_score, rules:{rid:{invalid,...}}}
#   executive_summary.overall_accuracy_score
# (see dq_issue_tracker._process_rule_dimension and dq_pipeline_2m._SCORE_KEYS)
# ============================================================================



def _acc_rule_sql(rule_id: str, existing: set) -> tuple[str, str] | None:
    """Return (total_expr, valid_expr) SQL aggregates for a rule, or None when the
    rule's columns (or a required constant) are unavailable — the rule is then skipped."""
    def has(*cols: str) -> bool:
        return all(c in existing for c in cols)

    if rule_id == "ACC-002":            # account_status in allowed numeric codes
        if not has("account_status"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(str(x) for x in VALID_ACCOUNT_STATUS))
        return (
            'SUM(CASE WHEN "account_status" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "account_status" IS NOT NULL AND "account_status"::TEXT IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-003":            # performance_class in allowed codes
        if not has("performance_class"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_PERFORMANCE_CLASS))
        return (
            'SUM(CASE WHEN "performance_class" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "performance_class" IS NOT NULL AND UPPER(TRIM("performance_class"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-004":            # customer_gender in allowed codes
        if not has("customer_gender"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_GENDER))
        return (
            'SUM(CASE WHEN "customer_gender" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "customer_gender" IS NOT NULL AND UPPER(TRIM("customer_gender"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-005":            # account_type in allowed codes
        if not has("account_type"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_ACCOUNT_TYPE))
        return (
            'SUM(CASE WHEN "account_type" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "account_type" IS NOT NULL AND UPPER(TRIM("account_type"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-010":            # corporate (legal_status) ⇒ gender must be 'C'
        if not has("customer_gender", "legal_status"):
            return None
        corp = ", ".join(f"'{v}'" for v in sorted(str(x) for x in CORPORATE_LEGAL_STATUS))
        return (
            'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL '
            f'AND ("legal_status"::TEXT NOT IN ({corp}) OR UPPER(TRIM("customer_gender"::TEXT)) = \'C\') THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-012":            # child (gender 'C') ⇒ marital_status must be 'NA'
        if not has("customer_gender", "marital_status"):
            return None
        return (
            'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL '
            'AND (UPPER(TRIM("customer_gender"::TEXT)) <> \'C\' OR UPPER(TRIM("marital_status"::TEXT)) = \'NA\') THEN 1 ELSE 0 END)',
        )

    return None


def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
                      window_days: int, watermarks: dict, output_path: str,
                      row_limit: int = 0,
                      tables: list[str] | None = None) -> dict:
    """Run accuracy checks in pure SQL via the shared rule-dimension runner."""
    return run_rule_dimension_sql(
        engine, schema, valid_le_books, window_days, watermarks, output_path,
        row_limit=row_limit, tables=tables,
        table_rules=ACC_TABLE_RULES, rule_meta=ACC_RULE_META,
        rule_sql_fn=_acc_rule_sql,
        score_key="accuracy_score", overall_key="overall_accuracy_score",
        dim_label="accuracy",
    )


#cli
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="dqp")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--output", default="dq_accuracy_report.json")
    parser.add_argument("--env", default=".env")
    parser.add_argument("--tables", nargs="+", default=list(ACCURACY_COLUMNS.keys()))

    args = parser.parse_args()

    if Path(args.env).exists():
        load_dotenv(args.env)

    engine = get_engine(build_connection_string())

    report = evaluate(
        engine,
        args.tables,
        args.schema,
        args.limit,
        args.output,
    )

    log.info(
        "DONE → overall accuracy: %.2f%%",
        report["executive_summary"]["overall_accuracy_score"],
    )


if __name__ == "__main__":
    main()