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

# Tables that actually have accuracy rules mapped.
_SQL_TARGET_TABLES = [t for t in ACC_TABLE_RULES if ACC_TABLE_RULES.get(t)]


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

    # ACC-011 (account_type vs vision_sbu pension consistency) is intentionally SKIPPED:
    # it needs PENSION_ACCOUNT_TYPES, which is not defined in dq_rules. Define that
    # constant and add an "ACC-011" branch here to enable it.

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
    """Run accuracy checks in pure SQL — one grouped query per table, no DataFrames."""
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
            rule_ids = ACC_TABLE_RULES.get(table, [])
            if not rule_ids:
                continue

            rule_cols: set = set()
            for rid in rule_ids:
                rule_cols.update(ACC_RULE_META.get(rid, {}).get("fields", []))

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
                ex = _acc_rule_sql(rid, existing)
                if ex:
                    rule_exprs[rid] = ex
            if not rule_exprs:
                report["tables"][table]   = {"status": "no_data", "row_count": 0}
                report["warnings"][table] = "No applicable accuracy columns found."
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
                rule_selects.append(f'{tot_expr} AS {k}_total, {val_expr} AS {k}_valid')

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

            lb_breakdown:      dict        = {}
            table_lb_scores:   list[float] = []
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
                    meta = ACC_RULE_META.get(rid, {})
                    lb_rules[rid] = {
                        "rule_name":      meta.get("name", rid),
                        "category":       meta.get("category", ""),
                        "fields":         meta.get("fields", []),
                        "valid":          val,
                        "invalid":        tot - val,
                        "total":          tot,
                        "accuracy_score": score,
                    }
                if not lb_rules:
                    continue
                lb_score = round(sum(lb_scores) / len(lb_scores), 2)
                table_lb_scores.append(lb_score)
                lb_breakdown[lb] = {
                    "row_count":      int(r["total_rows"]),
                    "accuracy_score": lb_score,
                    "rules":          lb_rules,
                }

            # Table-level score: mean of per-le_book scores, or flat rule rollup
            # when the table has no le_book column.
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
                "status":           "evaluated",
                "row_count":        total_rows,
                "rules_evaluated":  list(rule_exprs.keys()),
                "accuracy_score":   table_score,
                "le_book_breakdown": lb_breakdown,
            }
            log.info("  %-30s  score=%.2f%%  (%d rules, %d le_books)",
                     table, table_score, len(rule_exprs), len(lb_breakdown))

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    report["le_books"]          = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_accuracy_score": overall,
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