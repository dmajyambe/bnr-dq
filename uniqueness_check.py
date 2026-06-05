from __future__ import annotations
import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_uniqueness")

from db_utils import CATEGORY_TYPES, build_connection_string, get_engine, get_valid_le_books  # noqa: F401
from dq_rules import (  # noqa: E402
    UNI_RULE_META as RULE_META,
    UNIQUENESS_COLUMNS,
    UNI_TABLE_RULES as TABLE_RULES,
)

TARGET_TABLES = list(UNIQUENESS_COLUMNS.keys())


def _pct(valid: int, total: int) -> float:
    return round(valid / total * 100, 2) if total else 100.0


def fetch_table(engine, table_name: str, columns: list[str],
                db_schema: str, limit: int,
                valid_le_books: frozenset = frozenset()) -> pd.DataFrame:
    from sqlalchemy import inspect as sa_inspect
    try:
        inspector = sa_inspect(engine)
        db_cols: set[str] = set()
        for schema in (db_schema, None):
            try:
                cols = inspector.get_columns(table_name, schema=schema)
                if cols:
                    db_cols = {c["name"].lower() for c in cols}
                    break
            except Exception:
                continue
        columns = [c for c in columns if c in db_cols]
    except Exception as exc:
        log.warning("Cannot introspect '%s': %s", table_name, exc)

    if not columns:
        log.warning("  No uniqueness columns found in DB for '%s'", table_name)
        return pd.DataFrame()

    quoted = ", ".join(f'"{c}"' for c in columns)
    where = ""
    if valid_le_books and "le_book" in columns:
        codes = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        where = f' WHERE "le_book" IN ({codes})'

    if limit > 0:
        sql    = text(f'SELECT {quoted} FROM "{db_schema}"."{table_name}"{where} LIMIT :lim')
        params = {"lim": limit}
    else:
        sql    = text(f'SELECT {quoted} FROM "{db_schema}"."{table_name}"{where}')
        params = {}
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        df.columns = [c.lower() for c in df.columns]
        log.info("  '%s'  %d rows × %d cols fetched", table_name, len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("  Query failed for '%s': %s", table_name, exc)
        return pd.DataFrame()


def run_rule(rule_id: str, df: pd.DataFrame) -> Optional[tuple[int, int, int]]:
    """Return (valid, invalid, total) or None if rule not applicable."""
    if df.empty:
        return None

    # UNI-001: no identical key fields across consecutive YEAR_MONTH periods
    if rule_id == "UNI-001":
        key_cols = ["le_book", "contract_sequence_number", "date_of_provision",
                    "disbursed_amount", "prin_outstanding_amt_fcy", "prin_outstanding_amt_lcy"]
        needed = ["year_month"] + key_cols
        present = [c for c in needed if c in df.columns]
        if "year_month" not in present or len(present) < 3:
            return None
        work = df[present].copy().dropna(subset=["year_month"])
        if work.empty:
            return None
        # Sort by year_month; compare each row against the prior period
        ym = pd.to_numeric(work["year_month"].astype(str).str[:6], errors="coerce")
        work = work.assign(_ym=ym).sort_values("_ym")
        value_cols = [c for c in key_cols if c in work.columns and c != "year_month"]
        if not value_cols:
            return None
        # A row is a duplicate if its value_cols match the previous period's values
        # for the same contract sequence
        if "contract_sequence_number" in work.columns:
            grp = work.groupby("contract_sequence_number", sort=False)
            dup_mask = grp[value_cols].apply(
                lambda g: g.duplicated(keep="first")
            ).reset_index(level=0, drop=True).reindex(work.index, fill_value=False)
        else:
            dup_mask = work[value_cols].duplicated(keep="first")
        total = len(work)
        invalid = int(dup_mask.sum())
        return total - invalid, invalid, total

    # UNI-002: no identical disbursement figures on consecutive business dates
    if rule_id == "UNI-002":
        key_cols = ["current_disbursed_amt", "previous_disbursed_amt", "first_payment_date"]
        id_cols  = ["le_book", "contract_id", "currency"]
        needed   = id_cols + ["business_date"] + key_cols
        present  = [c for c in needed if c in df.columns]
        if "business_date" not in present:
            return None
        work = df[present].copy()
        bd = pd.to_datetime(work["business_date"], errors="coerce", utc=False)
        if getattr(bd.dt, "tz", None): bd = bd.dt.tz_localize(None)
        work["_bd"] = bd
        work = work.dropna(subset=["_bd"]).sort_values("_bd")
        val_cols = [c for c in key_cols if c in work.columns]
        grp_cols = [c for c in id_cols if c in work.columns]
        if not val_cols or not grp_cols:
            return None
        if grp_cols:
            dup_mask = work.groupby(grp_cols, sort=False)[val_cols].apply(
                lambda g: g.duplicated(keep="first")
            ).reset_index(level=list(range(len(grp_cols))), drop=True).reindex(
                work.index, fill_value=False)
        else:
            dup_mask = work[val_cols].duplicated(keep="first")
        total = len(work)
        invalid = int(dup_mask.sum())
        return total - invalid, invalid, total

    # UNI-003: no duplicate contract records on key identifying fields
    if rule_id == "UNI-003":
        key_cols = ["contract_sequence_number", "start_date", "maturity_date",
                    "principal_amount_lcy", "vision_sbu", "contract_status"]
        present = [c for c in key_cols if c in df.columns]
        if len(present) < 3:
            return None
        work = df[present].dropna(subset=present[:1])
        if work.empty:
            return None
        dup_mask = work.duplicated(subset=present, keep="first")
        total   = len(work)
        invalid = int(dup_mask.sum())
        return total - invalid, invalid, total

    log.warning("Unknown rule_id: %s", rule_id)
    return None


def run_rule_mask(rule_id: str, df: pd.DataFrame) -> pd.Series:
    """Return bool Series (True = row fails the rule, same index as df)."""
    false = pd.Series(False, index=df.index)
    if df.empty:
        return false

    if rule_id == "UNI-001":
        key_cols = ["le_book", "contract_sequence_number", "date_of_provision",
                    "disbursed_amount", "prin_outstanding_amt_fcy", "prin_outstanding_amt_lcy"]
        needed = ["year_month"] + key_cols
        present = [c for c in needed if c in df.columns]
        if "year_month" not in present or len(present) < 3:
            return false
        work = df[present].copy()
        ym = pd.to_numeric(work["year_month"].astype(str).str[:6], errors="coerce")
        work = work.assign(_ym=ym).sort_values("_ym")
        value_cols = [c for c in key_cols if c in work.columns]
        if not value_cols:
            return false
        if "contract_sequence_number" in work.columns:
            dup = work.groupby("contract_sequence_number", sort=False)[value_cols].apply(
                lambda g: g.duplicated(keep="first")
            ).reset_index(level=0, drop=True).reindex(df.index, fill_value=False)
        else:
            dup = work[value_cols].duplicated(keep="first").reindex(df.index, fill_value=False)
        return dup

    if rule_id == "UNI-002":
        key_cols = ["current_disbursed_amt", "previous_disbursed_amt", "first_payment_date"]
        id_cols  = ["le_book", "contract_id", "currency"]
        present  = [c for c in id_cols + ["business_date"] + key_cols if c in df.columns]
        if "business_date" not in present:
            return false
        work = df[present].copy()
        bd = pd.to_datetime(work["business_date"], errors="coerce", utc=False)
        if getattr(bd.dt, "tz", None): bd = bd.dt.tz_localize(None)
        work["_bd"] = bd
        work = work.sort_values("_bd")
        val_cols = [c for c in key_cols if c in work.columns]
        grp_cols = [c for c in id_cols if c in work.columns]
        if not val_cols:
            return false
        if grp_cols:
            dup = work.groupby(grp_cols, sort=False)[val_cols].apply(
                lambda g: g.duplicated(keep="first")
            ).reset_index(level=list(range(len(grp_cols))), drop=True).reindex(
                df.index, fill_value=False)
        else:
            dup = work[val_cols].duplicated(keep="first").reindex(df.index, fill_value=False)
        return dup

    if rule_id == "UNI-003":
        key_cols = ["contract_sequence_number", "start_date", "maturity_date",
                    "principal_amount_lcy", "vision_sbu", "contract_status"]
        present = [c for c in key_cols if c in df.columns]
        if len(present) < 3:
            return false
        return df[present].duplicated(subset=present, keep="first").reindex(
            df.index, fill_value=False)

    return false


def evaluate_table(df: pd.DataFrame, table_name: str) -> dict:
    rule_ids     = TABLE_RULES.get(table_name, [])
    rules_out:   dict = {}
    rule_scores: list[float] = []

    for rule_id in rule_ids:
        meta   = RULE_META[rule_id]
        result = run_rule(rule_id, df)
        if result is None:
            continue
        valid, invalid, total = result
        score = _pct(valid, total)
        rule_scores.append(score)

        lb_breakdown: dict = {}
        if "le_book" in df.columns:
            for le_val in sorted(df["le_book"].dropna().unique()):
                sub_df = df[df["le_book"] == le_val]
                sub    = run_rule(rule_id, sub_df)
                if sub is None:
                    continue
                sv, si, st = sub
                lb_breakdown[str(le_val)] = {
                    "valid": sv, "invalid": si, "total": st,
                    "uniqueness_score": _pct(sv, st),
                }

        rules_out[rule_id] = {
            "rule_name":         meta["name"],
            "category":          meta["category"],
            "fields":            meta["fields"],
            "valid":             valid,
            "invalid":           invalid,
            "total":             total,
            "uniqueness_score":  score,
            "le_book_breakdown": lb_breakdown,
        }
        log.info("  %s  score=%.2f%%  invalid=%d / %d", rule_id, score, invalid, total)

    overall = round(sum(rule_scores) / len(rule_scores), 2) if rule_scores else 0.0
    return {
        "status":           "evaluated",
        "row_count":        len(df),
        "rules_applied":    len(rules_out),
        "uniqueness_score": overall,
        "rules":            rules_out,
    }


def evaluate_from_dataframes(dataframes: dict, valid_le_books: frozenset,
                              output_path: str) -> dict:
    """Run uniqueness checks on pre-loaded DataFrames (no DB connection needed)."""
    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables":       {},
        "warnings":     {},
    }
    all_scores: list[float] = []

    for table_name in TARGET_TABLES:
        df = dataframes.get(table_name, pd.DataFrame())
        if df.empty:
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "No data in this period."
            continue
        tbl_report = evaluate_table(df, table_name)
        report["tables"][table_name] = tbl_report
        all_scores.append(tbl_report["uniqueness_score"])
        log.info("  %-30s  score=%.2f%%  (%d rules)",
                 table_name, tbl_report["uniqueness_score"], tbl_report["rules_applied"])

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    report["executive_summary"] = {
        "overall_uniqueness_score": overall,
        "total_tables":             len(report["tables"]),
        "evaluated_tables":         len([v for v in report["tables"].values()
                                         if v.get("status") == "evaluated"]),
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("Uniqueness report → %s  (overall %.2f%%)", output_path, overall)
    return report


def evaluate(engine, tables: list[str], db_schema: str,
             limit: int, output_path: str) -> dict:
    valid_le_books = get_valid_le_books(engine, db_schema)
    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "row_limit":    limit,
        "schema":       db_schema,
        "tables":       {},
        "warnings":     {},
    }
    all_scores: list[float] = []

    for table_name in tables:
        log.info("━━  Table: %s", table_name)
        columns = UNIQUENESS_COLUMNS.get(table_name, [])
        if not columns:
            log.warning("  No uniqueness columns defined — skipping.")
            continue
        df = fetch_table(engine, table_name, columns, db_schema, limit, valid_le_books)
        if df.empty:
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "Table returned 0 rows."
            continue
        tbl_report = evaluate_table(df, table_name)
        report["tables"][table_name] = tbl_report
        all_scores.append(tbl_report["uniqueness_score"])

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    report["executive_summary"] = {
        "overall_uniqueness_score": overall,
        "total_tables":             len(report["tables"]),
        "evaluated_tables":         len([v for v in report["tables"].values()
                                         if v.get("status") == "evaluated"]),
        "row_limit":                limit,
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    return report


def main():
    parser = argparse.ArgumentParser(
        description="DQ Uniqueness Engine — BNR Data Quality Programme")
    parser.add_argument("--tables", nargs="+", default=TARGET_TABLES)
    parser.add_argument("--schema", default="data_quality_program")
    parser.add_argument("--limit",  type=int, default=100000)
    parser.add_argument("--output", default="dq_uniqueness_report.json")
    parser.add_argument("--env",    default=".env")
    args = parser.parse_args()

    env_path = Path(args.env)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)

    log.info("DQ Uniqueness Engine")
    log.info("  Tables : %s", ", ".join(args.tables))
    log.info("  Schema : %s", args.schema)

    conn_str = build_connection_string()
    engine   = get_engine(conn_str)
    report   = evaluate(engine, args.tables, args.schema, args.limit, args.output)
    s        = report.get("executive_summary", {})
    log.info("Report written → %s", args.output)
    log.info("  Overall Uniqueness : %.2f%%", s.get("overall_uniqueness_score", 0.0))


if __name__ == "__main__":
    main()
