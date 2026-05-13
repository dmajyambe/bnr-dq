from __future__ import annotations
import argparse
import json
import logging
import os
import sys
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
log = logging.getLogger("dq_accuracy")

VALID_LE_BOOKS: frozenset = frozenset()  # populated at runtime via fetch_valid_le_books(engine)

from dq_rules import (  # noqa: E402
    VALID_ACCOUNT_STATUS, VALID_PERFORMANCE_CLASS, VALID_GENDER,
    VALID_ACCOUNT_TYPE, CORPORATE_LEGAL_STATUS, PENSION_ACCOUNT_TYPES,
    ACC_RULE_META as RULE_META,
    ACCURACY_COLUMNS,
    ACC_TABLE_RULES as TABLE_RULES,
)

TARGET_TABLES = list(ACCURACY_COLUMNS.keys())


def build_connection_string() -> str:  # assemble SQLAlchemy URL from env vars; exit if any missing
    required = [
        "MY_POSTGRES_USERNAME", "MY_POSTGRES_PASSWORD",
        "MY_POSTGRES_HOST", "MY_POSTGRES_PORT", "MY_POSTGRES_DB",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)
    return (
        f"postgresql+psycopg2://{os.environ['MY_POSTGRES_USERNAME']}:"
        f"{os.environ['MY_POSTGRES_PASSWORD']}@{os.environ['MY_POSTGRES_HOST']}:"
        f"{os.environ['MY_POSTGRES_PORT']}/{os.environ['MY_POSTGRES_DB']}"
    )


def get_engine(conn_str: str):  # create engine and verify connectivity with a probe query; exit on failure
    try:
        from sqlalchemy import create_engine
        engine = create_engine(
            conn_str, pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except ImportError:
        log.error("sqlalchemy or psycopg2-binary not installed.")
        sys.exit(1)
    except Exception as exc:
        log.error("Cannot connect to database: %s", exc)
        sys.exit(1)


# only analyse rows whose le_book belongs to these BNR category types
CATEGORY_TYPES = ('MF', 'SACCO', 'OSACCO', 'B')


def get_valid_le_books(engine, schema: str) -> frozenset:
    """Return le_book codes whose category_type is in CATEGORY_TYPES."""
    filter_list = ", ".join(f"'{t}'" for t in CATEGORY_TYPES)
    sql = text(f"""
        SELECT DISTINCT lb.le_book
        FROM "{schema}".le_book lb
        LEFT JOIN (
            SELECT alpha_tab      AS category_type_at,
                   alpha_sub_tab  AS category_type
            FROM   "{schema}".alpha_sub_tab
        ) ast ON lb.category_type_at = ast.category_type_at
             AND lb.category_type    = ast.category_type
        WHERE ast.category_type IN ({filter_list})
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        result = frozenset(str(r[0]).strip() for r in rows if r[0] is not None)
        log.info("Category filter %s → %d valid le_books", CATEGORY_TYPES, len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch valid le_books: %s — no filter applied.", exc)
        return frozenset()


def fetch_table(engine, table_name: str, columns: list[str],
                db_schema: str, limit: int,
                valid_le_books: frozenset = frozenset()) -> pd.DataFrame:
    # introspect schema to drop any requested columns that don't exist, then fetch
    from sqlalchemy import inspect as sa_inspect

    try:
        inspector = sa_inspect(engine)
        db_cols: set[str] = set()
        for schema in (db_schema, None):  # try specified schema then public fallback
            try:
                cols = inspector.get_columns(table_name, schema=schema)
                if cols:
                    db_cols = {c["name"].lower() for c in cols}
                    break
            except Exception:
                continue
        columns = [c for c in columns if c in db_cols]  # restrict to columns that actually exist
    except Exception as exc:
        log.warning("Cannot introspect '%s': %s", table_name, exc)

    if not columns:
        log.warning("  No accuracy columns found in DB for '%s'", table_name)
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


def _pct(valid: int, total: int) -> float:  # safe percentage: returns 100.0 when total is 0
    return round(valid / total * 100, 2) if total else 100.0


def _single_col(df: pd.DataFrame, col: str,
                valid_set: frozenset, normalise=None) -> Optional[tuple[int, int, int]]:
    # drop nulls (completeness concern), optionally normalise values, then check membership
    if col not in df.columns:
        return None
    series = df[col].dropna()
    if series.empty:
        return None
    if normalise:
        series = series.map(normalise)
    valid_mask = series.isin(valid_set)
    total   = len(series)
    valid   = int(valid_mask.sum())
    return valid, total - valid, total


def run_rule(rule_id: str, df: pd.DataFrame) -> Optional[tuple[int, int, int]]:
    # dispatch to per-rule validation logic; returns (valid, invalid, total) or None if not applicable
    if df.empty:
        return None

    if rule_id == "ACC-001":  # le_book must be in BNR institution code set
        return _single_col(df, "le_book", VALID_LE_BOOKS,
                           normalise=lambda x: str(x).strip())

    if rule_id == "ACC-002":  # account_status must be a valid numeric code; coerce to int first
        if "account_status" not in df.columns:
            return None
        series = df["account_status"].dropna()
        if series.empty:
            return None
        try:
            series = series.astype(int)
        except (ValueError, TypeError):
            pass
        valid_mask = series.isin(VALID_ACCOUNT_STATUS)
        total = len(series)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    if rule_id == "ACC-003":  # performance_class must match BNR loan classification codes
        return _single_col(df, "performance_class", VALID_PERFORMANCE_CLASS,
                           normalise=lambda x: str(x).strip().upper())

    if rule_id == "ACC-004":  # customer_gender must be M, F, or C
        return _single_col(df, "customer_gender", VALID_GENDER,
                           normalise=lambda x: str(x).strip().upper())

    if rule_id == "ACC-005":  # account_type must be a valid BNR product code
        return _single_col(df, "account_type", VALID_ACCOUNT_TYPE,
                           normalise=lambda x: str(x).strip().upper())

    if rule_id == "ACC-010":  # corporate legal_status requires gender == C
        needed = ["customer_gender", "legal_status"]
        if not all(c in df.columns for c in needed):
            return None
        sub = df[needed].dropna()
        if sub.empty:
            return None
        try:
            ls = sub["legal_status"].astype(int)
        except (ValueError, TypeError):
            ls = sub["legal_status"]
        is_corporate = ls.isin(CORPORATE_LEGAL_STATUS)
        gender_is_c  = sub["customer_gender"].astype(str).str.strip().str.upper() == "C"
        invalid_mask = is_corporate & ~gender_is_c  # corporate with non-C gender = invalid
        total   = len(sub)
        invalid = int(invalid_mask.sum())
        return total - invalid, invalid, total

    if rule_id == "ACC-011":  # pension account types must not appear in the RETL segment
        needed = ["account_type", "vision_sbu"]
        if not all(c in df.columns for c in needed):
            return None
        sub = df[needed].dropna()
        if sub.empty:
            return None
        is_pension = sub["account_type"].astype(str).str.strip().str.upper().isin(PENSION_ACCOUNT_TYPES)
        is_retl    = sub["vision_sbu"].astype(str).str.strip().str.upper() == "RETL"
        invalid_mask = is_pension & is_retl  # pension product in retail segment = invalid
        total   = len(sub)
        invalid = int(invalid_mask.sum())
        return total - invalid, invalid, total

    if rule_id == "ACC-012":  # corporate customers (gender == C) must have marital_status == NA
        needed = ["marital_status", "customer_gender"]
        if not all(c in df.columns for c in needed):
            return None
        sub = df[needed].dropna()
        if sub.empty:
            return None
        is_corporate  = sub["customer_gender"].astype(str).str.strip().str.upper() == "C"
        marital_is_na = sub["marital_status"].astype(str).str.strip().str.upper() == "NA"
        invalid_mask  = is_corporate & ~marital_is_na  # corporate with non-NA marital = invalid
        total   = len(sub)
        invalid = int(invalid_mask.sum())
        return total - invalid, invalid, total

    if rule_id == "ACC-013":  # le_book must be exactly 3 numeric characters (zero-padded)
        if "le_book" not in df.columns:
            return None
        series = df["le_book"].dropna()
        if series.empty:
            return None
        s = series.astype(str).str.strip()
        valid_mask = (s.str.len() == 3) & s.str.match(r"^\d{3}$")
        total = len(series)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    log.warning("Unknown rule_id: %s", rule_id)
    return None


def run_rule_mask(rule_id: str, df: pd.DataFrame) -> pd.Series:
    """Return bool Series (True = row fails the rule, same index as df)."""
    false = pd.Series(False, index=df.index)
    if df.empty:
        return false

    if rule_id == "ACC-001":
        if "le_book" not in df.columns:
            return false
        s = df["le_book"].astype(str).str.strip()
        return df["le_book"].notna() & ~s.isin(VALID_LE_BOOKS)

    if rule_id == "ACC-002":
        if "account_status" not in df.columns:
            return false
        s = pd.to_numeric(df["account_status"], errors="coerce")
        return s.notna() & ~s.isin(VALID_ACCOUNT_STATUS)

    if rule_id == "ACC-003":
        if "performance_class" not in df.columns:
            return false
        s = df["performance_class"].astype(str).str.strip().str.upper()
        return df["performance_class"].notna() & ~s.isin(VALID_PERFORMANCE_CLASS)

    if rule_id == "ACC-004":
        if "customer_gender" not in df.columns:
            return false
        s = df["customer_gender"].astype(str).str.strip().str.upper()
        return df["customer_gender"].notna() & ~s.isin(VALID_GENDER)

    if rule_id == "ACC-005":
        if "account_type" not in df.columns:
            return false
        s = df["account_type"].astype(str).str.strip().str.upper()
        return df["account_type"].notna() & ~s.isin(VALID_ACCOUNT_TYPE)

    if rule_id == "ACC-010":
        needed = ["customer_gender", "legal_status"]
        if not all(c in df.columns for c in needed):
            return false
        both_notna  = df["customer_gender"].notna() & df["legal_status"].notna()
        ls          = pd.to_numeric(df["legal_status"], errors="coerce")
        is_corp     = ls.isin(CORPORATE_LEGAL_STATUS)
        gender_is_c = df["customer_gender"].astype(str).str.strip().str.upper() == "C"
        return both_notna & is_corp & ~gender_is_c

    if rule_id == "ACC-011":
        needed = ["account_type", "vision_sbu"]
        if not all(c in df.columns for c in needed):
            return false
        both_notna = df["account_type"].notna() & df["vision_sbu"].notna()
        is_pension  = df["account_type"].astype(str).str.strip().str.upper().isin(PENSION_ACCOUNT_TYPES)
        is_retl     = df["vision_sbu"].astype(str).str.strip().str.upper() == "RETL"
        return both_notna & is_pension & is_retl

    if rule_id == "ACC-012":
        needed = ["marital_status", "customer_gender"]
        if not all(c in df.columns for c in needed):
            return false
        both_notna    = df["marital_status"].notna() & df["customer_gender"].notna()
        is_corp       = df["customer_gender"].astype(str).str.strip().str.upper() == "C"
        marital_is_na = df["marital_status"].astype(str).str.strip().str.upper() == "NA"
        return both_notna & is_corp & ~marital_is_na

    if rule_id == "ACC-013":
        if "le_book" not in df.columns:
            return false
        s = df["le_book"].astype(str).str.strip()
        return df["le_book"].notna() & ~((s.str.len() == 3) & s.str.match(r"^\d{3}$", na=False))

    return false


def evaluate_table(df: pd.DataFrame, table_name: str) -> dict:
    # run all applicable rules on a DataFrame; build per-rule and per-le_book score breakdowns
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
        if "le_book" in df.columns:  # per le_book breakdown for this rule
            for le_val in sorted(df["le_book"].dropna().unique()):
                sub_df = df[df["le_book"] == le_val]
                sub    = run_rule(rule_id, sub_df)
                if sub is None:
                    continue
                sv, si, st = sub
                lb_breakdown[str(le_val)] = {
                    "valid":          sv,
                    "invalid":        si,
                    "total":          st,
                    "accuracy_score": _pct(sv, st),
                }

        rules_out[rule_id] = {
            "rule_name":         meta["name"],
            "category":          meta["category"],
            "fields":            meta["fields"],
            "valid":             valid,
            "invalid":           invalid,
            "total":             total,
            "accuracy_score":    score,
            "le_book_breakdown": lb_breakdown,
        }
        log.info("  %s  score=%.2f%%  invalid=%d / %d",
                 rule_id, score, invalid, total)

    le_book_breakdown: dict = {}
    if "le_book" in df.columns:  # table-level le_book breakdown: average rule scores per entity
        for le_val in sorted(df["le_book"].dropna().unique()):
            lb_key         = str(le_val)
            lb_rule_scores: list[float] = []
            lb_rules:       dict = {}
            for rule_id, rdata in rules_out.items():
                lb = rdata["le_book_breakdown"].get(lb_key)
                if lb:
                    lb_rule_scores.append(lb["accuracy_score"])
                    lb_rules[rule_id] = {
                        "rule_name":      rules_out[rule_id]["rule_name"],
                        "accuracy_score": lb["accuracy_score"],
                        "valid":          lb["valid"],
                        "invalid":        lb["invalid"],
                        "total":          lb["total"],
                    }
            if lb_rule_scores:
                le_book_breakdown[lb_key] = {
                    "row_count":      int((df["le_book"] == le_val).sum()),
                    "accuracy_score": round(sum(lb_rule_scores) / len(lb_rule_scores), 2),
                    "rules":          lb_rules,
                }

    overall = round(sum(rule_scores) / len(rule_scores), 2) if rule_scores else 0.0

    return {
        "status":            "evaluated",
        "row_count":         len(df),
        "rules_applied":     len(rules_out),
        "accuracy_score":    overall,
        "rules":             rules_out,
        "le_book_breakdown": le_book_breakdown,
    }


def evaluate(engine, tables: list[str], db_schema: str,
             limit: int, output_path: str) -> dict:
    # orchestrate fetch → rule evaluation → report dict → JSON output for all tables
    valid_le_books = get_valid_le_books(engine, db_schema)
    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "row_limit":    limit,
        "schema":       db_schema,
        "tables":       {},
        "warnings":     {},
    }

    all_scores:   list[float] = []
    all_le_books: set         = set()

    for table_name in tables:
        log.info("━━  Table: %s", table_name)
        columns = ACCURACY_COLUMNS.get(table_name, [])
        if not columns:
            log.warning("  No accuracy columns defined — skipping.")
            continue

        df = fetch_table(engine, table_name, columns, db_schema, limit, valid_le_books)
        if df.empty:
            log.warning("  No data returned — skipping.")
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "Table returned 0 rows."
            continue

        tbl_report = evaluate_table(df, table_name)
        report["tables"][table_name] = tbl_report
        all_scores.append(tbl_report["accuracy_score"])
        all_le_books.update(tbl_report["le_book_breakdown"].keys())

        log.info("  Table accuracy: %.2f%%  (%d rules)", tbl_report["accuracy_score"],
                 tbl_report["rules_applied"])

    report["le_books"] = sorted(all_le_books)

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0  # average across evaluated tables

    report["executive_summary"] = {
        "overall_accuracy_score": overall,
        "total_tables":           len(report["tables"]),
        "evaluated_tables":       len(evaluated),
        "row_limit":              limit,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
#for table-level and overall accuracy scores
    # log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    # log.info("  OVERALL ACCURACY  %.2f%%  (%d table(s) evaluated)", overall, len(evaluated))
    # log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return report

def evaluate_from_dataframes(dataframes: dict, valid_le_books: frozenset,
                              output_path: str) -> dict:
    """Run accuracy checks on pre-loaded DataFrames (no DB connection needed)."""
    global VALID_LE_BOOKS
    VALID_LE_BOOKS = valid_le_books  # ACC-001 reads this global to check valid institution codes

    report: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables":       {},
        "warnings":     {},
    }
    all_scores:   list[float] = []
    all_le_books: set         = set()

    for table_name in TARGET_TABLES:
        df = dataframes.get(table_name, pd.DataFrame())
        if df.empty:
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "No data in this period."
            continue

        tbl_report = evaluate_table(df, table_name)
        report["tables"][table_name] = tbl_report
        all_scores.append(tbl_report["accuracy_score"])
        all_le_books.update(tbl_report["le_book_breakdown"].keys())
        log.info("  %-30s  score=%.2f%%  (%d rules)",
                 table_name, tbl_report["accuracy_score"], tbl_report["rules_applied"])

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_accuracy_score": overall,
        "total_tables":           len(report["tables"]),
        "evaluated_tables":       len(evaluated),
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    log.info("Accuracy report → %s  (overall %.2f%%)", output_path, overall)
    return report


#main function
def main():
    # CLI entrypoint: parse args, load .env, connect to DB, run evaluate, log summary
    parser = argparse.ArgumentParser(
        description="DQ Accuracy Engine — BNR Upload Format For Guidelines v4.1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dq_accuracy_engine.py
  python dq_accuracy_engine.py --limit 1000
  python dq_accuracy_engine.py --limit 0          # full tables
  python dq_accuracy_engine.py --tables accounts contracts_expanded
  python dq_accuracy_engine.py --schema data_quality_program --output dq_accuracy_report.json
  python dq_accuracy_engine.py --env /path/to/.env
        """,
    )
    parser.add_argument("--tables", nargs="+", default=TARGET_TABLES)
    parser.add_argument("--schema", default="data_quality_program")
    parser.add_argument("--limit",  type=int, default=100000)
    parser.add_argument("--output", default="dq_accuracy_report.json")
    parser.add_argument("--env",    default=".env")
    args = parser.parse_args()

    env_path = Path(args.env)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        log.info("Loaded .env from: %s", env_path.resolve())
    else:
        log.warning(".env not found at '%s' — using shell environment.", env_path)

    log.info("DQ Accuracy Engine")
    log.info("  Tables : %s", ", ".join(args.tables))
    log.info("  Schema : %s", args.schema)
    log.info("  Limit  : %s", f"{args.limit:,} rows" if args.limit else "full table")
    log.info("  Output : %s", args.output)

    conn_str = build_connection_string()
    engine   = get_engine(conn_str)

    report = evaluate(engine, args.tables, args.schema, args.limit, args.output)
    s      = report.get("executive_summary", {})
    log.info("Report written → %s", args.output)
    log.info("    Overall Accuracy : %.2f%%", s.get("overall_accuracy_score", 0.0))
    log.info("    Tables evaluated : %d / %d",
             s.get("evaluated_tables", 0), s.get("total_tables", 0))

    if report.get("warnings"):
        log.warning("Tables with issues:")
        for tbl, msg in report["warnings"].items():
            log.warning("  %-40s  %s", tbl, msg)


if __name__ == "__main__":
    main()
