from __future__ import annotations
import argparse
import json
import logging
import os
import re
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
log = logging.getLogger("dq_validity")

# only analyse rows whose le_book belongs to these BNR category types
CATEGORY_TYPES = ('MF', 'SACCO', 'OSACCO', 'B')

# compiled regexes
_RE_EMAIL    = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
_RE_CURRENCY = re.compile(r'^[A-Z]{3}$')
_RE_DIGITS   = re.compile(r'\d')

MIN_PHONE_DIGITS  = 7    # minimum digit characters in a phone number
MIN_NATIONAL_ID   = 5    # minimum character length for a national ID number
MIN_AGE_AT_OPEN   = 18   # minimum customer age (years) at account open date
INTEREST_RATE_MAX = 100  # maximum plausible interest rate (%)

from dq_rules import (  # noqa: E402
    MIN_PHONE_DIGITS, MIN_NATIONAL_ID, INTEREST_RATE_MAX, MIN_AGE_AT_OPEN,
    VAL_RULE_META as RULE_META,
    VALIDITY_COLUMNS,
    VAL_TABLE_RULES as TABLE_RULES,
)

TARGET_TABLES = list(VALIDITY_COLUMNS.keys())


# ── connection helpers ────────────────────────────────────────────────────────

def build_connection_string() -> str:
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


def get_engine(conn_str: str):
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


def get_valid_le_books(engine, schema: str) -> frozenset:
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
        log.warning("  No validity columns found in DB for '%s'", table_name)
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


# ── rule helpers ───────────────────────────────────────────────────────────────

def _pct(valid: int, total: int) -> float:
    return round(valid / total * 100, 2) if total else 100.0


def _check_col(df: pd.DataFrame, col: str, mask_fn) -> Optional[tuple[int, int, int]]:
    """Apply mask_fn to a single non-null series; return (valid, invalid, total)."""
    if col not in df.columns:
        return None
    series = df[col].dropna()
    if series.empty:
        return None
    valid_mask = mask_fn(series)
    total = len(series)
    valid = int(valid_mask.sum())
    return valid, total - valid, total


def _non_negative(df: pd.DataFrame, *cols: str) -> Optional[tuple[int, int, int]]:
    """
    Pool all specified columns: each non-null value is a separate observation.
    Returns (valid, invalid, total) where valid means value >= 0.
    """
    present = [c for c in cols if c in df.columns]
    if not present:
        return None
    all_valid, all_total = 0, 0
    for col in present:
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        all_valid += int((series >= 0).sum())
        all_total += len(series)
    return (all_valid, all_total - all_valid, all_total) if all_total else None


def _positive(df: pd.DataFrame, col: str) -> Optional[tuple[int, int, int]]:
    """Check col > 0 for non-null values."""
    if col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    if series.empty:
        return None
    valid = int((series > 0).sum())
    total = len(series)
    return valid, total - valid, total


def _rate_range(df: pd.DataFrame, col: str) -> Optional[tuple[int, int, int]]:
    """Check 0 <= col <= INTEREST_RATE_MAX for non-null values."""
    if col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    if series.empty:
        return None
    valid = int(((series >= 0) & (series <= INTEREST_RATE_MAX)).sum())
    total = len(series)
    return valid, total - valid, total


# ── rule dispatcher ────────────────────────────────────────────────────────────

def run_rule(rule_id: str, df: pd.DataFrame) -> Optional[tuple[int, int, int]]:
    """Return (valid, invalid, total) or None if rule is not applicable to this df."""
    if df.empty:
        return None

    # ── Format Validity ────────────────────────────────────────────────────────

    if rule_id == "VAL-001":
        # email_id must match basic email regex
        return _check_col(df, "email_id",
                          lambda s: s.astype(str).str.strip().str.match(_RE_EMAIL))

    if rule_id == "VAL-002":
        # work_telephone and home_telephone must each contain >= MIN_PHONE_DIGITS digits
        # Evaluate all non-null values across both columns as one pooled observation set.
        # A row with both columns null is excluded from the denominator entirely.
        cols = [c for c in ("work_telephone", "home_telephone") if c in df.columns]
        if not cols:
            return None
        all_valid, all_total = 0, 0
        for col in cols:
            series = df[col].dropna().astype(str).str.strip()
            series = series[series != ""]
            if series.empty:
                continue
            digit_counts = series.apply(lambda v: len(_RE_DIGITS.findall(v)))
            all_valid += int((digit_counts >= MIN_PHONE_DIGITS).sum())
            all_total += len(series)
        return (all_valid, all_total - all_valid, all_total) if all_total else None

    if rule_id == "VAL-003":
        # currency columns must match ^[A-Z]{3}$
        cols = [c for c in ("currency", "mis_currency") if c in df.columns]
        if not cols:
            return None
        all_valid, all_total = 0, 0
        for col in cols:
            series = df[col].dropna().astype(str).str.strip()
            series = series[series != ""]
            if series.empty:
                continue
            all_valid += int(series.str.match(_RE_CURRENCY).sum())
            all_total += len(series)
        return (all_valid, all_total - all_valid, all_total) if all_total else None

    if rule_id == "VAL-004":
        # when national_id_type is non-null/non-empty, national_id_number must be
        # non-null and at least MIN_NATIONAL_ID characters long
        if "national_id_type" not in df.columns or "national_id_number" not in df.columns:
            return None
        # rows where a type is specified
        has_type = df["national_id_type"].notna() & \
                   (df["national_id_type"].astype(str).str.strip() != "")
        sub = df[has_type]
        if sub.empty:
            return None
        id_num = sub["national_id_number"].fillna("").astype(str).str.strip()
        valid_mask = id_num.str.len() >= MIN_NATIONAL_ID
        total = len(sub)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    # ── Range Validity ─────────────────────────────────────────────────────────

    if rule_id == "VAL-010":
        return _rate_range(df, "interest_rate_dr")

    if rule_id == "VAL-011":
        return _rate_range(df, "interest_rate_cr")

    if rule_id == "VAL-012":
        return _non_negative(df, "current_disbursed_amt", "previous_disbursed_amt")

    if rule_id == "VAL-013":
        return _positive(df, "emi_amount")

    if rule_id == "VAL-014":
        # check whichever balance/due columns are present in this table
        return _non_negative(df, "outstanding_amount_lcy", "outstanding_amount",
                             "principal_amount_due", "int_amount_due",
                             "due_amount", "principal_amount_lcy")

    if rule_id == "VAL-015":
        return _positive(df, "applied_amount_lcy")

    if rule_id == "VAL-016":
        # num_of_instalments must be an integer >= 1
        if "num_of_instalments" not in df.columns:
            return None
        series = pd.to_numeric(df["num_of_instalments"], errors="coerce").dropna()
        if series.empty:
            return None
        valid = int((series >= 1).sum())
        total = len(series)
        return valid, total - valid, total

    # ── Cross-field Validity ───────────────────────────────────────────────────

    if rule_id == "VAL-020":
        # num_instalments_paid <= num_of_instalments
        needed = ["num_instalments_paid", "num_of_instalments"]
        if not all(c in df.columns for c in needed):
            return None
        sub = df[needed].copy()
        paid  = pd.to_numeric(sub["num_instalments_paid"],  errors="coerce")
        total_inst = pd.to_numeric(sub["num_of_instalments"], errors="coerce")
        pair = pd.DataFrame({"paid": paid, "total": total_inst}).dropna()
        if pair.empty:
            return None
        valid_mask = pair["paid"] <= pair["total"]
        total = len(pair)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    if rule_id == "VAL-021":
        # approved_amount_lcy <= applied_amount_lcy
        needed = ["approved_amount_lcy", "applied_amount_lcy"]
        if not all(c in df.columns for c in needed):
            return None
        approved = pd.to_numeric(df["approved_amount_lcy"], errors="coerce")
        applied  = pd.to_numeric(df["applied_amount_lcy"],  errors="coerce")
        pair = pd.DataFrame({"approved": approved, "applied": applied}).dropna()
        if pair.empty:
            return None
        valid_mask = pair["approved"] <= pair["applied"]
        total = len(pair)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    if rule_id == "VAL-022":
        # customer must be at least MIN_AGE_AT_OPEN years old at customer_open_date
        needed = ["date_of_birth", "customer_open_date"]
        if not all(c in df.columns for c in needed):
            return None
        dob  = pd.to_datetime(df["date_of_birth"],      errors="coerce", utc=False)
        open_= pd.to_datetime(df["customer_open_date"], errors="coerce", utc=False)
        if dob.dt.tz is not None:
            dob = dob.dt.tz_localize(None)
        if open_.dt.tz is not None:
            open_ = open_.dt.tz_localize(None)
        pair = pd.DataFrame({"dob": dob, "open": open_}).dropna()
        if pair.empty:
            return None
        age_days = (pair["open"] - pair["dob"]).dt.days
        valid_mask = age_days >= (MIN_AGE_AT_OPEN * 365)
        total = len(pair)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    log.warning("Unknown rule_id: %s", rule_id)
    return None


def run_rule_mask(rule_id: str, df: pd.DataFrame) -> pd.Series:
    """Return bool Series (True = row fails the rule, same index as df)."""
    false = pd.Series(False, index=df.index)
    if df.empty:
        return false

    if rule_id == "VAL-001":
        if "email_id" not in df.columns: return false
        s = df["email_id"].astype(str).str.strip()
        return df["email_id"].notna() & ~s.str.match(_RE_EMAIL, na=False)

    if rule_id == "VAL-002":
        cols = [c for c in ("work_telephone", "home_telephone") if c in df.columns]
        if not cols: return false
        result = pd.Series(False, index=df.index)
        for col in cols:
            nonempty = df[col].notna() & (df[col].astype(str).str.strip() != "")
            digits   = df[col].astype(str).apply(lambda v: len(_RE_DIGITS.findall(v)))
            result   = result | (nonempty & (digits < MIN_PHONE_DIGITS))
        return result

    if rule_id == "VAL-003":
        cols = [c for c in ("currency", "mis_currency") if c in df.columns]
        if not cols: return false
        result = pd.Series(False, index=df.index)
        for col in cols:
            s        = df[col].astype(str).str.strip()
            nonempty = df[col].notna() & (s != "")
            result   = result | (nonempty & ~s.str.match(_RE_CURRENCY, na=False))
        return result

    if rule_id == "VAL-004":
        if "national_id_type" not in df.columns or "national_id_number" not in df.columns:
            return false
        has_type = df["national_id_type"].notna() & \
                   (df["national_id_type"].astype(str).str.strip() != "")
        id_num   = df["national_id_number"].fillna("").astype(str).str.strip()
        return has_type & (id_num.str.len() < MIN_NATIONAL_ID)

    if rule_id == "VAL-010":
        if "interest_rate_dr" not in df.columns: return false
        s = pd.to_numeric(df["interest_rate_dr"], errors="coerce")
        return s.notna() & ~((s >= 0) & (s <= INTEREST_RATE_MAX))

    if rule_id == "VAL-011":
        if "interest_rate_cr" not in df.columns: return false
        s = pd.to_numeric(df["interest_rate_cr"], errors="coerce")
        return s.notna() & ~((s >= 0) & (s <= INTEREST_RATE_MAX))

    if rule_id == "VAL-012":
        cols = [c for c in ("current_disbursed_amt", "previous_disbursed_amt") if c in df.columns]
        if not cols: return false
        result = pd.Series(False, index=df.index)
        for col in cols:
            s      = pd.to_numeric(df[col], errors="coerce")
            result = result | (s.notna() & (s < 0))
        return result

    if rule_id == "VAL-013":
        if "emi_amount" not in df.columns: return false
        s = pd.to_numeric(df["emi_amount"], errors="coerce")
        return s.notna() & (s <= 0)

    if rule_id == "VAL-014":
        cols = [c for c in ("outstanding_amount_lcy", "outstanding_amount",
                            "principal_amount_due", "int_amount_due",
                            "due_amount", "principal_amount_lcy") if c in df.columns]
        if not cols: return false
        result = pd.Series(False, index=df.index)
        for col in cols:
            s      = pd.to_numeric(df[col], errors="coerce")
            result = result | (s.notna() & (s < 0))
        return result

    if rule_id == "VAL-015":
        if "applied_amount_lcy" not in df.columns: return false
        s = pd.to_numeric(df["applied_amount_lcy"], errors="coerce")
        return s.notna() & (s <= 0)

    if rule_id == "VAL-016":
        if "num_of_instalments" not in df.columns: return false
        s = pd.to_numeric(df["num_of_instalments"], errors="coerce")
        return s.notna() & (s < 1)

    if rule_id == "VAL-020":
        needed = ["num_instalments_paid", "num_of_instalments"]
        if not all(c in df.columns for c in needed): return false
        paid  = pd.to_numeric(df["num_instalments_paid"],  errors="coerce")
        total = pd.to_numeric(df["num_of_instalments"],    errors="coerce")
        return paid.notna() & total.notna() & (paid > total)

    if rule_id == "VAL-021":
        needed = ["approved_amount_lcy", "applied_amount_lcy"]
        if not all(c in df.columns for c in needed): return false
        approved = pd.to_numeric(df["approved_amount_lcy"], errors="coerce")
        applied  = pd.to_numeric(df["applied_amount_lcy"],  errors="coerce")
        return approved.notna() & applied.notna() & (approved > applied)

    if rule_id == "VAL-022":
        needed = ["date_of_birth", "customer_open_date"]
        if not all(c in df.columns for c in needed): return false
        dob   = pd.to_datetime(df["date_of_birth"],      errors="coerce", utc=False)
        open_ = pd.to_datetime(df["customer_open_date"], errors="coerce", utc=False)
        if getattr(dob.dt, "tz", None) is not None:
            dob = dob.dt.tz_localize(None)
        if getattr(open_.dt, "tz", None) is not None:
            open_ = open_.dt.tz_localize(None)
        age_days = (open_ - dob).dt.days
        return dob.notna() & open_.notna() & (age_days < (MIN_AGE_AT_OPEN * 365))

    return false


# ── per-table evaluation ───────────────────────────────────────────────────────

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
                    "valid":           sv,
                    "invalid":         si,
                    "total":           st,
                    "validity_score":  _pct(sv, st),
                }

        rules_out[rule_id] = {
            "rule_name":        meta["name"],
            "category":         meta["category"],
            "fields":           meta["fields"],
            "valid":            valid,
            "invalid":          invalid,
            "total":            total,
            "validity_score":   score,
            "le_book_breakdown": lb_breakdown,
        }
        log.info("  %s  score=%.2f%%  invalid=%d / %d",
                 rule_id, score, invalid, total)

    le_book_breakdown: dict = {}
    if "le_book" in df.columns:
        for le_val in sorted(df["le_book"].dropna().unique()):
            lb_key         = str(le_val)
            lb_rule_scores: list[float] = []
            lb_rules:       dict = {}
            for rule_id, rdata in rules_out.items():
                lb = rdata["le_book_breakdown"].get(lb_key)
                if lb:
                    lb_rule_scores.append(lb["validity_score"])
                    lb_rules[rule_id] = {
                        "rule_name":      rules_out[rule_id]["rule_name"],
                        "validity_score": lb["validity_score"],
                        "valid":          lb["valid"],
                        "invalid":        lb["invalid"],
                        "total":          lb["total"],
                    }
            if lb_rule_scores:
                le_book_breakdown[lb_key] = {
                    "row_count":      int((df["le_book"] == le_val).sum()),
                    "validity_score": round(sum(lb_rule_scores) / len(lb_rule_scores), 2),
                    "rules":          lb_rules,
                }

    overall = round(sum(rule_scores) / len(rule_scores), 2) if rule_scores else 0.0

    return {
        "status":            "evaluated",
        "row_count":         len(df),
        "rules_applied":     len(rules_out),
        "validity_score":    overall,
        "rules":             rules_out,
        "le_book_breakdown": le_book_breakdown,
    }


# ── orchestration ──────────────────────────────────────────────────────────────

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

    all_scores:   list[float] = []
    all_le_books: set         = set()

    for table_name in tables:
        log.info("━━  Table: %s", table_name)
        columns = VALIDITY_COLUMNS.get(table_name, [])
        if not columns:
            log.warning("  No validity columns defined — skipping.")
            continue

        df = fetch_table(engine, table_name, columns, db_schema, limit, valid_le_books)
        if df.empty:
            log.warning("  No data returned — skipping.")
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "Table returned 0 rows."
            continue

        tbl_report = evaluate_table(df, table_name)
        report["tables"][table_name] = tbl_report
        all_scores.append(tbl_report["validity_score"])
        all_le_books.update(tbl_report["le_book_breakdown"].keys())

        log.info("  Table validity: %.2f%%  (%d rules)",
                 tbl_report["validity_score"], tbl_report["rules_applied"])

    report["le_books"] = sorted(all_le_books)

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["executive_summary"] = {
        "overall_validity_score": overall,
        "total_tables":           len(report["tables"]),
        "evaluated_tables":       len(evaluated),
        "row_limit":              limit,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    return report


def evaluate_from_dataframes(dataframes: dict, valid_le_books: frozenset,
                              output_path: str) -> dict:
    """Run validity checks on pre-loaded DataFrames (no DB connection needed)."""
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
        all_scores.append(tbl_report["validity_score"])
        all_le_books.update(tbl_report["le_book_breakdown"].keys())
        log.info("  %-30s  score=%.2f%%  (%d rules)",
                 table_name, tbl_report["validity_score"], tbl_report["rules_applied"])

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_validity_score": overall,
        "total_tables":           len(report["tables"]),
        "evaluated_tables":       len(evaluated),
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    log.info("Validity report → %s  (overall %.2f%%)", output_path, overall)
    return report


# ── CLI entry-point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="DQ Validity Engine — BNR Data Quality Programme",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validity_check.py
  python validity_check.py --limit 0          # full tables
  python validity_check.py --tables accounts contracts_expanded
  python validity_check.py --schema data_quality_program --output dq_validity_report.json
        """,
    )
    parser.add_argument("--tables", nargs="+", default=TARGET_TABLES)
    parser.add_argument("--schema", default="data_quality_program")
    parser.add_argument("--limit",  type=int, default=100000)
    parser.add_argument("--output", default="dq_validity_report.json")
    parser.add_argument("--env",    default=".env")
    args = parser.parse_args()

    env_path = Path(args.env)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        log.info("Loaded .env from: %s", env_path.resolve())
    else:
        log.warning(".env not found at '%s' — using shell environment.", env_path)

    log.info("DQ Validity Engine")
    log.info("  Tables  : %s", ", ".join(args.tables))
    log.info("  Schema  : %s", args.schema)
    log.info("  Limit   : %s", f"{args.limit:,} rows" if args.limit else "full table")
    log.info("  Output  : %s", args.output)

    conn_str = build_connection_string()
    engine   = get_engine(conn_str)

    report = evaluate(engine, args.tables, args.schema, args.limit, args.output)
    s      = report.get("executive_summary", {})
    log.info("Report written → %s", args.output)
    log.info("    Overall Validity  : %.2f%%", s.get("overall_validity_score", 0.0))
    log.info("    Tables evaluated  : %d / %d",
             s.get("evaluated_tables", 0), s.get("total_tables", 0))

    if report.get("warnings"):
        log.warning("Tables with issues:")
        for tbl, msg in report["warnings"].items():
            log.warning("  %-40s  %s", tbl, msg)


if __name__ == "__main__":
    main()
