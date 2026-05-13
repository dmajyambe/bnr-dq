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
log = logging.getLogger("dq_timeliness")

TODAY = pd.Timestamp.today().normalize()   # midnight today, timezone-naive
DOB_MIN = pd.Timestamp("1900-01-01")
FRESHNESS_WINDOW_DAYS = 90                 # TIM-020: records stale beyond this

# only analyse rows whose le_book belongs to these BNR category types
CATEGORY_TYPES = ('MF', 'SACCO', 'OSACCO', 'B')

from dq_rules import (  # noqa: E402
    FRESHNESS_WINDOW_DAYS,
    TIM_RULE_META as RULE_META,
    TIMELINESS_COLUMNS,
    TIM_TABLE_RULES as TABLE_RULES,
)

TARGET_TABLES = list(TIMELINESS_COLUMNS.keys())


# ── connection helpers (mirrors accuracy_check.py) ────────────────────────────

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
    """Introspect schema, drop missing columns, then fetch data."""
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
        log.warning("  No timeliness columns found in DB for '%s'", table_name)
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
    """Safe percentage: returns 100.0 when total is 0."""
    return round(valid / total * 100, 2) if total else 100.0


def _to_dt(series: pd.Series) -> pd.Series:
    """Parse series to tz-naive datetime; invalid values become NaT."""
    dt = pd.to_datetime(series, errors="coerce", utc=False)
    if dt.dt.tz is not None:
        dt = dt.dt.tz_localize(None)
    return dt


def _no_future(series: pd.Series) -> Optional[tuple[int, int, int]]:
    """Generic check: parsed date ≤ today; NaT rows excluded from denominator."""
    dt = _to_dt(series).dropna()
    if dt.empty:
        return None
    valid_mask = dt <= TODAY
    total = len(dt)
    valid = int(valid_mask.sum())
    return valid, total - valid, total


def _ordered_pair(df: pd.DataFrame,
                  col_a: str, col_b: str,
                  strict: bool = False) -> Optional[tuple[int, int, int]]:
    """Check col_a <= col_b (or < when strict=True); NaT rows excluded."""
    if not all(c in df.columns for c in (col_a, col_b)):
        return None
    sub = df[[col_a, col_b]].copy()
    a = _to_dt(sub[col_a])
    b = _to_dt(sub[col_b])
    pair = pd.DataFrame({"a": a, "b": b}).dropna()
    if pair.empty:
        return None
    valid_mask = pair["a"] < pair["b"] if strict else pair["a"] <= pair["b"]
    total = len(pair)
    valid = int(valid_mask.sum())
    return valid, total - valid, total


# ── rule dispatcher ────────────────────────────────────────────────────────────

def run_rule(rule_id: str, df: pd.DataFrame) -> Optional[tuple[int, int, int]]:
    """Return (valid, invalid, total) or None if rule is not applicable."""
    if df.empty:
        return None

    # ── No Future Dates ────────────────────────────────────────────────────────
    if rule_id == "TIM-001":
        return None if "customer_open_date" not in df.columns \
               else _no_future(df["customer_open_date"])

    if rule_id == "TIM-002":
        if "date_of_birth" not in df.columns:
            return None
        dt = _to_dt(df["date_of_birth"]).dropna()
        if dt.empty:
            return None
        valid_mask = (dt >= DOB_MIN) & (dt <= TODAY)
        total = len(dt)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    if rule_id == "TIM-003":
        return None if "account_open_date" not in df.columns \
               else _no_future(df["account_open_date"])

    if rule_id == "TIM-004":
        return None if "date_creation" not in df.columns \
               else _no_future(df["date_creation"])

    if rule_id == "TIM-005":
        return None if "business_date" not in df.columns \
               else _no_future(df["business_date"])

    if rule_id == "TIM-006":
        return None if "approval_date" not in df.columns \
               else _no_future(df["approval_date"])

    if rule_id == "TIM-007":
        return None if "application_date" not in df.columns \
               else _no_future(df["application_date"])

    # ── Logical Date Order ─────────────────────────────────────────────────────
    if rule_id == "TIM-010":
        return _ordered_pair(df, "date_creation", "date_last_modified", strict=False)

    if rule_id == "TIM-011":
        return _ordered_pair(df, "start_date", "maturity_date", strict=True)

    if rule_id == "TIM-012":
        # Only evaluate rows where payment_date is non-null (payment has occurred)
        if not all(c in df.columns for c in ("schedule_date", "payment_date")):
            return None
        paid = df[df["payment_date"].notna()]
        if paid.empty:
            return None
        return _ordered_pair(paid, "schedule_date", "payment_date", strict=False)

    if rule_id == "TIM-013":
        return _ordered_pair(df, "commence_date", "benefit_expiry_date", strict=False)

    if rule_id == "TIM-014":
        return _ordered_pair(df, "commence_date", "ins_expiry_date", strict=False)

    # ── Data Freshness ─────────────────────────────────────────────────────────
    if rule_id == "TIM-020":
        if "date_last_modified" not in df.columns:
            return None
        dt = _to_dt(df["date_last_modified"]).dropna()
        if dt.empty:
            return None
        cutoff = TODAY - pd.Timedelta(days=FRESHNESS_WINDOW_DAYS)
        valid_mask = dt >= cutoff
        total = len(dt)
        valid = int(valid_mask.sum())
        return valid, total - valid, total

    log.warning("Unknown rule_id: %s", rule_id)
    return None


def run_rule_mask(rule_id: str, df: pd.DataFrame) -> pd.Series:
    """Return bool Series (True = row fails the rule, same index as df)."""
    false = pd.Series(False, index=df.index)
    if df.empty:
        return false

    if rule_id == "TIM-001":
        if "customer_open_date" not in df.columns: return false
        dt = _to_dt(df["customer_open_date"])
        return dt.notna() & (dt > TODAY)

    if rule_id == "TIM-002":
        if "date_of_birth" not in df.columns: return false
        dt = _to_dt(df["date_of_birth"])
        return dt.notna() & ~((dt >= DOB_MIN) & (dt <= TODAY))

    if rule_id == "TIM-003":
        if "account_open_date" not in df.columns: return false
        dt = _to_dt(df["account_open_date"])
        return dt.notna() & (dt > TODAY)

    if rule_id == "TIM-004":
        if "date_creation" not in df.columns: return false
        dt = _to_dt(df["date_creation"])
        return dt.notna() & (dt > TODAY)

    if rule_id == "TIM-005":
        if "business_date" not in df.columns: return false
        dt = _to_dt(df["business_date"])
        return dt.notna() & (dt > TODAY)

    if rule_id == "TIM-006":
        if "approval_date" not in df.columns: return false
        dt = _to_dt(df["approval_date"])
        return dt.notna() & (dt > TODAY)

    if rule_id == "TIM-007":
        if "application_date" not in df.columns: return false
        dt = _to_dt(df["application_date"])
        return dt.notna() & (dt > TODAY)

    if rule_id == "TIM-010":
        if not all(c in df.columns for c in ("date_creation", "date_last_modified")):
            return false
        a = _to_dt(df["date_creation"])
        b = _to_dt(df["date_last_modified"])
        return a.notna() & b.notna() & (a > b)

    if rule_id == "TIM-011":
        if not all(c in df.columns for c in ("start_date", "maturity_date")):
            return false
        a = _to_dt(df["start_date"])
        b = _to_dt(df["maturity_date"])
        return a.notna() & b.notna() & (a >= b)

    if rule_id == "TIM-012":
        if not all(c in df.columns for c in ("schedule_date", "payment_date")):
            return false
        has_payment = df["payment_date"].notna()
        a = _to_dt(df["schedule_date"])
        b = _to_dt(df["payment_date"])
        return has_payment & a.notna() & b.notna() & (a > b)

    if rule_id == "TIM-013":
        if not all(c in df.columns for c in ("commence_date", "benefit_expiry_date")):
            return false
        a = _to_dt(df["commence_date"])
        b = _to_dt(df["benefit_expiry_date"])
        return a.notna() & b.notna() & (a > b)

    if rule_id == "TIM-014":
        if not all(c in df.columns for c in ("commence_date", "ins_expiry_date")):
            return false
        a = _to_dt(df["commence_date"])
        b = _to_dt(df["ins_expiry_date"])
        return a.notna() & b.notna() & (a > b)

    if rule_id == "TIM-020":
        if "date_last_modified" not in df.columns: return false
        dt     = _to_dt(df["date_last_modified"])
        cutoff = TODAY - pd.Timedelta(days=FRESHNESS_WINDOW_DAYS)
        return dt.notna() & (dt < cutoff)

    return false


# ── per-table evaluation ───────────────────────────────────────────────────────

def evaluate_table(df: pd.DataFrame, table_name: str) -> dict:
    """Run all applicable rules; build per-rule and per-le_book breakdowns."""
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
                    "valid":            sv,
                    "invalid":          si,
                    "total":            st,
                    "timeliness_score": _pct(sv, st),
                }

        rules_out[rule_id] = {
            "rule_name":         meta["name"],
            "category":          meta["category"],
            "fields":            meta["fields"],
            "valid":             valid,
            "invalid":           invalid,
            "total":             total,
            "timeliness_score":  score,
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
                    lb_rule_scores.append(lb["timeliness_score"])
                    lb_rules[rule_id] = {
                        "rule_name":        rules_out[rule_id]["rule_name"],
                        "timeliness_score": lb["timeliness_score"],
                        "valid":            lb["valid"],
                        "invalid":          lb["invalid"],
                        "total":            lb["total"],
                    }
            if lb_rule_scores:
                le_book_breakdown[lb_key] = {
                    "row_count":        int((df["le_book"] == le_val).sum()),
                    "timeliness_score": round(sum(lb_rule_scores) / len(lb_rule_scores), 2),
                    "rules":            lb_rules,
                }

    overall = round(sum(rule_scores) / len(rule_scores), 2) if rule_scores else 0.0

    return {
        "status":            "evaluated",
        "row_count":         len(df),
        "rules_applied":     len(rules_out),
        "timeliness_score":  overall,
        "rules":             rules_out,
        "le_book_breakdown": le_book_breakdown,
    }


# ── orchestration ──────────────────────────────────────────────────────────────

def evaluate(engine, tables: list[str], db_schema: str,
             limit: int, output_path: str) -> dict:
    """Fetch → evaluate → write JSON report for all tables."""
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
        columns = TIMELINESS_COLUMNS.get(table_name, [])
        if not columns:
            log.warning("  No timeliness columns defined — skipping.")
            continue

        df = fetch_table(engine, table_name, columns, db_schema, limit, valid_le_books)
        if df.empty:
            log.warning("  No data returned — skipping.")
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "Table returned 0 rows."
            continue

        tbl_report = evaluate_table(df, table_name)
        report["tables"][table_name] = tbl_report
        all_scores.append(tbl_report["timeliness_score"])
        all_le_books.update(tbl_report["le_book_breakdown"].keys())

        log.info("  Table timeliness: %.2f%%  (%d rules)",
                 tbl_report["timeliness_score"], tbl_report["rules_applied"])

    report["le_books"] = sorted(all_le_books)

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["executive_summary"] = {
        "overall_timeliness_score": overall,
        "total_tables":             len(report["tables"]),
        "evaluated_tables":         len(evaluated),
        "row_limit":                limit,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    return report


def evaluate_from_dataframes(dataframes: dict, valid_le_books: frozenset,
                              output_path: str) -> dict:
    """Run timeliness checks on pre-loaded DataFrames (no DB connection needed)."""
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
        all_scores.append(tbl_report["timeliness_score"])
        all_le_books.update(tbl_report["le_book_breakdown"].keys())
        log.info("  %-30s  score=%.2f%%  (%d rules)",
                 table_name, tbl_report["timeliness_score"], tbl_report["rules_applied"])

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_timeliness_score": overall,
        "total_tables":             len(report["tables"]),
        "evaluated_tables":         len(evaluated),
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    log.info("Timeliness report → %s  (overall %.2f%%)", output_path, overall)
    return report


# ── CLI entry-point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="DQ Timeliness Engine — BNR Data Quality Programme",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python timeliness_check.py
  python timeliness_check.py --limit 0          # full tables
  python timeliness_check.py --tables accounts contracts_expanded
  python timeliness_check.py --schema data_quality_program --output dq_timeliness_report.json
        """,
    )
    parser.add_argument("--tables", nargs="+", default=TARGET_TABLES)
    parser.add_argument("--schema", default="data_quality_program")
    parser.add_argument("--limit",  type=int, default=100000)
    parser.add_argument("--output", default="dq_timeliness_report.json")
    parser.add_argument("--env",    default=".env")
    args = parser.parse_args()

    env_path = Path(args.env)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        log.info("Loaded .env from: %s", env_path.resolve())
    else:
        log.warning(".env not found at '%s' — using shell environment.", env_path)

    log.info("DQ Timeliness Engine")
    log.info("  Tables  : %s", ", ".join(args.tables))
    log.info("  Schema  : %s", args.schema)
    log.info("  Limit   : %s", f"{args.limit:,} rows" if args.limit else "full table")
    log.info("  Output  : %s", args.output)
    log.info("  Today   : %s", TODAY.date())

    conn_str = build_connection_string()
    engine   = get_engine(conn_str)

    report = evaluate(engine, args.tables, args.schema, args.limit, args.output)
    s      = report.get("executive_summary", {})
    log.info("Report written → %s", args.output)
    log.info("    Overall Timeliness : %.2f%%", s.get("overall_timeliness_score", 0.0))
    log.info("    Tables evaluated   : %d / %d",
             s.get("evaluated_tables", 0), s.get("total_tables", 0))

    if report.get("warnings"):
        log.warning("Tables with issues:")
        for tbl, msg in report["warnings"].items():
            log.warning("  %-40s  %s", tbl, msg)


if __name__ == "__main__":
    main()
