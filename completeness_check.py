#completeness check (mandatory columns)
from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_engine")


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


def get_engine(conn_str: str):  # create engine and verify connectivity;exit on failure
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(
            conn_str,
            pool_pre_ping=True,
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


# only analyse rows whose le_book belongs to these  category types
CATEGORY_TYPES = ('MF', 'SACCO', 'OSACCO', 'B')


def get_valid_le_books(engine, schema: str) -> frozenset:
    """Return le_book codes whose category_type is in CATEGORY_TYPES."""
    from sqlalchemy import text
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


from dq_rules import MANDATORY_COLUMNS  # noqa: E402 — rule definitions live in dq_rules.py

TARGET_TABLES = list(MANDATORY_COLUMNS.keys())

#check if columns are present or missing
def resolve_columns(engine, table_name: str, mandatory: list[str],
                    db_schema: str) -> tuple[list[str], list[str]]:
    # introspect DB schema; split mandatory columns into found vs missing
    from sqlalchemy import inspect as sa_inspect
    db_cols: set[str] = set()
    try:
        inspector = sa_inspect(engine)
        for schema in (db_schema, None):  # try specified schema then public fallback
            try:
                cols = inspector.get_columns(table_name, schema=schema)
                if cols:
                    db_cols = {c["name"].lower() for c in cols}
                    break
            except Exception:
                continue
    except Exception as exc:
        log.warning("Cannot introspect '%s': %s", table_name, exc)

    found   = [c for c in mandatory if c in db_cols]
    missing = [c for c in mandatory if c not in db_cols]
    return found, missing #return two lists: mandatory columns found in DB, and those missing


def fetch_table(engine, table_name: str, columns: list[str],
                db_schema: str, limit: int,
                valid_le_books: frozenset = frozenset()) -> pd.DataFrame:
    # run SELECT for specified columns; return empty DataFrame on query failure
    from sqlalchemy import text
    quoted = ", ".join(f'"{c}"' for c in columns)

    # restrict to valid le_book category types when column is present
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


def check_completeness(df: pd.DataFrame, cols: list[str]) -> dict:
    # score = non-null cells / (rows × mandatory cols present) × 100
    present = [c for c in cols if c in df.columns]
    if not present or df.empty:
        return {"score": 100.0, "null_counts": {}, "null_cells": 0, "total_cells": 0}

    null_counts = {c: int(df[c].isnull().sum()) for c in present}
    null_cells  = sum(null_counts.values())
    total_cells = len(df) * len(present)
    score       = round((1 - null_cells / total_cells) * 100, 2)

    return {
        "score":       score,
        "null_counts": null_counts,
        "null_cells":  null_cells,
        "total_cells": total_cells,
    }

#create report(per table and overall) and write to a JSON file
def evaluate(engine, tables: list[str], db_schema: str,
             limit: int, output_path: str) -> dict:
    # orchestrate per-table fetch → completeness score → report dict → JSON output
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

        mandatory = MANDATORY_COLUMNS.get(table_name, [])
        if not mandatory:
            log.warning("  No mandatory columns defined — skipping.")
            continue

        found_cols, missing_cols = resolve_columns(engine, table_name, mandatory, db_schema)
        if not found_cols:
            log.warning("  No mandatory columns found in DB — skipping.")
            report["tables"][table_name] = {"status": "not_found"}
            report["warnings"][table_name] = "Table not found or no mandatory columns accessible."
            continue

        if missing_cols:
            log.warning("  %d mandatory col(s) absent from DB: %s",
                        len(missing_cols), ", ".join(missing_cols))

        df = fetch_table(engine, table_name, found_cols, db_schema, limit, valid_le_books)
        if df.empty:
            log.warning("  No data returned — skipping.")
            report["tables"][table_name] = {"status": "no_data", "row_count": 0}
            report["warnings"][table_name] = "Table returned 0 rows."
            continue

        result = check_completeness(df, found_cols)  # overall score across all mandatory cols
        all_scores.append(result["score"])
        log.info("  completeness  score=%.2f%%  null=%d / %d cells",
                 result["score"], result["null_cells"], result["total_cells"])

        le_book_breakdown: dict = {}
        if "le_book" in df.columns:  # sub-score per le_book entity within the same table
            for le_val in sorted(df["le_book"].dropna().unique()):
                sub_df = df[df["le_book"] == le_val].reset_index(drop=True)
                if sub_df.empty:
                    continue
                sub = check_completeness(sub_df, found_cols)
                le_book_breakdown[str(le_val)] = {
                    "row_count":          len(sub_df),
                    "completeness_score": sub["score"],
                    "null_counts":        sub["null_counts"],
                    "null_cells":         sub["null_cells"],
                    "total_cells":        sub["total_cells"],
                }
            if le_book_breakdown:
                log.info("  le_book groups: %s",
                         ", ".join(f"{k}({v['row_count']}r)" for k, v in le_book_breakdown.items()))

        report["tables"][table_name] = {
            "status":             "evaluated",
            "row_count":          len(df),
            "mandatory_count":    len(mandatory),
            "found_in_db":        len(found_cols),
            "missing_from_db":    missing_cols,
            "completeness_score": result["score"],
            "null_counts":        result["null_counts"],
            "null_cells":         result["null_cells"],
            "total_cells":        result["total_cells"],
            "le_book_breakdown":  le_book_breakdown,
        }

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0  # average across evaluated tables

    all_le_books: set = set()
    for tdata in report["tables"].values():
        all_le_books.update(tdata.get("le_book_breakdown", {}).keys())
    report["le_books"] = sorted(all_le_books)

    report["executive_summary"] = {  # top-level summary written to report root
        "overall_completeness_score": overall,
        "total_tables":               len(report["tables"]),
        "evaluated_tables":           len(evaluated),
        "row_limit":                  limit,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    # log.info("*****************************************************************************")
    # log.info("  OVERALL COMPLETENESS  %.2f%%  (%d table(s) evaluated)", overall, len(evaluated))
    # log.info("******************************************************************************")

    return report

def evaluate_from_dataframes(dataframes: dict, valid_le_books: frozenset,
                              output_path: str) -> dict:
    """Run completeness checks on pre-loaded DataFrames (no DB connection needed)."""
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

        mandatory    = MANDATORY_COLUMNS.get(table_name, [])
        found_cols   = [c for c in mandatory if c in df.columns]
        missing_cols = [c for c in mandatory if c not in df.columns]

        if not found_cols:
            report["tables"][table_name] = {"status": "not_found"}
            report["warnings"][table_name] = "No mandatory columns found in DataFrame."
            continue

        result = check_completeness(df, found_cols)
        all_scores.append(result["score"])

        le_book_breakdown: dict = {}
        if "le_book" in df.columns:
            for le_val in sorted(df["le_book"].dropna().unique()):
                sub_df = df[df["le_book"] == le_val].reset_index(drop=True)
                if sub_df.empty:
                    continue
                sub = check_completeness(sub_df, found_cols)
                le_book_breakdown[str(le_val)] = {
                    "row_count":          len(sub_df),
                    "completeness_score": sub["score"],
                    "null_counts":        sub["null_counts"],
                    "null_cells":         sub["null_cells"],
                    "total_cells":        sub["total_cells"],
                }
                all_le_books.add(str(le_val))

        report["tables"][table_name] = {
            "status":             "evaluated",
            "row_count":          len(df),
            "mandatory_count":    len(mandatory),
            "found_in_db":        len(found_cols),
            "missing_from_db":    missing_cols,
            "completeness_score": result["score"],
            "null_counts":        result["null_counts"],
            "null_cells":         result["null_cells"],
            "total_cells":        result["total_cells"],
            "le_book_breakdown":  le_book_breakdown,
        }
        log.info("  %-30s  score=%.2f%%  null=%d/%d cells",
                 table_name, result["score"], result["null_cells"], result["total_cells"])

    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    overall   = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_completeness_score": overall,
        "total_tables":               len(report["tables"]),
        "evaluated_tables":           len(evaluated),
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    log.info("Completeness report → %s  (overall %.2f%%)", output_path, overall)
    return report


#main function
def main():
    #parse args, load .env, connect to DB, run evaluate, log summary
    parser = argparse.ArgumentParser(
        description="DQ Engine — Completeness on mandatory columns per table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dq_engine.py
  python dq_engine.py --limit 1000
  python dq_engine.py --limit 0          # full tables, no cap
  python dq_engine.py --tables accounts contracts_expanded
  python dq_engine.py --schema data_quality_program --output dq_report.json
  python dq_engine.py --env /path/to/.env
        """,
    )
    parser.add_argument("--tables", nargs="+", default=TARGET_TABLES,
                        help="Tables to evaluate (default: all defined tables)")
    parser.add_argument("--schema", default="data_quality_program",
                        help="PostgreSQL schema (default: data_quality_program)")
    parser.add_argument("--limit",  type=int, default=100,
                        help="Max rows per table (default: 100 | 0 = full table)")
    parser.add_argument("--output", default="dq_report.json",
                        help="Output JSON path (default: dq_report.json)")
    parser.add_argument("--env",    default=".env",
                        help="Path to .env file (default: .env)")
    args = parser.parse_args()

    env_path = Path(args.env)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        log.info("Loaded .env from: %s", env_path.resolve())
    else:
        log.warning(".env not found at '%s' — using shell environment.", env_path)

    log.info("DQ Check — Completeness")
    log.info("  Tables : %s", ", ".join(args.tables))
    log.info("  Schema : %s", args.schema)
    log.info("  Limit  : %s", f"{args.limit:,} rows" if args.limit > 0 else "full table")
    log.info("  Output : %s", args.output)

    conn_str = build_connection_string()
    engine   = get_engine(conn_str)

    report = evaluate(
        engine,
        tables      = args.tables,
        db_schema   = args.schema,
        limit       = args.limit,
        output_path = args.output,
    )

    s = report.get("executive_summary", {})
    log.info("Report written → %s", args.output)
    log.info("    Overall Completeness : %.2f%%", s.get("overall_completeness_score", 0.0))
    log.info("    Tables evaluated     : %d / %d",
             s.get("evaluated_tables", 0), s.get("total_tables", 0))

    if report.get("warnings"):
        log.warning("Tables with issues:")
        for tbl, msg in report["warnings"].items():
            log.warning("  %-40s  %s", tbl, msg)


if __name__ == "__main__":
    main()
