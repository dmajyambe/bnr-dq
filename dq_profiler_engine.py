import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_profiler")

#tables
TABLES_TO_PROFILE = [
    "customers_expanded",
    "accounts",
    "contracts_disburse",
    "contract_loans",
    "contract_schedules",
    "contracts_expanded",
    "loan_applications_2",
    "prev_loan_applications",
]

# data-type families used for UI badges
_ORDERABLE_FAMILIES = {"numeric", "date", "timestamp"}
_TYPE_FAMILY_MAP = {
    "integer":           "numeric",
    "bigint":            "numeric",
    "smallint":          "numeric",
    "numeric":           "numeric",
    "decimal":           "numeric",
    "real":              "numeric",
    "double precision":  "numeric",
    "float":             "numeric",
    "float4":            "numeric",
    "float8":            "numeric",
    "int2":              "numeric",
    "int4":              "numeric",
    "int8":              "numeric",
    "date":              "date",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone":    "timestamp",
    "timestamptz":       "timestamp",
    "boolean":           "boolean",
    "bool":              "boolean",
    "character varying": "text",
    "varchar":           "text",
    "char":              "text",
    "text":              "text",
    "uuid":              "text",
    "json":              "json",
    "jsonb":             "json",
}

SCRIPT_DIR = Path(__file__).parent


def _load_env() -> None:
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)


def _build_conn_string() -> str:
    required = ["MY_POSTGRES_USERNAME", "MY_POSTGRES_PASSWORD",
                "MY_POSTGRES_HOST", "MY_POSTGRES_PORT", "MY_POSTGRES_DB"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)
    u, pw, h, p, db = (os.environ[k] for k in required)
    return f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}"


def _get_engine(conn_str: str):
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(conn_str, pool_pre_ping=True,
                               connect_args={"connect_timeout": 10})
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        return engine
    except ImportError:
        log.error("sqlalchemy or psycopg2-binary not installed.")
        sys.exit(1)
    except Exception as exc:
        log.error("Cannot connect to database: %s", exc)
        sys.exit(1)


# only profile rows whose le_book belongs to these  category types
CATEGORY_TYPES = ('MF', 'SACCO', 'OSACCO', 'B')


def _get_valid_le_books(conn, schema: str) -> frozenset:
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
        rows = conn.execute(sql).fetchall()
        result = frozenset(str(r[0]).strip() for r in rows if r[0] is not None)
        log.info("Category filter %s → %d valid le_books", CATEGORY_TYPES, len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch valid le_books: %s — no filter applied.", exc)
        return frozenset()


def _type_family(pg_type: str) -> str:
    return _TYPE_FAMILY_MAP.get(pg_type.lower(), "other")


def _fetch_column_types(conn, schema: str, table: str) -> dict[str, str]:
    """Return {column_name: pg_data_type} from information_schema."""
    from sqlalchemy import text
    sql = text("""
        SELECT column_name, data_type
        FROM   information_schema.columns
        WHERE  table_schema = :schema
          AND  table_name   = :table
        ORDER  BY ordinal_position
    """)
    rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
    return {r[0]: r[1] for r in rows}


def _load_existing_null_counts(dq_report_path: Path) -> dict:
    """
    Return {table_name: {"row_count": int, "null_counts": {col: int}}}
    from the already-computed completeness report so we don't re-scan the DB.
    """
    if not dq_report_path.exists():
        return {}
    with open(dq_report_path) as f:
        raw = json.load(f)
    result = {}
    for tname, tdata in raw.get("tables", {}).items():
        if tdata.get("status") == "evaluated":
            result[tname] = {
                "row_count":   tdata.get("row_count", 0),
                "null_counts": tdata.get("null_counts", {}),
            }
    return result


def _profile_table(conn, schema: str, table: str,
                   col_types: dict[str, str],
                   precomputed: dict,
                   sample: int,
                   valid_le_books: frozenset = frozenset()) -> dict:
    """
    Build per-column profile using pre-computed null counts where available.
    Only touches the DB for data types (cheap) and per-column sampled stats
    (distinct / min / max / top-3 values) to stay within VMEM limits.
    """
    from sqlalchemy import text

    if not col_types:
        log.warning("  No columns found for %s — skipping.", table)
        return {}

    sq = f'"{schema}"."{table}"'

    # build le_book filter clause for this table
    where_sql = ""
    if valid_le_books and "le_book" in col_types:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        where_sql = f' WHERE "le_book" IN ({codes})'

    sq_lim = f'(SELECT * FROM {sq}{where_sql} LIMIT {sample})'

    row_count   = precomputed.get("row_count", 0)
    null_counts = precomputed.get("null_counts", {})

    # If not in existing report, fall back to a COUNT(*) query (cheap aggregate)
    if not row_count:
        try:
            row_count = conn.execute(
                text(f"SELECT COUNT(*) FROM {sq}{where_sql}")
            ).scalar() or 0
        except Exception as exc:
            log.warning("  COUNT(*) failed for %s: %s", table, exc)

    columns_out = {}
    for col, pg_type in col_types.items():
        qcol   = f'"{col}"'
        family = _type_family(pg_type)

        # ── null / non-null from pre-computed report ──────────────────────────
        null_c   = int(null_counts.get(col, 0))
        non_null = max(0, row_count - null_c)
        fill_rate = round((non_null / row_count * 100), 2) if row_count else 0.0

        # ── distinct count on sample ──────────────────────────────────────────
        distinct = None
        try:
            row = conn.execute(text(
                f"SELECT COUNT(DISTINCT {qcol}) FROM {sq_lim} AS _s"
            )).fetchone()
            distinct = int(row[0]) if row else None
        except Exception:
            pass

        # ── min / max on sample (orderable types only) ────────────────────────
        min_val = max_val = None
        if family in _ORDERABLE_FAMILIES and non_null > 0:
            try:
                mm = conn.execute(text(
                    f"SELECT MIN({qcol})::TEXT, MAX({qcol})::TEXT FROM {sq_lim} AS _s"
                )).fetchone()
                if mm:
                    min_val, max_val = mm[0], mm[1]
            except Exception:
                pass

        # ── top-3 frequent values on sample ───────────────────────────────────
        top_values = []
        want_top = (
            family in {"text", "boolean"}
            or (family == "numeric" and distinct is not None and 0 < distinct <= 30)
        )
        if want_top:
            try:
                tv_rows = conn.execute(text(
                    f"""SELECT {qcol}::TEXT AS val, COUNT(*) AS cnt
                        FROM   {sq_lim} AS _s
                        WHERE  {qcol} IS NOT NULL
                        GROUP  BY {qcol}
                        ORDER  BY cnt DESC
                        LIMIT  3"""
                )).fetchall()
                top_values = [{"value": str(r[0]), "count": int(r[1])}
                              for r in tv_rows]
            except Exception:
                pass

        columns_out[col] = {
            "data_type":      pg_type,
            "type_family":    family,
            "row_count":      row_count,
            "non_null_count": non_null,
            "null_count":     null_c,
            "fill_rate":      fill_rate,
            "distinct_count": distinct,
            "min_value":      min_val,
            "max_value":      max_val,
            "top_values":     top_values,
        }

    return columns_out


def _pandas_type_family(dtype) -> tuple[str, str]:
    """Map a pandas dtype to (pg_type_approximation, type_family)."""
    if dtype.kind == "M":
        return "timestamp without time zone", "timestamp"
    if dtype.kind in ("i", "u"):
        return "integer", "numeric"
    if dtype.kind == "f":
        return "double precision", "numeric"
    if dtype.kind == "b":
        return "boolean", "boolean"
    return "character varying", "text"


def profile_all_from_dataframes(dataframes: dict, schema: str, sample: int = 0) -> dict:
    """Profile tables from pre-loaded DataFrames — no DB connection needed."""
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "schema":       schema,
        "tables":       {},
    }

    existing = _load_existing_null_counts(SCRIPT_DIR / "dq_report.json")

    for table in TABLES_TO_PROFILE:
        df = dataframes.get(table)
        if df is None or df.empty:
            log.warning("  %s: no data in dataframe — skipped.", table)
            continue

        log.info("Profiling %s …", table)
        working = (df.sample(n=sample, random_state=42)
                   if sample and sample < len(df) else df)
        row_count       = len(working)
        pre_null_counts = existing.get(table, {}).get("null_counts", {})

        columns_out = {}
        for col in working.columns:
            if col == "data_processed":
                continue

            series   = working[col]
            pg_type, family = _pandas_type_family(series.dtype)
            null_c   = int(pre_null_counts.get(col, series.isna().sum()))
            non_null = max(0, row_count - null_c)
            fill_rate = round(non_null / row_count * 100, 2) if row_count else 0.0
            distinct  = int(series.nunique(dropna=True))

            min_val = max_val = None
            if family in _ORDERABLE_FAMILIES and non_null > 0:
                try:
                    clean = series.dropna()
                    min_val, max_val = str(clean.min()), str(clean.max())
                except Exception:
                    pass

            top_values = []
            want_top = (
                family in {"text", "boolean"}
                or (family == "numeric" and 0 < distinct <= 30)
            )
            if want_top:
                try:
                    vc = series.dropna().value_counts().head(3)
                    top_values = [{"value": str(v), "count": int(c)}
                                  for v, c in vc.items()]
                except Exception:
                    pass

            columns_out[col] = {
                "data_type":      pg_type,
                "type_family":    family,
                "row_count":      row_count,
                "non_null_count": non_null,
                "null_count":     null_c,
                "fill_rate":      fill_rate,
                "distinct_count": distinct,
                "min_value":      min_val,
                "max_value":      max_val,
                "top_values":     top_values,
            }

        if not columns_out:
            continue

        fill_rates  = [c["fill_rate"] for c in columns_out.values()]
        table_fill  = round(sum(fill_rates) / len(fill_rates), 2) if fill_rates else 0.0
        null_col_ct = sum(1 for c in columns_out.values() if c["null_count"] > 0)

        report["tables"][table] = {
            "row_count":       row_count,
            "column_count":    len(columns_out),
            "table_fill_rate": table_fill,
            "null_col_count":  null_col_ct,
            "columns":         columns_out,
        }
        log.info("  ✓ %d columns  |  fill rate %.1f%%", len(columns_out), table_fill)

    return report


def profile_all(engine, schema: str, sample: int,
                dq_report_path: Path) -> dict:
    """Profile every table and return the full report dict."""
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "schema":       schema,
        "tables":       {},
    }

    existing = _load_existing_null_counts(dq_report_path)
    log.info("Loaded pre-computed null counts for %d table(s) from %s",
             len(existing), dq_report_path.name)

    with engine.connect() as conn:
        valid_le_books = _get_valid_le_books(conn, schema)

        for table in TABLES_TO_PROFILE:
            log.info("Profiling %s …", table)
            col_types = _fetch_column_types(conn, schema, table)
            if not col_types:
                log.warning("  Table %s not found / empty schema — skipped.", table)
                continue

            precomputed = existing.get(table, {})
            columns     = _profile_table(conn, schema, table, col_types,
                                         precomputed, sample, valid_le_books)
            if not columns:
                continue

            row_count   = next(iter(columns.values()))["row_count"] if columns else 0
            fill_rates  = [c["fill_rate"] for c in columns.values()]
            table_fill  = round(sum(fill_rates) / len(fill_rates), 2) if fill_rates else 0.0
            null_col_ct = sum(1 for c in columns.values() if c["null_count"] > 0)

            report["tables"][table] = {
                "row_count":       row_count,
                "column_count":    len(columns),
                "table_fill_rate": table_fill,
                "null_col_count":  null_col_ct,
                "columns":         columns,
            }
            log.info("  ✓ %d columns  |  fill rate %.1f%%", len(columns), table_fill)

    return report


def main() -> None:
    _load_env()

    parser = argparse.ArgumentParser(description="Column-level data profiler")
    parser.add_argument("--schema", default="data_quality_program",
                        help="PostgreSQL schema (default: data_quality_program)")
    parser.add_argument("--sample", type=int, default=5000,
                        help="Row sample size for distinct/min/max queries (default: 5000)")
    parser.add_argument("--output", default=str(SCRIPT_DIR / "dq_profile_report.json"),
                        help="Output JSON path")
    args = parser.parse_args()

    dq_report_path = SCRIPT_DIR / "dq_report.json"

    log.info("Connecting to database …")
    engine = _get_engine(_build_conn_string())

    log.info("Starting profiling  (schema: %s, sample: %d rows) …",
             args.schema, args.sample)
    report = profile_all(engine, args.schema, args.sample, dq_report_path)

    output_path = Path(args.output)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    log.info("Profile report written → %s", output_path)
    log.info("Done.")


if __name__ == "__main__":
    main()
