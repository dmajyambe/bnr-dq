from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_relationship")

# ── BNR category types used to scope all checks ───────────────────────────────
CATEGORY_TYPES = ("MF", "SACCO", "OSACCO", "B")

from dq_rules import REL_RULE_META as RULE_META  # noqa: E402

# rules grouped by child table (built once at import time)
_TABLE_RULES: dict[str, list[str]] = {}
for _rid, _m in RULE_META.items():
    _TABLE_RULES.setdefault(_m["child_table"], []).append(_rid)


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


def get_valid_le_books(conn, schema: str) -> frozenset:
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
        rows = conn.execute(sql).fetchall()
        result = frozenset(str(r[0]).strip() for r in rows if r[0] is not None)
        log.info("Category filter %s → %d valid le_books", CATEGORY_TYPES, len(result))
        return result
    except Exception as exc:
        log.warning("Could not fetch valid le_books: %s — no filter applied.", exc)
        return frozenset()


# ── RI rule execution ─────────────────────────────────────────────────────────

def run_rule(
    rule_id: str,
    meta: dict,
    conn,
    schema: str,
    valid_le_books: frozenset,
    sample: int,
) -> dict | None:
    """
    Execute one RI rule entirely in SQL.

    Returns a dict with aggregated totals and a per-le_book breakdown, or None
    on error.

    Design notes
    ─────────────
    • Both mandatory and optional FK rules exclude NULL child-column values
      from the denominator.  For mandatory FKs a NULL is a completeness
      violation tracked by completeness_check.py.  For optional FKs a NULL is
      intentional.  In both cases, checking a NULL against the parent table
      produces no meaningful signal, so we skip it.

    • The child table is filtered to valid le_books; the parent table is NOT
      filtered.  A parent row may legitimately have a different (or no)
      le_book, and filtering the parent would produce false orphans.

    • The GROUP BY le_book in the child CTE gives us institution-level orphan
      counts in a single query per rule.
    """
    child_t  = meta["child_table"]
    child_c  = meta["child_col"]
    parent_t = meta["parent_table"]
    parent_c = meta["parent_col"]

    lb_filter    = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_filter = f'AND c.le_book IN ({codes})'

    limit_clause = f"LIMIT {sample}" if sample > 0 else ""

    sql = text(f"""
        WITH child AS (
            SELECT c.le_book,
                   c."{child_c}"
            FROM   "{schema}"."{child_t}" c
            WHERE  c."{child_c}" IS NOT NULL
                   {lb_filter}
            {limit_clause}
        )
        SELECT
            c.le_book,
            COUNT(*)                                                        AS total_rows,
            COUNT(DISTINCT c."{child_c}")                                   AS distinct_child_keys,
            SUM(CASE WHEN p."{parent_c}" IS NULL THEN 1 ELSE 0 END)        AS orphan_rows,
            COUNT(DISTINCT
                CASE WHEN p."{parent_c}" IS NULL THEN c."{child_c}" END
            )                                                               AS orphan_keys
        FROM  child c
        LEFT  JOIN "{schema}"."{parent_t}" p
            ON c."{child_c}" = p."{parent_c}"
        GROUP BY c.le_book
    """)

    try:
        rows = conn.execute(sql).fetchall()
    except Exception as exc:
        log.warning("  %s query failed: %s", rule_id, exc)
        return None

    if not rows:
        return None

    # aggregate totals across all le_books
    total       = sum(int(r[1]) for r in rows)
    orphan_rows = sum(int(r[3]) for r in rows)
    orphan_keys = sum(int(r[4]) for r in rows)
    valid       = total - orphan_rows
    score       = round(valid / total * 100, 2) if total else 100.0

    # per-le_book breakdown
    lb_breakdown: dict = {}
    for r in rows:
        lb_code    = str(r[0]).strip() if r[0] else "unknown"
        lb_total   = int(r[1])
        lb_orphans = int(r[3])
        lb_valid   = lb_total - lb_orphans
        lb_breakdown[lb_code] = {
            "row_count":  lb_total,
            "valid":      lb_valid,
            "invalid":    lb_orphans,
            "total":      lb_total,
            "ri_score":   round(lb_valid / lb_total * 100, 2) if lb_total else 100.0,
        }

    return {
        "valid":             valid,
        "invalid":           orphan_rows,
        "total":             total,
        "orphan_keys":       orphan_keys,
        "ri_score":          score,
        "le_book_breakdown": lb_breakdown,
    }


# ── orchestration ─────────────────────────────────────────────────────────────

def evaluate_all(engine, schema: str, sample: int) -> dict:
    """Run all RI rules; group results by child table; return report dict."""
    report: dict = {
        "generated_at": datetime.utcnow().isoformat(),
        "schema":        schema,
        "tables":        {},
        "le_books":      [],
        "warnings":      {},
    }

    all_scores:   list[float] = []
    all_le_books: set         = set()

    with engine.connect() as conn:
        valid_le_books = get_valid_le_books(conn, schema)

        for table_name, rule_ids in sorted(_TABLE_RULES.items()):
            log.info("━━  Table: %s  (%d rule(s))", table_name, len(rule_ids))

            rules_out:       dict                   = {}
            rule_scores:     list[float]             = []
            # accumulate per-le_book rule scores to compute table-level averages
            lb_rule_scores:  dict[str, list[float]]  = {}

            for rule_id in rule_ids:
                meta   = RULE_META[rule_id]
                result = run_rule(rule_id, meta, conn, schema, valid_le_books, sample)

                if result is None:
                    report["warnings"][rule_id] = f"Rule {rule_id} could not be evaluated."
                    continue

                score = result["ri_score"]
                rule_scores.append(score)
                all_le_books.update(result["le_book_breakdown"].keys())

                for lb_code, lb_data in result["le_book_breakdown"].items():
                    lb_rule_scores.setdefault(lb_code, []).append(lb_data["ri_score"])

                rules_out[rule_id] = {
                    "rule_name":         meta["name"],
                    "category":          meta["category"],
                    "child_table":       meta["child_table"],
                    "child_col":         meta["child_col"],
                    "parent_table":      meta["parent_table"],
                    "parent_col":        meta["parent_col"],
                    "nullable":          meta["nullable"],
                    "valid":             result["valid"],
                    "invalid":           result["invalid"],
                    "total":             result["total"],
                    "orphan_keys":       result["orphan_keys"],
                    "ri_score":          score,
                    "le_book_breakdown": result["le_book_breakdown"],
                }
                log.info(
                    "  %s  score=%.2f%%  orphan_rows=%d  orphan_keys=%d  (of %d checked)",
                    rule_id, score,
                    result["invalid"], result["orphan_keys"], result["total"],
                )

            if not rule_scores:
                continue

            # table-level le_book breakdown: average ri_score across rules per institution
            table_lb_out: dict = {}
            for lb_code, lb_scores in lb_rule_scores.items():
                # row_count: take the max across rules (avoids double-counting
                # when two rules check different nullable columns on the same table)
                lb_row_count = max(
                    rules_out[rid]["le_book_breakdown"].get(lb_code, {}).get("row_count", 0)
                    for rid in rules_out
                )
                table_lb_out[lb_code] = {
                    "row_count": lb_row_count,
                    "ri_score":  round(sum(lb_scores) / len(lb_scores), 2),
                }

            table_score = round(sum(rule_scores) / len(rule_scores), 2)
            all_scores.append(table_score)

            # row_count at table level: max total across rules (see above reasoning)
            table_row_count = max(r["total"] for r in rules_out.values())

            report["tables"][table_name] = {
                "status":            "evaluated",
                "row_count":         table_row_count,
                "rules_applied":     len(rules_out),
                "ri_score":          table_score,
                "rules":             rules_out,
                "le_book_breakdown": table_lb_out,
            }
            log.info("  Table RI score: %.2f%%  (%d rule(s))", table_score, len(rules_out))

    overall = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    evaluated = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    failed_rules = sum(
        sum(1 for r in tbl["rules"].values() if r.get("ri_score", 100) < 100)
        for tbl in evaluated
    )

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_ri_score":  overall,
        "total_tables":      len(report["tables"]),
        "evaluated_tables":  len(evaluated),
        "failed_rules":      failed_rules,
        "sample":            sample,
    }

    return report


def _run_rule_pandas(rule_id: str, meta: dict, dataframes: dict,
                     valid_le_books: frozenset,
                     parent_dataframes: dict | None = None) -> dict | None:
    """
    Pandas equivalent of run_rule(): check child FK values exist in the parent set.

    - NULL child FK values are excluded from the denominator (not violations).
    - Only the child table is le_book-filtered.
    - Parent keys come from parent_dataframes (full table, no date filter) when
      supplied, so parents created before the 7-day window are not treated as
      missing references.
    """
    import pandas as pd

    child_t  = meta["child_table"]
    child_c  = meta["child_col"]
    parent_t = meta["parent_table"]
    parent_c = meta["parent_col"]

    child_df  = dataframes.get(child_t, pd.DataFrame())
    parent_df = (parent_dataframes or dataframes).get(parent_t, pd.DataFrame())

    if child_df.empty or child_c not in child_df.columns:
        return None
    if parent_df.empty or parent_c not in parent_df.columns:
        return None

    if valid_le_books and "le_book" in child_df.columns:
        child_df = child_df[child_df["le_book"].isin(valid_le_books)]

    child_df = child_df[child_df[child_c].notna()].copy()
    if child_df.empty:
        return None

    parent_keys        = frozenset(parent_df[parent_c].dropna().astype(str))
    child_df["_fk"]    = child_df[child_c].astype(str)
    child_df["_valid"] = child_df["_fk"].isin(parent_keys)

    total       = len(child_df)
    orphan_rows = int((~child_df["_valid"]).sum())
    orphan_keys = int(child_df.loc[~child_df["_valid"], "_fk"].nunique())
    valid       = total - orphan_rows
    score       = round(valid / total * 100, 2) if total else 100.0

    lb_breakdown: dict = {}
    if "le_book" in child_df.columns:
        for le_val in sorted(child_df["le_book"].dropna().unique()):
            lb_df     = child_df[child_df["le_book"] == le_val]
            lb_total  = len(lb_df)
            lb_orphan = int((~lb_df["_valid"]).sum())
            lb_valid  = lb_total - lb_orphan
            lb_breakdown[str(le_val)] = {
                "row_count": lb_total,
                "valid":     lb_valid,
                "invalid":   lb_orphan,
                "total":     lb_total,
                "ri_score":  round(lb_valid / lb_total * 100, 2) if lb_total else 100.0,
            }

    return {
        "valid":             valid,
        "invalid":           orphan_rows,
        "total":             total,
        "orphan_keys":       orphan_keys,
        "ri_score":          score,
        "le_book_breakdown": lb_breakdown,
    }


def evaluate_all_from_dataframes(dataframes: dict, valid_le_books: frozenset,
                                  parent_dataframes: dict | None = None) -> dict:
    """Run all RI rules on pre-loaded DataFrames; return report dict (no file write)."""
    report: dict = {
        "generated_at": datetime.utcnow().isoformat(),
        "tables":        {},
        "le_books":      [],
        "warnings":      {},
    }

    all_scores:   list[float] = []
    all_le_books: set         = set()

    for table_name, rule_ids in sorted(_TABLE_RULES.items()):
        log.info("━━  Table: %s  (%d rule(s))", table_name, len(rule_ids))

        rules_out:      dict                   = {}
        rule_scores:    list[float]             = []
        lb_rule_scores: dict[str, list[float]]  = {}

        for rule_id in rule_ids:
            meta   = RULE_META[rule_id]
            result = _run_rule_pandas(rule_id, meta, dataframes, valid_le_books, parent_dataframes)

            if result is None:
                report["warnings"][rule_id] = f"Rule {rule_id} could not be evaluated."
                continue

            score = result["ri_score"]
            rule_scores.append(score)
            all_le_books.update(result["le_book_breakdown"].keys())

            for lb_code, lb_data in result["le_book_breakdown"].items():
                lb_rule_scores.setdefault(lb_code, []).append(lb_data["ri_score"])

            rules_out[rule_id] = {
                "rule_name":         meta["name"],
                "category":          meta["category"],
                "child_table":       meta["child_table"],
                "child_col":         meta["child_col"],
                "parent_table":      meta["parent_table"],
                "parent_col":        meta["parent_col"],
                "nullable":          meta["nullable"],
                "valid":             result["valid"],
                "invalid":           result["invalid"],
                "total":             result["total"],
                "orphan_keys":       result["orphan_keys"],
                "ri_score":          score,
                "le_book_breakdown": result["le_book_breakdown"],
            }
            log.info(
                "  %s  score=%.2f%%  orphan_rows=%d  orphan_keys=%d  (of %d checked)",
                rule_id, score,
                result["invalid"], result["orphan_keys"], result["total"],
            )

        if not rule_scores:
            continue

        table_lb_out: dict = {}
        for lb_code, lb_scores in lb_rule_scores.items():
            lb_row_count = max(
                rules_out[rid]["le_book_breakdown"].get(lb_code, {}).get("row_count", 0)
                for rid in rules_out
            )
            table_lb_out[lb_code] = {
                "row_count": lb_row_count,
                "ri_score":  round(sum(lb_scores) / len(lb_scores), 2),
            }

        table_score     = round(sum(rule_scores) / len(rule_scores), 2)
        table_row_count = max(r["total"] for r in rules_out.values())
        all_scores.append(table_score)

        report["tables"][table_name] = {
            "status":            "evaluated",
            "row_count":         table_row_count,
            "rules_applied":     len(rules_out),
            "ri_score":          table_score,
            "rules":             rules_out,
            "le_book_breakdown": table_lb_out,
        }
        log.info("  Table RI score: %.2f%%  (%d rule(s))", table_score, len(rules_out))

    overall      = round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0
    evaluated    = [v for v in report["tables"].values() if v.get("status") == "evaluated"]
    failed_rules = sum(
        sum(1 for r in tbl["rules"].values() if r.get("ri_score", 100) < 100)
        for tbl in evaluated
    )

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"] = {
        "overall_ri_score":  overall,
        "total_tables":      len(report["tables"]),
        "evaluated_tables":  len(evaluated),
        "failed_rules":      failed_rules,
    }

    return report


# ── CLI entry-point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DQ Relationship Engine — Referential Integrity checks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python relationship_check.py
  python relationship_check.py --sample 50000   # fast estimate on 50 k rows per child
  python relationship_check.py --sample 0       # full tables (may be slow)
  python relationship_check.py --schema data_quality_program --output dq_relationship_report.json
        """,
    )
    parser.add_argument("--schema", default="data_quality_program")
    parser.add_argument("--sample", type=int, default=10000,
                        help="Row sample per child table (0 = full scan, default 10 000)")
    parser.add_argument("--output", default="dq_relationship_report.json")
    parser.add_argument("--env",    default=".env")
    args = parser.parse_args()

    env_path = Path(args.env)
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path, override=True)
        log.info("Loaded .env from: %s", env_path.resolve())
    else:
        log.warning(".env not found at '%s' — using shell environment.", env_path)

    log.info("DQ Relationship Engine")
    log.info("  Schema  : %s", args.schema)
    log.info("  Sample  : %s", f"{args.sample:,} rows/table" if args.sample else "full scan")
    log.info("  Output  : %s", args.output)

    conn_str = build_connection_string()
    engine   = get_engine(conn_str)

    report = evaluate_all(engine, args.schema, args.sample)

    output_path = Path(args.output)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    s = report.get("executive_summary", {})
    log.info("Report written → %s", output_path)
    log.info("    Overall RI score   : %.2f%%", s.get("overall_ri_score", 0.0))
    log.info("    Tables evaluated   : %d / %d",
             s.get("evaluated_tables", 0), s.get("total_tables", 0))
    log.info("    Failed rules       : %d", s.get("failed_rules", 0))

    if report.get("warnings"):
        log.warning("Rules with issues:")
        for rid, msg in report["warnings"].items():
            log.warning("  %-12s  %s", rid, msg)


if __name__ == "__main__":
    main()
