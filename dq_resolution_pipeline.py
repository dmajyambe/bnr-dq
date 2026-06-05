from __future__ import annotations
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text


SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq_resolution")

#rule engine dispatchers — for dimension rules, we need to know which module to call for run_rule().
def _build_dispatch() -> dict:
    """Return {rule_id: module} for all dimension rules that have run_rule()."""
    dispatch: dict = {}
    try:
        import validity_check
        from validity_check import RULE_META as VAL_META
        for rid in VAL_META:
            dispatch[rid] = validity_check
    except Exception as exc:
        log.warning("validity_check unavailable for dispatch: %s", exc)

    try:
        import accuracy_check
        from accuracy_check import RULE_META as ACC_META
        for rid in ACC_META:
            dispatch[rid] = accuracy_check
    except Exception as exc:
        log.warning("accuracy_check unavailable for dispatch: %s", exc)

    try:
        import timeliness_check
        from timeliness_check import RULE_META as TIM_META
        for rid in TIM_META:
            dispatch[rid] = timeliness_check
    except Exception as exc:
        log.warning("timeliness_check unavailable for dispatch: %s", exc)

    log.info("Resolution dispatch: %d rule(s) registered", len(dispatch))
    return dispatch


def _rule_fields(rule_id: str, dispatch: dict) -> list[str]:
    mod = dispatch.get(rule_id)
    if not mod:
        return []
    return list((mod.RULE_META.get(rule_id) or {}).get("fields", []))

# db helpers
def _db_columns(conn, schema: str, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": schema, "t": table}).fetchall()
    return {r[0].lower() for r in rows}


def _load_inst_df(conn, schema: str, table: str,
                  le_book: str, cols: list[str]) -> "pd.DataFrame":
    """Pull all rows for one institution """
    import pandas as pd
    quoted = ", ".join(f'"{c}"' for c in sorted(cols))
    df = pd.read_sql(
        text(f'SELECT {quoted} FROM "{schema}"."{table}" WHERE le_book = :lb'),
        conn,
        params={"lb": le_book},
    )
    df.columns = [c.lower() for c in df.columns]
    return df


# ── REL and COMP re-checkers ───────────────────────────────────────────────────

# def _recheck_rel(conn, schema: str, rule_id: str, le_book: str) -> int:
#     """Full LEFT JOIN for one REL rule + institution. Returns orphan count; -1 on error."""
#     from dq_rules import REL_RULE_META
#     m = REL_RULE_META.get(rule_id, {})
#     ct, cc, pt, pc = (m.get(k, "") for k in
#                       ("child_table", "child_col", "parent_table", "parent_col"))
#     if not all([ct, cc, pt, pc]):
#         return -1
#     row = conn.execute(text(f"""
#         SELECT SUM(CASE WHEN p."{pc}" IS NULL THEN 1 ELSE 0 END) AS orphans
#         FROM   "{schema}"."{ct}" c
#         LEFT JOIN "{schema}"."{pt}" p ON c."{cc}" = p."{pc}"
#         WHERE  c."{cc}" IS NOT NULL
#           AND  c.le_book = :lb
#     """), {"lb": le_book}).fetchone()
#     return int(row[0] or 0) if row else -1


def _recheck_comp(conn, schema: str, table: str, le_book: str) -> int:
    """Count total null cells across mandatory columns for one institution."""
    from dq_rules import MANDATORY_COLUMNS
    cols = MANDATORY_COLUMNS.get(table, [])
    if not cols:
        return 0
    existing = _db_columns(conn, schema, table)
    cols = [c for c in cols if c in existing]
    if not cols:
        return 0
    null_expr = " + ".join(
        f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END)' for c in cols
    )
    row = conn.execute(text(f"""
        SELECT {null_expr} AS total_nulls
        FROM "{schema}"."{table}"
        WHERE le_book = :lb
    """), {"lb": le_book}).fetchone()
    return int(row[0] or 0) if row else 0


#outcome recorder 

def _record(iss: dict, invalid: int, run_id: str, stats: dict) -> None:
    from dq_issue_tracker import resolve_issue, update_issue_count
    lb, tbl, rid = iss["le_book"], iss["table_name"], iss["rule_id"]
    if invalid == 0:
        resolve_issue(iss["issue_id"], run_id)
        log.info("    ✓ RESOLVED  %-6s  %s / %s", lb, tbl, rid)
        stats["resolved"] += 1
    else:
        update_issue_count(iss["issue_id"], invalid)
        band = iss.get("urgency_band", "open")
        if band == "overdue":
            log.info("    ⚠ OVERDUE   %-6s  %s / %s  %d rows", lb, tbl, rid, invalid)
            stats["overdue"] += 1
        else:
            log.info("    — OPEN      %-6s  %s / %s  %d rows", lb, tbl, rid, invalid)
            stats["unchanged"] += 1


#main scanner
def run_resolution_scan(engine, schema: str, run_id: str) -> dict:
    """
    Re-scan every open issue against the full DB table.
    Returns {resolved, unchanged, overdue, errors}.
    """
    from dq_issue_tracker import get_issues, _refresh_urgency_bands, _conn

    # Fetch all open issues (includes urgency_band='overdue' ones)
    open_issues = get_issues(status="open")

    if not open_issues:
        log.info("No open issues — nothing to scan.")
        return {"resolved": 0, "unchanged": 0, "overdue": 0, "errors": 0}

    log.info("Resolution scan: %d open issue(s)", len(open_issues))

    dispatch = _build_dispatch()

    # Group by (table, le_book) → one DB query per pair
    groups: dict[tuple, list] = {}
    for iss in open_issues:
        groups.setdefault((iss["table_name"], iss["le_book"]), []).append(iss)

    stats = {"resolved": 0, "unchanged": 0, "overdue": 0, "errors": 0}

    for (table, le_book), issues in sorted(groups.items()):
        log.info("  ━━  %-30s  le_book=%s  (%d rule(s))",
                 table, le_book, len(issues))

        rel_issues  = [i for i in issues if i["rule_id"].startswith("REL")]
        comp_issues = [i for i in issues if i["rule_id"].startswith("COMP")]
        dim_issues  = [i for i in issues
                       if not i["rule_id"].startswith(("REL", "COMP"))]

        with engine.connect() as conn:
            conn.execute(text("SET work_mem = '256MB'"))

            # ── dimension rules (ACC / TIM / VAL) ────────────────────────────
            if dim_issues:
                needed: set[str] = {"le_book"}
                for iss in dim_issues:
                    needed.update(_rule_fields(iss["rule_id"], dispatch))

                existing = _db_columns(conn, schema, table)
                cols     = sorted(needed & existing)

                if not cols:
                    log.warning("    no matching columns for %s — skipping", table)
                    stats["errors"] += len(dim_issues)
                else:
                    try:
                        df = _load_inst_df(conn, schema, table, le_book, cols)
                        for iss in dim_issues:
                            mod = dispatch.get(iss["rule_id"])
                            if not mod:
                                log.warning("    no engine for %s", iss["rule_id"])
                                stats["errors"] += 1
                                continue
                            try:
                                result = mod.run_rule(iss["rule_id"], df)
                                if result is None:
                                    stats["errors"] += 1
                                else:
                                    _, invalid, _ = result
                                    _record(iss, invalid, run_id, stats)
                            except Exception as exc:
                                log.warning("    rule %s error: %s", iss["rule_id"], exc)
                                stats["errors"] += 1
                    except Exception as exc:
                        log.warning("    load error %s/%s: %s", table, le_book, exc)
                        stats["errors"] += len(dim_issues)

            # ── REL rules ─────────────────────────────────────────────────────
            for iss in rel_issues:
                try:
                    orphans = _recheck_rel(conn, schema, iss["rule_id"], le_book)
                    if orphans < 0:
                        stats["errors"] += 1
                    else:
                        _record(iss, orphans, run_id, stats)
                except Exception as exc:
                    log.warning("    REL %s error: %s", iss["rule_id"], exc)
                    stats["errors"] += 1

            # ── COMP rules ────────────────────────────────────────────────────
            for iss in comp_issues:
                try:
                    null_cells = _recheck_comp(conn, schema, table, le_book)
                    _record(iss, null_cells, run_id, stats)
                except Exception as exc:
                    log.warning("    COMP %s error: %s", iss["rule_id"], exc)
                    stats["errors"] += 1

    # Refresh urgency bands — this is what transitions open → overdue
    # for issues whose sla_deadline has now passed.
    con = _conn()
    try:
        _refresh_urgency_bands(con)
        con.commit()
    finally:
        con.close()

    try:
        _precompute_resolved_reports()
    except Exception as exc:
        log.warning("Resolved report precompute failed: %s", exc)

    return stats


def _precompute_resolved_reports() -> None:
    """
    For every institution with resolved issues, regenerate the resolved ZIP
    from the stored detection ZIP: filter to resolved rows, add resolved_at /
    sla_deadline / on_time columns, save to issue_reports/{lb}_{YYYY-MM}_resolved.zip
    """
    import zipfile
    import io as _io
    import pandas as pd
    from dq_issue_tracker import get_issues
    from dq_rules import COMP_RULE_META

    issue_reports_dir = SCRIPT_DIR / "issue_reports"
    if not issue_reports_dir.exists():
        log.info("_precompute_resolved_reports: issue_reports/ dir not found, skipping.")
        return

    resolved_all = get_issues(status="resolved")
    if not resolved_all:
        log.info("_precompute_resolved_reports: no resolved issues.")
        return

    comp_rule_ids = set(COMP_RULE_META.keys())

    # Group by (le_book, detected_month)
    by_lb_month: dict[tuple, list] = {}
    for iss in resolved_all:
        month = (iss.get("detected_at") or "")[:7]
        if month:
            by_lb_month.setdefault((iss["le_book"], month), []).append(iss)

    zips_written = 0
    for (lb, month), issues in sorted(by_lb_month.items()):
        src_zip = issue_reports_dir / f"{lb}_{month}.zip"
        if not src_zip.exists():
            continue

        out_buf = _io.BytesIO()
        found_any = False

        # Group issues by table
        by_table: dict[str, list] = {}
        for iss in issues:
            by_table.setdefault(iss["table_name"], []).append(iss)

        try:
            with zipfile.ZipFile(src_zip) as src_zf:
                with zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as out_zf:
                    for table, tbl_issues in by_table.items():
                        if f"{table}.csv" not in src_zf.namelist():
                            continue
                        try:
                            df = pd.read_csv(
                                _io.BytesIO(src_zf.read(f"{table}.csv")),
                                encoding="utf-8-sig", low_memory=False,
                            )
                        except Exception as exc:
                            log.warning("_precompute: read error %s/%s: %s", lb, table, exc)
                            continue

                        if df.empty or "issue_type" not in df.columns:
                            continue

                        has_comp  = any(i["rule_id"] in comp_rule_ids for i in tbl_issues)
                        dim_types = {i["rule_name"] for i in tbl_issues
                                     if i["rule_id"] not in comp_rule_ids and i.get("rule_name")}

                        mask = pd.Series(False, index=df.index)
                        if dim_types:
                            mask |= df["issue_type"].isin(dim_types)
                        if has_comp:
                            mask |= df["issue_type"].str.startswith("Missing ", na=False)

                        filtered = df[mask].copy()
                        if filtered.empty:
                            continue

                        meta: dict[str, dict] = {}
                        for iss in tbl_issues:
                            if iss["rule_id"] in comp_rule_ids:
                                meta["__comp__"] = iss
                            elif iss.get("rule_name"):
                                meta[iss["rule_name"]] = iss

                        def _resolved_at(itype: str) -> str:
                            key = "__comp__" if str(itype).startswith("Missing ") else itype
                            return (meta.get(key) or {}).get("resolved_at", "")

                        def _sla(itype: str) -> str:
                            key = "__comp__" if str(itype).startswith("Missing ") else itype
                            return (meta.get(key) or {}).get("sla_deadline", "")

                        filtered["resolved_at"]  = filtered["issue_type"].map(_resolved_at)
                        filtered["sla_deadline"] = filtered["issue_type"].map(_sla)
                        filtered["on_time"] = filtered.apply(
                            lambda r: (
                                "On Time" if r["resolved_at"] and r["sla_deadline"]
                                and r["resolved_at"] <= r["sla_deadline"] else
                                "Late"    if r["resolved_at"] and r["sla_deadline"] else ""
                            ), axis=1,
                        )

                        fname = f"{table}.csv"
                        out_zf.writestr(fname, filtered.to_csv(index=False, encoding="utf-8-sig"))
                        found_any = True
        except Exception as exc:
            log.warning("_precompute: zip error for %s/%s: %s", lb, month, exc)
            continue

        if found_any:
            out_path = issue_reports_dir / f"{lb}_{month}_resolved.zip"
            out_buf.seek(0)
            out_path.write_bytes(out_buf.read())
            zips_written += 1
            log.info("_precompute_resolved_reports: wrote %s", out_path.name)

    log.info("_precompute_resolved_reports: %d ZIP(s) written.", zips_written)


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    load_dotenv(SCRIPT_DIR / ".env")

    parser = argparse.ArgumentParser(
        description="BNR DQ Daily Resolution Scanner — re-checks open issues against live data."
    )
    parser.add_argument("--schema", default=os.environ.get("DQ_SCHEMA", "dqp"),
                        help="PostgreSQL schema (default: dqp)")
    args = parser.parse_args()

    from db_utils import get_engine, build_connection_string
    engine = get_engine(build_connection_string())

    run_id = datetime.now().strftime("RES-%Y%m%d-%H%M%S")

    log.info("=" * 60)
    log.info("Daily resolution scanner started  run_id=%s", run_id)
    log.info("=" * 60)

    stats = run_resolution_scan(engine, args.schema, run_id)

    log.info("=" * 60)
    log.info("Done: %d resolved  %d open  %d overdue  %d errors",
             stats["resolved"], stats["unchanged"],
             stats["overdue"], stats["errors"])
    log.info("=" * 60)

    sys.exit(0 if stats["errors"] == 0 else 1)
