
from __future__ import annotations

import json
import logging

from sqlalchemy import text

log = logging.getLogger("dq.profiling.engine")

# Columns with more distinct values than this threshold are considered too
# high-cardinality for top-value aggregation (e.g. account_no, contract_id).
_TOP_VALUES_DIST_LIMIT = 1_0000
_TOP_N = 10   # values to keep per institution per column

# Max columns per stats query chunk — keeps Greenplum VMEM usage manageable
# for wide tables (e.g. customers_expanded has 150+ columns).
_STATS_CHUNK = 25


def profile_table(
    conn,
    schema: str,
    table: str,
    valid_le_books: frozenset,
    run_date: str,
) -> list[dict]:
    """Return one profile dict per (le_book, column) for the given table.

    Returns [] if the table doesn't exist or has no le_book column.
    """
    from dq.sql.metadata import all_columns, column_types

    existing = sorted(all_columns(conn, schema, table))
    col_type_map = column_types(conn, schema, table)
    if not existing or "le_book" not in existing:
        log.warning("profile_table: %s.%s has no le_book — skipping", schema, table)
        return []

    data_cols = [c for c in existing if c != "le_book"]
    if not data_cols:
        return []

    sq      = f'"{schema}"."{table}"'
    lb_list = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
    lb_where = f"le_book IN ({lb_list})"

    # ── 1. Chunked stats queries — _STATS_CHUNK columns per scan ─────────────
    # Wide tables (e.g. customers_expanded, 150+ cols) blow Greenplum VMEM when
    # all aggregations are packed into a single SELECT.  We run one query per
    # chunk and merge results.  Row count comes from the first chunk only.
    chunks = [data_cols[i:i + _STATS_CHUNK]
              for i in range(0, len(data_cols), _STATS_CHUNK)]

    # raw_by_lb[lb] = merged mapping dict across all chunks
    raw_by_lb: dict[str, dict] = {}

    for chunk_idx, chunk in enumerate(chunks):
        parts = ["le_book::TEXT AS le_book"]
        if chunk_idx == 0:
            parts.append("COUNT(*) AS _row_count")
        for col in chunk:
            parts += [
                f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}__null"',
                f'COUNT(DISTINCT "{col}") AS "{col}__dist"',
                f'MIN("{col}"::TEXT) AS "{col}__min"',
                f'MAX("{col}"::TEXT) AS "{col}__max"',
            ]

        stats_sql = (
            f'SELECT {", ".join(parts)} '
            f'FROM {sq} WHERE {lb_where} GROUP BY le_book'
        )
        try:
            rows = conn.execute(text(stats_sql)).fetchall()
        except Exception as exc:
            log.error("Stats chunk %d/%d failed for %s.%s: %s",
                      chunk_idx + 1, len(chunks), schema, table, exc)
            return []

        for row in rows:
            m  = dict(row._mapping)
            lb = str(m["le_book"])
            if lb not in raw_by_lb:
                raw_by_lb[lb] = {}
            raw_by_lb[lb].update(m)

    # Build per-lb stats lookup
    stats_by_lb: dict[str, tuple[int, dict[str, dict]]] = {}
    for lb, m in raw_by_lb.items():
        row_count = int(m.get("_row_count") or 0)
        col_stats: dict[str, dict] = {}
        for col in data_cols:
            null_c = int(m.get(f"{col}__null") or 0)
            dist_c = int(m.get(f"{col}__dist") or 0)
            col_stats[col] = {
                "null_count":     null_c,
                "null_pct":       round(null_c / row_count * 100, 4) if row_count else 0.0,
                "distinct_count": dist_c,
                "distinct_pct":   round(dist_c / row_count * 100, 4) if row_count else 0.0,
                "min_val": m.get(f"{col}__min"),
                "max_val": m.get(f"{col}__max"),
            }
        stats_by_lb[lb] = (row_count, col_stats)

    if not stats_by_lb:
        return []

    # ── 2. Top-values queries — one CTE per low-cardinality col ─────────────────
    top_vals: dict[str, dict[str, list]] = {}   # {col: {lb: [[val, cnt], ...]}}

    n_lbs = len(stats_by_lb)
    for col in data_cols:
        avg_dist = (
            sum(cs[1].get(col, {}).get("distinct_count", 0)
                for cs in stats_by_lb.values()) / n_lbs
        )
        if avg_dist > _TOP_VALUES_DIST_LIMIT:
            log.debug("  skip top-values for %s.%s — high cardinality (avg %.0f)",
                      table, col, avg_dist)
            continue

        top_sql = f"""
            WITH grouped AS (
                SELECT le_book::TEXT AS le_book,
                       "{col}"::TEXT AS val,
                       COUNT(*)      AS cnt
                FROM   {sq}
                WHERE  {lb_where} AND "{col}" IS NOT NULL
                GROUP  BY le_book, "{col}"::TEXT
            ),
            ranked AS (
                SELECT le_book, val, cnt,
                       ROW_NUMBER() OVER (PARTITION BY le_book
                                          ORDER BY cnt DESC) AS rn
                FROM   grouped
            )
            SELECT le_book, val, cnt
            FROM   ranked
            WHERE  rn <= {_TOP_N}
            ORDER  BY le_book, cnt DESC
        """
        try:
            rows = conn.execute(text(top_sql)).fetchall()
        except Exception as exc:
            log.warning("Top-values query failed %s.%s: %s", table, col, exc)
            continue

        col_top: dict[str, list] = {}
        for row in rows:
            m  = dict(row._mapping)
            lb = str(m["le_book"])
            col_top.setdefault(lb, []).append([m["val"], int(m["cnt"])])
        top_vals[col] = col_top

    # ── 3. Assemble final records ─────────────────────────────────────────────
    results: list[dict] = []
    for lb, (row_count, col_stats) in stats_by_lb.items():
        for col, stats in col_stats.items():
            pairs   = top_vals.get(col, {}).get(lb, [])
            tv_json = json.dumps(pairs) if pairs else None
            results.append({
                "le_book":        lb,
                "table_name":     table,
                "column_name":    col,
                "run_date":       run_date,
                "row_count":      row_count,
                "null_count":     stats["null_count"],
                "null_pct":       stats["null_pct"],
                "distinct_count": stats["distinct_count"],
                "distinct_pct":   stats["distinct_pct"],
                "min_val":        stats["min_val"],
                "max_val":        stats["max_val"],
                "top_values":     tv_json,
                "data_type":      col_type_map.get(col, "Text"),
            })

    return results
