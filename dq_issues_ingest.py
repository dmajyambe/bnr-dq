# """
# dq_issues_ingest.py — Ingest pre-detected issues from the issues.* schema.

# NOT YET ACTIVE. Placeholders are marked with # PLACEHOLDER.
# Wire into dq_pipeline_2m.py --load stage once the other pipeline
# confirms run_date and dimension columns are in place.

# Flow:
#   1. For each table, query issues.{table} for new runs since the watermark
#      and within the 30-day window.
#   2. Query dqp.{table} for total rows per le_book → compute real score.
#   3. Derive rule_id from dimension + issue_type.
#   4. Feed into dq_issue_tracker.ingest_issues().
#   5. Advance watermark to latest run_date processed.

# Relationships remain handled entirely by the existing pipeline (detect_and_update_issues).
# """
# from __future__ import annotations
# import logging
# from datetime import date, timedelta
# import pandas as pd
# from sqlalchemy import text
# log = logging.getLogger("dq_issues_ingest")

# #Schema names
# ISSUES_SCHEMA = "issues"   # schema the other pipeline writes to
# DQP_SCHEMA    = "dqp"      # source schema (for total row counts)

# WINDOW_DAYS = 30
# # ── Column names ───────────────────────────────────────────────────────────────
# # PLACEHOLDER: confirm these match the other pipeline's output columns exactly
# COL_LE_BOOK    = "le_book"
# COL_INST_NAME  = "stakeholder_name"
# COL_ISSUE_TYPE = "issue_type"
# COL_DIMENSION  = "dimension"    # PLACEHOLDER: must be added by the other pipeline
# COL_RUN_DATE   = "run_date"     # PLACEHOLDER: must be added by the other pipeline


# # ── rule_id derivation ─────────────────────────────────────────────────────────

# # PLACEHOLDER: populate once all issue_type values from the other pipeline are known.
# # Keys are exact issue_type strings; values are the rule_ids to use in the tracker.
# _ISSUE_TYPE_TO_RULE_ID: dict[str, str] = {
#     # "Corp_Special characters in CUSTOMER_NAME": "VAL-SPEC-CUSTOMER_NAME",
#     # "Corp_Duplicated CUSTOMER_TIN":             "VAL-DUP-CUSTOMER_TIN",
#     # "Corp_Invalid HOME_TELEPHONE":              "VAL-FMT-HOME_TELEPHONE",
#     # "Indiv_Missing NATIONAL_ID_NUMBER":         "COMP-NATIONAL_ID_NUMBER",
#     # add remaining mappings here …
# }

# _DIM_PREFIX = {
#     "validity":     "VAL",
#     "completeness": "COMP",
#     "timeliness":   "TIM",
#     "accuracy":     "ACC",
# }


# def _derive_rule_id(dimension: str, issue_type: str) -> str:
#     if issue_type in _ISSUE_TYPE_TO_RULE_ID:
#         return _ISSUE_TYPE_TO_RULE_ID[issue_type]
#     # Fallback: slugify issue_type after stripping Corp_/Indiv_ prefix
#     # PLACEHOLDER: review fallback output against all issue_type values before going live
#     prefix = _DIM_PREFIX.get((dimension or "").lower(), "UNK")
#     slug = (
#         issue_type
#         .split("_", 1)[-1]       # strip Corp_ / Indiv_ prefix
#         .upper()
#         .replace(" ", "_")
#         .replace("-", "_")
#     )
#     return f"{prefix}-{slug}"


# # ── DB queries ─────────────────────────────────────────────────────────────────

# def _fetch_failing(conn, table: str, watermark: str | None) -> pd.DataFrame:
#     """
#     Read issues.{table} for runs within the 30-day window and after the watermark.
#     Returns columns: le_book, institution_name, dimension, issue_type, run_date, failing_rows.
#     """
#     cutoff = (date.today() - timedelta(days=WINDOW_DAYS)).isoformat()
#     hwm    = watermark or cutoff

#     # PLACEHOLDER: verify column names in f-string match COL_* constants above
#     sql = text(f"""
#         SELECT
#             {COL_LE_BOOK}   AS le_book,
#             {COL_INST_NAME} AS institution_name,
#             {COL_DIMENSION} AS dimension,
#             {COL_ISSUE_TYPE} AS issue_type,
#             {COL_RUN_DATE}  AS run_date,
#             COUNT(*)        AS failing_rows
#         FROM "{ISSUES_SCHEMA}"."{table}"
#         WHERE {COL_RUN_DATE} > :hwm
#           AND {COL_RUN_DATE} >= :cutoff
#         GROUP BY
#             {COL_LE_BOOK}, {COL_INST_NAME},
#             {COL_DIMENSION}, {COL_ISSUE_TYPE}, {COL_RUN_DATE}
#     """)
#     return pd.read_sql(sql, conn, params={"hwm": hwm, "cutoff": cutoff})


# def _fetch_totals(conn, table: str) -> dict[str, int]:
#     """Return {le_book: total_row_count} from dqp.{table} for score computation."""
#     # PLACEHOLDER: confirm dqp.{table} window/filter matches what the other pipeline used
#     sql  = text(f'SELECT le_book, COUNT(*) AS total FROM "{DQP_SCHEMA}"."{table}" GROUP BY le_book')
#     rows = conn.execute(sql).fetchall()
#     return {r[0]: r[1] for r in rows}


# # main entry point

# def ingest_from_issues_schema(
#     engine,
#     tables: list[str],
#     watermarks: dict,
#     run_date: str,
# ) -> dict:
#     """
#     Read issues from issues.* schema and feed into the issue tracker.

#     NOT YET ACTIVE — placeholders must be resolved before calling this.

#     Called from dq_pipeline_2m.py --load stage after detect_and_update_issues().
#     Returns updated watermarks dict (caller is responsible for saving it).
#     """
#     from dq_issue_tracker import ingest_issues   # imported here to avoid circular import

#     updated_watermarks = dict(watermarks)

#     with engine.connect() as conn:
#         for table in tables:
#             # Watermark keyed separately from dqp tables to avoid collision
#             hwm_key = f"issues.{table}"
#             hwm     = watermarks.get(hwm_key)

#             failing = _fetch_failing(conn, table, hwm)

#             if failing.empty:
#                 log.info("ingest_issues: no new runs in issues.%s since %s", table, hwm)
#                 continue

#             totals = _fetch_totals(conn, table)
#             issues = []

#             for _, row in failing.iterrows():
#                 total = totals.get(row["le_book"], 0)
#                 score = (
#                     round((total - int(row["failing_rows"])) / total * 100, 2)
#                     if total > 0 else 0.0
#                 )
#                 issues.append({
#                     "le_book":          row["le_book"],
#                     "institution_name": row["institution_name"],
#                     "table":            table,
#                     "rule_id":          _derive_rule_id(row["dimension"], row["issue_type"]),
#                     "dimension":        row["dimension"],
#                     "failing_rows":     int(row["failing_rows"]),
#                     "score":            score,
#                 })

#             ingest_issues(issues, run_date)

#             # Advance watermark to latest run_date processed for this table
#             latest = str(failing["run_date"].max())
#             updated_watermarks[hwm_key] = latest
#             log.info("ingest_issues: %s — %d issue group(s), watermark → %s",
#                      table, len(issues), latest)

#     return updated_watermarks
