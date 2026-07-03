# # Timeliness engine — date validity (no-future / logical-order / freshness), pure SQL.
# #
# # Has a working evaluate_from_sql, but dq.rules.timeliness.TIM_TABLE_RULES is
# # currently empty (dimension disabled — see that module's docstring), so this
# # evaluates zero rules for every table until restored. Not called by
# # jobs/monthly_detection.py today.
# from __future__ import annotations

# import argparse
# import json
# import logging
# from pathlib import Path

# from dotenv import load_dotenv

# from dq.rules.timeliness import TIM_RULE_META, TIM_TABLE_RULES, FRESHNESS_WINDOW_DAYS
# from dq.sql.builders import run_rule_dimension_sql
# from storage.postgres.connection import build_connection_string, get_engine
# from storage.postgres.institutions import get_valid_le_books

# logging.basicConfig(
#     format="%(asctime)s  %(levelname)-8s  %(message)s",
#     datefmt="%H:%M:%S",
#     level=logging.INFO,
# )
# log = logging.getLogger("dq.engines.timeliness")

# # Dead: `RULE_META = TIM_RULE_META` / `TABLE_RULES = TIM_TABLE_RULES` aliases
# # had zero external importers — confirmed via grep, same as validity/uniqueness. Dropped.

# # Single-date "must not be in the future" rules: field <= CURRENT_DATE.
# _NO_FUTURE = {
#     "TIM-001": "customer_open_date",
#     "TIM-003": "account_open_date",
#     "TIM-004": "date_creation",
#     "TIM-005": "business_date",
#     "TIM-006": "approval_date",
#     "TIM-007": "application_date",
# }

# # Logical "date A must be on/before date B" rules: (a, b, strict?).
# _ORDER = {
#     "TIM-010": ("date_creation", "date_last_modified", False),
#     "TIM-011": ("start_date",    "maturity_date",      True),   # strictly before
#     "TIM-012": ("schedule_date", "payment_date",       False),  # payment on/after schedule
#     "TIM-013": ("commence_date", "benefit_expiry_date", False),
#     "TIM-014": ("commence_date", "ins_expiry_date",     False),
# }


# # ============================================================================
# # Per-rule SQL — each rule is a (total_expr, valid_expr) pair of SUM(CASE...)
# # aggregates over the rule's date column(s). invalid = total - valid.
# # Dates are compared as ::date; rows are only counted when the field(s) exist.
# # ============================================================================
# def _tim_rule_sql(rule_id: str, existing: set) -> "tuple[str, str] | None":
#     """Return (total_expr, valid_expr) for a timeliness rule, or None when its
#     column(s) are unavailable (the rule is then skipped for that table)."""
#     def has(*cols: str) -> bool:
#         return all(c in existing for c in cols)

#     if rule_id in _NO_FUTURE:                       # field must not be in the future
#         c = _NO_FUTURE[rule_id]
#         if not has(c):
#             return None
#         return (
#             f'SUM(CASE WHEN "{c}" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "{c}" IS NOT NULL AND "{c}"::date <= CURRENT_DATE THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "TIM-002":                        # DOB in [1900-01-01, today]
#         if not has("date_of_birth"):
#             return None
#         return (
#             'SUM(CASE WHEN "date_of_birth" IS NOT NULL THEN 1 ELSE 0 END)',
#             'SUM(CASE WHEN "date_of_birth" IS NOT NULL '
#             'AND "date_of_birth"::date >= DATE \'1900-01-01\' '
#             'AND "date_of_birth"::date <= CURRENT_DATE THEN 1 ELSE 0 END)',
#         )

#     if rule_id in _ORDER:                            # date A on/before (or strictly before) B
#         a, b, strict = _ORDER[rule_id]
#         if not has(a, b):
#             return None
#         op = "<" if strict else "<="
#         both = f'"{a}" IS NOT NULL AND "{b}" IS NOT NULL'
#         return (
#             f'SUM(CASE WHEN {both} THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN {both} AND "{a}"::date {op} "{b}"::date THEN 1 ELSE 0 END)',
#         )

#     if rule_id == "TIM-020":                         # freshness: modified within N days
#         if not has("date_last_modified"):
#             return None
#         return (
#             'SUM(CASE WHEN "date_last_modified" IS NOT NULL THEN 1 ELSE 0 END)',
#             f'SUM(CASE WHEN "date_last_modified" IS NOT NULL '
#             f'AND "date_last_modified"::date >= CURRENT_DATE - INTERVAL \'{FRESHNESS_WINDOW_DAYS} days\' '
#             f'THEN 1 ELSE 0 END)',
#         )

#     return None


# def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
#                       window_days: int, watermarks: dict, output_path: str,
#                       row_limit: int = 0,
#                       tables: list[str] | None = None,
#                       extra_where: str = "") -> dict:
#     """Run timeliness checks in pure SQL via the shared rule-dimension runner."""
#     return run_rule_dimension_sql(
#         engine, schema, valid_le_books, window_days, watermarks, output_path,
#         row_limit=row_limit, tables=tables,
#         table_rules=TIM_TABLE_RULES, rule_meta=TIM_RULE_META,
#         rule_sql_fn=_tim_rule_sql,
#         score_key="timeliness_score", overall_key="overall_timeliness_score",
#         dim_label="timeliness", extra_where=extra_where,
#     )


# def main():
#     parser = argparse.ArgumentParser(description="BNR DQ — timeliness (date) checks.")
#     parser.add_argument("--schema", default="dqp")
#     parser.add_argument("--limit", type=int, default=0)
#     parser.add_argument("--le-book", nargs="+", default=None)
#     args = parser.parse_args()

#     load_dotenv(Path(__file__).resolve().parents[2] / ".env")
#     engine = get_engine(build_connection_string())
#     vlb = (frozenset(args.le_book) if args.le_book
#            else get_valid_le_books(engine, args.schema))
#     rep = evaluate_from_sql(engine, args.schema, vlb, 0, {},
#                             str(Path(__file__).resolve().parents[2] / "dq_timeliness_report.json"),
#                             row_limit=args.limit)
#     print(json.dumps({"overall": rep.get("overall_timeliness_score"),
#                       "tables": list(rep.get("tables", {}).keys())}, indent=2))


# if __name__ == "__main__":
#     main()
