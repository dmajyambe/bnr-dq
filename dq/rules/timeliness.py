# Timeliness rule registry — currently DISABLED (empty), not dead code.
#
# All definitions below are commented out from the pre-refactor version, not
# deleted, so the dimension can be restored without re-deriving the rules.
# dq.engines.timeliness still has a working evaluate_from_sql; with these
# registries empty it simply evaluates zero rules for every table, which is
# why `timeliness` shows up as a phantom 0.0 in dq_history.json today (see
# jobs/monthly_detection.py — it isn't even in the R dict passed to scoring).
from __future__ import annotations

FRESHNESS_WINDOW_DAYS = 90

TIM_RULE_META: dict[str, dict] = {}
TIM_TABLE_RULES: dict[str, list[str]] = {}

# TIM_RULE_META full definitions (restore when timeliness is re-enabled):
# TIM_RULE_META: dict[str, dict] = {
#     "TIM-001": {
#         "name":     "Customer open date must not be in the future",
#         "category": "No Future Dates",
#         "fields":   ["customer_open_date"],
#     },
#     "TIM-002": {
#         "name":     "Date of birth must be between 1900-01-01 and today",
#         "category": "No Future Dates",
#         "fields":   ["date_of_birth"],
#     },
#     "TIM-003": {
#         "name":     "Account open date must not be in the future",
#         "category": "No Future Dates",
#         "fields":   ["account_open_date"],
#     },
#     "TIM-004": {
#         "name":     "Record creation date must not be in the future",
#         "category": "No Future Dates",
#         "fields":   ["date_creation"],
#     },
#     "TIM-005": {
#         "name":     "Business date must not be in the future",
#         "category": "No Future Dates",
#         "fields":   ["business_date"],
#     },
#     "TIM-006": {
#         "name":     "Loan approval date must not be in the future",
#         "category": "No Future Dates",
#         "fields":   ["approval_date"],
#     },
#     "TIM-007": {
#         "name":     "Loan application date must not be in the future",
#         "category": "No Future Dates",
#         "fields":   ["application_date"],
#     },
#     "TIM-010": {
#         "name":     "Record creation date must be on or before last modification date",
#         "category": "Logical Date Order",
#         "fields":   ["date_creation", "date_last_modified"],
#     },
#     "TIM-011": {
#         "name":     "Contract start date must be strictly before maturity date",
#         "category": "Logical Date Order",
#         "fields":   ["start_date", "maturity_date"],
#     },
#     "TIM-012": {
#         "name":     "Payment date must be on or after schedule date when payment is recorded",
#         "category": "Logical Date Order",
#         "fields":   ["schedule_date", "payment_date"],
#     },
#     "TIM-013": {
#         "name":     "Insurance commence date must be on or before benefit expiry date",
#         "category": "Logical Date Order",
#         "fields":   ["commence_date", "benefit_expiry_date"],
#     },
#     "TIM-014": {
#         "name":     "Insurance commence date must be on or before insurance expiry date",
#         "category": "Logical Date Order",
#         "fields":   ["commence_date", "ins_expiry_date"],
#     },
#     "TIM-020": {
#         "name":     f"Record must have been modified within the past {FRESHNESS_WINDOW_DAYS} days",
#         "category": "Data Freshness",
#         "fields":   ["date_last_modified"],
#     },
# }

# TIM_TABLE_RULES: dict[str, list[str]] = {
#     "customers_expanded":     ["TIM-001", "TIM-002", "TIM-004", "TIM-010", "TIM-020"],
#     "accounts":               ["TIM-003", "TIM-004", "TIM-010", "TIM-020"],
#     "contracts_disburse":     ["TIM-004", "TIM-005", "TIM-010", "TIM-020"],
#     "contract_loans":         ["TIM-004", "TIM-006", "TIM-010", "TIM-020"],
#     "contract_schedules":     ["TIM-004", "TIM-010", "TIM-012", "TIM-020"],
#     "contracts_expanded":     ["TIM-004", "TIM-010", "TIM-011", "TIM-013", "TIM-014", "TIM-020"],
#     "loan_applications_2":    ["TIM-005", "TIM-007"],
#     "prev_loan_applications": ["TIM-004", "TIM-005", "TIM-010", "TIM-020"],
# }

# Dead (in addition to being disabled): never read anywhere outside its own
# import line, same as ACCURACY_COLUMNS/VALIDITY_COLUMNS.
# TIMELINESS_COLUMNS: dict[str, list[str]] = {
#     "customers_expanded":     ["le_book", "customer_open_date", "date_of_birth",
#                                "date_creation", "date_last_modified"],
#     "accounts":               ["le_book", "account_open_date",
#                                "date_creation", "date_last_modified"],
#     "contracts_disburse":     ["le_book", "business_date",
#                                "date_creation", "date_last_modified"],
#     "contract_loans":         ["le_book", "approval_date",
#                                "date_creation", "date_last_modified"],
#     "contract_schedules":     ["le_book", "schedule_date", "payment_date",
#                                "date_creation", "date_last_modified"],
#     "contracts_expanded":     ["le_book", "start_date", "maturity_date",
#                                "commence_date", "benefit_expiry_date", "ins_expiry_date",
#                                "date_creation", "date_last_modified"],
#     "loan_applications_2":    ["le_book", "business_date", "application_date"],
#     "prev_loan_applications": ["le_book", "business_date",
#                                "date_creation", "date_last_modified"],
# }
