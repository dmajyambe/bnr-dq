# Validity rule registry — WHAT format/range/cross-field rules apply per table.
from __future__ import annotations

MIN_PHONE_DIGITS  = 7
MIN_NATIONAL_ID   = 5
INTEREST_RATE_MAX = 100
MIN_AGE_AT_OPEN   = 18

VAL_RULE_META: dict[str, dict] = {
    "VAL-001": {
        "name":     "Email address must match a valid email format",
        "category": "Format Validity",
        "fields":   ["email_id"],
    },
    "VAL-002": {
        "name":     f"Phone number must contain at least {MIN_PHONE_DIGITS} digits",
        "category": "Format Validity",
        "fields":   ["work_telephone", "home_telephone"],
    },
    "VAL-003": {
        "name":     "Currency code must be a 3-letter uppercase ISO 4217 code",
        "category": "Format Validity",
        "fields":   ["currency", "mis_currency"],
    },
    "VAL-004": {
        "name":     f"National ID number must be at least {MIN_NATIONAL_ID} characters when ID type is set",
        "category": "Format Validity",
        "fields":   ["national_id_number", "national_id_type"],
    },
    "VAL-010": {
        "name":     f"Debit interest rate must be between 0 and {INTEREST_RATE_MAX}%",
        "category": "Range Validity",
        "fields":   ["interest_rate_dr"],
    },
    "VAL-011": {
        "name":     f"Credit interest rate must be between 0 and {INTEREST_RATE_MAX}%",
        "category": "Range Validity",
        "fields":   ["interest_rate_cr"],
    },
    "VAL-012": {
        "name":     "Disbursement amounts must be non-negative",
        "category": "Range Validity",
        "fields":   ["current_disbursed_amt", "previous_disbursed_amt"],
    },
    "VAL-013": {
        "name":     "EMI / scheduled payment amount must be greater than zero",
        "category": "Range Validity",
        "fields":   ["emi_amount"],
    },
    "VAL-014": {
        "name":     "Outstanding and due amounts must be non-negative",
        "category": "Range Validity",
        "fields":   ["outstanding_amount_lcy", "outstanding_amount",
                     "principal_amount_due", "int_amount_due", "due_amount",
                     "principal_amount_lcy"],
    },
    "VAL-015": {
        "name":     "Applied loan amount must be greater than zero",
        "category": "Range Validity",
        "fields":   ["applied_amount_lcy"],
    },
    "VAL-016": {
        "name":     "Number of instalments must be at least 1",
        "category": "Range Validity",
        "fields":   ["num_of_instalments"],
    },
    "VAL-020": {
        "name":     "Instalments paid must not exceed total number of instalments",
        "category": "Cross-field Validity",
        "fields":   ["num_instalments_paid", "num_of_instalments"],
    },
    "VAL-021": {
        "name":     "Approved loan amount must not exceed applied amount",
        "category": "Cross-field Validity",
        "fields":   ["approved_amount_lcy", "applied_amount_lcy"],
    },
    "VAL-022": {
        "name":     f"Customer must be at least {MIN_AGE_AT_OPEN} years old at account open date",
        "category": "Cross-field Validity",
        "fields":   ["date_of_birth", "customer_open_date"],
    },
    "VAL-030": {
        "name":     "RWF-denominated disbursement amount must be at least 1000",
        "category": "Range Validity",
        "fields":   ["currency", "current_disbursed_amt"],
    },
    "VAL-031": {
        "name":     "Foreign-currency disbursement amount must be at least 1",
        "category": "Range Validity",
        "fields":   ["currency", "current_disbursed_amt"],
    },
    "VAL-032": {
        "name":     "Disbursed loan amount must be at least 1000",
        "category": "Range Validity",
        "fields":   ["disbursed_amount"],
    },
    "VAL-033": {
        "name":     "Distributed (principal) contract amount must be at least 1000",
        "category": "Range Validity",
        "fields":   ["principal_amount_lcy"],
    },
    "VAL-034": {
        "name":     "Application date must be on or before business date",
        "category": "Cross-field Validity",
        "fields":   ["application_date", "business_date"],
    },
}


VAL_TABLE_RULES: dict[str, list[str]] = {
    "customers_expanded":  ["VAL-001", "VAL-002", "VAL-004", "VAL-022"],
    "accounts":            ["VAL-003", "VAL-010", "VAL-011"],
    "contracts_disburse":  ["VAL-003", "VAL-012", "VAL-030", "VAL-031"],
    "contract_loans":      ["VAL-010", "VAL-013", "VAL-014", "VAL-016", "VAL-020", "VAL-032"],
    "contract_schedules":  ["VAL-013", "VAL-014"],
    "contracts_expanded":  ["VAL-003", "VAL-010", "VAL-011", "VAL-014", "VAL-033"],
    "loan_applications_2": ["VAL-003", "VAL-015", "VAL-021", "VAL-034"],
}
