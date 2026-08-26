# Uniqueness rule registry — WHAT key-tuples must not repeat within a window.
from __future__ import annotations

UNI_RULE_META: dict[str, dict] = {
    "UNI-001": {
        "name":     "No duplicate loan records within the period",
        "category": "Duplicate Detection",
        "fields":   ["contract_sequence_number", "date_of_provision", "disbursed_amount",
                     "prin_outstanding_amt_fcy", "prin_outstanding_amt_lcy"],
    },
    "UNI-002": {
        "name":     "No duplicate disbursement records within the period",
        "category": "Duplicate Detection",
        "fields":   ["contract_id", "currency", "current_disbursed_amt",
                     "previous_disbursed_amt", "first_payment_date"],
    },
    "UNI-003": {
        "name":     "No duplicate contract records within the period",
        "category": "Duplicate Detection",
        "fields":   ["contract_sequence_number", "start_date", "maturity_date",
                     "principal_amount_lcy", "vision_sbu", "contract_status"],
    },
    "UNI-004": {
        "name":     "Account Number must not repeat within the period",
        "category": "Duplicate Detection",
        "fields":   ["account_no"],
    },
    "UNI-005": {
        "name":     "Contract ID must not repeat within the period",
        "category": "Duplicate Detection",
        "fields":   ["contract_id"],
    },
    "UNI-006": {
        "name":     "Customer ID must not repeat within the period",
        "category": "Duplicate Detection",
        "fields":   ["customer_id"],
    },
    "UNI-007": {
        "name":     "Loan Application ID must not repeat within the period",
        "category": "Duplicate Detection",
        "fields":   ["loan_application_id"],
    },
}

UNI_TABLE_RULES: dict[str, list[str]] = {
    "contract_loans":      ["UNI-001"],
    "contracts_disburse":  ["UNI-002"],
    "contracts_expanded":  ["UNI-003", "UNI-005"],
    "accounts":            ["UNI-004"],
    "customers_expanded":  ["UNI-006"],
    "loan_applications_2": ["UNI-007"],
}

