#accuracy related rules and metadata
from __future__ import annotations

VALID_ACCOUNT_STATUS    = frozenset({0, 1, 2, 3, 4, 5, 9})
VALID_PERFORMANCE_CLASS = frozenset({"NL", "WL", "SL", "DL", "LL", "WO"})
VALID_GENDER            = frozenset({"M", "F", "C"})
VALID_ACCOUNT_TYPE      = frozenset({
    "CAA", "SBA", "TDA", "SED", "LAA", "OAB", "IP", "TRUSTAC",
    "MPSDC", "MPSDB", "VCOPSDC", "VCOPSDB", "VPPSDC", "VPPSDB",
})
CORPORATE_LEGAL_STATUS  = frozenset({3, 4, 5, 6, 7})


ACC_RULE_META: dict[str, dict] = {

    "ACC-002": {
        "name":     "Account Status must be within allowed numeric codes",
        "category": "Code Domain Validity",
        "fields":   ["account_status"],
    },
    "ACC-003": {
        "name":     "Performance Class must match loan classification codes",
        "category": "Code Domain Validity",
        "fields":   ["performance_class"],
    },
    "ACC-004": {
        "name":     "Customer Gender must be M, F, or C only",
        "category": "Code Domain Validity",
        "fields":   ["customer_gender"],
    },
    "ACC-005": {
        "name":     "Account Type must be a valid  code",
        "category": "Code Domain Validity",
        "fields":   ["account_type"],
    },
    "ACC-010": {
        "name":     "Gender must be C when Legal Status indicates a corporate entity",
        "category": "Cross-field Consistency",
        "fields":   ["customer_gender", "legal_status"],
    },
    "ACC-012": {
        "name":     "Marital Status must be NA for corporate customers",
        "category": "Cross-field Consistency",
        "fields":   ["marital_status", "customer_gender"],
    },

    
    "REL-001": {
        "name":         "Every account must reference a known customer",
        "category":     "Referential Integrity",
        "fields":       ["customer_id"],
        "child_table":  "accounts",
        "child_col":    "customer_id",
        "parent_table": "customers_expanded",
        "parent_col":   "customer_id",
        "nullable":     False,
    },
    "REL-002": {
        "name":         "Every contract must reference a known customer",
        "category":     "Referential Integrity",
        "fields":       ["customer_id"],
        "child_table":  "contracts_expanded",
        "child_col":    "customer_id",
        "parent_table": "customers_expanded",
        "parent_col":   "customer_id",
        "nullable":     False,
    },
    "REL-003": {
        "name":         "Every loan application must reference a known customer",
        "category":     "Referential Integrity",
        "fields":       ["customer_id"],
        "child_table":  "loan_applications_2",
        "child_col":    "customer_id",
        "parent_table": "customers_expanded",
        "parent_col":   "customer_id",
        "nullable":     False,
    },
    "REL-004": {
        "name":         "Every contract-loan detail must reference a known contract",
        "category":     "Referential Integrity",
        "fields":       ["contract_sequence_number"],
        "child_table":  "contract_loans",
        "child_col":    "contract_sequence_number",
        "parent_table": "contracts_expanded",
        "parent_col":   "contract_sequence_number",
        "nullable":     False,
    },
    "REL-005": {
        "name":         "Every payment schedule must reference a known contract",
        "category":     "Referential Integrity",
        "fields":       ["contract_sequence_number"],
        "child_table":  "contract_schedules",
        "child_col":    "contract_sequence_number",
        "parent_table": "contracts_expanded",
        "parent_col":   "contract_sequence_number",
        "nullable":     False,
    },
    "REL-006": {
        "name":         "Every disbursement record must reference a known contract",
        "category":     "Referential Integrity",
        "fields":       ["contract_id"],
        "child_table":  "contracts_disburse",
        "child_col":    "contract_id",
        "parent_table": "contracts_expanded",
        "parent_col":   "contract_id",
        "nullable":     False,
    },
    "REL-007": {
        "name":         "Every previous-application record must reference a known current application",
        "category":     "Referential Integrity",
        "fields":       ["loan_application_id"],
        "child_table":  "prev_loan_applications",
        "child_col":    "loan_application_id",
        "parent_table": "loan_applications_2",
        "parent_col":   "loan_application_id",
        "nullable":     False,
    },
    "REL-008": {
        "name":         "Contract linked application ID, when present, must reference a known loan application",
        "category":     "Optional Reference",
        "fields":       ["loan_application_id"],
        "child_table":  "contracts_expanded",
        "child_col":    "loan_application_id",
        "parent_table": "loan_applications_2",
        "parent_col":   "loan_application_id",
        "nullable":     True,
    },
}



ACC_TABLE_RULES: dict[str, list[str]] = {
    "customers_expanded":     ["ACC-004", "ACC-010", "ACC-012"],
    "accounts":               ["ACC-002", "ACC-005", "REL-001"],
    "contracts_expanded":     ["ACC-003", "REL-002", "REL-008"],
    "contract_loans":         ["ACC-003", "REL-004"],
    "loan_applications_2":    ["ACC-004", "REL-003"],
    "contract_schedules":     ["REL-005"],
    "contracts_disburse":     ["REL-006"],
    "prev_loan_applications": ["REL-007"],
}

