"""
dq_rules.py — Central rule registry for all DQ dimensions.

Every rule across completeness, accuracy, timeliness, and validity
is defined here.  Referential-integrity (relationship) rules are classified under accuracy.
Engines import their slice; the dashboard reads the full table.

User-defined rules are stored in PostgreSQL (dqp.dq_user_rules).
Built-in rules live in-memory (no DB required to read them).
The local dq_rules.db (SQLite) is kept only for test isolation.
"""
from __future__ import annotations
import logging
import os
import sqlite3
from pathlib import Path

log = logging.getLogger("dq_rules")

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR / "dq_rules.db"   # SQLite — kept for test isolation only
PG_SCHEMA  = "dqp"                         # PostgreSQL schema for user rules


# ── PostgreSQL connection ───────────────────────────────────────────────────────

def _pg_conn():
    """Open a psycopg2 connection using .env credentials."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(SCRIPT_DIR / ".env", override=False)
    return psycopg2.connect(
        host     = os.environ["MY_POSTGRES_HOST"],
        port     = int(os.environ.get("MY_POSTGRES_PORT", 5432)),
        dbname   = os.environ["MY_POSTGRES_DB"],
        user     = os.environ["MY_POSTGRES_USERNAME"],
        password = os.environ["MY_POSTGRES_PASSWORD"],
    )

# ── SHARED ─────────────────────────────────────────────────────────────────────
CATEGORY_TYPES = ("MF", "SACCO", "OSACCO", "B")

# ── COMPLETENESS ───────────────────────────────────────────────────────────────

MANDATORY_COLUMNS: dict[str, list[str]] = {
    "customers_expanded": [
        "country", "le_book", "customer_id", "salutation", "customer_name",
        "surname", "forename_1", "forename_2", "customer_gender",
        "customer_acronym", "vision_ouc", "vision_sbu", "account_officer",
        "naics_code", "nationality", "residence", "economic_sub_sector_code_isic",
        "related_party", "customer_open_date", "customer_tin", "ssn_number",
        "national_id_type", "national_id_number", "health_insurance_number",
        "customer_status", "employer_name", "date_of_birth",
        "perm_address_1", "perm_address_2", "perm_village", "perm_country",
        "comm_address_1", "comm_address_2", "comm_village", "comm_country",
        "comm_residence_type", "place_of_birth", "marital_status", "spouse_name",
        "number_of_dependants", "emp_address_1", "emp_address_2",
        "emp_village", "emp_country", "email_id", "work_telephone",
        "home_telephone", "fax_number_1", "fax_number_2", "relationship_type",
        "internet_banking_subscription", "mobile_banking_subscription",
        "employee_id", "occupation", "income", "income_frequency",
        "group_name", "group_number", "legal_status", "local_govt_member",
        "date_last_modified", "education", "social_economic_class",
        "next_of_kin_name", "next_of_kin_id_type", "next_of_kin_telephone",
        "next_of_kin_email_id", "account_mandate_name", "account_mandate_id_type",
        "account_mandate_id_number", "related_party_name",
    ],
    "accounts": [
        "country", "le_book", "account_no", "customer_id", "currency",
        "account_name", "vision_gl", "vision_ouc", "vision_sbu",
        "account_officer", "account_type", "account_indicator", "gl_enrich_id",
        "account_open_date", "freeze_status", "economic_sub_sector_code",
        "economic_sub_sector_code_isic", "public_sector_code",
        "institutional_sector_code", "credit_nature", "credit_category",
        "interest_rate_dr", "interest_rate_cr", "account_status",
        "account_status_date", "record_indicator", "maker", "verifier",
        "internal_status", "date_last_modified", "date_creation",
    ],
    "contracts_disburse": [
        "country", "le_book", "business_date", "contract_id", "currency",
        "current_disbursed_amt", "previous_disbursed_amt", "first_payment_date",
        "contract_status_nt", "contract_status", "record_indicator_nt",
        "record_indicator", "maker", "verifier", "internal_status",
        "date_last_modified", "date_creation",
    ],
    "contract_loans": [
        "contract_sequence_number", "country", "le_book", "year_month",
        "prev_loan_paid", "loan_in_other_institution", "loan_purpose",
        "renigo_flag", "approval_date", "declaration_date", "available_amount",
        "emi_amount", "outstanding_amount_fcy", "outstanding_amount_lcy",
        "general_provision_lcy", "specific_provision_lcy",
        "loan_includ_interest", "overdraft_includ_interest",
        "other_cr_includ_interest", "collateral_accepted", "suspense_interest",
        "provision_held", "overdft_outstand_amt", "overdft_provisions",
        "oth_credit_outstand_amt", "oth_credit_provision", "coll_accept_bnr",
        "cash_back", "security_issued", "collateral_received",
        "guarante_iss_rw_bank", "guarante_iss_fg_bank",
        "securities_regularly_trend", "interest_rate_dr", "physical_guarantee",
        "guarantee_amount", "compulsory_saving", "loan_reschedule_date",
        "loan_reschedule_type", "repayment_frequency", "performance_class",
        "grace_period_accorded", "sche_first_pay_date", "latest_payment_date",
        "instalments_in_arrears", "num_of_instalments", "num_instalments_paid",
        "total_amt_repaid_prin_lcy", "non_perf_loan_bal_lcy",
        "shareholder_paid_amt_lcy", "defaulter_action_taken",
        "total_instalments_paid", "total_instalments_outstanding",
        "written_off_date", "written_off_loans_lcy", "govt_schemes",
        "govt_scheme_financed", "contract_loan_status", "record_indicator",
        "maker", "verifier", "internal_status", "date_last_modified",
        "date_creation", "disbursed_amount", "prin_outstanding_amt_fcy",
        "prin_outstanding_amt_lcy", "interest_due_fcy", "interest_due_lcy",
        "regulatory_provision", "date_of_provision", "other_cr_penalties",
        "other_charges", "date_past_due", "due_amount",
    ],
    "contract_schedules": [
        "contract_sequence_number", "country", "le_book", "schedule_date",
        "payment_date", "emi_amount", "due_amount", "principal_amount_due",
        "int_amount_due", "add_int_amount_due", "principal_amount_paid",
        "int_amount_paid", "add_int_amount_paid", "outstanding_amount",
        "maker", "verifier", "internal_status", "date_last_modified",
        "date_creation", "principal_amount_due_fcy", "int_amount_due_fcy",
        "principal_amount_paid_fcy", "int_amount_paid_fcy",
        "outstanding_amount_fcy",
    ],
    "contracts_expanded": [
        "contract_sequence_number", "country", "le_book", "customer_id",
        "contract_id", "record_type", "source_id", "account_officer",
        "issue_id", "deal_type", "deal_sub_type", "start_date", "maturity_date",
        "settlement_date", "days_remaining_mat", "days_start_mat",
        "contract_status", "principal_gl", "principal_amount_lcy",
        "principal_amount_fcy", "mis_currency", "interest_rate_dr",
        "interest_rate_cr", "interest_rate_method", "cost_of_funds_rate",
        "gl_enrich_id", "status", "number_of_units", "strike_price",
        "vega", "gamma", "delta", "option_type", "put_call_ind",
        "hedge_trading_ind", "portfolio_type", "volatility_index_basket",
        "undly_vol_index_basket", "underlying", "trade_ref_no",
        "trade_product_status", "option_description", "trader_id", "purpose",
        "underlying_maturity_date", "underlying_start_date", "yield",
        "hedge_p_and_l", "agent", "commissson_ccy",
        "cash_collateral_percentage", "commission_percentage",
        "current_account_no", "extension_date", "last_negotiation_date",
        "beneficiary", "res_mortage_collateral_type", "res_mortage_market_value",
        "res_dis_value", "com_mortage_collateral_type",
        "com_mortage_market_value", "com_dis_value", "guarantor_credit_line",
        "guarantor_customer_id", "borrower_category_code", "purpose_of_advance",
        "type_of_advance", "nature_of_advance", "guarantee_cover_code",
        "performance_class", "sub_performance_class", "lcy_equiv_inception",
        "exchange_rate", "option_exercise_date", "futures_market_price",
        "futures_cost_price", "fut_var_margin", "contract_attribute_1",
        "contract_attribute_2", "contract_attribute_3",
        "latest_repayment_schedule", "latest_repayent_frequency",
        "no_of_tenors", "applicable_date", "date_past_due", "dpd_amount",
        "contract_dpd_status", "record_indicator", "maker", "verifier",
        "internal_status", "date_last_modified", "date_creation",
        "govt_schemes_flag", "apr_rate", "contract_application_fee",
        "contract_administrative_fee", "other_contract_charges", "commissions",
        "insured_flag", "contract_insurance_charges", "ins_expiry_date",
        "interest_gl", "interest_amount_fcy", "interest_amount_lcy",
        "loan_application_id", "policy_type", "num_of_covered_persons",
        "sum_insured", "assured", "commence_date", "benefit_expiry_date",
        "premium_payment_freq", "sum_reassured", "pension_contract_status",
    ],
    "loan_applications_2": [
        "country", "le_book", "loan_application_id", "loan_application_type",
        "business_date", "customer_id", "customer_name", "customer_gender",
        "vision_ouc", "loan_purpose", "loan_utilization_location",
        "vision_sbu", "economic_sector_code", "application_date",
        "application_status", "currency", "applied_amount_lcy",
        "applied_amount_fcy", "approved_amount_lcy", "approved_amount_fcy",
        "rejection_reason", "prev_loan_paid",
    ],
    "prev_loan_applications": [
        "country", "le_book", "business_date", "loan_application_id",
        "prev_contract_id", "prev_cont_modif_reason_at", "prev_cont_modif_reason",
        "prev_loan_app_status_nt", "prev_loan_app_status",
        "record_indicator_nt", "record_indicator", "maker", "verifier",
        "internal_status", "date_last_modified", "date_creation",
    ],
}

# One completeness rule per table
_COMP_TABLE_NAMES = {
    "COMP-001": "customers_expanded",
    "COMP-002": "accounts",
    "COMP-003": "contracts_disburse",
    "COMP-004": "contract_loans",
    "COMP-005": "contract_schedules",
    "COMP-006": "contracts_expanded",
    "COMP-007": "loan_applications_2",
    "COMP-008": "prev_loan_applications",
}

COMP_RULE_META: dict[str, dict] = {
    rid: {
        "name":      f"All mandatory columns must be non-null ({tbl})",
        "category":  "Field Completeness",
        "dimension": "completeness",
        "tables":    [tbl],
        "fields":    MANDATORY_COLUMNS[tbl],
    }
    for rid, tbl in _COMP_TABLE_NAMES.items()
}

# ── ACCURACY ───────────────────────────────────────────────────────────────────

VALID_ACCOUNT_STATUS    = frozenset({0, 1, 2, 3, 4, 5, 9})
VALID_PERFORMANCE_CLASS = frozenset({"NL", "WL", "SL", "DL", "LL", "WO"})
VALID_GENDER            = frozenset({"M", "F", "C"})
VALID_ACCOUNT_TYPE      = frozenset({
    "CAA", "SBA", "TDA", "SED", "LAA", "OAB", "IP", "TRUSTAC",
    "MPSDC", "MPSDB", "VCOPSDC", "VCOPSDB", "VPPSDC", "VPPSDB",
})
CORPORATE_LEGAL_STATUS  = frozenset({3, 4, 5, 6, 7})
PENSION_ACCOUNT_TYPES   = frozenset({
    "MPSDC", "MPSDB", "VCOPSDC", "VCOPSDB", "VPPSDC", "VPPSDB",
})

ACC_RULE_META: dict[str, dict] = {
    "ACC-001": {
        "name":     "LE Book must be a valid BNR-registered institution code",
        "category": "Code Domain Validity",
        "fields":   ["le_book"],
    },
    "ACC-002": {
        "name":     "Account Status must be within allowed numeric codes (0–9)",
        "category": "Code Domain Validity",
        "fields":   ["account_status"],
    },
    "ACC-003": {
        "name":     "Performance Class must match BNR loan classification codes",
        "category": "Code Domain Validity",
        "fields":   ["performance_class"],
    },
    "ACC-004": {
        "name":     "Customer Gender must be M, F, or C only",
        "category": "Code Domain Validity",
        "fields":   ["customer_gender"],
    },
    "ACC-005": {
        "name":     "Account Type must be a valid BNR product code",
        "category": "Code Domain Validity",
        "fields":   ["account_type"],
    },
    "ACC-010": {
        "name":     "Gender must be C when Legal Status indicates a corporate entity",
        "category": "Cross-field Consistency",
        "fields":   ["customer_gender", "legal_status"],
    },
    "ACC-011": {
        "name":     "Pension account types must not appear for RETL segment",
        "category": "Cross-field Consistency",
        "fields":   ["account_type", "vision_sbu"],
    },
    "ACC-012": {
        "name":     "Marital Status must be NA for corporate customers",
        "category": "Cross-field Consistency",
        "fields":   ["marital_status", "customer_gender"],
    },
    "ACC-013": {
        "name":     "LE Book code must be exactly 3 characters, zero-padded numeric",
        "category": "Format and Type",
        "fields":   ["le_book"],
    },
}

ACCURACY_COLUMNS: dict[str, list[str]] = {
    "customers_expanded":     ["le_book", "customer_gender", "legal_status", "marital_status"],
    "accounts":               ["le_book", "account_status", "account_type", "vision_sbu"],
    "contracts_expanded":     ["le_book", "performance_class"],
    "contract_loans":         ["le_book", "performance_class"],
    "contracts_disburse":     ["le_book"],
    "contract_schedules":     ["le_book"],
    "loan_applications_2":    ["le_book", "customer_gender"],
    "prev_loan_applications": ["le_book"],
}

ACC_TABLE_RULES: dict[str, list[str]] = {
    "customers_expanded":     ["ACC-001", "ACC-004", "ACC-010", "ACC-012", "ACC-013"],
    "accounts":               ["ACC-001", "ACC-002", "ACC-005", "ACC-011", "ACC-013"],
    "contracts_expanded":     ["ACC-001", "ACC-003", "ACC-013"],
    "contract_loans":         ["ACC-001", "ACC-003", "ACC-013"],
    "contracts_disburse":     ["ACC-001", "ACC-013"],
    "contract_schedules":     ["ACC-001", "ACC-013"],
    "loan_applications_2":    ["ACC-001", "ACC-004", "ACC-013"],
    "prev_loan_applications": ["ACC-001", "ACC-013"],
}

# ── TIMELINESS ─────────────────────────────────────────────────────────────────

FRESHNESS_WINDOW_DAYS = 90
MIN_AGE_AT_OPEN       = 18

TIM_RULE_META: dict[str, dict] = {
    "TIM-001": {
        "name":     "Customer open date must not be in the future",
        "category": "No Future Dates",
        "fields":   ["customer_open_date"],
    },
    "TIM-002": {
        "name":     "Date of birth must be between 1900-01-01 and today",
        "category": "No Future Dates",
        "fields":   ["date_of_birth"],
    },
    "TIM-003": {
        "name":     "Account open date must not be in the future",
        "category": "No Future Dates",
        "fields":   ["account_open_date"],
    },
    "TIM-004": {
        "name":     "Record creation date must not be in the future",
        "category": "No Future Dates",
        "fields":   ["date_creation"],
    },
    "TIM-005": {
        "name":     "Business date must not be in the future",
        "category": "No Future Dates",
        "fields":   ["business_date"],
    },
    "TIM-006": {
        "name":     "Loan approval date must not be in the future",
        "category": "No Future Dates",
        "fields":   ["approval_date"],
    },
    "TIM-007": {
        "name":     "Loan application date must not be in the future",
        "category": "No Future Dates",
        "fields":   ["application_date"],
    },
    "TIM-010": {
        "name":     "Record creation date must be on or before last modification date",
        "category": "Logical Date Order",
        "fields":   ["date_creation", "date_last_modified"],
    },
    "TIM-011": {
        "name":     "Contract start date must be strictly before maturity date",
        "category": "Logical Date Order",
        "fields":   ["start_date", "maturity_date"],
    },
    "TIM-012": {
        "name":     "Payment date must be on or after schedule date when payment is recorded",
        "category": "Logical Date Order",
        "fields":   ["schedule_date", "payment_date"],
    },
    "TIM-013": {
        "name":     "Insurance commence date must be on or before benefit expiry date",
        "category": "Logical Date Order",
        "fields":   ["commence_date", "benefit_expiry_date"],
    },
    "TIM-014": {
        "name":     "Insurance commence date must be on or before insurance expiry date",
        "category": "Logical Date Order",
        "fields":   ["commence_date", "ins_expiry_date"],
    },
    "TIM-020": {
        "name":     f"Record must have been modified within the past {FRESHNESS_WINDOW_DAYS} days",
        "category": "Data Freshness",
        "fields":   ["date_last_modified"],
    },
}

TIMELINESS_COLUMNS: dict[str, list[str]] = {
    "customers_expanded":     ["le_book", "customer_open_date", "date_of_birth",
                               "date_creation", "date_last_modified"],
    "accounts":               ["le_book", "account_open_date",
                               "date_creation", "date_last_modified"],
    "contracts_disburse":     ["le_book", "business_date",
                               "date_creation", "date_last_modified"],
    "contract_loans":         ["le_book", "approval_date",
                               "date_creation", "date_last_modified"],
    "contract_schedules":     ["le_book", "schedule_date", "payment_date",
                               "date_creation", "date_last_modified"],
    "contracts_expanded":     ["le_book", "start_date", "maturity_date",
                               "commence_date", "benefit_expiry_date", "ins_expiry_date",
                               "date_creation", "date_last_modified"],
    "loan_applications_2":    ["le_book", "business_date", "application_date"],
    "prev_loan_applications": ["le_book", "business_date",
                               "date_creation", "date_last_modified"],
}

TIM_TABLE_RULES: dict[str, list[str]] = {
    "customers_expanded":     ["TIM-001", "TIM-002", "TIM-004", "TIM-010", "TIM-020"],
    "accounts":               ["TIM-003", "TIM-004", "TIM-010", "TIM-020"],
    "contracts_disburse":     ["TIM-004", "TIM-005", "TIM-010", "TIM-020"],
    "contract_loans":         ["TIM-004", "TIM-006", "TIM-010", "TIM-020"],
    "contract_schedules":     ["TIM-004", "TIM-010", "TIM-012", "TIM-020"],
    "contracts_expanded":     ["TIM-004", "TIM-010", "TIM-011", "TIM-013", "TIM-014", "TIM-020"],
    "loan_applications_2":    ["TIM-005", "TIM-007"],
    "prev_loan_applications": ["TIM-004", "TIM-005", "TIM-010", "TIM-020"],
}

# ── VALIDITY ───────────────────────────────────────────────────────────────────

MIN_PHONE_DIGITS  = 7
MIN_NATIONAL_ID   = 5
INTEREST_RATE_MAX = 100

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
}

VALIDITY_COLUMNS: dict[str, list[str]] = {
    "customers_expanded":  ["le_book", "email_id", "work_telephone", "home_telephone",
                            "national_id_number", "national_id_type",
                            "date_of_birth", "customer_open_date"],
    "accounts":            ["le_book", "currency", "interest_rate_dr", "interest_rate_cr"],
    "contracts_disburse":  ["le_book", "currency",
                            "current_disbursed_amt", "previous_disbursed_amt"],
    "contract_loans":      ["le_book", "interest_rate_dr", "emi_amount",
                            "outstanding_amount_lcy", "num_of_instalments",
                            "num_instalments_paid"],
    "contract_schedules":  ["le_book", "emi_amount", "due_amount", "outstanding_amount",
                            "principal_amount_due", "int_amount_due"],
    "contracts_expanded":  ["le_book", "currency", "mis_currency",
                            "interest_rate_dr", "interest_rate_cr",
                            "principal_amount_lcy"],
    "loan_applications_2": ["le_book", "currency",
                            "applied_amount_lcy", "approved_amount_lcy"],
}

VAL_TABLE_RULES: dict[str, list[str]] = {
    "customers_expanded":  ["VAL-001", "VAL-002", "VAL-004", "VAL-022"],
    "accounts":            ["VAL-003", "VAL-010", "VAL-011"],
    "contracts_disburse":  ["VAL-003", "VAL-012"],
    "contract_loans":      ["VAL-010", "VAL-013", "VAL-014", "VAL-016", "VAL-020"],
    "contract_schedules":  ["VAL-013", "VAL-014"],
    "contracts_expanded":  ["VAL-003", "VAL-010", "VAL-011", "VAL-014"],
    "loan_applications_2": ["VAL-003", "VAL-015", "VAL-021"],
}

# ── RELATIONSHIP ───────────────────────────────────────────────────────────────

REL_RULE_META: dict[str, dict] = {
    "REL-001": {
        "name":         "Every account must reference a known customer",
        "category":     "Referential Integrity",
        "child_table":  "accounts",
        "child_col":    "customer_id",
        "parent_table": "customers_expanded",
        "parent_col":   "customer_id",
        "nullable":     False,
    },
    "REL-002": {
        "name":         "Every contract must reference a known customer",
        "category":     "Referential Integrity",
        "child_table":  "contracts_expanded",
        "child_col":    "customer_id",
        "parent_table": "customers_expanded",
        "parent_col":   "customer_id",
        "nullable":     False,
    },
    "REL-003": {
        "name":         "Every loan application must reference a known customer",
        "category":     "Referential Integrity",
        "child_table":  "loan_applications_2",
        "child_col":    "customer_id",
        "parent_table": "customers_expanded",
        "parent_col":   "customer_id",
        "nullable":     False,
    },
    "REL-004": {
        "name":         "Every contract-loan detail must reference a known contract",
        "category":     "Referential Integrity",
        "child_table":  "contract_loans",
        "child_col":    "contract_sequence_number",
        "parent_table": "contracts_expanded",
        "parent_col":   "contract_sequence_number",
        "nullable":     False,
    },
    "REL-005": {
        "name":         "Every payment schedule must reference a known contract",
        "category":     "Referential Integrity",
        "child_table":  "contract_schedules",
        "child_col":    "contract_sequence_number",
        "parent_table": "contracts_expanded",
        "parent_col":   "contract_sequence_number",
        "nullable":     False,
    },
    "REL-006": {
        "name":         "Every disbursement record must reference a known contract",
        "category":     "Referential Integrity",
        "child_table":  "contracts_disburse",
        "child_col":    "contract_id",
        "parent_table": "contracts_expanded",
        "parent_col":   "contract_id",
        "nullable":     False,
    },
    "REL-007": {
        "name":         "Every previous-application record must reference a known current application",
        "category":     "Referential Integrity",
        "child_table":  "prev_loan_applications",
        "child_col":    "loan_application_id",
        "parent_table": "loan_applications_2",
        "parent_col":   "loan_application_id",
        "nullable":     False,
    },
    "REL-008": {
        "name":         "Contract linked application ID, when present, must reference a known loan application",
        "category":     "Optional Reference",
        "child_table":  "contracts_expanded",
        "child_col":    "loan_application_id",
        "parent_table": "loan_applications_2",
        "parent_col":   "loan_application_id",
        "nullable":     True,
    },
}

# ── FLAT TABLE ─────────────────────────────────────────────────────────────────

def _build_rows() -> list[dict]:
    """Build a flat list of rule dicts suitable for inserting into dq_rules table."""
    rows: list[dict] = []

    # completeness
    for rid, meta in COMP_RULE_META.items():
        rows.append({
            "rule_id":   rid,
            "dimension": "completeness",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    ", ".join(meta["tables"]),
            "fields":    f"{len(meta['fields'])} mandatory columns",
        })

    # accuracy
    for rid, meta in ACC_RULE_META.items():
        tables = sorted({t for t, rules in ACC_TABLE_RULES.items() if rid in rules})
        rows.append({
            "rule_id":   rid,
            "dimension": "accuracy",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    ", ".join(tables),
            "fields":    ", ".join(meta["fields"]),
        })

    # timeliness
    for rid, meta in TIM_RULE_META.items():
        tables = sorted({t for t, rules in TIM_TABLE_RULES.items() if rid in rules})
        rows.append({
            "rule_id":   rid,
            "dimension": "timeliness",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    ", ".join(tables),
            "fields":    ", ".join(meta["fields"]),
        })

    # validity
    for rid, meta in VAL_RULE_META.items():
        tables = sorted({t for t, rules in VAL_TABLE_RULES.items() if rid in rules})
        rows.append({
            "rule_id":   rid,
            "dimension": "validity",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    ", ".join(tables),
            "fields":    ", ".join(meta["fields"]),
        })

    # referential-integrity rules — classified under accuracy
    for rid, meta in REL_RULE_META.items():
        rows.append({
            "rule_id":   rid,
            "dimension": "accuracy",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    f"{meta['child_table']} → {meta['parent_table']}",
            "fields":    f"{meta['child_col']} → {meta['parent_col']}",
        })

    return rows


def ensure_db(db_path: Path = DB_PATH) -> None:
    """Create (or refresh) the dq_rules SQLite table from the in-memory registry."""
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_rules (
                rule_id    TEXT PRIMARY KEY,
                dimension  TEXT NOT NULL,
                category   TEXT,
                rule_name  TEXT NOT NULL,
                tables     TEXT NOT NULL,
                fields     TEXT
            )
        """)
        rows = _build_rows()
        con.executemany(
            """
            INSERT INTO dq_rules (rule_id, dimension, category, rule_name, tables, fields)
            VALUES (:rule_id, :dimension, :category, :rule_name, :tables, :fields)
            ON CONFLICT(rule_id) DO UPDATE SET
                dimension = excluded.dimension,
                category  = excluded.category,
                rule_name = excluded.rule_name,
                tables    = excluded.tables,
                fields    = excluded.fields
            """,
            rows,
        )
        con.commit()
    finally:
        con.close()


def get_all_rules() -> list[dict]:
    """Return all built-in rules as a list of dicts (from in-memory registry)."""
    return _build_rows()


def get_rules_df():
    """Return all built-in rules as a pandas DataFrame."""
    import pandas as pd
    return pd.DataFrame(_build_rows())


# ── USER RULES — PostgreSQL (dqp schema) ───────────────────────────────────────
#
# Status lifecycle:
#   draft   → submitted by user, awaiting admin approval
#   pending → approved by admin, will run on next pipeline
#   active  → successfully executed by pipeline
#   error   → pipeline attempted to run but the check could not evaluate

def ensure_pg_tables() -> None:
    """
    Create dqp.dq_rules and dqp.dq_user_rules if they don't exist, then sync
    all built-in rules into dqp.dq_rules. Safe to call repeatedly.
    """
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            # ── built-in rules reference table ────────────────────────────────
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.dq_rules (
                    rule_id    TEXT PRIMARY KEY,
                    dimension  TEXT NOT NULL,
                    category   TEXT,
                    rule_name  TEXT NOT NULL,
                    tables     TEXT NOT NULL,
                    fields     TEXT
                )
            """)

            # ── user-defined rules with workflow status ────────────────────────
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.dq_user_rules (
                    rule_id      TEXT        PRIMARY KEY,
                    dimension    TEXT        NOT NULL,
                    category     TEXT,
                    rule_name    TEXT        NOT NULL,
                    tables       TEXT        NOT NULL,
                    fields       TEXT,
                    check_type   TEXT        NOT NULL,
                    check_params TEXT,
                    status       TEXT        NOT NULL DEFAULT 'draft',
                    created_at   TIMESTAMPTZ DEFAULT NOW(),
                    last_run_at  TIMESTAMPTZ
                )
            """)

        # ── sync built-in rules atomically ────────────────────────────────────
        # Fetch existing IDs in one round-trip, then batch-insert/update.
        rows = _build_rows()
        with conn.cursor() as cur:
            cur.execute(f"SELECT rule_id FROM {PG_SCHEMA}.dq_rules")
            existing = {r[0] for r in cur.fetchall()}

        for r in rows:
            with conn.cursor() as cur:
                if r["rule_id"] in existing:
                    cur.execute(f"""
                        UPDATE {PG_SCHEMA}.dq_rules
                           SET dimension=%s, category=%s, rule_name=%s,
                               tables=%s, fields=%s
                         WHERE rule_id=%s
                    """, (r["dimension"], r.get("category", ""), r["rule_name"],
                          r["tables"], r.get("fields", ""), r["rule_id"]))
                else:
                    cur.execute(f"""
                        INSERT INTO {PG_SCHEMA}.dq_rules
                            (rule_id, dimension, category, rule_name, tables, fields)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (r["rule_id"], r["dimension"], r.get("category", ""),
                          r["rule_name"], r["tables"], r.get("fields", "")))

        conn.commit()
        log.debug("dqp.dq_rules synced — %d built-in rules", len(rows))
    finally:
        conn.close()


def next_user_rule_id() -> str:
    """Return the next available USR-NNN id from PostgreSQL."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT rule_id FROM {PG_SCHEMA}.dq_user_rules "
                f"WHERE rule_id ~ '^USR-[0-9]+$' "
                f"ORDER BY LENGTH(rule_id) DESC, rule_id DESC LIMIT 1"
            )
            row = cur.fetchone()
    finally:
        conn.close()
    n = int(row[0].split("-")[1]) + 1 if row else 1
    return f"USR-{n:03d}"


def add_user_rule(rule: dict, db_path: Path | None = None) -> None:
    """
    Insert a user-defined rule as a draft (awaiting admin approval).

    db_path is accepted for test compatibility (writes to SQLite when provided).
    In production (db_path=None) writes to PostgreSQL dqp.dq_user_rules.
    """
    from datetime import datetime as _dt

    if db_path is not None:
        # --- test / SQLite path -------------------------------------------
        _ensure_user_rules_table(db_path)
        con = sqlite3.connect(db_path)
        try:
            con.execute(
                """
                INSERT INTO dq_user_rules
                    (rule_id, dimension, category, rule_name, tables, fields,
                     check_type, check_params, status, created_at)
                VALUES
                    (:rule_id, :dimension, :category, :rule_name, :tables, :fields,
                     :check_type, :check_params, 'draft', :created_at)
                """,
                {**rule, "created_at": _dt.now().isoformat(timespec="seconds")},
            )
            con.commit()
        finally:
            con.close()
        return

    # --- production / PostgreSQL path -------------------------------------
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {PG_SCHEMA}.dq_user_rules
                    (rule_id, dimension, category, rule_name, tables, fields,
                     check_type, check_params, status)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, 'draft')
                """,
                (
                    rule["rule_id"], rule["dimension"],
                    rule.get("category", ""), rule["rule_name"],
                    rule["tables"],           rule.get("fields", ""),
                    rule["check_type"],       rule.get("check_params"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_user_rules(status: str | None = None,
                   db_path: Path | None = None) -> list[dict]:
    """
    Return user-defined rules ordered by creation time.

    status   — filter to a specific status ('draft', 'pending', 'active', 'error').
               None returns all non-draft rules (for dashboard main table).
    db_path  — SQLite path for test isolation; None uses PostgreSQL.
    """
    if db_path is not None:
        # --- test / SQLite path -------------------------------------------
        _ensure_user_rules_table(db_path)
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        try:
            if status is not None:
                rows = con.execute(
                    "SELECT * FROM dq_user_rules WHERE status=? ORDER BY created_at",
                    (status,),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM dq_user_rules WHERE status != 'draft' ORDER BY created_at"
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            con.close()

    # --- production / PostgreSQL path -------------------------------------
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cols = ("rule_id", "dimension", "category", "rule_name",
                    "tables", "fields", "check_type", "check_params",
                    "status", "created_at", "last_run_at")
            if status is not None:
                cur.execute(
                    f"SELECT {', '.join(cols)} FROM {PG_SCHEMA}.dq_user_rules "
                    f"WHERE status = %s ORDER BY created_at",
                    (status,),
                )
            else:
                cur.execute(
                    f"SELECT {', '.join(cols)} FROM {PG_SCHEMA}.dq_user_rules "
                    f"WHERE status != 'draft' ORDER BY created_at"
                )
            rows = cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def get_draft_rules() -> list[dict]:
    """Return all draft rules awaiting admin review."""
    return get_user_rules(status="draft")


def approve_draft_rule(rule_id: str) -> None:
    """Promote a draft rule to pending so the pipeline will execute it."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {PG_SCHEMA}.dq_user_rules "
                f"SET status = 'pending' "
                f"WHERE rule_id = %s AND status = 'draft'",
                (rule_id,),
            )
        conn.commit()
    finally:
        conn.close()


def delete_draft_rule(rule_id: str) -> None:
    """Permanently remove a draft rule (admin rejection). Only works on drafts."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {PG_SCHEMA}.dq_user_rules "
                f"WHERE rule_id = %s AND status = 'draft'",
                (rule_id,),
            )
        conn.commit()
    finally:
        conn.close()


def mark_user_rule_run(rule_id: str, status: str,
                       db_path: Path | None = None) -> None:
    """Update status and last_run_at after pipeline execution."""
    if db_path is not None:
        # --- test / SQLite path -------------------------------------------
        from datetime import datetime as _dt
        con = sqlite3.connect(db_path)
        try:
            con.execute(
                "UPDATE dq_user_rules SET status=?, last_run_at=? WHERE rule_id=?",
                (status, _dt.now().isoformat(timespec="seconds"), rule_id),
            )
            con.commit()
        finally:
            con.close()
        return

    # --- production / PostgreSQL path -------------------------------------
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {PG_SCHEMA}.dq_user_rules "
                f"SET status = %s, last_run_at = NOW() "
                f"WHERE rule_id = %s",
                (status, rule_id),
            )
        conn.commit()
    finally:
        conn.close()


# ── SQLite helpers kept for test isolation ────────────────────────────────────

def _ensure_user_rules_table(db_path: Path = DB_PATH) -> None:
    """Create dq_user_rules in a SQLite file (used only in tests)."""
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_user_rules (
                rule_id      TEXT PRIMARY KEY,
                dimension    TEXT NOT NULL,
                category     TEXT,
                rule_name    TEXT NOT NULL,
                tables       TEXT NOT NULL,
                fields       TEXT,
                check_type   TEXT NOT NULL,
                check_params TEXT,
                status       TEXT NOT NULL DEFAULT 'draft',
                created_at   TEXT,
                last_run_at  TEXT
            )
        """)
        con.commit()
    finally:
        con.close()


# ── DB INIT ────────────────────────────────────────────────────────────────────

# Seed the SQLite built-in rules table (used by tests and the Validations chart).
ensure_db()

# Provision PostgreSQL tables and sync built-in rules.
# Failures are logged but do not abort import — the dashboard can still serve
# cached history even if the DB is temporarily unreachable at startup.
try:
    ensure_pg_tables()
except Exception as _pg_init_err:
    log.warning("PostgreSQL init skipped at import time: %s", _pg_init_err)
