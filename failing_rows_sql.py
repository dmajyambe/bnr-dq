# Build SQL that selects FAILING rows across all dimensions for a table, tagged
# with issue_type — used to produce the per-institution CSV issue reports.
#
# Each rule contributes one UNION ALL branch selecting the rows that violate it.
# Output column order is: identifier/context columns (left) → issue_type → the
# rule-target "failing" columns (rightmost), so the columns responsible for each
# issue are easy to find without cell colouring.
from __future__ import annotations

from dq_rules import (
    MANDATORY_COLUMNS,
    ACC_TABLE_RULES, ACC_RULE_META,
    VAL_TABLE_RULES, VAL_RULE_META,
    UNI_TABLE_RULES, UNI_RULE_META,
    VALID_ACCOUNT_STATUS, VALID_PERFORMANCE_CLASS, VALID_GENDER,
    VALID_ACCOUNT_TYPE, CORPORATE_LEGAL_STATUS,
    MIN_PHONE_DIGITS, MIN_NATIONAL_ID, INTEREST_RATE_MAX, MIN_AGE_AT_OPEN,
)

# Identifier/context columns shown on the left so a record can be located.
IDENTIFIER_COLS: dict[str, list[str]] = {
    "customers_expanded":     ["le_book", "customer_id", "customer_name"],
    "accounts":               ["le_book", "account_no", "customer_id", "account_name"],
    "contracts_disburse":     ["le_book", "contract_id", "business_date"],
    "contract_loans":         ["le_book", "contract_sequence_number"],
    "contract_schedules":     ["le_book", "contract_sequence_number", "schedule_date"],
    "contracts_expanded":     ["le_book", "contract_sequence_number", "customer_id"],
    "loan_applications_2":    ["le_book", "loan_application_id", "customer_name"],
    "prev_loan_applications": ["le_book", "loan_application_id"],
}


def _sqlstr(s: str) -> str:
    """Quote a Python string as a SQL string literal."""
    return "'" + s.replace("'", "''") + "'"


def _acc_invalid(rule_id: str, existing: set) -> tuple[str, list[str]] | None:
    """Row-level predicate selecting accuracy-INVALID rows, + the failing column(s)."""
    def has(*cols: str) -> bool:
        return all(c in existing for c in cols)

    if rule_id == "ACC-002":
        if not has("account_status"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(str(x) for x in VALID_ACCOUNT_STATUS))
        return (f'"account_status" IS NOT NULL AND "account_status"::TEXT NOT IN ({vals})',
                ["account_status"])
    if rule_id == "ACC-003":
        if not has("performance_class"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_PERFORMANCE_CLASS))
        return (f'"performance_class" IS NOT NULL AND UPPER(TRIM("performance_class"::TEXT)) NOT IN ({vals})',
                ["performance_class"])
    if rule_id == "ACC-004":
        if not has("customer_gender"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_GENDER))
        return (f'"customer_gender" IS NOT NULL AND UPPER(TRIM("customer_gender"::TEXT)) NOT IN ({vals})',
                ["customer_gender"])
    if rule_id == "ACC-005":
        if not has("account_type"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_ACCOUNT_TYPE))
        return (f'"account_type" IS NOT NULL AND UPPER(TRIM("account_type"::TEXT)) NOT IN ({vals})',
                ["account_type"])
    if rule_id == "ACC-010":
        if not has("customer_gender", "legal_status"):
            return None
        corp = ", ".join(f"'{v}'" for v in sorted(str(x) for x in CORPORATE_LEGAL_STATUS))
        return (f'"customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL '
                f'AND "legal_status"::TEXT IN ({corp}) AND UPPER(TRIM("customer_gender"::TEXT)) <> \'C\'',
                ["customer_gender", "legal_status"])
    if rule_id == "ACC-012":
        if not has("customer_gender", "marital_status"):
            return None
        return (f'"customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL '
                f'AND UPPER(TRIM("customer_gender"::TEXT)) = \'C\' AND UPPER(TRIM("marital_status"::TEXT)) <> \'NA\'',
                ["customer_gender", "marital_status"])
    return None


def _val_invalid(rule_id: str, existing: set) -> tuple[str, list[str]] | None:
    """Row-level predicate selecting validity-INVALID rows, + the failing column(s)."""
    def has(*cols: str) -> bool:
        return all(c in existing for c in cols)

    if rule_id == "VAL-001":
        if not has("email_id"):
            return None
        return (r"""("email_id" IS NOT NULL AND "email_id"::TEXT !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$')""",
                ["email_id"])
    if rule_id == "VAL-002":
        cols = [c for c in ("work_telephone", "home_telephone") if c in existing]
        if not cols:
            return None
        pred = " OR ".join(
            f'("{c}" IS NOT NULL AND TRIM("{c}"::TEXT) <> \'\' '
            f'AND LENGTH(REGEXP_REPLACE("{c}"::TEXT, \'[^0-9]\', \'\', \'g\')) < {MIN_PHONE_DIGITS})'
            for c in cols)
        return (f"({pred})", cols)
    if rule_id == "VAL-003":
        cols = [c for c in ("currency", "mis_currency") if c in existing]
        if not cols:
            return None
        pred = " OR ".join(
            f'("{c}" IS NOT NULL AND TRIM("{c}"::TEXT) <> \'\' AND TRIM("{c}"::TEXT) !~ \'^[A-Z]{{3}}$\')'
            for c in cols)
        return (f"({pred})", cols)
    if rule_id == "VAL-004":
        if not has("national_id_type", "national_id_number"):
            return None
        return (f'"national_id_type" IS NOT NULL AND TRIM("national_id_type"::TEXT) <> \'\' '
                f'AND LENGTH(TRIM(COALESCE("national_id_number"::TEXT, \'\'))) < {MIN_NATIONAL_ID}',
                ["national_id_number"])
    if rule_id == "VAL-010":
        if not has("interest_rate_dr"):
            return None
        return (f'"interest_rate_dr" IS NOT NULL AND ("interest_rate_dr"::NUMERIC < 0 '
                f'OR "interest_rate_dr"::NUMERIC > {INTEREST_RATE_MAX})', ["interest_rate_dr"])
    if rule_id == "VAL-011":
        if not has("interest_rate_cr"):
            return None
        return (f'"interest_rate_cr" IS NOT NULL AND ("interest_rate_cr"::NUMERIC < 0 '
                f'OR "interest_rate_cr"::NUMERIC > {INTEREST_RATE_MAX})', ["interest_rate_cr"])
    if rule_id == "VAL-012":
        cols = [c for c in ("current_disbursed_amt", "previous_disbursed_amt") if c in existing]
        if not cols:
            return None
        pred = " OR ".join(f'("{c}" IS NOT NULL AND "{c}"::NUMERIC < 0)' for c in cols)
        return (f"({pred})", cols)
    if rule_id == "VAL-013":
        if not has("emi_amount"):
            return None
        return ('"emi_amount" IS NOT NULL AND "emi_amount"::NUMERIC <= 0', ["emi_amount"])
    if rule_id == "VAL-014":
        cols = [c for c in ("outstanding_amount_lcy", "outstanding_amount",
                            "principal_amount_due", "int_amount_due",
                            "due_amount", "principal_amount_lcy") if c in existing]
        if not cols:
            return None
        pred = " OR ".join(f'("{c}" IS NOT NULL AND "{c}"::NUMERIC < 0)' for c in cols)
        return (f"({pred})", cols)
    if rule_id == "VAL-015":
        if not has("applied_amount_lcy"):
            return None
        return ('"applied_amount_lcy" IS NOT NULL AND "applied_amount_lcy"::NUMERIC <= 0',
                ["applied_amount_lcy"])
    if rule_id == "VAL-016":
        if not has("num_of_instalments"):
            return None
        return ('"num_of_instalments" IS NOT NULL AND "num_of_instalments"::NUMERIC < 1',
                ["num_of_instalments"])
    if rule_id == "VAL-020":
        if not has("num_instalments_paid", "num_of_instalments"):
            return None
        return ('"num_instalments_paid" IS NOT NULL AND "num_of_instalments" IS NOT NULL '
                'AND "num_instalments_paid"::NUMERIC > "num_of_instalments"::NUMERIC',
                ["num_instalments_paid", "num_of_instalments"])
    if rule_id == "VAL-021":
        if not has("approved_amount_lcy", "applied_amount_lcy"):
            return None
        return ('"approved_amount_lcy" IS NOT NULL AND "applied_amount_lcy" IS NOT NULL '
                'AND "approved_amount_lcy"::NUMERIC > "applied_amount_lcy"::NUMERIC',
                ["approved_amount_lcy", "applied_amount_lcy"])
    if rule_id == "VAL-022":
        if not has("date_of_birth", "customer_open_date"):
            return None
        return (f'"date_of_birth" IS NOT NULL AND "customer_open_date" IS NOT NULL '
                f'AND ("customer_open_date"::DATE - "date_of_birth"::DATE) < {MIN_AGE_AT_OPEN * 365}',
                ["date_of_birth", "customer_open_date"])
    return None


def _failing_columns(table: str, existing: set) -> list[str]:
    """All rule-target columns for a table (the 'failing' columns), present in DB."""
    cols: set[str] = {c for c in MANDATORY_COLUMNS.get(table, []) if c in existing}
    for rid in ACC_TABLE_RULES.get(table, []):
        cols |= {c for c in ACC_RULE_META.get(rid, {}).get("fields", []) if c in existing}
    for rid in VAL_TABLE_RULES.get(table, []):
        cols |= {c for c in VAL_RULE_META.get(rid, {}).get("fields", []) if c in existing}
    for rid in UNI_TABLE_RULES.get(table, []):
        cols |= {c for c in UNI_RULE_META.get(rid, {}).get("fields", []) if c in existing}
    return sorted(cols)


def build_failing_union(schema: str, table: str, existing: set,
                        valid_le_books: frozenset, limit: int = 0):
    """Return (sql, output_columns) selecting failing rows across all dimensions for
    `table`, or None if nothing is applicable.

    output_columns order: identifiers → 'issue_type' → failing columns (rightmost).
    Rows are ORDER BY le_book so the caller can group per institution while streaming.
    """
    if "le_book" not in existing:
        return None

    id_cols   = [c for c in IDENTIFIER_COLS.get(table, ["le_book"]) if c in existing]
    if "le_book" not in id_cols:
        id_cols = ["le_book"] + id_cols
    fail_cols = [c for c in _failing_columns(table, existing) if c not in id_cols]
    if not fail_cols:
        return None

    id_sel   = ", ".join(f'"{c}"' for c in id_cols)
    fail_sel = ", ".join(f'"{c}"' for c in fail_cols)
    data_sel = ", ".join(f'"{c}"' for c in (id_cols + fail_cols))

    lb_clause = ""
    if valid_le_books:
        codes     = ", ".join(f"'{lb}'" for lb in sorted(valid_le_books))
        lb_clause = f'AND "le_book" IN ({codes})'

    sq = f'"{schema}"."{table}"'
    branches: list[str] = []

    # ── completeness + accuracy + validity: ONE table scan via LATERAL unnest ──
    # Each row yields one output row per rule it fails. A row-level (cond, label)
    # is collected per rule; the table is pre-filtered to rows failing ANY rule,
    # then unnest expands only those — avoids both one-scan-per-rule and an M×N
    # CASE blow-up over all rows.
    rule_conds: list[tuple[str, str]] = []
    for col in MANDATORY_COLUMNS.get(table, []):
        if col in existing:
            rule_conds.append((f'"{col}" IS NULL', f"Missing {col}"))
    for rid in ACC_TABLE_RULES.get(table, []):
        r = _acc_invalid(rid, existing)
        if r:
            rule_conds.append((r[0], "{}: {}".format(rid, ACC_RULE_META[rid].get("name", rid))))
    for rid in VAL_TABLE_RULES.get(table, []):
        r = _val_invalid(rid, existing)
        if r:
            rule_conds.append((r[0], "{}: {}".format(rid, VAL_RULE_META[rid].get("name", rid))))

    if rule_conds:
        case_arr = ",\n            ".join(
            f'CASE WHEN ({cond}) THEN {_sqlstr(label)} END' for cond, label in rule_conds)
        any_fail = " OR ".join(f'({cond})' for cond, _ in rule_conds)
        branches.append(
            f'SELECT {id_sel}, _u.issue_type AS issue_type, {fail_sel}\n'
            f'FROM (SELECT {data_sel} FROM {sq} WHERE ({any_fail}) {lb_clause}) _f\n'
            f'CROSS JOIN LATERAL unnest(ARRAY[\n            {case_arr}\n        ]::text[]) AS _u(issue_type)\n'
            f'WHERE _u.issue_type IS NOT NULL')

    # ── uniqueness: duplicates via window function (one scan per uni rule) ──────
    for rid in UNI_TABLE_RULES.get(table, []):
        fields = UNI_RULE_META[rid]["fields"]
        keys   = [c for c in fields if c in existing]
        anchor = fields[0]
        if anchor in existing and keys:
            part = ", ".join(f'"{c}"' for c in (["le_book"] + keys))
            issue = f"{rid}: {UNI_RULE_META[rid]['name']}"
            sub = (f'SELECT {data_sel}, '
                   f'ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {part}) AS _rn '
                   f'FROM {sq} WHERE "{anchor}" IS NOT NULL {lb_clause}')
            branches.append(
                f'SELECT {id_sel}, {_sqlstr(issue)} AS issue_type, {fail_sel} '
                f'FROM ({sub}) _q WHERE _rn > 1')

    if not branches:
        return None

    sql = " UNION ALL ".join(branches) + ' ORDER BY "le_book"'
    if limit and limit > 0:
        sql += f" LIMIT {limit}"

    output_cols = id_cols + ["issue_type"] + fail_cols
    return sql, output_cols
