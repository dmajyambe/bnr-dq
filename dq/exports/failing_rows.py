# Builds and streams the per-institution failing-row Excel/ZIP issue reports.
#
# Moved out of dq/reports/ per review feedback: "reports" should mean the
# history/trend time series (dq/reports/history.py); this is a different
# kind of artifact (raw per-row evidence dumps), so it gets its own package
# rather than being a sibling of history.py or pushed into jobs/ (which is
# meant to stay thin/orchestration-only) or storage/ (generic IO primitives,
# not business-shaped Excel/ZIP construction).
#
# build_failing_union() (originally failing_rows_sql.py) builds the SQL; the
# write_institution_zips() family (originally dq_monthly_detection.py) streams
# the query results into issue_reports/{le_book}_{month}.zip.
from __future__ import annotations

import logging
import os
import re
import tempfile
import zipfile
from datetime import date as _date, datetime as _datetime
from decimal import Decimal
from pathlib import Path
from sqlalchemy import text

from dq.rules.accuracy import (
    ACC_RULE_META, ACC_TABLE_RULES,
    VALID_ACCOUNT_STATUS, VALID_PERFORMANCE_CLASS, VALID_GENDER,
    VALID_ACCOUNT_TYPE, CORPORATE_LEGAL_STATUS,
)
from dq.rules.completeness import MANDATORY_COLUMNS
from dq.rules.uniqueness import UNI_RULE_META, UNI_TABLE_RULES
from dq.rules.validity import (
    VAL_RULE_META, VAL_TABLE_RULES,
    MIN_PHONE_DIGITS, MIN_NATIONAL_ID, INTEREST_RATE_MAX, MIN_AGE_AT_OPEN,
)
from dq.sql.metadata import all_columns

log = logging.getLogger("dq.exports.failing_rows")

SCRIPT_DIR        = Path(__file__).resolve().parents[2]
ISSUE_REPORTS_DIR = SCRIPT_DIR / "issue_reports"

# Referential-integrity rules (REL-001..008, folded into ACC_RULE_META — see
# dq/rules/accuracy.py), identified the same way dq/engines/accuracy.py does:
# by having a parent_table in their metadata.
_REL_RULE_IDS = {rid for rid, m in ACC_RULE_META.items() if "parent_table" in m}

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
                        valid_le_books: frozenset, limit: int = 0,
                        extra_where: str = "", per_issue_cap: int = 0):
    """Return (sql, output_columns, issue_cols) selecting failing rows across all
    dimensions for `table`, or None if nothing is applicable.

    output_columns order: identifiers → 'issue_type' → failing columns (rightmost).
    issue_cols: {issue_label: [affected column(s)]} — for red-highlighting per sheet.
    Rows are ORDER BY le_book, issue_type so the caller can stream one (institution,
    issue) group at a time. per_issue_cap > 0 caps rows per (institution, issue)
    server-side (one Excel sheet's worth).
    """
    if "le_book" not in existing:
        return None

    id_cols   = [c for c in IDENTIFIER_COLS.get(table, ["le_book"]) if c in existing]
    if "le_book" not in id_cols:
        id_cols = ["le_book"] + id_cols
    # context columns — appended when the table actually has them
    for ctx_col in ("date_creation", "date_last_modified"):
        if ctx_col in existing and ctx_col not in id_cols:
            id_cols.append(ctx_col)
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
    # optional extra row filter (e.g. a date_last_modified month) applied to every branch
    if extra_where:
        lb_clause += f' AND ({extra_where})'

    sq = f'"{schema}"."{table}"'
    branches: list[str] = []

  #faling rows for completness, accuracy, validity, uniqueness, referential integrity
    issue_cols: dict[str, list[str]] = {}
    rule_conds: list[tuple[str, str]] = []
    for col in MANDATORY_COLUMNS.get(table, []):
        if col in existing:
            label = f"Missing {col}"
            rule_conds.append((f'"{col}" IS NULL', label))
            issue_cols[label] = [col]
    for rid in ACC_TABLE_RULES.get(table, []):
        if rid in _REL_RULE_IDS:
            continue  # referential integrity — own branch below, needs a real JOIN
        r = _acc_invalid(rid, existing)
        if r:
            label = "{}: {}".format(rid, ACC_RULE_META[rid].get("name", rid))
            rule_conds.append((r[0], label))
            issue_cols[label] = r[1]
    for rid in VAL_TABLE_RULES.get(table, []):
        r = _val_invalid(rid, existing)
        if r:
            label = "{}: {}".format(rid, VAL_RULE_META[rid].get("name", rid))
            rule_conds.append((r[0], label))
            issue_cols[label] = r[1]

    if rule_conds:
        case_arr = ",\n            ".join(
            f'CASE WHEN ({cond}) THEN {_sqlstr(label)} END' for cond, label in rule_conds)
        any_fail = " OR ".join(f'({cond})' for cond, _ in rule_conds)
        branches.append(
            f'SELECT {id_sel}, _u.issue_type AS issue_type, {fail_sel}\n'
            f'FROM (SELECT {data_sel} FROM {sq} WHERE ({any_fail}) {lb_clause}) _f\n'
            f'CROSS JOIN LATERAL unnest(ARRAY[\n            {case_arr}\n        ]::text[]) AS _u(issue_type)\n'
            f'WHERE _u.issue_type IS NOT NULL')

    # uniqueness rules — one branch per rule, ROW_NUMBER() to find duplicates
    for rid in UNI_TABLE_RULES.get(table, []):
        fields = UNI_RULE_META[rid]["fields"]
        keys   = [c for c in fields if c in existing]
        anchor = fields[0]
        if anchor in existing and keys:
            part = ", ".join(f'"{c}"' for c in (["le_book"] + keys))
            issue = f"{rid}: {UNI_RULE_META[rid]['name']}"
            issue_cols[issue] = keys
            sub = (f'SELECT {data_sel}, '
                   f'ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {part}) AS _rn '
                   f'FROM {sq} WHERE "{anchor}" IS NOT NULL {lb_clause}')
            branches.append(
                f'SELECT {id_sel}, {_sqlstr(issue)} AS issue_type, {fail_sel} '
                f'FROM ({sub}) _q WHERE _rn > 1')

    #rel rules
    for rid in ACC_TABLE_RULES.get(table, []):
        if rid not in _REL_RULE_IDS:
            continue
        meta    = ACC_RULE_META[rid]
        child_c = meta["child_col"]
        if child_c not in existing:
            continue
        parent_t, parent_c = meta["parent_table"], meta["parent_col"]
        issue = f"{rid}: {meta['name']}"
        issue_cols[issue] = [child_c]
        branches.append(
            f'SELECT {id_sel}, {_sqlstr(issue)} AS issue_type, {fail_sel}\n'
            f'FROM {sq}\n'
            f'LEFT JOIN (SELECT DISTINCT "{parent_c}" AS _parent_key '
            f'FROM "{schema}"."{parent_t}") p ON "{child_c}" = p._parent_key\n'
            f'WHERE "{child_c}" IS NOT NULL AND p._parent_key IS NULL {lb_clause}')

    if not branches:
        return None

    output_cols = id_cols + ["issue_type"] + fail_cols
    inner = " UNION ALL ".join(branches)

    if per_issue_cap and per_issue_cap > 0:
        # Cap rows per (institution, issue) server-side so the client receives a
        # bounded set (one Excel sheet's worth) instead of streaming millions.
        cols_q = ", ".join("issue_type" if c == "issue_type" else f'"{c}"'
                           for c in output_cols)
        sql = (f'SELECT {cols_q} FROM ('
               f'SELECT {cols_q}, ROW_NUMBER() OVER (PARTITION BY "le_book", issue_type) AS _srn '
               f'FROM ({inner}) _w) _c WHERE _srn <= {per_issue_cap} '
               f'ORDER BY "le_book", issue_type')
    else:
        # order by institution then issue so the writer can stream one
        # (institution, issue) group at a time → one Excel sheet per issue.
        sql = inner + ' ORDER BY "le_book", issue_type'

    if limit and limit > 0:
        sql += f" LIMIT {limit}"

    return sql, output_cols, issue_cols


def write_institution_zips(engine, schema: str, table: str,
                           valid_le_books: frozenset, categories: dict,
                           month: str, limit: int = 0,
                           max_rows_per_sheet: int = 50000,
                           extra_where: str = "") -> None:
    """Stream FAILING rows (all dimensions) for `table` from SQL and write a
    per-institution {table}.xlsx into their monthly ZIP — ONE SHEET PER ISSUE
    (sheet name = issue type), with the affected column(s) highlighted red.

    Memory-safe: rows are capped per (institution, issue) server-side, streamed
    ORDER BY le_book, issue_type, and written through an openpyxl write_only
    workbook saved to a temp file per institution. Excel caps a sheet at ~1.05M
    rows; max_rows_per_sheet bounds it well below that (truncation noted on-sheet).
    """
    from openpyxl import Workbook
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.styles import Font, PatternFill

    RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    RED_FONT = Font(color="9C0006")
    HDR_FONT = Font(bold=True)
    HDR_RED  = Font(bold=True, color="9C0006")

    def _coerce(v):
        if v is None or isinstance(v, (str, int, float, bool, _date, _datetime)):
            return v
        if isinstance(v, Decimal):
            return float(v)
        return str(v)

    with engine.connect() as conn:
        try:
            # Referential-integrity branches (see build_failing_union) join
            # against a deduplicated parent table that can run to tens of
            # millions of rows — default work_mem spills that to disk.
            conn.execute(text("SET work_mem = '512MB'"))
        except Exception:
            conn.rollback()
        existing = all_columns(conn, schema, table)
        built = build_failing_union(schema, table, existing, valid_le_books, limit,
                                    extra_where=extra_where,
                                    per_issue_cap=max_rows_per_sheet + 1)
        if not built:
            return
        sql, out_cols, issue_cols = built

        # sheet columns = output cols minus issue_type; insert enrichment after le_book
        sheet_cols = [c for c in out_cols if c != "issue_type"]
        header: list[str] = []
        for c in sheet_cols:
            header.append(c)
            if c == "le_book":
                header += ["stakeholder_name", "category_type"]
        header.append("issue_type")

        def _sheet_title(label: str, used: set) -> str:
            name = re.sub(r"[\[\]:*?/\\]", " ", label)[:31].strip() or "issue"
            base, i = name, 1
            while name.lower() in used:
                sfx = f" ({i})"
                name = base[:31 - len(sfx)] + sfx
                i += 1
            used.add(name.lower())
            return name

        result = conn.execution_options(stream_results=True).execute(text(sql))
        st = {"wb": None, "path": None, "ws": None, "issue": None,
              "affected": set(), "n": 0, "used": set(), "trunc": False}

        def _finish_sheet():
            if st["ws"] is not None and st["trunc"]:
                st["ws"].append([WriteOnlyCell(
                    st["ws"], value=f"... truncated at {max_rows_per_sheet:,} rows")])

        def _start_sheet(issue: str):
            _finish_sheet()
            ws = st["wb"].create_sheet(title=_sheet_title(issue, st["used"]))
            affected = set(issue_cols.get(issue, []))
            cells = []
            for col in header:
                cell = WriteOnlyCell(ws, value=col)
                if col in affected:
                    cell.font, cell.fill = HDR_RED, RED_FILL
                else:
                    cell.font = HDR_FONT
                cells.append(cell)
            ws.append(cells)
            st.update(ws=ws, issue=issue, affected=affected, n=0, trunc=False)

        def _write_row(m: dict, name: str, ctype: str):
            if st["n"] >= max_rows_per_sheet:
                st["trunc"] = True
                return
            affected = st["affected"]
            cells = []
            for col in sheet_cols:
                cell = WriteOnlyCell(st["ws"], value=_coerce(m.get(col)))
                if col in affected:
                    cell.fill, cell.font = RED_FILL, RED_FONT
                cells.append(cell)
                if col == "le_book":
                    cells.append(WriteOnlyCell(st["ws"], value=name))
                    cells.append(WriteOnlyCell(st["ws"], value=ctype))
            cells.append(WriteOnlyCell(st["ws"], value=st["issue"]))
            st["ws"].append(cells)
            st["n"] += 1

        def _new_workbook():
            fd, path = tempfile.mkstemp(suffix=".xlsx")
            os.close(fd)
            st.update(wb=Workbook(write_only=True), path=path, ws=None,
                      issue=None, used=set())

        def _close(lb: str):
            if st["wb"] is None:
                return
            _finish_sheet()
            st["wb"].save(st["path"])
            zip_path = ISSUE_REPORTS_DIR / f"{lb}_{month}.zip"
            with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as zf:
                zf.write(st["path"], arcname=f"{table}.xlsx")
            os.unlink(st["path"])
            log.info("  ZIP %-6s  %s.xlsx", lb, table)
            st.update(wb=None, path=None, ws=None)

        cur_lb: str | None = None
        for row in result:
            m  = dict(row._mapping)
            lb = str(m["le_book"]).strip() if m.get("le_book") is not None else None
            if lb is None:
                continue
            issue = m.get("issue_type")
            info  = categories.get(lb, {})
            name  = info.get("name") or lb
            name  = name.title() if isinstance(name, str) else name
            ctype = info.get("category_type") or ""
            if lb != cur_lb:
                if cur_lb is not None:
                    _close(cur_lb)
                cur_lb = lb
                _new_workbook()
                _start_sheet(issue)
            elif issue != st["issue"]:
                _start_sheet(issue)
            _write_row(m, name, ctype)
        if cur_lb is not None:
            _close(cur_lb)
