from __future__ import annotations
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import accuracy_check
#import timeliness_check
# import validity_check
from completeness_check import MANDATORY_COLUMNS
#from accuracy_check    import RULE_META as ACC_META,  TABLE_RULES as ACC_TABLE_RULES
#from timeliness_check  import RULE_META as TIM_META,  TABLE_RULES as TIM_TABLE_RULES
# from validity_check    import RULE_META as VAL_META,  TABLE_RULES as VAL_TABLE_RULES
from relationship_check import RULE_META as REL_META
from dq_rules import REL_RULE_META

log = logging.getLogger("dq_issue_export")

#tables to export
ALL_TABLES = [
    "accounts", "contract_loans", "contract_schedules",
    "contracts_disburse", "contracts_expanded",
    "customers_expanded", "loan_applications_2", "prev_loan_applications",
]

# PK table
TABLE_PK: dict[str, list[str]] = {
    "customers_expanded":    ["customer_id", "le_book"],
    "accounts":              ["account_no", "le_book"],
    "contracts_expanded":    ["le_book", "contract_id","customer_id"],
    "contracts_disburse":    ["contract_id","le_book","business_date"],#commented out (, "business_date")
    "contract_loans":        ["contract_sequence_number"],#commented out (, "year_month")
    "contract_schedules":    ["contract_sequence_number", "schedule_date"],
    "loan_applications_2":   ["loan_application_id"],
    "prev_loan_applications": ["loan_application_id"],
}

#human-readable context columns per table
TABLE_CONTEXT: dict[str, list[str]] = {
    "customers_expanded":    ["customer_name", "customer_open_date"],
    "accounts":              ["account_name", "customer_id", "account_open_date"],
    "contracts_expanded":    ["customer_id", "start_date", "deal_type", "deal_sub_type"],
    "contracts_disburse":    ["business_date", "currency"],
    "contract_loans":        ["performance_class", "date_creation"],
    "contract_schedules":    ["schedule_date", "payment_date"],
    "loan_applications_2":   ["customer_name", "application_date", "application_status"],
    "prev_loan_applications": ["business_date"],
}
#csv cols
TABLE_CSV_COLS: dict[str, list[str]] = {
    "accounts": [
        "le_book", "stakeholder_name", "category_type",
        "account_no", "customer_id", "account_name",
        "account_type", "account_type_desc",
        # "vision_ouc", "currency",
        # "account_status_date", "account_open_date", "account_closing_date",
        # "performance_class", "performance_class_desc",
        # "vision_sbu", "vision_sbu_desc",
        # "account_status", "account_status_desc",
        # "last_tran_date"
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "customers_expanded": [
        "le_book", "stakeholder_name", "category_type",
        "customer_id", "salutation", "salutation_desc",
        "customer_name", "surname", "forename_1", "forename_2",
        "customer_gender", "customer_gender_desc",
        # "customer_acronym", "vision_ouc", "vision_sbu", "vision_sbu_desc",
        # "customer_open_date", "customer_tin", "passport_number",
        # "national_id_type", "national_id_type_desc", "national_id_number",
        # "customer_status", "customer_status_desc",
        # "date_of_birth", "place_of_birth",
        # "email_id", "work_telephone", "home_telephone",
        # "legal_status", "legal_status_desc",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "contract_loans": [
        "le_book", "stakeholder_name", "category_type",
        "contract_sequence_number", "year_month",
        "performance_class", "date_past_due",
        "outstanding_amount_lcy", "disbursed_amount",
        "emi_amount", "num_of_instalments", "num_instalments_paid",
        "prin_outstanding_amt_lcy",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "contract_schedules": [
        "le_book", "stakeholder_name", "category_type",
        "contract_sequence_number", "schedule_date", "payment_date",
        "emi_amount", "due_amount", "outstanding_amount",
        "principal_amount_due", "int_amount_due",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "contracts_disburse": [
        "le_book", "stakeholder_name", "category_type",
        "contract_id", "business_date", "currency",
        "current_disbursed_amt", "previous_disbursed_amt",
        "first_payment_date", "contract_status",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "contracts_expanded": [
        "le_book", "stakeholder_name", "category_type",
        "contract_sequence_number", "contract_id", "customer_id",
        # "deal_type", "deal_sub_type",
        # "start_date", "maturity_date",
        # "performance_class", "contract_status",
        # "outstanding_amount_lcy", "principal_amount_lcy",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "loan_applications_2": [
        "le_book", "stakeholder_name", "category_type",
        "loan_application_id", "customer_id", "customer_name",
        "customer_gender", "application_date", "application_status",
        "currency", "applied_amount_lcy", "approved_amount_lcy",
        "rejection_reason",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
    "prev_loan_applications": [
        "le_book", "stakeholder_name", "category_type",
        "loan_application_id", "prev_contract_id",
        "prev_loan_app_status", "record_indicator",
        "date_last_modified", "date_creation",
        "issue_type",
    ],
}


# cache

def _build_mask_cache(table: str, df: pd.DataFrame) -> dict:
    """Pre-compute all accuracy and timeliness masks for one table. Keyed by rule_id."""
    cache: dict = {}
    # for rule_id in ACC_TABLE_RULES.get(table, []):
    #     try:
    #         cache[rule_id] = accuracy_check.run_rule_mask(rule_id, df)
    #     except Exception:
    #         log.exception("Rule failed: %s", rule_id)
    # for rule_id in TIM_TABLE_RULES.get(table, []):
    #     try:
    #         cache[rule_id] = timeliness_check.run_rule_mask(rule_id, df)
    #     except Exception:
    #         log.exception("Rule failed: %s", rule_id)
    return cache


def build_mask_caches(dataframes: dict, valid_le_books: frozenset) -> dict:
    """
    Pre-compute rule masks for every table. Returns {table: {rule_id: mask}}.
    Pass to both export functions to avoid recomputing masks across the two calls.
    """
    valid_lb_strs = {str(lb) for lb in valid_le_books} if valid_le_books else None
    caches: dict = {}
    for table in ALL_TABLES:
        df = dataframes.get(table, pd.DataFrame())
        if df.empty:
            continue
        if valid_lb_strs and "le_book" in df.columns:
            df = df[df["le_book"].astype(str).isin(valid_lb_strs)]
        if df.empty:
            continue
        caches[table] = _build_mask_cache(table, df)
    return caches


# generate report
def _collect_failing_df(table: str, df: pd.DataFrame,
                        all_frames: dict, parent_frames: dict | None = None,
                        categories: dict = None,
                        mask_cache: dict | None = None,
                        output_cols: list | None = None) -> pd.DataFrame:
    """
    Collect ALL failing records for this table across every dimension,
    tagged with issue_type = rule_name.  Returns a single concatenated DataFrame.

    output_cols: if provided, each chunk is trimmed to these columns immediately
    after the mask is applied — avoids holding wide full-column copies in RAM
    for large tables.  Must include 'le_book'.
    """
    chunks: list[pd.DataFrame] = []

    def _tag(sdf: pd.DataFrame, issue_name: str) -> pd.DataFrame:
        if sdf.empty:
            return sdf
        out = sdf if output_cols is None else sdf[[c for c in output_cols if c in sdf.columns]]
        out = out.copy()
        out["issue_type"] = issue_name
        return out

    # completeness 
    for col in MANDATORY_COLUMNS.get(table, []):
        if col not in df.columns:
            continue
        mask = df[col].isna()
        if mask.any():
            chunks.append(_tag(df[mask], f"Missing {col}"))

    # accuracy 
    # for rule_id in ACC_TABLE_RULES.get(table, []):
    #     if mask_cache is not None:
    #         mask = mask_cache.get(rule_id)
    #         if mask is None:
    #             continue  # rule failed during cache build -- already logged
    #     else:
    #         try: 
    #             mask = accuracy_check.run_rule_mask(rule_id, df)
    #         except Exception:
    #             log.exception("Rule failed: %s", rule_id)
    #             continue
    #     if mask.any():
    #         chunks.append(_tag(df[mask],
    #                            ACC_META.get(rule_id, {}).get("name", rule_id)))

    # timeliness 
    # for rule_id in TIM_TABLE_RULES.get(table, []):
    #     if mask_cache is not None:
    #         mask = mask_cache.get(rule_id)
    #         if mask is None:
    #             continue  # rule failed during cache build -- already logged
        # else:
        #     try:
        #         mask = timeliness_check.run_rule_mask(rule_id, df)
        #     except Exception:
        #         log.exception("Rule failed: %s", rule_id)
        #         continue
        # if mask.any():
        #     chunks.append(_tag(df[mask],
        #                        TIM_META.get(rule_id, {}).get("name", rule_id)))

    # validity (commented out) 
    # for rule_id in VAL_TABLE_RULES.get(table, []):
    #     try:
    #         mask = validity_check.run_rule_mask(rule_id, df)
    #         if mask.any():
    #             chunks.append(_tag(df[mask].copy(),
    #                                VAL_META.get(rule_id, {}).get("name", rule_id)))
    #     except Exception:
    #         pass

    # -- relationship (child table only; commented out) ------------------------
    # for rule_id, meta in REL_RULE_META.items():
    #     if meta["child_table"] != table:
    #         continue
    #     try:
    #         sdf = _rel_sheet(table, rule_id, df, all_frames, parent_frames, categories)
    #         if not sdf.empty:
    #             sdf["issue_type"] = REL_META.get(rule_id, {}).get("name", rule_id)
    #             chunks.append(sdf)
    #     except Exception:
    #         pass

    if not chunks:
        return pd.DataFrame()

    combined = pd.concat(chunks, ignore_index=True)
    return combined


def _enrich_csv_df(df: pd.DataFrame, table: str, categories: dict) -> pd.DataFrame:
    """
    Add stakeholder_name + category_type columns (from le_book_categories),
    then reorder to match TABLE_CSV_COLS template order.
    """
    if df.empty:
        return df

    cat_name = {str(lb): (info.get("name") or str(lb)).title()
                for lb, info in categories.items()}
    cat_type = {str(lb): (info.get("category_type") or "")
                for lb, info in categories.items()}

    df = df.copy()
    df["stakeholder_name"] = df["le_book"].astype(str).map(
        lambda lb: cat_name.get(lb, lb))
    df["category_type"]    = df["le_book"].astype(str).map(
        lambda lb: cat_type.get(lb, ""))

    # reorder: keep only columns in the template set, in template order;
    # append any extra columns before issue_type
    template = TABLE_CSV_COLS.get(table, [])
    fixed    = [c for c in template if c != "issue_type" and c in df.columns]
    extra    = [c for c in df.columns
                if c not in set(template) and c not in ("institution", "issue_type")]
    ordered  = fixed + extra + (["issue_type"] if "issue_type" in df.columns else [])
    return df[[c for c in ordered if c in df.columns]]


def export_csv_reports(
    dataframes: dict,
    le_book_categories: dict,
    valid_le_books: frozenset,
    output_dir: Path,
    parent_dataframes: dict | None = None,
    mask_caches: dict | None = None,
) -> int:
    """
    Write one CSV per institution per table.

    Filename: {table}_{le_book}_{YYYY-MM}.csv
    Columns:  le_book | stakeholder_name | category_type | <table cols> | issue_type

    Returns total number of CSV files written.
    """
    import gc

    csv_dir = Path(output_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)

    parent_frames   = parent_dataframes or dataframes
    run_month       = datetime.now().strftime("%Y-%m")
    categories      = le_book_categories
    n_files         = 0
    valid_lb_strs   = {str(lb) for lb in valid_le_books} if valid_le_books else None

    if not dataframes:
        log.warning("No dataframes -- skipping CSV export.")
        return 0

    log.info("Writing per-institution CSV reports -> %s", csv_dir)

    for table in ALL_TABLES:
        df = dataframes.get(table, pd.DataFrame())
        if df.empty:
            continue

        if valid_lb_strs and "le_book" in df.columns:
            df = df[df["le_book"].astype(str).isin(valid_lb_strs)].copy()
        if df.empty:
            continue

        tbl_cache = mask_caches.get(table) if mask_caches is not None else None
        combined  = _collect_failing_df(table, df, dataframes, parent_frames,
                                        categories, mask_cache=tbl_cache)
        if combined.empty:
            log.info("  -- %-30s  no failing records", table)
            continue

        combined = _enrich_csv_df(combined, table, categories)

        # split by institution and write one CSV per le_book
        for lb, inst_df in combined.groupby("le_book"):
            lb_str = str(lb).strip()
            fname  = f"{table}_{lb_str}_{run_month}.csv"
            path   = csv_dir / fname
            inst_df.to_csv(path, index=False, encoding="utf-8-sig")
            n_issues = len(inst_df["issue_type"].unique()) if "issue_type" in inst_df else 0
            n_files += 1
            log.info("  \u2713 %-6s  %-28s  %5d rows  %d issue type(s)  -> %s",
                     lb_str, table, len(inst_df), n_issues, fname)

        gc.collect()

    log.info("CSV export complete -- %d file(s) written.", n_files)
    return n_files

