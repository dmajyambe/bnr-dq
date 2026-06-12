from __future__ import annotations

import logging
import pandas as pd
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text, inspect
from dq_rules import VAL_RULE_META
log = logging.getLogger("dq_validity")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
_thread_local = threading.local()

def log_table(table: str, msg: str):
    log.info(f"[{table}] {msg}")


ACTIVE_TABLE_RULES = {
    "customers_expanded": "VAL-001",
    "accounts": "VAL-003",
    "contracts_disburse": "VAL-012",
    "contract_loans": "VAL-016",
    "contract_schedules": "VAL-013",
    "contracts_expanded": "VAL-010",
    "loan_applications_2": "VAL-021",
}

#schema validation

def validate_schema(engine, schema: str, table: str, required_cols: list[str]) -> bool:
    try:
        inspector = inspect(engine)

        if table not in inspector.get_table_names(schema=schema):
            log_table(table, "Missing table in schema")
            return False

        cols = inspector.get_columns(table, schema=schema)
        existing = {c["name"].lower() for c in cols}

        missing = [c for c in required_cols if c.lower() not in existing]

        if missing:
            log_table(table, f"Missing columns: {missing}")
            return False

        log_table(table, "✅ Schema validation passed")
        return True

    except Exception as e:
        log_table(table, f"Schema error: {e}")
        return False



def fetch_table(engine, schema: str, table: str, limit: int = 100000) -> pd.DataFrame:
    try:
        sql = f'SELECT * FROM "{schema}"."{table}"'
        if limit:
            sql += f" LIMIT {limit}"

        df = pd.read_sql(text(sql), engine)
        df.columns = [c.lower() for c in df.columns]

        return df

    except Exception as e:
        log_table(table, f"Fetch failed: {e}")
        return pd.DataFrame()



def run_sql_rule(engine, schema: str, table: str, total_expr: str, valid_expr: str):

    log_table(table, "⚡ Executing SQL pushdown")

    sql = f"""
    SELECT
        ({total_expr}) AS total,
        ({valid_expr}) AS valid
    FROM "{schema}"."{table}"
    """

    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()

    if not row or row["total"] == 0:
        log_table(table, "⚠️ No data returned from SQL")
        return None

    total = row["total"]
    valid = row["valid"]

    log_table(table, f"📊 SQL result → valid={valid}, invalid={total-valid}, total={total}")

    return valid, total - valid, total



def sql_val_001():
    return (
        "SUM(CASE WHEN email_id IS NOT NULL THEN 1 ELSE 0 END)",
        "SUM(CASE WHEN email_id ~ '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$' THEN 1 ELSE 0 END)"
    )

def sql_val_003():
    return (
        "SUM(CASE WHEN currency IS NOT NULL THEN 1 ELSE 0 END)",
        "SUM(CASE WHEN currency ~ '^[A-Z]{3}$' THEN 1 ELSE 0 END)"
    )

def sql_val_012():
    return (
        """
        SUM(CASE WHEN current_disbursed_amt IS NOT NULL
              OR previous_disbursed_amt IS NOT NULL
        THEN 1 ELSE 0 END)
        """,
        """
        SUM(CASE WHEN (current_disbursed_amt IS NULL OR current_disbursed_amt >= 0)
              AND (previous_disbursed_amt IS NULL OR previous_disbursed_amt >= 0)
        THEN 1 ELSE 0 END)
        """
    )

def sql_val_016():
    return (
        "SUM(CASE WHEN num_of_instalments IS NOT NULL THEN 1 ELSE 0 END)",
        "SUM(CASE WHEN num_of_instalments IS NOT NULL AND num_of_instalments >= 1 THEN 1 ELSE 0 END)"
    )

def sql_val_013():
    return (
        "SUM(CASE WHEN emi_amount IS NOT NULL THEN 1 ELSE 0 END)",
        "SUM(CASE WHEN emi_amount IS NOT NULL AND emi_amount > 0 THEN 1 ELSE 0 END)"
    )

def sql_val_010():
    return (
        "SUM(CASE WHEN interest_rate_dr IS NOT NULL THEN 1 ELSE 0 END)",
        "SUM(CASE WHEN interest_rate_dr BETWEEN 0 AND 100 THEN 1 ELSE 0 END)"
    )

def sql_val_021():
    return (
        "SUM(CASE WHEN approved_amount_lcy IS NOT NULL AND applied_amount_lcy IS NOT NULL THEN 1 ELSE 0 END)",
        "SUM(CASE WHEN approved_amount_lcy <= applied_amount_lcy THEN 1 ELSE 0 END)"
    )


SQL_RULES = {
    "customers_expanded": {"VAL-001": sql_val_001},
    "accounts": {"VAL-003": sql_val_003},
    "contracts_disburse": {"VAL-012": sql_val_012},
    "contract_loans": {"VAL-016": sql_val_016},
    "contract_schedules": {"VAL-013": sql_val_013},
    "contracts_expanded": {"VAL-010": sql_val_010},
    "loan_applications_2": {"VAL-021": sql_val_021},
}

#table evaluation engine

def evaluate_table(engine, schema: str, table: str, rule_id: str, limit: int = 100000):

    start = time.time()

    log_table(table, f"🚀 Starting evaluation | rule={rule_id}")

    meta = VAL_RULE_META[rule_id]

    # schema validation
    if not validate_schema(engine, schema, table, meta["fields"]):
        log_table(table, "⛔ Schema invalid → skipping")
        return {"table": table, "status": "schema_invalid", "rule": rule_id}

    sql_rule = SQL_RULES.get(table, {}).get(rule_id)

    if sql_rule:
        log_table(table, "⚡ SQL pushdown path selected")

        total_expr, valid_expr = sql_rule()
        result = run_sql_rule(engine, schema, table, total_expr, valid_expr)

        if result:
            valid, invalid, total = result
            score = round(valid / total * 100, 2)

            log_table(table, f"✅ Completed → score={score}% in {time.time()-start:.2f}s")

            return {
                "table": table,
                "rule": rule_id,
                "rule_name": meta["name"],
                "status": "evaluated_sql",
                "valid": valid,
                "invalid": invalid,
                "total": total,
                "score": score
            }

    log_table(table, "No SQL rule found → fallback triggered")

    df = fetch_table(engine, schema, table, limit)

    if df.empty:
        log_table(table, " No data loaded")
        return {"table": table, "status": "no_data"}

    log_table(table, "Fallback not implemented")

    return {
        "table": table,
        "status": "no_sql_rule_available",
        "message": "Fallback path not implemented for this rule yet"
    }


#cli

def run_all_parallel(engine, schema: str, limit: int = 100000, max_workers: int = 6):

    log.info(f"Starting DQ parallel execution | tables={len(ACTIVE_TABLE_RULES)}")

    results = {}

    def task(table, rule_id):
        log.info(f"Thread picked → {table} ({rule_id})")
        return table, evaluate_table(engine, schema, table, rule_id, limit)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = [
            executor.submit(task, table, rule_id)
            for table, rule_id in ACTIVE_TABLE_RULES.items()
        ]

        for f in as_completed(futures):
            table, result = f.result()
            results[table] = result

    log.info("DQ validation completed for all tables")

    return results