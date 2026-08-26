# Rule registry rollup — flattens every dimension's rule metadata into one
# list, for the dashboard's Validations tab and CSV export, and syncs it into
# a local SQLite table (test isolation only).
#
# Kept deliberately lightweight per review feedback: this should stay a thin
# aggregator over dq/rules/*.py, not a place where unrelated concerns
# accumulate. A previous version of this file also carried ~300 lines of
# commented-out, disabled Postgres user-rules workflow code (ensure_pg_tables,
# add_user_rule, approve_draft_rule, etc.) — that's dropped from the live file
# now; it's fully recoverable from git history if that feature is ever
# revived. Restoring it is a separate decision from keeping this file thin.
#
# NOTE — live bug (found auditing this file, not fixed here — a behavior
# change, not a move): dq_dashboard_dash.py used to call add_user_rule(),
# approve_draft_rule(), and delete_draft_rule() — the Validations tab's
# "submit a custom rule" and admin approve/delete-draft actions — none of
# which exist anymore. That dead UI was removed during the dashboard split
# (see dashboard/pages/validations.py), so the live bug is gone too, but the
# backend functions it called were never restored.
from __future__ import annotations

from pathlib import Path

from dq.rules.completeness import COMP_RULE_META
from dq.rules.accuracy import ACC_RULE_META, ACC_TABLE_RULES
from dq.rules.validity import VAL_RULE_META, VAL_TABLE_RULES
from dq.rules.uniqueness import UNI_RULE_META, UNI_TABLE_RULES
# Timeliness rule metadata is currently empty (see dq/rules/timeliness.py) —
# not imported here because _build_rows() never iterated it. Relationship
# rules are REL-* entries inside ACC_RULE_META (see dq/rules/accuracy.py)
# and come in via the accuracy loop below.

SCRIPT_DIR = Path(__file__).resolve().parents[2]
DB_PATH    = SCRIPT_DIR / "dq_rules.db"   # SQLite — kept for test isolation only


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

    # accuracy (includes REL-* referential-integrity rules — child_table →
    # parent_table, not a list of tables, since each is a fixed FK check)
    for rid, meta in ACC_RULE_META.items():
        if rid.startswith("REL-"):
            tables = f'{meta["child_table"]} → {meta["parent_table"]}'
        else:
            tables = ", ".join(sorted({t for t, rules in ACC_TABLE_RULES.items() if rid in rules}))
        rows.append({
            "rule_id":   rid,
            "dimension": "accuracy",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    tables,
            "fields":    ", ".join(meta["fields"]),
        })

    # uniqueness
    for rid, meta in UNI_RULE_META.items():
        tables = sorted({t for t, rules in UNI_TABLE_RULES.items() if rid in rules})
        rows.append({
            "rule_id":   rid,
            "dimension": "uniqueness",
            "category":  meta["category"],
            "rule_name": meta["name"],
            "tables":    ", ".join(tables),
            "fields":    ", ".join(meta["fields"]),
        })

    # timeliness — disabled, see dq/rules/timeliness.py
    # for rid, meta in TIM_RULE_META.items():
    #     tables = sorted({t for t, rules in TIM_TABLE_RULES.items() if rid in rules})
    #     rows.append({
    #         "rule_id":   rid,
    #         "dimension": "timeliness",
    #         "category":  meta["category"],
    #         "rule_name": meta["name"],
    #         "tables":    ", ".join(tables),
    #         "fields":    ", ".join(meta["fields"]),
    #     })

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

    return rows


def ensure_db(db_path: Path = DB_PATH) -> None:
    """Sync the in-memory rule registry into dq_rules in Greenplum."""
    from storage.postgres.app_db import get_connection
    con = get_connection()
    try:
        rows = _build_rows()
        con.execute("DELETE FROM dq_rules")
        con.executemany(
            """
            INSERT INTO dq_rules (rule_id, dimension, category, rule_name, tables, fields)
            VALUES (%(rule_id)s, %(dimension)s, %(category)s, %(rule_name)s, %(tables)s, %(fields)s)
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


# Seed the SQLite built-in rules table (used by tests and the Validations chart).
ensure_db()
