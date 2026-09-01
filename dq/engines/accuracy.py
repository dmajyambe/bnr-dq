# Accuracy engine
#creates sql queries for accuracy checks
from __future__ import annotations
import json
import logging
from sqlalchemy import text
from dq.engines.relationship import run_rule as _ri_run_rule
from dq.rules.accuracy import (
    VALID_ACCOUNT_STATUS,
    VALID_PERFORMANCE_CLASS,
    VALID_GENDER,
    VALID_ACCOUNT_TYPE,
    CORPORATE_LEGAL_STATUS,
    ACC_RULE_META,
    ACC_TABLE_RULES,
)
from dq.sql.builders import run_rule_dimension_sql

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("dq.engines.accuracy")

#ps: referential-integrity rules are handled in a separate pass
def _acc_rule_sql(rule_id: str, existing: set) -> tuple[str, str] | None:
    def has(*cols: str) -> bool:
        return all(c in existing for c in cols)

    if rule_id == "ACC-002":            # account_status in allowed numeric codes
        if not has("account_status"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(str(x) for x in VALID_ACCOUNT_STATUS))
        #print(vals)
        return (
            'SUM(CASE WHEN "account_status" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "account_status" IS NOT NULL AND "account_status"::TEXT IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-003":            # performance_class in allowed codes
        if not has("performance_class"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_PERFORMANCE_CLASS))
        return (
            'SUM(CASE WHEN "performance_class" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "performance_class" IS NOT NULL AND UPPER(TRIM("performance_class"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-004":            # customer_gender in allowed codes
        if not has("customer_gender"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_GENDER))
        return (
            'SUM(CASE WHEN "customer_gender" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "customer_gender" IS NOT NULL AND UPPER(TRIM("customer_gender"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-005":            # account_type in allowed codes
        if not has("account_type"):
            return None
        vals = ", ".join(f"'{v}'" for v in sorted(VALID_ACCOUNT_TYPE))
        return (
            'SUM(CASE WHEN "account_type" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "account_type" IS NOT NULL AND UPPER(TRIM("account_type"::TEXT)) IN ({vals}) THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-010":            # if legal_status is corporate, gender must be 'C' (corporate)
        if not has("customer_gender", "legal_status"):
            return None
        corp = ", ".join(f"'{v}'" for v in sorted(str(x) for x in CORPORATE_LEGAL_STATUS))
        return (
            'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "legal_status" IS NOT NULL '
            f'AND ("legal_status"::TEXT NOT IN ({corp}) OR UPPER(TRIM("customer_gender"::TEXT)) = \'C\') THEN 1 ELSE 0 END)',
        )

    if rule_id == "ACC-012":            # if gender is not 'C', marital_status must not be 'NA'
        if not has("customer_gender", "marital_status"):
            return None
        return (
            'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN "customer_gender" IS NOT NULL AND "marital_status" IS NOT NULL '
            'AND (UPPER(TRIM("customer_gender"::TEXT)) = \'C\' OR UPPER(TRIM("marital_status"::TEXT)) <> \'NA\') THEN 1 ELSE 0 END)',
        )

    return None

_REL_RULE_IDS = {rid for rid, m in ACC_RULE_META.items() if "parent_table" in m}
def _merge_ri_rules(conn, schema: str, valid_le_books: frozenset,
                    row_limit: int, extra_where: str,
                    target_tables: set[str], report: dict) -> None:
   
    for rid in sorted(_REL_RULE_IDS):
        meta = ACC_RULE_META[rid]
        table = meta["child_table"]
        if table not in target_tables:
            continue
        try:
            result = _ri_run_rule(rid, meta, conn, schema, valid_le_books,
                                  row_limit, extra_where)
        except Exception as exc:
            log.warning("  %s failed: %s", rid, exc)
            conn.rollback()
            continue
        if result is None:
            continue

        tdata = report["tables"].setdefault(
            table, {"status": "no_data", "row_count": 0, "le_book_breakdown": {}})
        was_evaluated = tdata.get("status") == "evaluated"
        tdata["status"] = "evaluated"
        tdata.setdefault("rules_evaluated", []).append(rid)
        lb_breakdown = tdata.setdefault("le_book_breakdown", {})

        for lb, lb_data in result["le_book_breakdown"].items():
            is_new   = lb not in lb_breakdown
            lb_entry = lb_breakdown.setdefault(lb, {"row_count": lb_data["total"], "rules": {}})
            if is_new:
                lb_entry["row_count"] = lb_data["total"]
            lb_entry["rules"][rid] = {
                "rule_name":      meta["name"],
                "category":       meta["category"],
                "fields":         meta["fields"],
                "valid":          lb_data["valid"],
                "invalid":        lb_data["invalid"],
                "total":          lb_data["total"],
                "accuracy_score": lb_data["ri_score"],
            }

        log.info("  %s  score=%.2f%%  orphans=%d  (of %d)",
                 rid, result["ri_score"], result["invalid"], result["total"])

        if not was_evaluated:
            tdata["row_count"] = sum(e["row_count"] for e in lb_breakdown.values())

    # recompute scores for every table this pass touched (and only those —
    # tables untouched by any RI rule keep whatever run_rule_dimension_sql set).
    touched_tables = {ACC_RULE_META[rid]["child_table"] for rid in _REL_RULE_IDS
                      if ACC_RULE_META[rid]["child_table"] in target_tables}
    for table in touched_tables:
        tdata = report["tables"].get(table)
        if not tdata or tdata.get("status") != "evaluated":
            continue
        lb_breakdown = tdata.get("le_book_breakdown", {})
        for lb_entry in lb_breakdown.values():
            scores = [r["accuracy_score"] for r in lb_entry["rules"].values()]
            lb_entry["accuracy_score"] = round(sum(scores) / len(scores), 2) if scores else 0.0
        if lb_breakdown:
            table_scores = [e["accuracy_score"] for e in lb_breakdown.values()]
            tdata["accuracy_score"] = round(sum(table_scores) / len(table_scores), 2)

    all_scores   = [t["accuracy_score"] for t in report["tables"].values()
                    if t.get("status") == "evaluated" and "accuracy_score" in t]
    all_le_books: set = set()
    for t in report["tables"].values():
        all_le_books.update(t.get("le_book_breakdown", {}).keys())

    report["le_books"] = sorted(all_le_books)
    report["executive_summary"]["overall_accuracy_score"] = (
        round(sum(all_scores) / len(all_scores), 2) if all_scores else 0.0)
    report["executive_summary"]["total_tables"]     = len(report["tables"])
    report["executive_summary"]["evaluated_tables"] = sum(
        1 for t in report["tables"].values() if t.get("status") == "evaluated")

#run the accuracy engine against a database schema, writing the report to a JSON file
def evaluate_from_sql(engine, schema: str, valid_le_books: frozenset,
                      window_days: int, watermarks: dict, output_path: str,
                      row_limit: int = 0,
                      tables: list[str] | None = None,
                      extra_where: str = "") -> dict:
    
    domain_table_rules = {
        t: [r for r in rs if r not in _REL_RULE_IDS]
        for t, rs in ACC_TABLE_RULES.items()
    }
    domain_table_rules = {t: rs for t, rs in domain_table_rules.items() if rs}

    report = run_rule_dimension_sql(
        engine, schema, valid_le_books, window_days, watermarks, output_path,
        row_limit=row_limit, tables=tables,
        table_rules=domain_table_rules, rule_meta=ACC_RULE_META,
        rule_sql_fn=_acc_rule_sql,
        score_key="accuracy_score", overall_key="overall_accuracy_score",
        dim_label="accuracy", extra_where=extra_where,
    )

    target_tables = set(tables) if tables is not None else set(ACC_TABLE_RULES.keys())
    with engine.connect() as conn:
        try:
            
            conn.execute(text("SET work_mem = '512MB'"))
        except Exception:
            conn.rollback()
        _merge_ri_rules(conn, schema, valid_le_books, row_limit, extra_where,
                        target_tables, report)

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("Report written → %s  (overall %.2f%%, incl. referential integrity)",
             output_path, report["executive_summary"]["overall_accuracy_score"])
    return report
