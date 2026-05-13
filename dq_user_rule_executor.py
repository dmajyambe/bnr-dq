"""
dq_user_rule_executor.py — Runs approved user-defined rules from PostgreSQL
(dqp.dq_user_rules, status='pending') against pre-loaded DataFrames.

Supported check_type values and their check_params (JSON):
  not_null        — field must not be null                        (no params)
  positive        — numeric field > 0                             (no params)
  non_negative    — numeric field >= 0                            (no params)
  date_not_future — date field must not be in the future          (no params)
  domain          — field value in an allowed set                 {"values": ["A","B","C"]}
  range           — numeric field between min and max             {"min": 0, "max": 100}
  pattern         — field matches a regex                         {"pattern": "^[A-Z]{3}$"}
  sql_condition   — pandas df.query() expression for failing rows {"condition": "col > 0"}
  description     — documentation-only; skipped by pipeline       (no params)

Only rules with status='pending' are executed. Draft rules are ignored until
an admin approves them through the dashboard.
"""
from __future__ import annotations
import json
import logging
import re
from pathlib import Path

import pandas as pd

log = logging.getLogger("dq_user_rules")


def _pct(valid: int, total: int) -> float:
    return round(valid / total * 100, 2) if total else 100.0


def run_user_rule(rule: dict, df: pd.DataFrame) -> dict | None:
    """
    Execute one user-defined rule on *df*.
    Returns {"valid": int, "invalid": int, "total": int, "score": float} or None
    if the rule cannot be evaluated (wrong table, missing column, etc.).
    """
    if df.empty:
        return None

    check_type  = (rule.get("check_type") or "").strip()
    fields_raw  = (rule.get("fields") or "").strip()
    col         = fields_raw.split(",")[0].strip() if fields_raw else ""

    params: dict = {}
    if rule.get("check_params"):
        try:
            params = json.loads(rule["check_params"])
        except (ValueError, TypeError):
            pass

    # ── not_null ───────────────────────────────────────────────────────────────
    if check_type == "not_null":
        if col not in df.columns:
            return None
        total = len(df)
        valid = int(df[col].notna().sum())
        return _result(valid, total)

    # ── positive ───────────────────────────────────────────────────────────────
    if check_type == "positive":
        if col not in df.columns:
            return None
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            return None
        valid = int((s > 0).sum())
        return _result(valid, len(s))

    # ── non_negative ───────────────────────────────────────────────────────────
    if check_type == "non_negative":
        if col not in df.columns:
            return None
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            return None
        valid = int((s >= 0).sum())
        return _result(valid, len(s))

    # ── date_not_future ────────────────────────────────────────────────────────
    if check_type == "date_not_future":
        if col not in df.columns:
            return None
        dt = pd.to_datetime(df[col], errors="coerce", utc=False)
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_localize(None)
        dt = dt.dropna()
        if dt.empty:
            return None
        today = pd.Timestamp.today().normalize()
        valid = int((dt <= today).sum())
        return _result(valid, len(dt))

    # ── domain ─────────────────────────────────────────────────────────────────
    if check_type == "domain":
        if col not in df.columns:
            return None
        allowed = {str(v).strip() for v in params.get("values", [])}
        if not allowed:
            return None
        s = df[col].dropna().astype(str).str.strip()
        if s.empty:
            return None
        valid = int(s.isin(allowed).sum())
        return _result(valid, len(s))

    # ── range ──────────────────────────────────────────────────────────────────
    if check_type == "range":
        if col not in df.columns:
            return None
        lo = params.get("min")
        hi = params.get("max")
        if lo is None or hi is None:
            return None
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            return None
        valid = int(((s >= float(lo)) & (s <= float(hi))).sum())
        return _result(valid, len(s))

    # ── pattern ────────────────────────────────────────────────────────────────
    if check_type == "pattern":
        if col not in df.columns:
            return None
        pat = params.get("pattern", "")
        if not pat:
            return None
        try:
            regex = re.compile(pat)
        except re.error:
            log.warning("Invalid regex in rule %s: %s", rule.get("rule_id"), pat)
            return None
        s = df[col].dropna().astype(str).str.strip()
        if s.empty:
            return None
        valid = int(s.str.match(regex).sum())
        return _result(valid, len(s))

    # ── sql_condition ─────────────────────────────────────────────────────────
    # A pandas-compatible df.query() expression that identifies FAILING rows.
    if check_type == "sql_condition":
        condition = params.get("condition", "").strip()
        if not condition:
            return None
        try:
            failing = df.query(condition)
            total   = len(df)
            invalid = len(failing)
            return _result(total - invalid, total)
        except Exception as exc:
            log.warning("sql_condition eval failed for rule %s: %s",
                        rule.get("rule_id"), exc)
            return None

    # ── description ───────────────────────────────────────────────────────────
    # Documentation-only rule; requires manual evaluation — skip execution.
    if check_type == "description":
        return None

    log.warning("Unknown check_type '%s' for rule %s", check_type, rule.get("rule_id"))
    return None


def _result(valid: int, total: int) -> dict:
    return {"valid": valid, "invalid": total - valid,
            "total": total, "score": _pct(valid, total)}


def run_all_user_rules(dataframes: dict, valid_le_books: frozenset,
                       db_path: Path | None = None) -> dict:
    """
    Run all PENDING user rules against the pre-loaded DataFrames.
    Draft rules are skipped — they must be approved by an admin first.
    Updates each rule's status and returns a summary dict.
    Called by dq_pipeline_2m.py after the main engines finish.
    """
    from dq_rules import get_user_rules, mark_user_rule_run

    # Only run rules in 'pending' status; draft rules wait for admin approval.
    rules = get_user_rules(status="pending", db_path=db_path)
    if not rules:
        return {}

    summary: dict = {}
    for rule in rules:
        table = (rule.get("tables") or "").strip()
        df    = dataframes.get(table, pd.DataFrame())

        # apply le_book filter the same way the main engines do
        if not df.empty and valid_le_books and "le_book" in df.columns:
            df = df[df["le_book"].isin(valid_le_books)]

        result = run_user_rule(rule, df)
        rid    = rule["rule_id"]

        if result:
            new_status = "active"
            log.info("  %s  score=%.2f%%  invalid=%d / %d",
                     rid, result["score"], result["invalid"], result["total"])
        elif df.empty:
            new_status = "pending"   # table not loaded this window; keep pending
            log.info("  %s  skipped — no data for table '%s'", rid, table)
        else:
            new_status = "error"     # table present but check couldn't run (missing col etc.)
            log.warning("  %s  could not be evaluated (check column/params)", rid)

        mark_user_rule_run(rid, new_status, db_path=db_path)

        summary[rid] = {
            "rule_id":   rid,
            "rule_name": rule["rule_name"],
            "dimension": rule["dimension"],
            "table":     table,
            "status":    new_status,
            "result":    result,
        }

    log.info("User rules run: %d total, %d active, %d skipped/error",
             len(rules),
             sum(1 for v in summary.values() if v["status"] == "active"),
             sum(1 for v in summary.values() if v["status"] != "active"))
    return summary
