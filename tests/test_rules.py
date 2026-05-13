"""
Tests for the rule registry (dq_rules.py) and SQLite store (dq_rules.db).
"""
import sqlite3
import tempfile
from pathlib import Path

import pytest

import dq_rules


# ── Rule registry content ──────────────────────────────────────────────────────

class TestRuleRegistry:
    @pytest.fixture(autouse=True)
    def rows(self):
        self._rows = dq_rules._build_rows()

    def test_no_relationship_dimension(self):
        dims = {r["dimension"] for r in self._rows}
        assert "relationship" not in dims

    def test_only_four_dimensions(self):
        dims = {r["dimension"] for r in self._rows}
        assert dims == {"completeness", "accuracy", "timeliness", "validity"}

    def test_rel_rules_classified_as_accuracy(self):
        rel_rules = [r for r in self._rows if r["rule_id"].startswith("REL-")]
        assert len(rel_rules) == 8
        for r in rel_rules:
            assert r["dimension"] == "accuracy", \
                f"{r['rule_id']} still has dimension={r['dimension']!r}"

    def test_all_rules_have_required_fields(self):
        for r in self._rows:
            assert r.get("rule_id"),   f"Missing rule_id: {r}"
            assert r.get("dimension"), f"Missing dimension: {r}"
            assert r.get("rule_name"), f"Missing rule_name: {r}"
            assert r.get("tables"),    f"Missing tables: {r}"

    def test_rel_rule_tables_use_arrow_format(self):
        rel_rules = [r for r in self._rows if r["rule_id"].startswith("REL-")]
        for r in rel_rules:
            assert "→" in r["tables"], \
                f"{r['rule_id']} tables field should use '→' format: {r['tables']}"

    def test_total_rule_count(self):
        assert len(self._rows) > 40, \
            f"Expected >40 rules, got {len(self._rows)}"

    def test_rule_ids_are_unique(self):
        ids = [r["rule_id"] for r in self._rows]
        assert len(ids) == len(set(ids)), "Duplicate rule IDs detected"


# ── SQLite store ───────────────────────────────────────────────────────────────

class TestSQLiteStore:
    @pytest.fixture
    def temp_db(self, tmp_path):
        db_path = tmp_path / "test_rules.db"
        dq_rules.ensure_db(db_path)
        return db_path

    def test_ensure_db_creates_table(self, temp_db):
        con = sqlite3.connect(temp_db)
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "dq_rules" in tables

    def test_no_relationship_dim_in_db(self, temp_db):
        con = sqlite3.connect(temp_db)
        count = con.execute(
            "SELECT COUNT(*) FROM dq_rules WHERE dimension='relationship'"
        ).fetchone()[0]
        assert count == 0

    def test_four_dimensions_in_db(self, temp_db):
        con = sqlite3.connect(temp_db)
        dims = {r[0] for r in con.execute(
            "SELECT DISTINCT dimension FROM dq_rules").fetchall()}
        assert dims == {"completeness", "accuracy", "timeliness", "validity"}

    def test_ensure_db_is_idempotent(self, temp_db):
        count_before = sqlite3.connect(temp_db).execute(
            "SELECT COUNT(*) FROM dq_rules").fetchone()[0]
        dq_rules.ensure_db(temp_db)
        count_after = sqlite3.connect(temp_db).execute(
            "SELECT COUNT(*) FROM dq_rules").fetchone()[0]
        assert count_before == count_after

    def test_rel_rules_stored_as_accuracy(self, temp_db):
        con = sqlite3.connect(temp_db)
        rows = con.execute(
            "SELECT dimension FROM dq_rules WHERE rule_id LIKE 'REL-%'"
        ).fetchall()
        assert len(rows) == 8
        for (dim,) in rows:
            assert dim == "accuracy"


# ── User rules CRUD ────────────────────────────────────────────────────────────
# IMPORTANT: dq_rules functions use `db_path: Path = DB_PATH` as a default
# parameter bound at definition time. Monkeypatching DB_PATH does NOT affect
# those defaults. Always pass db_path= explicitly to stay isolated.

class TestUserRules:
    @pytest.fixture
    def temp_db(self, tmp_path):
        db_path = tmp_path / "test_rules.db"
        dq_rules.ensure_db(db_path)
        dq_rules._ensure_user_rules_table(db_path)
        return db_path

    def test_add_and_retrieve_user_rule(self, temp_db):
        dq_rules.add_user_rule({
            "rule_id":      "USR-001",
            "dimension":    "validity",
            "category":     "Range",
            "rule_name":    "Income must be non-negative",
            "tables":       "customers_expanded",
            "fields":       "income",
            "check_type":   "non_negative",
            "check_params": None,
        }, db_path=temp_db)
        rules = dq_rules.get_user_rules(db_path=temp_db)
        ids = [r["rule_id"] for r in rules]
        assert "USR-001" in ids

    def test_user_rule_default_status_is_pending(self, temp_db):
        dq_rules.add_user_rule({
            "rule_id":   "USR-001",
            "dimension": "accuracy",
            "category":  "Format",
            "rule_name": "Test rule",
            "tables":    "accounts",
            "fields":    "account_no",
            "check_type":   "not_null",
            "check_params": None,
        }, db_path=temp_db)
        rules = dq_rules.get_user_rules(db_path=temp_db)
        rule = next(r for r in rules if r["rule_id"] == "USR-001")
        assert rule["status"] == "pending"

    def test_next_user_rule_id_increments(self, temp_db):
        id1 = dq_rules.next_user_rule_id(db_path=temp_db)
        assert id1.startswith("USR-")
        dq_rules.add_user_rule({
            "rule_id": id1, "dimension": "validity",
            "category": "X", "rule_name": "X",
            "tables": "accounts", "fields": "f",
            "check_type": "not_null", "check_params": None,
        }, db_path=temp_db)
        id2 = dq_rules.next_user_rule_id(db_path=temp_db)
        n1 = int(id1.split("-")[1])
        n2 = int(id2.split("-")[1])
        assert n2 > n1

    def test_mark_user_rule_run(self, temp_db):
        dq_rules.add_user_rule({
            "rule_id": "USR-001", "dimension": "completeness",
            "category": "Y", "rule_name": "Y",
            "tables": "accounts", "fields": "g",
            "check_type": "not_null", "check_params": None,
        }, db_path=temp_db)
        dq_rules.mark_user_rule_run("USR-001", "active", db_path=temp_db)
        rules = dq_rules.get_user_rules(db_path=temp_db)
        rule = next(r for r in rules if r["rule_id"] == "USR-001")
        assert rule["status"] == "active"
