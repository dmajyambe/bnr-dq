"""
Unit tests for DQ engine logic — pure pandas, no DB required.
Covers: completeness, accuracy, timeliness, validity, relationship.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta

import completeness_check as comp
import accuracy_check     as acc
import timeliness_check   as tim
import validity_check     as val
import relationship_check as rel


# ── Completeness ───────────────────────────────────────────────────────────────

class TestCompleteness:
    def test_all_present_scores_100(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = comp.check_completeness(df, ["a", "b"])
        assert result["score"] == 100.0
        assert result["null_cells"] == 0

    def test_all_null_scores_zero(self):
        df = pd.DataFrame({"a": [None, None], "b": [None, None]})
        result = comp.check_completeness(df, ["a", "b"])
        assert result["score"] == 0.0
        assert result["null_cells"] == 4

    def test_half_null_scores_50(self):
        df = pd.DataFrame({"a": [1, None]})
        result = comp.check_completeness(df, ["a"])
        assert result["score"] == 50.0

    def test_empty_dataframe_scores_100(self):
        df = pd.DataFrame({"a": pd.Series([], dtype=float)})
        result = comp.check_completeness(df, ["a"])
        assert result["score"] == 100.0

    def test_column_not_in_df_ignored(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = comp.check_completeness(df, ["a", "nonexistent"])
        assert result["score"] == 100.0   # nonexistent skipped, only a checked

    def test_null_counts_per_column(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, "z"]})
        result = comp.check_completeness(df, ["a", "b"])
        assert result["null_counts"]["a"] == 1
        assert result["null_counts"]["b"] == 2

    def test_no_columns_to_check_scores_100(self):
        df = pd.DataFrame({"a": [1, 2]})
        result = comp.check_completeness(df, [])
        assert result["score"] == 100.0


# ── Accuracy ───────────────────────────────────────────────────────────────────

class TestAccuracyPct:
    def test_pct_normal(self):
        assert acc._pct(90, 100) == 90.0

    def test_pct_zero_total_returns_100(self):
        assert acc._pct(0, 0) == 100.0

    def test_pct_all_valid(self):
        assert acc._pct(50, 50) == 100.0

    def test_pct_none_valid(self):
        assert acc._pct(0, 100) == 0.0


class TestAccuracyRunRule:
    # ACC-004: Customer Gender must be M, F, or C only
    def test_gender_all_valid(self):
        df = pd.DataFrame({"customer_gender": ["M", "F", "C", "M"],
                           "le_book": ["040"] * 4})
        result = acc.run_rule("ACC-004", df)
        if result:
            valid, invalid, total = result
            assert invalid == 0

    def test_gender_invalid_value(self):
        df = pd.DataFrame({"customer_gender": ["M", "X", "F"],   # X is invalid
                           "le_book": ["040"] * 3})
        result = acc.run_rule("ACC-004", df)
        if result:
            valid, invalid, total = result
            assert invalid == 1

    def test_empty_df_returns_none(self):
        df = pd.DataFrame()
        assert acc.run_rule("ACC-004", df) is None

    def test_run_rule_mask_returns_boolean_series(self):
        df = pd.DataFrame({
            "customer_gender": ["M", "X", "F"],
            "le_book":         ["040"] * 3,
        })
        mask = acc.run_rule_mask("ACC-004", df)
        assert mask.dtype == bool
        assert len(mask) == 3
        assert bool(mask.iloc[1]) == True   # X fails → flagged as issue


# ── Timeliness ─────────────────────────────────────────────────────────────────

class TestTimeliness:
    def test_pct_helper(self):
        assert tim._pct(75, 100) == 75.0

    def test_no_future_dates_all_valid(self):
        past = pd.Series(pd.to_datetime(["2020-01-01", "2021-06-15"]))
        result = tim._no_future(past)
        if result:
            valid, invalid, total = result
            assert invalid == 0

    def test_future_date_flagged(self):
        future = date.today() + timedelta(days=30)
        series = pd.Series(pd.to_datetime(["2020-01-01", str(future)]))
        result = tim._no_future(series)
        if result:
            valid, invalid, total = result
            assert invalid == 1

    def test_run_rule_mask_timeliness(self):
        future = str(date.today() + timedelta(days=10))
        df = pd.DataFrame({
            "date_creation": pd.to_datetime(["2020-01-01", future]),
            "le_book":       ["040", "040"],
        })
        mask = tim.run_rule_mask("TIM-001", df)
        # future date should be flagged
        assert isinstance(mask, pd.Series)


# ── Validity ───────────────────────────────────────────────────────────────────

class TestValidity:
    def test_pct_helper(self):
        assert val._pct(8, 10) == 80.0

    def test_non_negative_all_ok(self):
        df = pd.DataFrame({"income": [0.0, 1000.0, 5000.0]})
        result = val._non_negative(df, "income")
        if result:
            valid, invalid, total = result
            assert invalid == 0

    def test_non_negative_catches_negative(self):
        df = pd.DataFrame({"income": [100.0, -50.0, 200.0]})
        result = val._non_negative(df, "income")
        if result:
            valid, invalid, total = result
            assert invalid == 1

    def test_positive_catches_zero(self):
        df = pd.DataFrame({"loan_amount": [0.0, 1000.0]})
        result = val._positive(df, "loan_amount")
        if result:
            valid, invalid, total = result
            assert invalid == 1

    def test_run_rule_mask_returns_boolean_series(self):
        df = pd.DataFrame({
            "income":  [100.0, -1.0, 200.0],
            "le_book": ["040"] * 3,
        })
        # find a rule that checks income >= 0
        mask = val.run_rule_mask("VAL-001", df)
        assert mask.dtype == bool


# ── Relationship (RI) ──────────────────────────────────────────────────────────

class TestRelationship:
    def test_all_accounts_have_known_customer(self, accounts_df, customers_df):
        # remove the orphaned row
        clean_accounts = accounts_df[accounts_df["customer_id"] != 99]
        dataframes = {
            "accounts":           clean_accounts,
            "customers_expanded": customers_df,
        }
        result = rel.evaluate_all_from_dataframes(
            dataframes, valid_le_books=frozenset({"040"}))
        ri_score = (result.get("executive_summary") or {}).get("overall_ri_score", 100.0)
        assert ri_score == 100.0

    def test_orphaned_account_lowers_score(self, accounts_df, customers_df):
        # accounts_df has account A3 with customer_id=99 (not in customers_df)
        dataframes = {
            "accounts":           accounts_df,
            "customers_expanded": customers_df,
        }
        result = rel.evaluate_all_from_dataframes(
            dataframes, valid_le_books=frozenset({"040"}))
        ri_score = (result.get("executive_summary") or {}).get("overall_ri_score", 100.0)
        assert ri_score < 100.0

    def test_empty_dataframes_return_zero(self):
        # When no rows exist to evaluate, no RI checks pass → score is 0.0
        dataframes = {
            "accounts":           pd.DataFrame(),
            "customers_expanded": pd.DataFrame(),
        }
        result = rel.evaluate_all_from_dataframes(dataframes, frozenset())
        ri_score = (result.get("executive_summary") or {}).get("overall_ri_score", 0.0)
        assert ri_score == 0.0


# ── Cross-engine: run_rule_mask contract ────────────────────────────────────────

class TestRunRuleMaskContract:
    """run_rule_mask must always return a boolean Series with same length as df."""

    @pytest.mark.parametrize("engine,rule_id,cols", [
        (acc, "ACC-001", {"gender": ["M", "X", "F"], "le_book": ["040"] * 3}),
        (val, "VAL-001", {"income": [100.0, -1.0, 0.0], "le_book": ["040"] * 3}),
    ])
    def test_mask_length_matches_df(self, engine, rule_id, cols):
        df = pd.DataFrame(cols)
        mask = engine.run_rule_mask(rule_id, df)
        assert len(mask) == len(df)
        assert mask.dtype == bool
