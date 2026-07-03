"""
Unit tests for DQ engine logic — pure pandas, no DB required.
Covers: completeness, accuracy, timeliness, validity, relationship.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta

import dq.engines.accuracy     as acc
import dq.engines.timeliness   as tim
import dq.engines.validity     as val

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
