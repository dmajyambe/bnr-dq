"""
Data integrity tests for dq_history.json.
Validates that the refactor (relationship merged into accuracy) is consistent
across all stored history entries.
"""
import json
import pytest
from pathlib import Path

HISTORY_FILE = Path(__file__).parent.parent / "dq_history.json"
DIMS_4 = {"completeness", "accuracy", "timeliness", "validity"}


@pytest.fixture(scope="module")
def history():
    assert HISTORY_FILE.exists(), "dq_history.json not found"
    data = json.loads(HISTORY_FILE.read_text())
    assert isinstance(data, list) and len(data) > 0
    return data


class TestHistoryStructure:
    def test_has_entries(self, history):
        assert len(history) >= 1

    def test_each_entry_has_required_keys(self, history):
        for i, entry in enumerate(history):
            for key in ("date", "overall", "by_category", "by_institution"):
                assert key in entry, f"Entry {i} missing key '{key}'"

    def test_dates_are_unique(self, history):
        dates = [e["date"] for e in history]
        assert len(dates) == len(set(dates)), "Duplicate dates in history"

    def test_dates_in_chronological_order(self, history):
        dates = [e["date"] for e in history]
        assert dates == sorted(dates), "History entries not in chronological order"


class TestHistoryNoDimensionRelationship:
    def test_no_relationship_in_overall(self, history):
        for entry in history:
            assert "relationship" not in entry["overall"], \
                f"Date {entry['date']}: 'relationship' found in overall"

    def test_no_relationship_in_by_category(self, history):
        for entry in history:
            for ct, scores in entry.get("by_category", {}).items():
                assert "relationship" not in scores, \
                    f"Date {entry['date']}, category {ct}: 'relationship' found"

    def test_no_relationship_in_by_institution(self, history):
        for entry in history:
            for lb, inst in entry.get("by_institution", {}).items():
                assert "relationship" not in inst, \
                    f"Date {entry['date']}, inst {lb}: 'relationship' found"


class TestHistoryFourDimensions:
    def test_overall_has_exactly_four_dims(self, history):
        for entry in history:
            actual = set(entry["overall"].keys())
            assert actual == DIMS_4, \
                f"Date {entry['date']}: overall has dims {actual}"

    def test_by_category_entries_have_four_dims(self, history):
        for entry in history:
            for ct, scores in entry.get("by_category", {}).items():
                actual = set(scores.keys())
                assert actual == DIMS_4, \
                    f"Date {entry['date']}, cat {ct}: dims are {actual}"

    def test_by_institution_entries_have_four_dim_scores(self, history):
        for entry in history:
            for lb, inst in entry.get("by_institution", {}).items():
                for dim in DIMS_4:
                    assert dim in inst, \
                        f"Date {entry['date']}, inst {lb}: missing dim '{dim}'"


class TestHistoryScoreSanity:
    def test_all_scores_in_0_100_range(self, history):
        for entry in history:
            for dim, score in entry["overall"].items():
                assert 0.0 <= score <= 100.0, \
                    f"Date {entry['date']}: {dim}={score} out of [0,100]"
            for lb, inst in entry["by_institution"].items():
                for dim in DIMS_4:
                    s = inst.get(dim, 0.0)
                    assert 0.0 <= s <= 100.0, \
                        f"Date {entry['date']}, inst {lb}, {dim}={s} out of [0,100]"

    def test_institution_overall_is_mean_of_four_dims(self, history):
        for entry in history:
            for lb, inst in entry.get("by_institution", {}).items():
                dim_vals = [inst.get(d, 0.0) for d in DIMS_4]
                expected = round(sum(dim_vals) / 4, 2)
                actual   = round(inst.get("overall", 0.0), 2)
                assert abs(actual - expected) < 0.1, \
                    f"Date {entry['date']}, inst {lb}: overall={actual}, expected≈{expected}"

    def test_institutions_have_valid_category_types(self, history):
        valid_types = {"B", "MF", "SACCO", "OSACCO"}
        for entry in history:
            for lb, inst in entry.get("by_institution", {}).items():
                ct = inst.get("category_type", "")
                assert ct in valid_types, \
                    f"Date {entry['date']}, inst {lb}: unknown category_type '{ct}'"

    def test_institutions_have_names(self, history):
        for entry in history:
            for lb, inst in entry.get("by_institution", {}).items():
                assert inst.get("name"), \
                    f"Date {entry['date']}, inst {lb}: missing name"


class TestHistoryCategoryConsistency:
    def test_institution_categories_match_by_category_keys(self, history):
        for entry in history:
            inst_types = {
                inst["category_type"]
                for inst in entry["by_institution"].values()
                if inst.get("category_type")
            }
            cat_keys = set(entry.get("by_category", {}).keys())
            # every category_type in institutions should have a by_category entry
            assert inst_types == cat_keys, \
                f"Date {entry['date']}: inst types {inst_types} != by_category keys {cat_keys}"
