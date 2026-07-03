"""
Unit tests for the pure-Python history-aggregation functions, now split across
dq/scoring/calculator.py (merge_rel, inst_scores_from_report) and
dq/reports/history.py (build_history_entry) — formerly all in
dq_monthly_detection.py. No database connection required — DB calls are mocked.
"""
import pytest

from dq.reports.history import build_history_entry
from dq.scoring.calculator import inst_scores_from_report, merge_rel


# ── _merge_rel ─────────────────────────────────────────────────────────────────

class TestMergeRel:
    def test_averages_accuracy_and_rel(self):
        scores = {"completeness": 90.0, "accuracy": 100.0,
                  "timeliness": 95.0, "validity": 88.0, "_rel": 0.0}
        result = merge_rel(scores)
        assert result["accuracy"] == 50.0   # (100 + 0) / 2

    def test_removes_rel_key(self):
        scores = {"accuracy": 80.0, "_rel": 60.0}
        result = merge_rel(scores)
        assert "_rel" not in result
        assert "relationship" not in result

    def test_no_rel_key_returns_unchanged(self):
        scores = {"completeness": 90.0, "accuracy": 80.0}
        result = merge_rel(scores)
        assert result == {"completeness": 90.0, "accuracy": 80.0}

    def test_rel_zero_halves_accuracy(self):
        scores = {"accuracy": 100.0, "_rel": 0.0}
        result = merge_rel(scores)
        assert result["accuracy"] == 50.0

    def test_both_perfect_stays_perfect(self):
        scores = {"accuracy": 100.0, "_rel": 100.0}
        result = merge_rel(scores)
        assert result["accuracy"] == 100.0

    def test_missing_accuracy_key(self):
        scores = {"_rel": 80.0, "completeness": 90.0}
        result = merge_rel(scores)
        # accuracy treated as 0.0 when missing
        assert result["accuracy"] == 40.0   # (0 + 80) / 2

    def test_rounding_to_two_decimals(self):
        scores = {"accuracy": 100.0, "_rel": 1.0}
        result = merge_rel(scores)
        assert result["accuracy"] == 50.5


# ── _inst_scores_from_report ───────────────────────────────────────────────────

class TestInstScoresFromReport:
    def test_extracts_per_institution_score(self):
        report = {
            "tables": {
                "accounts": {
                    "status": "evaluated",
                    "le_book_breakdown": {
                        "040": {"completeness_score": 88.0},
                        "010": {"completeness_score": 92.0},
                    },
                }
            }
        }
        result = inst_scores_from_report(report, "completeness_score")
        assert result["040"] == 88.0
        assert result["010"] == 92.0

    def test_averages_across_multiple_tables(self):
        report = {
            "tables": {
                "accounts":            {"status": "evaluated",
                                        "le_book_breakdown": {"040": {"accuracy_score": 80.0}}},
                "customers_expanded":  {"status": "evaluated",
                                        "le_book_breakdown": {"040": {"accuracy_score": 60.0}}},
            }
        }
        result = inst_scores_from_report(report, "accuracy_score")
        assert result["040"] == 70.0   # (80 + 60) / 2

    def test_skips_non_evaluated_tables(self):
        report = {
            "tables": {
                "accounts": {"status": "skipped",
                             "le_book_breakdown": {"040": {"accuracy_score": 99.0}}},
            }
        }
        result = inst_scores_from_report(report, "accuracy_score")
        assert "040" not in result

    def test_empty_report_returns_empty(self):
        assert inst_scores_from_report({}, "accuracy_score") == {}

    def test_missing_score_key_ignored(self):
        report = {
            "tables": {
                "accounts": {
                    "status": "evaluated",
                    "le_book_breakdown": {"040": {"other_score": 99.0}},
                }
            }
        }
        result = inst_scores_from_report(report, "accuracy_score")
        assert "040" not in result


# ── _build_history_entry ───────────────────────────────────────────────────────

class TestBuildHistoryEntry:
    def test_produces_four_dims_no_relationship(
            self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        assert set(entry["overall"].keys()) == {
            "completeness", "accuracy", "timeliness", "validity"}
        assert "relationship" not in entry["overall"]

    def test_rel_merged_into_accuracy_overall(
            self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        # comp acc=80, rel=60 → merged = (80+60)/2 = 70
        assert entry["overall"]["accuracy"] == 70.0

    def test_institution_overall_is_mean_of_four_dims(
            self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        inst = entry["by_institution"]["040"]
        expected_overall = round(
            (inst["completeness"] + inst["accuracy"] +
             inst["timeliness"]  + inst["validity"]) / 4, 2)
        assert inst["overall"] == expected_overall

    def test_institution_rel_merged_into_accuracy(
            self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        # inst 040: acc_score=100, ri_score=0.48 → (100+0.48)/2 = 50.24
        assert entry["by_institution"]["040"]["accuracy"] == pytest.approx(50.24, abs=0.1)

    def test_no_relationship_key_in_institutions(
            self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        for lb, inst in entry["by_institution"].items():
            assert "relationship" not in inst, f"relationship found in inst {lb}"
            assert "_rel" not in inst

    def test_category_grouping(self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        # both institutions are B → only B in by_category
        assert "B" in entry["by_category"]
        assert entry["by_category"]["B"]["completeness"] > 0

    def test_empty_engine_results_handled_gracefully(self, sample_categories):
        empty_R = {k: {} for k in ("comp", "acc", "tim", "val", "rel")}
        entry = build_history_entry("2026-05-13", empty_R, sample_categories)
        assert entry["overall"]["completeness"] == 0.0
        assert entry["by_institution"] == {}

    def test_date_stored_correctly(self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-01-15", mock_engine_results, sample_categories)
        assert entry["date"] == "2026-01-15"

    def test_institution_name_and_category_type(
            self, mock_engine_results, sample_categories):
        entry = build_history_entry(
            "2026-05-13", mock_engine_results, sample_categories)
        inst = entry["by_institution"]["040"]
        assert inst["name"] == "bank of kigali plc"
        assert inst["category_type"] == "B"


