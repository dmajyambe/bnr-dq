"""
Shared fixtures and path setup for all test modules.
"""
import sys
from pathlib import Path

# Make the dashboard package importable from any working directory
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import pytest
import pandas as pd


# ---------------------------------------------------------------------------
# Minimal history entry (4 dims, no relationship)
# ---------------------------------------------------------------------------

@pytest.fixture
def history_entry():
    return {
        "date": "2026-05-13",
        "overall": {
            "completeness": 88.0,
            "accuracy":     72.0,
            "timeliness":   95.0,
            "validity":     91.0,
        },
        "by_category": {
            "B": {
                "completeness": 92.0,
                "accuracy":     80.0,
                "timeliness":   97.0,
                "validity":     95.0,
            },
            "MF": {
                "completeness": 85.0,
                "accuracy":     65.0,
                "timeliness":   93.0,
                "validity":     88.0,
            },
            "OSACCO": {
                "completeness": 80.0,
                "accuracy":     60.0,
                "timeliness":   90.0,
                "validity":     82.0,
            },
        },
        "by_institution": {
            "040": {
                "name": "bank of kigali plc",
                "category_type": "B",
                "overall": 91.25,
                "completeness": 91.29,
                "accuracy":     50.24,
                "timeliness":  100.0,
                "validity":     94.99,
            },
            "010": {
                "name": "i&m bank rwanda plc",
                "category_type": "B",
                "overall": 87.0,
                "completeness": 90.0,
                "accuracy":     80.0,
                "timeliness":   90.0,
                "validity":     88.0,
            },
            "421": {
                "name": "jali sc plc",
                "category_type": "MF",
                "overall": 82.0,
                "completeness": 85.0,
                "accuracy":     70.0,
                "timeliness":   88.0,
                "validity":     85.0,
            },
            "917": {
                "name": "umwalimusacco",
                "category_type": "OSACCO",
                "overall": 75.0,
                "completeness": 80.0,
                "accuracy":     60.0,
                "timeliness":   85.0,
                "validity":     75.0,
            },
        },
    }


@pytest.fixture
def yesterday_entry(history_entry):
    """Previous day — slightly lower scores for delta testing."""
    import copy, json
    entry = copy.deepcopy(history_entry)
    entry["date"] = "2026-05-12"
    for inst in entry["by_institution"].values():
        for dim in ["completeness", "accuracy", "timeliness", "validity"]:
            inst[dim] = round(inst[dim] - 2.0, 2)
    return entry


@pytest.fixture
def mock_engine_results():
    """
    Simulates the R dict returned by _run_parallel in dq_pipeline_2m.
    Keys: comp, acc, tim, val, rel — each structured as an engine report.
    """
    def _make_report(overall_key, overall_score, lb_score_key, lb_scores):
        return {
            "executive_summary": {overall_key: overall_score},
            "tables": {
                "accounts": {
                    "status": "evaluated",
                    "le_book_breakdown": {
                        lb: {lb_score_key: score}
                        for lb, score in lb_scores.items()
                    },
                }
            },
        }

    return {
        "comp": _make_report(
            "overall_completeness_score", 90.0,
            "completeness_score", {"040": 91.0, "010": 89.0}
        ),
        "acc": _make_report(
            "overall_accuracy_score", 80.0,
            "accuracy_score", {"040": 100.0, "010": 60.0}
        ),
        "tim": _make_report(
            "overall_timeliness_score", 95.0,
            "timeliness_score", {"040": 100.0, "010": 90.0}
        ),
        "val": _make_report(
            "overall_validity_score", 88.0,
            "validity_score", {"040": 95.0, "010": 81.0}
        ),
        "rel": _make_report(
            "overall_ri_score", 60.0,
            "ri_score", {"040": 0.48, "010": 100.0}
        ),
    }


@pytest.fixture
def sample_categories():
    return {
        "040": {"name": "bank of kigali plc",  "category_type": "B"},
        "010": {"name": "i&m bank rwanda plc", "category_type": "B"},
    }


@pytest.fixture
def customers_df():
    """Minimal customers_expanded DataFrame for engine tests."""
    return pd.DataFrame({
        "le_book":       ["040", "040", "040", "010"],
        "customer_id":   [1, 2, 3, 4],
        "customer_name": ["Alice", None, "Carol", "Dave"],
        "gender":        ["M", "F", None, "M"],
        "date_creation": pd.to_datetime(
            ["2025-01-01", "2025-02-01", "2025-03-01", "2025-04-01"]
        ),
    })


@pytest.fixture
def accounts_df():
    """accounts with known/unknown customer_id for RI tests."""
    return pd.DataFrame({
        "le_book":     ["040", "040", "040"],
        "account_no":  ["A1",  "A2",  "A3"],
        "customer_id": [1, 2, 99],  # 99 is orphaned
    })
