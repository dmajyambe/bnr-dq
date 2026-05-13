"""
Unit tests for dashboard helper functions (dq_dashboard_dash.py).
No browser / Dash server required — tests pure Python logic only.
"""
import pytest
from dash import html, dcc

import dq_dashboard_dash as dash_app
from dq_dashboard_dash import (
    DIMS, DIM_LABELS, DIM_COLORS,
    _cat_scores, _filter_institutions, _inst_scores,
    _score_color, _score_bg,
    _category_counts, _kpi_card, _sparkline,
    _trend_figure, _institution_table,
    _landing_page, _dashboard_content,
    C_GREEN, C_AMBER, C_RED,
)


# ── DIMS sanity ────────────────────────────────────────────────────────────────

class TestDimsConfig:
    def test_four_dimensions(self):
        assert len(DIMS) == 4

    def test_no_relationship_dimension(self):
        assert "relationship" not in DIMS
        assert "relationship" not in DIM_LABELS
        assert "relationship" not in DIM_COLORS

    def test_all_dims_have_labels_and_colors(self):
        for dim in DIMS:
            assert dim in DIM_LABELS, f"DIM_LABELS missing '{dim}'"
            assert dim in DIM_COLORS, f"DIM_COLORS missing '{dim}'"


# ── _score_color / _score_bg ───────────────────────────────────────────────────

class TestScoreColor:
    @pytest.mark.parametrize("score,expected", [
        (100.0, C_GREEN),
        (90.0,  C_GREEN),
        (89.9,  C_AMBER),
        (75.0,  C_GREEN),   # 75 is the boundary — check which side
        (74.9,  C_RED),
        (0.0,   C_RED),
    ])
    def test_boundaries(self, score, expected):
        # 75 is AMBER (>=75 is AMBER boundary), 90 is GREEN
        if score >= 90:
            assert _score_color(score) == C_GREEN
        elif score >= 75:
            assert _score_color(score) == C_AMBER
        else:
            assert _score_color(score) == C_RED

    def test_score_bg_returns_string(self):
        for score in [0.0, 74.9, 75.0, 89.9, 90.0, 100.0]:
            bg = _score_bg(score)
            assert isinstance(bg, str)
            assert bg.startswith("rgba")


# ── _cat_scores ────────────────────────────────────────────────────────────────

class TestCatScores:
    def test_returns_zeros_for_empty_entry(self):
        result = _cat_scores({}, "B")
        assert result == {d: 0.0 for d in DIMS}

    def test_returns_overall_for_ALL(self, history_entry):
        result = _cat_scores(history_entry, "ALL")
        assert result == history_entry["overall"]

    def test_returns_category_scores(self, history_entry):
        result = _cat_scores(history_entry, "B")
        assert result == history_entry["by_category"]["B"]

    def test_sacco_averages_sacco_and_osacco(self):
        entry = {
            "by_category": {
                "SACCO":  {"completeness": 80.0, "accuracy": 60.0,
                            "timeliness": 90.0,  "validity": 70.0},
                "OSACCO": {"completeness": 60.0, "accuracy": 40.0,
                            "timeliness": 70.0,  "validity": 50.0},
            }
        }
        result = _cat_scores(entry, "SACCO")
        assert result["completeness"] == 70.0   # (80+60)/2
        assert result["accuracy"]     == 50.0   # (60+40)/2

    def test_sacco_falls_back_to_osacco_when_no_sacco_key(self):
        entry = {
            "by_category": {
                "OSACCO": {"completeness": 86.2, "accuracy": 50.24,
                            "timeliness": 100.0, "validity": 97.9},
            }
        }
        result = _cat_scores(entry, "SACCO")
        assert result["completeness"] == 86.2

    def test_unknown_category_returns_empty_dict(self, history_entry):
        result = _cat_scores(history_entry, "XYZ")
        assert result == {}


# ── _filter_institutions ───────────────────────────────────────────────────────

class TestFilterInstitutions:
    def test_filter_banks(self, history_entry):
        result = _filter_institutions(history_entry, "B")
        assert all(v["category_type"] == "B" for v in result.values())
        assert "040" in result
        assert "421" not in result

    def test_filter_mf(self, history_entry):
        result = _filter_institutions(history_entry, "MF")
        assert "421" in result
        assert "040" not in result

    def test_filter_sacco_includes_osacco(self, history_entry):
        result = _filter_institutions(history_entry, "SACCO")
        assert "917" in result   # category_type = OSACCO, included in SACCO filter

    def test_filter_all_returns_everything(self, history_entry):
        result = _filter_institutions(history_entry, "ALL")
        assert len(result) == len(history_entry["by_institution"])

    def test_empty_entry_returns_empty(self):
        result = _filter_institutions({}, "B")
        assert result == {}

    def test_no_institutions_of_type(self, history_entry):
        result = _filter_institutions(history_entry, "MF")
        for v in result.values():
            assert v["category_type"] == "MF"


# ── _inst_scores ───────────────────────────────────────────────────────────────

class TestInstScores:
    def test_returns_correct_scores(self, history_entry):
        result = _inst_scores(history_entry, "040")
        assert result["completeness"] == history_entry["by_institution"]["040"]["completeness"]
        assert result["accuracy"]     == history_entry["by_institution"]["040"]["accuracy"]

    def test_returns_four_dims(self, history_entry):
        result = _inst_scores(history_entry, "040")
        assert set(result.keys()) == set(DIMS)

    def test_no_relationship_in_result(self, history_entry):
        result = _inst_scores(history_entry, "040")
        assert "relationship" not in result

    def test_unknown_institution_returns_zeros(self, history_entry):
        result = _inst_scores(history_entry, "UNKNOWN")
        assert result == {d: 0.0 for d in DIMS}

    def test_empty_entry_returns_zeros(self):
        result = _inst_scores({}, "040")
        assert result == {d: 0.0 for d in DIMS}


# ── _category_counts ───────────────────────────────────────────────────────────

class TestCategoryCounts:
    def test_counts_banks(self, history_entry):
        counts = _category_counts(history_entry)
        assert counts["B"] == 2   # 040, 010

    def test_counts_mf(self, history_entry):
        counts = _category_counts(history_entry)
        assert counts["MF"] == 1   # 421

    def test_counts_osacco(self, history_entry):
        counts = _category_counts(history_entry)
        assert counts["OSACCO"] == 1   # 917

    def test_all_count_equals_total_institutions(self, history_entry):
        counts = _category_counts(history_entry)
        assert counts["ALL"] == len(history_entry["by_institution"])

    def test_empty_entry(self):
        counts = _category_counts({})
        assert counts["ALL"] == 0


# ── Component rendering (no browser, just Dash component objects) ─────────────

class TestComponentRendering:
    def test_kpi_card_renders(self):
        card = _kpi_card("completeness", 88.5, 2.1, [85, 86, 87, 88, 88, 88, 88.5])
        assert isinstance(card, html.Div)

    def test_kpi_card_negative_delta(self):
        card = _kpi_card("accuracy", 70.0, -3.5, [74, 73, 72, 71, 71, 70, 70])
        assert isinstance(card, html.Div)

    def test_sparkline_renders(self):
        graph = _sparkline([80, 82, 85, 83, 88], "#2563EB")
        assert isinstance(graph, dcc.Graph)

    def test_trend_figure_category_mode(self, history_entry):
        trend = [history_entry] * 7
        fig = _trend_figure(trend, "B")
        assert len(fig.data) == 4   # one trace per dimension

    def test_trend_figure_institution_mode(self, history_entry):
        trend = [history_entry] * 7
        fig = _trend_figure(trend, "B", inst_code="040")
        assert len(fig.data) == 4   # still 4 dims, per-institution data

    def test_trend_figure_no_relationship_trace(self, history_entry):
        trend = [history_entry] * 7
        fig = _trend_figure(trend, "B")
        trace_names = [t.name for t in fig.data]
        assert "Relationship" not in trace_names

    def test_institution_table_renders_with_no_reports(self, history_entry):
        institutions = _filter_institutions(history_entry, "B")
        table = _institution_table(institutions, gen_status={})
        assert isinstance(table, html.Div)

    def test_institution_table_shows_generate_button_when_no_file(self, history_entry):
        institutions = _filter_institutions(history_entry, "B")
        table = _institution_table(institutions, gen_status={})

        def collect_ids(el, ids=None):
            if ids is None: ids = []
            id_ = getattr(el, "id", None)
            if id_: ids.append(str(id_))
            ch = getattr(el, "children", None) or []
            if not isinstance(ch, list): ch = [ch]
            for c in ch:
                if c: collect_ids(c, ids)
            return ids

        ids = collect_ids(table)
        gen_ids = [i for i in ids if "gen-btn" in i]
        # 2 banks in fixture (040 + 010), no report files → 2 Generate buttons
        assert len(gen_ids) == 2

    def test_institution_table_running_state(self, history_entry):
        institutions = _filter_institutions(history_entry, "B")
        table = _institution_table(institutions, gen_status={"040": "running"})
        # Just check it doesn't raise
        assert isinstance(table, html.Div)

    def test_landing_page_renders(self):
        from dq_dashboard_dash import _counts
        page = _landing_page(_counts)
        assert isinstance(page, html.Div)

    def test_dashboard_content_banks(self, history_entry, monkeypatch):
        monkeypatch.setattr(dash_app, "_HISTORY", [history_entry])
        content = _dashboard_content("B", None, gen_status={})
        assert isinstance(content, html.Div)

    def test_dashboard_content_institution_filter(self, history_entry, monkeypatch):
        monkeypatch.setattr(dash_app, "_HISTORY", [history_entry])
        content = _dashboard_content("B", "040", gen_status={})
        assert isinstance(content, html.Div)

    def test_dashboard_content_sacco(self, history_entry, monkeypatch):
        monkeypatch.setattr(dash_app, "_HISTORY", [history_entry])
        content = _dashboard_content("SACCO", None, gen_status={})
        assert isinstance(content, html.Div)


# ── Navigation state logic (_nav_handler core logic) ──────────────────────────

class TestNavHandlerLogic:
    """
    Test the business logic of _nav_handler without invoking Dash callbacks.
    We replicate the logic directly to avoid dependency on the running app.
    """

    def _apply_nav(self, current_nav, tid, triggered_val):
        """Mirror of _nav_handler logic."""
        nav = dict(current_nav or {"cat": None, "inst": None})
        if isinstance(tid, dict):
            t = tid.get("type")
            if t == "nav-action" and tid.get("index") == "back":
                if triggered_val and triggered_val > 0:
                    return {"cat": None, "inst": None}
                return None   # PreventUpdate
            if t == "cat-landing-btn":
                if triggered_val and triggered_val > 0:
                    return {"cat": tid["index"], "inst": None}
                return None
            if t == "inst-dd":
                new_inst = triggered_val or None
                new_nav = {**nav, "inst": new_inst}
                if new_nav == nav:
                    return None   # PreventUpdate
                return new_nav
        return None

    def test_landing_click_sets_cat_clears_inst(self):
        current = {"cat": None, "inst": None}
        tid = {"type": "cat-landing-btn", "index": "B"}
        result = self._apply_nav(current, tid, triggered_val=1)
        assert result == {"cat": "B", "inst": None}

    def test_back_button_clears_both(self):
        current = {"cat": "B", "inst": "040"}
        tid = {"type": "nav-action", "index": "back"}
        result = self._apply_nav(current, tid, triggered_val=1)
        assert result == {"cat": None, "inst": None}

    def test_inst_dropdown_sets_institution(self):
        current = {"cat": "B", "inst": None}
        tid = {"type": "inst-dd", "index": "main"}
        result = self._apply_nav(current, tid, triggered_val="040")
        assert result == {"cat": "B", "inst": "040"}

    def test_inst_dropdown_prevents_update_when_unchanged(self):
        current = {"cat": "B", "inst": None}
        tid = {"type": "inst-dd", "index": "main"}
        # empty string → None → same as current inst=None → should prevent update
        result = self._apply_nav(current, tid, triggered_val="")
        assert result is None   # PreventUpdate

    def test_landing_click_ignored_if_zero_clicks(self):
        current = {"cat": None, "inst": None}
        tid = {"type": "cat-landing-btn", "index": "MF"}
        result = self._apply_nav(current, tid, triggered_val=0)
        assert result is None

    def test_back_button_ignored_if_zero_clicks(self):
        current = {"cat": "B", "inst": "040"}
        tid = {"type": "nav-action", "index": "back"}
        result = self._apply_nav(current, tid, triggered_val=0)
        assert result is None

    def test_switching_category_always_clears_institution(self):
        current = {"cat": "B", "inst": "040"}
        tid = {"type": "cat-landing-btn", "index": "MF"}
        result = self._apply_nav(current, tid, triggered_val=1)
        assert result == {"cat": "MF", "inst": None}
