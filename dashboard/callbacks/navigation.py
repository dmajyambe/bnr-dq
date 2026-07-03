# Page routing + nav-state callbacks — moved from dq_dashboard_dash.py.
from __future__ import annotations

import dash
from dash import ALL, MATCH, Input, Output, State, ctx, html

from auth.users import is_executive
from dashboard.app import app
from dashboard.components import _executive_nav, _nav_tabs
from dashboard.data import CATEGORIES_FILE, _counts
from dashboard.pages.alerts import _alerts_page
from dashboard.pages.dashboard_tab import _dashboard_content
from dashboard.pages.executive import _executive_page
from dashboard.pages.landing import _landing_page
from dashboard.pages.login import _login_page
from dashboard.pages.remediation import _remediation_page
from dashboard.pages.validations import _validations_page


@app.callback(
    Output({"type": "null-col-body", "index": MATCH}, "style"),
    Output({"type": "null-col-btn",  "index": MATCH}, "children"),
    Input({"type": "null-col-btn",   "index": MATCH}, "n_clicks"),
    State({"type": "null-col-body",  "index": MATCH}, "style"),
    prevent_initial_call=True,
)
def _toggle_null_cols(_n, current_style):
    is_open  = (current_style or {}).get("display") != "none"
    new_style = {"display": "none" if is_open else "block", "marginTop": "4px"}
    label     = "▼ hide" if not is_open else "▶ show"
    return new_style, label


@app.callback(
    Output("active-page", "data"),
    Input({"type": "page-nav", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _on_page_nav(_n_clicks):
    triggered = ctx.triggered_id
    if isinstance(triggered, dict) and "index" in triggered:
        return triggered["index"]
    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("page-nav-bar", "children"),
    Output("page-content",  "children"),
    Input("active-page",    "data"),
    Input("nav-state",      "data"),
    Input("rules-version",  "data"),
    Input("auth-store",     "data"),
)
def _render_page(page: str, nav_state, _rv, auth_data):
    auth = auth_data or {}
    if auth.get("tab_switch"):
        lt = auth.get("tab_switch", "bnr")
        return html.Div(), _login_page(login_type=lt)
    if auth.get("error") or not auth.get("email"):
        lt = auth.get("tab", "bnr")
        return html.Div(), _login_page(auth.get("error", ""), login_type=lt)

    role     = auth.get("role", "viewer")
    le_books = auth.get("le_books", [])

    # ── executive → high-level Management view only ───────────────────────────
    if is_executive(role):
        return _executive_nav(auth.get("name", "")), _executive_page(role, le_books)

    # ── institution user → hand off to inst portal ────────────────────────────
    if role == "inst_user":
        import dashboard.portal.pages as inst_mod
        import json as _json
        try:
            categories = _json.loads(CATEGORIES_FILE.read_text())
        except Exception:
            categories = {}

        page = page or "inst_dashboard"
        uid  = auth.get("user_id", "")
        try:
            from remediation.notifications import get_unread_count
            unread = get_unread_count(uid) if uid else 0
        except Exception:
            unread = 0

        nav_bar = inst_mod.inst_nav_bar(
            active_page=page,
            user_name=auth.get("name", ""),
            le_book=le_books[0] if le_books else "—",
            unread_count=unread,
        )

        if page == "inst_issues":
            content = inst_mod.inst_issues_page(le_books)
        elif page == "inst_remediation":
            content = inst_mod.inst_remediation_page(le_books, role=role)
        elif page == "inst_validations":
            content = _validations_page()
        else:
            content = inst_mod.inst_dashboard_page(le_books, categories)

        return nav_bar, content

    # ── BNR user → existing portal ────────────────────────────────────────────
    page = page or "dashboard"
    nav  = nav_state or {"cat": None, "inst": None}
    cat  = nav.get("cat")
    inst = nav.get("inst")

    nav_bar = _nav_tabs(page)

    if page == "validations":
        return nav_bar, _validations_page()

    if page == "alerts":
        return nav_bar, _alerts_page(cat=cat or "")

    if page == "remediation":
        return nav_bar, _remediation_page(role=role)

    if not cat:
        return nav_bar, _landing_page(_counts)

    return nav_bar, _dashboard_content(cat, inst)


@app.callback(
    Output("nav-state", "data"),
    Input({"type": "cat-landing-btn", "index": ALL}, "n_clicks"),
    Input({"type": "nav-action",      "index": ALL}, "n_clicks"),
    Input({"type": "inst-dd",         "index": ALL}, "value"),
    State("nav-state", "data"),
    prevent_initial_call=True,
)
def _nav_handler(landing_clicks, nav_action_clicks, inst_values, current_nav):
    """Single callback that owns all navigation state changes."""
    nav = dict(current_nav or {"cat": None, "inst": None})
    tid = ctx.triggered_id
    triggered_val = ctx.triggered[0]["value"] if ctx.triggered else None

    if isinstance(tid, dict):
        t = tid.get("type")

        if t == "nav-action" and tid.get("index") == "back":
            if triggered_val and triggered_val > 0:
                return {"cat": None, "inst": None}
            raise dash.exceptions.PreventUpdate

        if t == "cat-landing-btn":
            if triggered_val and triggered_val > 0:
                return {"cat": tid["index"], "inst": None}
            raise dash.exceptions.PreventUpdate

        if t == "inst-dd":
            new_inst = triggered_val or None
            new_nav  = {**nav, "inst": new_inst}
            # prevent spurious re-renders when the dropdown first appears in the DOM
            if new_nav == nav:
                raise dash.exceptions.PreventUpdate
            return new_nav

    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("unscored-body",   "style"),
    Output("unscored-toggle", "children"),
    Input("unscored-toggle",  "n_clicks"),
    State("unscored-body",    "style"),
    State("unscored-toggle",  "children"),
    prevent_initial_call=True,
)
def _toggle_unscored(n_clicks, body_style, btn_text):
    visible = (body_style or {}).get("display") != "none"
    if visible:
        new_label = (btn_text or "").replace("▼", "▶").replace("Hide", "Show")
        return {"display": "none"}, new_label
    new_label = (btn_text or "").replace("▶", "▼").replace("Show", "Hide")
    return {"display": "block"}, new_label
