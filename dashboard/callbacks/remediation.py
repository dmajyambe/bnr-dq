# Remediation / Change-Request tab callbacks — moved from dq_dashboard_dash.py.
#
# BUG FIXED while porting (not present before this split, found here): the
# original _approve_table_cb referenced a bare name `_auth.is_admin(role)`
# with no import anywhere in scope — `import dq_auth as _auth` only ever
# existed as a *local* import inside the unrelated _build_cr_list function,
# which doesn't leak into this callback's namespace. Clicking "Approve Table"
# on a submitted CR would raise NameError before ever reaching
# cr_mod.approve_table(). Fixed by importing auth.users.is_admin properly.
from __future__ import annotations

import dash
from dash import ALL, Input, Output, State, ctx, html

from auth.users import is_admin
from dashboard.app import app
from dashboard.components import _fmt_int
from dashboard.pages.remediation import _build_cr_list
from dashboard.theme import BRAND, CARD, MUTED, TABLE_NAMES_PRETTY, C_GREEN, C_RED


@app.callback(
    Output("cr-form-panel", "style"),
    Input("cr-form-toggle-btn", "n_clicks"),
    State("cr-form-panel", "style"),
    prevent_initial_call=True,
)
def _toggle_cr_form(n_clicks, current_style):
    style = dict(current_style or {})
    style["display"] = "none" if style.get("display") != "none" else "block"
    return style


@app.callback(
    Output("cr-issue-checklist", "options"),
    Output("cr-issue-checklist", "value"),
    Output("cr-issue-hint",      "children"),
    Output("cr-table-dl-area",   "children"),
    Input("cr-inst-filter", "value"),
    prevent_initial_call=True,
)
def _update_issue_checklist(le_book):
    """Populate the table selector when the specialist picks an institution."""
    _empty = ([], [], "Select an institution above to see its tables with open issues.", html.Div())
    if not le_book:
        return _empty
    from issues.repositories import get_open_issues
    issues = get_open_issues(le_book)
    if not issues:
        return [], [], "No open issues found for this institution.", html.Div()

    _BO = {"new": 0, "attention": 1, "urgent": 2, "critical": 3, "overdue": 4}
    by_table: dict[str, list] = {}
    for iss in issues:
        by_table.setdefault(iss["table_name"], []).append(iss)

    options = []
    for tbl in sorted(by_table.keys()):
        tbl_issues = by_table[tbl]
        tbl_label  = TABLE_NAMES_PRETTY.get(tbl, tbl.replace("_", " ").title())
        n_issues   = len(tbl_issues)
        worst_band = max(
            (iss.get("urgency_band") or "new" for iss in tbl_issues),
            key=lambda b: _BO.get(b, 0),
        )
        options.append({"label": f"{tbl_label}  —  {n_issues} issue(s)  —  {worst_band.upper()}", "value": tbl})

    hint = f"{len(by_table)} table(s) with open issues — select those to include in this Change Request."

    dl_buttons = html.Div([
        html.Div(
            "⬇ " + TABLE_NAMES_PRETTY.get(tbl, tbl),
            id={"type": "cr-tbl-dl-btn", "index": f"{le_book}|{tbl}"},
            n_clicks=0,
            style={"display": "inline-block", "background": BRAND, "color": CARD,
                   "fontSize": "10px", "fontWeight": "700", "padding": "3px 10px",
                   "borderRadius": "4px", "cursor": "pointer", "userSelect": "none"},
        )
        for tbl in sorted(by_table.keys())
    ], style={"display": "flex", "gap": "6px", "flexWrap": "wrap"})

    return options, [], hint, dl_buttons


@app.callback(
    Output("cr-issue-checklist", "value", allow_duplicate=True),
    Input("cr-select-all", "n_clicks"),
    Input("cr-clear-all", "n_clicks"),
    State("cr-issue-checklist", "options"),
    prevent_initial_call=True,
)
def _cr_select_all_issues(sel_clicks, clr_clicks, options):
    """Select-all / clear shortcuts for the issue checklist."""
    trig = ctx.triggered_id
    if trig == "cr-clear-all":
        return []
    if trig == "cr-select-all":
        return [o["value"] if isinstance(o, dict) else o for o in (options or [])]
    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("cr-assigned-to", "options"),
    Output("cr-assigned-to", "value"),
    Output("cr-assigned-to", "placeholder"),
    Input("cr-inst-filter",  "value"),
    prevent_initial_call=True,
)
def _populate_assigned_to(le_book):
    if not le_book:
        return [], None, "Select an institution first…"

    import json as _json
    from auth.users import get_users_by_le_book
    from dashboard.data import CATEGORIES_FILE

    users = get_users_by_le_book(le_book)
    if not users:
        # Get institution name for a helpful placeholder
        try:
            cats = _json.loads(CATEGORIES_FILE.read_text())
            inst_name = (cats.get(le_book, {}).get("name") or le_book).title()
        except Exception:
            inst_name = le_book
        return [], None, f"No users registered under {inst_name}"

    options = [
        {"label": f"{u['name']} ({u['email']})", "value": u["email"]}
        for u in users
    ]
    return options, None, "Select a user…"


@app.callback(
    Output("cr-feedback",  "children"),
    Output("cr-version",   "data", allow_duplicate=True),
    Input("cr-create-btn", "n_clicks"),
    State("cr-issue-checklist", "value"),
    State("cr-title",           "value"),
    State("cr-description",     "value"),
    State("cr-assigned-to",     "value"),
    State("cr-target-date",     "value"),
    State("cr-inst-filter",     "value"),
    State("cr-version",         "data"),
    State("auth-store",         "data"),
    prevent_initial_call=True,
)
def _create_cr(n_clicks, tables, title, description,
               assigned_to, target_date, le_book, version, auth_data):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    _err = lambda msg: (html.Span(msg, style={"color": C_RED}), version)

    if not le_book:
        return _err("Select an institution first.")
    if not tables:
        return _err("Select at least one table to include in this change request.")
    if not (title or "").strip():
        return _err("A title is required.")

    import remediation.change_requests as cr_mod
    from issues.repositories import get_open_issues

    issues     = get_open_issues(le_book)
    tables_set = set(tables)
    inst_name  = ""
    total_fail = 0
    dims: set[str] = set()
    for iss in issues:
        if iss["table_name"] not in tables_set:
            continue
        inst_name  = (iss.get("institution_name") or le_book).title()
        total_fail += int(iss.get("failing_rows") or 0)
        if iss.get("dimension"):
            dims.add(iss["dimension"])

    dimension  = next(iter(dims)) if len(dims) == 1 else ("Multiple" if dims else "")
    created_by = (auth_data or {}).get("email", "")

    try:
        cr_id = cr_mod.create_cr(
            tables           = list(tables),
            le_book          = le_book,
            institution_name = inst_name or le_book,
            title            = title.strip(),
            description      = (description or "").strip(),
            assigned_to      = (assigned_to  or "").strip(),
            created_by       = created_by,
            target_date      = (target_date  or "").strip(),
            dimension        = dimension,
            failing_rows     = total_fail,
        )
    except Exception as exc:
        return _err(f"Could not save change request: {exc}")

    return (
        html.Span([
            html.Span("✓ ", style={"color": C_GREEN, "fontWeight": "900"}),
            html.Span(
                f"{cr_id} created for {inst_name} "
                f"({len(tables)} table(s), {total_fail:,} failing rows).",
                style={"color": C_GREEN},
            ),
        ]),
        (version or 0) + 1,
    )


@app.callback(
    Output("cr-version",        "data", allow_duplicate=True),
    Output("cr-action-feedback", "children"),
    Input({"type": "cr-action-btn", "index": ALL}, "n_clicks"),
    State("cr-review-notes", "value"),
    State("auth-store",       "data"),
    State("cr-version",       "data"),
    prevent_initial_call=True,
)
def _cr_action(clicks, review_notes, auth_data, version):
    if (auth_data or {}).get("role") == "inst_user":
        raise dash.exceptions.PreventUpdate
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "cr-action-btn":
        raise dash.exceptions.PreventUpdate
    if not (ctx.triggered[0]["value"] or 0) > 0:
        raise dash.exceptions.PreventUpdate

    import remediation.change_requests as cr_mod

    raw = tid["index"]          # e.g. "CR-20260519-0001|approved"
    if "|" not in raw:
        raise dash.exceptions.PreventUpdate
    cr_id, new_status = raw.split("|", 1)

    actor = (auth_data or {}).get("email", "system")
    notes = (review_notes or "").strip()

    ok, msg = cr_mod.update_status(cr_id, new_status, actor=actor, notes=notes)

    if ok:
        label = cr_mod.STATUS_LABELS.get(new_status, new_status)
        feedback = html.Span([
            html.Span("✓ ", style={"color": C_GREEN, "fontWeight": "900"}),
            html.Span(f"{cr_id} updated to \"{label}\".", style={"color": C_GREEN}),
        ])
        return (version or 0) + 1, feedback
    else:
        return version, html.Span(msg, style={"color": C_RED})


@app.callback(
    Output("cr-list-container", "children"),
    Input("cr-version",      "data"),
    Input("cr-status-filter", "value"),
    Input("notif-poll",       "n_intervals"),
    State("auth-store",       "data"),
)
def _refresh_cr_list(version, status_filter, _poll, auth_data):
    import remediation.change_requests as cr_mod
    role = (auth_data or {}).get("role", "viewer")
    crs  = cr_mod.get_crs(status=status_filter if status_filter != "all" else None)
    return _build_cr_list(crs, role=role)


@app.callback(
    Output("cr-summary-bar", "children"),
    Input("cr-version",  "data"),
    Input("notif-poll",  "n_intervals"),
    prevent_initial_call=False,
)
def _refresh_cr_stats(version, _poll):
    import remediation.change_requests as cr_mod
    stats = cr_mod.get_stats()
    chips = []
    for key in ("open", "in_progress", "submitted"):   # minimalist: active pipeline only
        lbl = cr_mod.STATUS_LABELS.get(key, key.title())
        n   = stats.get(key, 0)
        clr = cr_mod.STATUS_COLORS[key]
        chips.append(html.Div([
            html.Span(_fmt_int(n, str(n)), style={
                "fontSize": "24px", "fontWeight": "900",
                "color": clr, "lineHeight": "1",
            }),
            html.Span(lbl, style={
                "fontSize": "10px", "color": MUTED,
                "marginTop": "3px", "lineHeight": "1.2",
                "textAlign": "center",
            }),
        ], style={
            "display":       "flex",
            "flexDirection": "column",
            "alignItems":    "center",
            "background":    CARD,
            "borderRadius":  "8px",
            "padding":       "12px 18px",
            "border":        f"2px solid {clr}",
            "minWidth":      "90px",
        }))
    return chips


@app.callback(
    Output("cr-version",         "data", allow_duplicate=True),
    Output("cr-action-feedback", "children", allow_duplicate=True),
    Input({"type": "cr-tbl-approve-btn", "index": ALL}, "n_clicks"),
    State("auth-store", "data"),
    State("cr-version", "data"),
    prevent_initial_call=True,
)
def _approve_table_cb(clicks, auth_data, version):
    role = (auth_data or {}).get("role", "viewer")
    if not is_admin(role):
        raise dash.exceptions.PreventUpdate
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "cr-tbl-approve-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    raw = tid["index"]
    if "|" not in raw:
        raise dash.exceptions.PreventUpdate
    cr_id, table_name = raw.split("|", 1)

    import remediation.change_requests as cr_mod
    actor  = (auth_data or {}).get("email", "")
    ok, msg = cr_mod.approve_table(cr_id, table_name, actor=actor)

    feedback = (
        html.Span([html.Span("✓ ", style={"color": C_GREEN, "fontWeight": "900"}),
                   html.Span(msg, style={"color": C_GREEN})])
        if ok else html.Span(msg, style={"color": C_RED})
    )
    return (version or 0) + 1 if ok else version, feedback
