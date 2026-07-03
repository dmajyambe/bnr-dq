# Alerts tab callbacks — moved from dq_dashboard_dash.py.
from __future__ import annotations

import json
import logging

import dash
from dash import ALL, MATCH, Input, Output, State, ctx, dcc, html

from dashboard.app import app
from dashboard.data import CATEGORIES_FILE
from dashboard.pages.alerts import _build_resolved_by_institution, _issues_to_xlsx
from dashboard.theme import CARD, DIVIDER, MUTED, TEXT, C_AMBER, C_GREEN, C_RED, _URGENCY_COLORS

log = logging.getLogger("dashboard.callbacks.alerts")


@app.callback(
    Output("issue-list",        "children"),
    Output("alerts-summary-bar","children"),
    Input("alerts-cat-filter",  "value"),
    Input("notif-poll",         "n_intervals"),
)
def _refresh_issue_list(cat_filter, _poll):
    from issues.repositories import get_issues
    from datetime import date as _date

    cat_filter = cat_filter or ""

    # Build le_book → category_type map for filtering
    try:
        _cats = json.loads(CATEGORIES_FILE.read_text())
    except Exception:
        _cats = {}

    def _matches(lb: str) -> bool:
        if not cat_filter:
            return True
        ct = (_cats.get(str(lb), {}).get("category_type") or "")
        if cat_filter == "SACCO":
            return ct in ("SACCO", "OSACCO")
        return ct == cat_filter

    all_resolved_raw = get_issues(status="resolved")
    all_open_raw     = get_issues(status="open")

    all_resolved = [i for i in all_resolved_raw if _matches(i["le_book"])]
    all_open     = [i for i in all_open_raw     if _matches(i["le_book"])]

    # ── summary chips ─────────────────────────────────────────────────────────
    today         = _date.today()
    this_month    = today.strftime("%Y-%m")
    month_res     = sum(1 for i in all_resolved
                        if (i.get("resolved_at") or "").startswith(this_month))
    overdue_count = sum(1 for i in all_open if i.get("urgency_band") == "overdue")
    on_time       = sum(1 for i in all_resolved
                        if i.get("sla_deadline") and i.get("resolved_at")
                        and i["resolved_at"] <= i["sla_deadline"])
    late          = len(all_resolved) - on_time

    fix_days = []
    for i in all_resolved:
        try:
            fix_days.append(
                (_date.fromisoformat(i["resolved_at"]) -
                 _date.fromisoformat(i["detected_at"])).days
            )
        except Exception:
            pass
    avg_fix = f"{sum(fix_days) // len(fix_days)}d" if fix_days else "—"

    def _chip(value, label, color=None, border_color=None):
        bc = border_color or DIVIDER
        return html.Div([
            html.Span(str(value), style={"fontWeight": "900", "fontSize": "22px",
                                         "color": color or TEXT}),
            html.Span(label,      style={"fontSize": "11px", "color": MUTED,
                                         "marginTop": "2px"}),
        ], style={
            "display": "flex", "flexDirection": "column", "alignItems": "center",
            "background": CARD, "borderRadius": "8px", "padding": "12px 24px",
            "border": f"1px solid {bc}", "minWidth": "110px",
        })

    # Minimalist: the three that matter for triage — current load, what's breached, momentum.
    summary_bar = html.Div([
        _chip(len(all_open), "Open Issues",         C_AMBER),
        _chip(overdue_count, "Overdue",             _URGENCY_COLORS["overdue"],
              border_color=_URGENCY_COLORS["overdue"]),
        _chip(month_res,     "Resolved This Month", C_GREEN),
    ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"})

    # ── issue list ─────────────────────────────────────────────────────────────
    all_resolved_raw.sort(key=lambda x: x.get("resolved_at") or "", reverse=True)
    issue_list = _build_resolved_by_institution(all_resolved_raw, cat_filter)

    return issue_list, summary_bar


@app.callback(
    Output({"type": "alert-inst-collapse", "index": MATCH}, "style"),
    Output({"type": "alert-inst-toggle",   "index": MATCH}, "children"),
    Input({"type": "alert-inst-toggle",    "index": MATCH}, "n_clicks"),
    State({"type": "alert-inst-collapse",  "index": MATCH}, "style"),
    prevent_initial_call=True,
)
def _toggle_alert_inst(n_clicks, current_style):
    hidden    = (current_style or {}).get("display") == "none"
    new_style = {"display": "block"} if hidden else {"display": "none"}
    return new_style, "▼" if hidden else "▶"


@app.callback(
    Output({"type": "res-inst-collapse", "index": MATCH}, "style"),
    Output({"type": "res-inst-toggle",   "index": MATCH}, "children"),
    Input({"type": "res-inst-toggle",    "index": MATCH}, "n_clicks"),
    State({"type": "res-inst-collapse",  "index": MATCH}, "style"),
    prevent_initial_call=True,
)
def _toggle_res_inst(n_clicks, current_style):
    hidden    = (current_style or {}).get("display") == "none"
    new_style = {"display": "block"} if hidden else {"display": "none"}
    return new_style, "▼" if hidden else "▶"


@app.callback(
    Output("resolved-dl-lb", "data"),
    Input({"type": "res-dl-btn", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _resolved_dl_select(clicks):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "res-dl-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate
    return tid["index"]


@app.callback(
    Output("notify-status", "data"),
    Input({"type": "notify-btn", "index": ALL}, "n_clicks"),
    State("notify-status", "data"),
    prevent_initial_call=True,
)
def _on_notify(clicks, current):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "notify-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    lb = tid["index"]
    try:
        from issues.repositories import get_open_issues
        from issues.alerts import send_notification
        issues = get_open_issues(lb)
        if not issues:
            result = "no_issues"
        else:
            inst_name = (issues[0].get("institution_name") or lb).title()
            sent = send_notification(lb, inst_name, issues, force=True)
            result = "sent" if sent else "no_email"
    except Exception as exc:
        log.error("Notify callback error: %s", exc)
        result = "error"

    updated = dict(current or {})
    updated[lb] = result
    return updated


@app.callback(
    Output("notify-feedback", "children"),
    Input("notify-status", "data"),
    prevent_initial_call=True,
)
def _show_notify_feedback(status_data):
    if not status_data:
        raise dash.exceptions.PreventUpdate

    msgs = []
    for lb, result in status_data.items():
        if result == "sent":
            msgs.append(html.Span(
                f"✓ Reminder sent for institution {lb}.",
                style={"color": C_GREEN, "fontSize": "12px", "marginRight": "12px"},
            ))
        elif result == "no_email":
            msgs.append(html.Span(
                f"⚠ No contact email configured for {lb}. "
                "Set one in dq_institution_contacts (SQLite) or configure SMTP.",
                style={"color": C_AMBER, "fontSize": "12px", "marginRight": "12px"},
            ))
        elif result == "no_issues":
            msgs.append(html.Span(
                f"No open issues found for {lb}.",
                style={"color": MUTED, "fontSize": "12px", "marginRight": "12px"},
            ))
        elif result == "error":
            msgs.append(html.Span(
                f"Error sending notification for {lb}. Check logs.",
                style={"color": C_RED, "fontSize": "12px", "marginRight": "12px"},
            ))
    return html.Div(msgs) if msgs else dash.no_update


@app.callback(
    Output("issues-download", "data"),
    Input("issues-download-btn", "n_clicks"),
    State("alerts-cat-filter",   "value"),
    prevent_initial_call=True,
)
def _on_issues_download(n_clicks, cat_filter):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    from issues.repositories import get_issues
    issues = get_issues(status="resolved")
    issues.sort(key=lambda x: x.get("resolved_at") or "", reverse=True)
    return dcc.send_bytes(_issues_to_xlsx(issues), "dq_resolved_issues.xlsx")
