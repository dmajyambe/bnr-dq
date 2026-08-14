# Notification bell callbacks (institution portal) — moved from dq_dashboard_dash.py.
from __future__ import annotations

import dash
from dash import ALL, Input, Output, State, ctx, html

from dashboard.app import app


@app.callback(
    Output("inst-notif-show", "data"),
    Input("inst-bell-btn",     "n_clicks"),
    Input("page-content",      "n_clicks"),
    State("inst-notif-show",   "data"),
    prevent_initial_call=True,
)
def _toggle_notif_panel(bell_clicks, page_clicks, currently_shown):
    trig = ctx.triggered_id
    if trig == "inst-bell-btn" and bell_clicks:
        return not bool(currently_shown)
    if trig == "page-content" and currently_shown:
        return False
    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("notif-overlay", "children"),
    Output("notif-overlay", "style"),
    Input("inst-notif-show", "data"),
    Input("notif-poll",      "n_intervals"),
    State("auth-store",      "data"),
    prevent_initial_call=False,
)
def _notif_panel_render(show, _poll, auth_data):
    import dashboard.portal.pages as inst_mod

    hidden = {"position": "fixed", "top": "52px", "right": "80px",
              "zIndex": "500", "display": "none"}
    shown  = {"position": "fixed", "top": "52px", "right": "80px",
              "zIndex": "500", "display": "block"}

    auth    = auth_data or {}
    role    = auth.get("role", "")
    user_id = auth.get("user_id", "")

    if role != "inst_user" or not user_id:
        return html.Div(), hidden

    if show:
        return inst_mod.inst_notification_panel(user_id), shown

    return html.Div(), hidden


@app.callback(
    Output("inst-notif-show", "data", allow_duplicate=True),
    Input("inst-mark-all-read", "n_clicks"),
    State("auth-store", "data"),
    prevent_initial_call=True,
)
def _mark_all_read(n, auth_data):
    if not n:
        raise dash.exceptions.PreventUpdate
    from remediation.notifications import mark_all_read
    user_id = (auth_data or {}).get("user_id", "")
    if user_id:
        mark_all_read(user_id)
    return True  # keep panel open, re-render with cleared notifications


@app.callback(
    Output("active-page",     "data", allow_duplicate=True),
    Output("inst-notif-show", "data", allow_duplicate=True),
    Input({"type": "notif-row", "index": ALL}, "n_clicks"),
    State("auth-store", "data"),
    prevent_initial_call=True,
)
def _notif_row_click(clicks, auth_data):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "notif-row":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    raw      = tid["index"]          # "{notif_id}|{cr_id}"
    parts    = raw.split("|", 1)
    notif_id = parts[0]

    # Mark this notification read
    user_id = (auth_data or {}).get("user_id", "")
    if user_id and notif_id:
        try:
            from remediation.notifications import mark_read
            mark_read(notif_id)
        except Exception:
            pass

    return "inst_remediation", False
