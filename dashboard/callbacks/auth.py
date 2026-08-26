# Login / logout / user-header callbacks
from __future__ import annotations
import dash
from dash import Input, Output, State, ctx, html
from flask import session as flask_session
from auth import users as auth_mod
from dashboard.app import app


# ── session hydration on page load ───────────────────────────────────────────
# Fires once on (re)load via url.pathname. If auth-store is empty but the Flask
# session cookie is still valid (e.g. browser was closed and reopened), this
# repopulates the store so the user lands on their last page, not login.

@app.callback(
    Output("auth-store", "data"),
    Input("url", "pathname"),
    State("auth-store", "data"),
)
def _hydrate_from_flask_session(_, auth_data):
    auth = auth_data or {}
    if auth.get("email"):
        raise dash.exceptions.PreventUpdate   # sessionStorage already populated

    email = flask_session.get("user_email", "")
    if not email:
        raise dash.exceptions.PreventUpdate   # no server session either → show login

    user = auth_mod.get_user_by_email(email)
    if not user:
        flask_session.clear()
        raise dash.exceptions.PreventUpdate

    le_books = (auth_mod.get_user_institutions(user["user_id"])
                if user["role"] in auth_mod.INST_ROLES else [])
    return {
        "email":    user["email"],
        "name":     user["name"],
        "role":     user["role"],
        "user_id":  user["user_id"],
        "le_books": le_books,
    }


# ── login tab switching ───────────────────────────────────────────────────────

@app.callback(
    Output("auth-store",  "data", allow_duplicate=True),
    Output("login-type",  "data"),
    Input("login-tab-bnr",  "n_clicks"),
    Input("login-tab-inst", "n_clicks"),
    prevent_initial_call=True,
)
def _switch_login_tab(n_bnr, n_inst):
    tid = ctx.triggered_id
    lt  = "inst" if tid == "login-tab-inst" else "bnr"
    return {"tab_switch": lt}, lt


# ── login callback ───────────────────────────────────────────────────────────

@app.callback(
    Output("auth-store", "data", allow_duplicate=True),
    Input("login-btn",      "n_clicks"),
    Input("login-password", "n_submit"),
    State("login-email",    "value"),
    State("login-password", "value"),
    State("login-type",     "data"),
    prevent_initial_call=True,
)
def _do_login(n_clicks, n_submit, email, password, login_type):
    if not (n_clicks or n_submit):
        raise dash.exceptions.PreventUpdate

    email      = (email    or "").strip().lower()
    password   = (password or "").strip()
    login_type = login_type or "bnr"

    # Enforce @bnr.rw for the BNR tab before even hitting the DB
    if login_type == "bnr" and not auth_mod.is_valid_bnr_email(email):
        return {"error": "Inspector login requires a @bnr.rw email address.", "tab": login_type}

    user = auth_mod.verify_credentials(email, password)
    if not user:
        return {"error": "Incorrect email or password.", "tab": login_type}

    role = user["role"]
    if login_type == "bnr":
        if role in auth_mod.INST_ROLES:
            return {"error": "Use the Institution login for this account.", "tab": login_type}
    elif login_type == "inst":
        if role not in auth_mod.INST_ROLES:
            return {"error": "Use the Inspector login for this account.", "tab": login_type}

    flask_session["user_email"] = user["email"]
    flask_session["user_name"]  = user["name"]
    flask_session["user_role"]  = user["role"]
    flask_session.permanent     = True

    le_books = auth_mod.get_user_institutions(user["user_id"]) if user["role"] in auth_mod.INST_ROLES else []

    return {
        "email":    user["email"],
        "name":     user["name"],
        "role":     user["role"],
        "user_id":  user["user_id"],
        "le_books": le_books,
    }


# ── logout callback ───────────────────────────────────────────────────────────

@app.callback(
    Output("auth-store", "data", allow_duplicate=True),
    Input("logout-btn",  "n_clicks"),
    prevent_initial_call=True,
)
def _do_logout(n):
    if not n:
        raise dash.exceptions.PreventUpdate
    flask_session.clear()
    return {}   # empty auth-store → triggers login page re-render


# ── user info header ──────────────────────────────────────────────────────────

@app.callback(
    Output("user-info-header", "children"),
    Input("auth-store", "data"),
    prevent_initial_call=True,
)
def _update_user_header(auth_data):
    auth  = auth_data or {}
    email = auth.get("email", "")
    name  = auth.get("name",  "")
    if not email:
        return html.Div()
    # Institution users have their own logout button in the portal nav bar
    if auth.get("le_books"):
        return html.Div()
    display = name.split()[0] if name else email.split("@")[0]
    return html.Div([
        html.Span(display, style={
            "fontSize": "12px", "color": "rgba(255,255,255,0.85)",
            "marginRight": "10px",
        }),
        html.Div(
            "Sign out",
            id="logout-btn",
            n_clicks=0,
            style={
                "fontSize": "11px", "fontWeight": "700",
                "color": "rgba(255,255,255,0.60)",
                "cursor": "pointer", "userSelect": "none",
                "border": "1px solid rgba(255,255,255,0.25)",
                "borderRadius": "4px", "padding": "4px 10px",
            },
        ),
    ], style={"display": "flex", "alignItems": "center"})
