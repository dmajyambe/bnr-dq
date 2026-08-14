# Dash app instance + layout — moved from dq_dashboard_dash.py.
#
# gunicorn target: gunicorn dashboard.app:server
#
# Import order matters here: `app`/`server` must exist before dashboard.routes
# and the dashboard.callbacks.* submodules are imported at the bottom, since
# each of those does `from dashboard.app import app` (or `server`) and
# registers @app.callback / @server.route at import time.
from __future__ import annotations

import os

import dash
from dash import dcc, html

from auth.users import ensure_users_table
from dashboard.data import _DIR
from dashboard.theme import BG, BRAND, CARD, DIVIDER, FONT, MUTED, TEXT

app = dash.Dash(
    __name__,
    title="BNR Data Quality Monitoring",
    suppress_callback_exceptions=True,
    # dash.Dash(__name__) resolves assets_folder relative to this file's own
    # directory by default — now dashboard/assets/ instead of the project
    # root's assets/ (where bnr_img.png and style.css actually live). Point
    # it back explicitly.
    assets_folder=str(_DIR / "assets"),
)
server = app.server
server.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

ensure_users_table()

app.layout = html.Div([

    # ── header ────────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Img(
                src="/assets/bnr_img.png",
                style={"height": "50px", "marginRight": "16px", "flexShrink": "0"},
            ),
            html.Div([
                html.Div("DATA QUALITY MONITORING", style={
                    "fontSize": "14px", "fontWeight": "700",
                    "color": CARD, "letterSpacing": "0.06em",
                    "lineHeight": "1.15",
                }),
                html.Div(
                    "National Bank of Rwanda — Data Quality Program",
                    style={
                        "fontSize": "11px", "fontWeight": "400",
                        "color": "rgba(255,255,255,0.65)",
                        "lineHeight": "1.15", "marginTop": "3px",
                    },
                ),
            ]),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div([
            html.Div(id="pipeline-status-banner", style={
                "textAlign": "right", "lineHeight": "1.5",
            }),
            html.Div(id="user-info-header"),
        ], style={"display": "flex", "alignItems": "center", "gap": "20px"}),
    ], style={
        "background":     "#753918",
        "padding":        "14px 32px",
        "display":        "flex",
        "alignItems":     "center",
        "justifyContent": "space-between",
        "boxShadow":      "0 2px 8px rgba(0,0,0,0.18)",
    }),

    # ── page nav ──────────────────────────────────────────────────────────────
    html.Div(id="page-nav-bar"),

    # ── page content (wrapped in a loader → spinner on every page transition) ──
    dcc.Loading(
        id="page-loading", type="circle", color=BRAND,
        delay_show=250,                      # don't flash on instant renders
        children=html.Div(id="page-content", style={
            "maxWidth":   "1440px",
            "margin":     "0 auto",
            "padding":    "24px 32px",
            "fontFamily": FONT,
            "minHeight":  "60vh",
        }),
    ),

    # ── notification overlay (fixed, shown on top of page content) ────────────
    html.Div(id="notif-overlay", style={
        "position": "fixed", "top": "52px", "right": "80px",
        "zIndex": "500", "display": "none",
    }),

    # ── stores ────────────────────────────────────────────────────────────────
    # nav-state: {"cat": None|"B"|"MF"|"SACCO", "inst": None|"<code>"}
    # cat=None means landing page; inst=None means show all in category
    dcc.Interval(id="status-poll",   interval=30_000,  n_intervals=0),
    dcc.Interval(id="notif-poll",    interval=60_000,  n_intervals=0),
    dcc.Store(id="nav-state",        data={"cat": None, "inst": None}),
    dcc.Store(id="active-page",      data="dashboard"),
    dcc.Store(id="rules-version",    data=0),
    dcc.Store(id="cr-version",       data=0),
    dcc.Store(id="cr-cat-filter",    data=""),
    dcc.Store(id="notify-status",    data={}),
    dcc.Store(id="auth-store",       data={}),
    dcc.Store(id="inst-active-page", data="inst_dashboard"),
    dcc.Store(id="inst-notif-show",  data=False),
    dcc.Store(id="login-type",       data="bnr"),
    # Downloads wrapped in a fullscreen loader → "preparing your download" overlay
    # while a report/zip is built server-side (no effect on instant route redirects).
    dcc.Loading(
        id="download-loading", fullscreen=True, type="circle", color=BRAND,
        delay_show=300,
        children=html.Div([
            dcc.Download(id="inst-download"),
            dcc.Download(id="issues-download"),
            dcc.Download(id="resolved-inst-download"),
            dcc.Download(id="open-issue-dl"),
            dcc.Location(id="dl-nav", refresh=True),  # large-file downloads via /download route
            dcc.Download(id="cr-tbl-dl"),
        ]),
    ),
    dcc.Store(id="resolved-dl-lb",  data=None),
    dcc.Store(id="dl-preview-lb", data=None),

    # ── download preview modal ────────────────────────────────────────────────
    html.Div(
        id="dl-preview-modal",
        style={"display": "none"},
        children=[
            # backdrop
            html.Div(style={
                "position": "fixed", "inset": "0",
                "background": "rgba(28,28,39,0.55)", "zIndex": "900",
            }),
            # dialog
            html.Div([
                # header
                html.Div([
                    html.Div([
                        html.Span("⬇", style={"fontSize": "18px", "marginRight": "10px",
                                              "color": BRAND}),
                        html.Span(id="dl-modal-title",
                                  style={"fontSize": "15px", "fontWeight": "700",
                                         "color": TEXT}),
                    ], style={"display": "flex", "alignItems": "center"}),
                    html.Div(id="dl-modal-subtitle",
                             style={"fontSize": "12px", "color": MUTED,
                                    "marginTop": "2px"}),
                ], style={
                    "padding": "18px 22px 14px",
                    "borderBottom": f"1px solid {DIVIDER}",
                }),

                # issues table
                html.Div(
                    id="dl-modal-table",
                    style={
                        "maxHeight": "420px", "overflowY": "auto",
                        "padding": "0",
                    },
                ),

                # footer buttons
                html.Div([
                    html.Div("Cancel", id="dl-modal-cancel", n_clicks=0,
                             style={
                                 "padding": "8px 22px", "borderRadius": "5px",
                                 "border": f"1px solid {DIVIDER}",
                                 "fontSize": "13px", "fontWeight": "600",
                                 "color": MUTED, "cursor": "pointer",
                                 "background": CARD,
                             }),
                    html.Div("Download Report", id="dl-modal-confirm", n_clicks=0,
                             style={
                                 "padding": "8px 22px", "borderRadius": "5px",
                                 "fontSize": "13px", "fontWeight": "700",
                                 "color": CARD, "cursor": "pointer",
                                 "background": BRAND,
                             }),
                ], style={
                    "display": "flex", "justifyContent": "flex-end",
                    "gap": "10px", "padding": "14px 22px",
                    "borderTop": f"1px solid {DIVIDER}",
                }),
            ], style={
                "position": "fixed",
                "top": "50%", "left": "50%",
                "transform": "translate(-50%, -50%)",
                "zIndex": "901",
                "background": CARD,
                "borderRadius": "10px",
                "boxShadow": "0 8px 40px rgba(28,28,39,0.22)",
                "width": "min(860px, 92vw)",
                "fontFamily": FONT,
            }),
        ],
    ),

    # ── resolved-download "not ready" toast ──────────────────────────────────
    html.Div(
        id="dl-no-report-toast",
        children=[],
        style={"display": "none"},
    ),

], style={"background": BG, "minHeight": "100vh", "fontFamily": FONT})


# ── register Flask routes + callbacks (must come after app/server/layout) ─────
import dashboard.routes  # noqa: F401,E402

from dashboard.callbacks import (  # noqa: F401,E402
    alerts as _cb_alerts,
    auth as _cb_auth,
    downloads as _cb_downloads,
    institution_portal as _cb_institution_portal,
    navigation as _cb_navigation,
    notifications as _cb_notifications,
    pipeline_status as _cb_pipeline_status,
    remediation as _cb_remediation,
)


# ── dev server ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
