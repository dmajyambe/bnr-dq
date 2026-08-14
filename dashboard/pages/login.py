# Login page (BNR / Institution / Management tabs) — moved from dq_dashboard_dash.py.
from __future__ import annotations

from dash import dcc, html

from dashboard.theme import BG, BRAND, CARD, C_RED, DIVIDER, FONT, MUTED, TEXT


def _login_page(error: str = "", login_type: str = "bnr") -> html.Div:
    inp = {
        "width": "100%", "padding": "10px 12px",
        "border": f"1px solid {DIVIDER}", "borderRadius": "6px",
        "fontSize": "14px", "fontFamily": FONT, "color": TEXT,
        "boxSizing": "border-box", "outline": "none", "background": CARD,
    }

    is_bnr  = (login_type == "bnr")
    is_inst = (login_type == "inst")

    def _tab(label, tab_id, active):
        return html.Div(
            label,
            id=tab_id,
            n_clicks=0,
            style={
                "flex": "1", "textAlign": "center",
                "padding": "11px 0",
                "fontSize": "12px", "fontWeight": "900",
                "cursor": "pointer", "userSelect": "none",
                "letterSpacing": "0.04em",
                "background":   CARD                       if active else "rgba(117,57,24,0.10)",
                "color":        BRAND                      if active else MUTED,
                "borderBottom": f"3px solid {BRAND}"      if active else f"3px solid transparent",
                "transition":   "all 0.15s",
            },
        )

    tab_bar = html.Div([
        _tab("🏛  Inspector",   "login-tab-bnr",  is_bnr),
        _tab("🏦  Institution", "login-tab-inst", is_inst),
    ], style={
        "display": "flex",
        "borderBottom": f"1px solid {DIVIDER}",
        "background": CARD,
    })

    if is_bnr:
        placeholder = "Write your BNR email here"
        hint = "BNR staff accounts only"
    else:
        placeholder = "focal.point@yourbank.com"
        hint = "Use the email address provided to you by BNR."
    btn_color = BRAND

    return html.Div([
        html.Div([
            # Card header
            html.Div([
                html.Img(src="/assets/bnr_img.png",
                         style={"height": "48px", "marginBottom": "10px"}),
                html.Div("DATA QUALITY PROGRAM", style={
                    "fontSize": "13px", "fontWeight": "900", "color": CARD,
                    "letterSpacing": "0.07em",
                }),
                html.Div("National Bank of Rwanda", style={
                    "fontSize": "11px", "color": "rgba(255,255,255,0.65)", "marginTop": "3px",
                }),
            ], style={
                "background": BRAND, "padding": "28px 32px 22px",
                "textAlign": "center", "borderRadius": "12px 12px 0 0",
            }),

            # Login type tabs
            tab_bar,

            # Form body
            html.Div([
                html.Div(
                    "Inspector Sign In" if is_bnr else "Institution Sign In",
                    style={
                        "fontSize": "15px", "fontWeight": "900", "color": TEXT,
                        "marginBottom": "22px", "textAlign": "center",
                    },
                ),

                html.Div("Email", style={
                    "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                    "textTransform": "uppercase", "letterSpacing": "0.05em",
                    "marginBottom": "5px",
                }),
                dcc.Input(
                    id="login-email", type="email",
                    placeholder=placeholder,
                    debounce=False, n_submit=0,
                    style={**inp, "marginBottom": "16px"},
                ),

                html.Div("Password", style={
                    "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                    "textTransform": "uppercase", "letterSpacing": "0.05em",
                    "marginBottom": "5px",
                }),
                dcc.Input(
                    id="login-password", type="password",
                    placeholder="••••••••",
                    debounce=False, n_submit=0,
                    style={**inp, "marginBottom": "8px"},
                ),

                html.Div(hint, id="login-hint", style={
                    "fontSize": "11px", "color": MUTED, "marginBottom": "20px",
                }),

                html.Div(
                    error,
                    id="login-error",
                    style={
                        "fontSize": "12px", "color": C_RED, "marginBottom": "14px",
                        "minHeight": "16px", "textAlign": "center",
                        "display": "block" if error else "none",
                    },
                ),

                html.Div(
                    "Sign In",
                    id="login-btn",
                    n_clicks=0,
                    style={
                        "width": "100%", "padding": "11px 0",
                        "background": btn_color, "color": CARD,
                        "fontSize": "14px", "fontWeight": "900",
                        "textAlign": "center", "borderRadius": "6px",
                        "cursor": "pointer", "userSelect": "none",
                        "letterSpacing": "0.04em",
                    },
                ),
            ], style={"padding": "28px 32px", "background": BG}),

        ], style={
            "background": CARD, "borderRadius": "12px",
            "boxShadow": "0 8px 32px rgba(0,0,0,0.14)",
            "width": "400px",
        }),
    ], style={
        "display": "flex", "alignItems": "center", "justifyContent": "center",
        "minHeight": "100vh", "background": BG,
    })
