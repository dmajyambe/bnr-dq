# Category-picker landing page — moved from dq_dashboard_dash.py.
from __future__ import annotations
from dash import html
from dashboard.data import _today_entry
from dashboard.theme import CARD, LANDING_CATS, MUTED, TEXT


def _landing_page(counts: dict) -> html.Div:
    """Full-screen landing page prompting the user to pick a category type."""
    today = _today_entry()
    all_inst = today.get("by_institution", {})

    cards = []
    for cat_def in LANDING_CATS:
        code    = cat_def["code"]
        label   = cat_def["label"]
        subtitle = cat_def["subtitle"]
        color   = cat_def["color"]
        types   = cat_def["types"]

        # count institutions whose category_type matches this landing card
        n_inst = sum(
            1 for d in all_inst.values()
            if d.get("category_type") in types
        )

        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)

        cards.append(html.Div(
            id={"type": "cat-landing-btn", "index": code},
            n_clicks=0,
            children=[
                html.Div(label, style={
                    "fontSize": "28px",
                    "fontWeight": "900",
                    "color": color,
                    "lineHeight": "1.1",
                    "marginBottom": "8px",
                    "letterSpacing": "-0.01em",
                }),
                html.Div(subtitle, style={
                    "fontSize": "12px",
                    "color": MUTED,
                    "lineHeight": "1.5",
                    "marginBottom": "24px",
                    "minHeight": "36px",
                }),
                html.Div([
                    html.Span(str(n_inst), style={
                        "fontSize": "36px",
                        "fontWeight": "900",
                        "color": color,
                        "fontVariantNumeric": "tabular-nums",
                        "lineHeight": "1",
                    }),
                    html.Span(
                        " institution" + ("s" if n_inst != 1 else ""),
                        style={"fontSize": "13px", "color": MUTED, "marginLeft": "4px"},
                    ),
                ], style={"marginBottom": "24px"}),
                html.Div("View dashboard →", style={
                    "display": "inline-block",
                    "fontSize": "12px",
                    "fontWeight": "700",
                    "color": CARD,
                    "background": color,
                    "padding": "8px 18px",
                    "borderRadius": "6px",
                }),
            ],
            style={
                "background":   CARD,
                "border":       f"1px solid rgba({r},{g},{b},0.20)",
                "borderTop":    f"4px solid {color}",
                "borderRadius": "10px",
                "padding":      "32px 28px",
                "cursor":       "pointer",
                "flex":         "1",
                "minWidth":     "220px",
                "boxShadow":    "0 2px 8px rgba(117,57,24,0.07)",
                "userSelect":   "none",
                "textAlign":    "left",
                "transition":   "box-shadow .15s",
            },
        ))

    return html.Div([
        html.Div([
            html.Div("Select Category Type", style={
                "fontSize": "26px",
                "fontWeight": "900",
                "color": TEXT,
                "marginBottom": "8px",
                "letterSpacing": "-0.01em",
                "lineHeight": "1.15",
            }),
            html.Div(
                "Choose a financial institution category to explore its data quality metrics.",
                style={
                    "fontSize": "14px",
                    "color": MUTED,
                    "marginBottom": "48px",
                    "lineHeight": "1.5",
                },
            ),
            html.Div(cards, style={
                "display":  "flex",
                "gap":      "24px",
                "flexWrap": "wrap",
            }),
        ], style={
            "maxWidth": "960px",
            "margin":   "80px auto",
            "padding":  "0 24px",
        }),
    ])
