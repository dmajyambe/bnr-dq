from __future__ import annotations
from dash import dcc, html
from dashboard.theme import (
    BG, BRAND, CARD, DIVIDER, MUTED, TABLE_NAMES_PRETTY, TEXT,
)

_FONT = "'Inter','Franklin Gothic Medium',Arial,sans-serif"


def _profiling_page(categories: dict) -> html.Div:
    inst_options = sorted(
        [{"label": v.get("name", lb), "value": lb}
         for lb, v in categories.items()],
        key=lambda x: x["label"],
    )
    table_options = [{"label": lbl, "value": tbl}
                     for tbl, lbl in TABLE_NAMES_PRETTY.items()]

    filter_bar = html.Div([
        html.Div([
            html.Span("Institution", style={"fontSize": "11px", "color": MUTED,
                                            "fontWeight": "700", "marginBottom": "4px",
                                            "display": "block"}),
            dcc.Dropdown(
                id="prof-inst-dd",
                options=inst_options,
                placeholder="Select institution…",
                clearable=True,
                style={"fontSize": "13px", "minWidth": "260px"},
            ),
        ], style={"flex": "1"}),

        html.Div([
            html.Span("Table", style={"fontSize": "11px", "color": MUTED,
                                      "fontWeight": "700", "marginBottom": "4px",
                                      "display": "block"}),
            dcc.Dropdown(
                id="prof-table-dd",
                options=table_options,
                placeholder="Select table…",
                clearable=True,
                style={"fontSize": "13px", "minWidth": "220px"},
            ),
        ], style={"flex": "1"}),

        html.Div([
            html.Span("Run date", style={"fontSize": "11px", "color": MUTED,
                                         "fontWeight": "700", "marginBottom": "4px",
                                         "display": "block"}),
            html.Div(id="prof-run-date-dd"),
        ], style={"minWidth": "180px"}),
    ], style={
        "display": "flex", "gap": "20px", "flexWrap": "wrap",
        "background": CARD, "borderRadius": "8px", "padding": "16px 20px",
        "border": f"1px solid {DIVIDER}", "marginBottom": "20px",
    })

    return html.Div([
        html.Div([
            html.H2("Data Profiling", style={
                "fontSize": "18px", "fontWeight": "900", "color": TEXT,
                "margin": "0",
            }),
        ], style={"display": "flex", "alignItems": "baseline", "marginBottom": "20px"}),

        filter_bar,

        html.Div(id="prof-summary-cards", style={"marginBottom": "20px"}),
        html.Div(id="prof-column-table"),
    ], style={"padding": "28px 32px", "maxWidth": "1400px", "margin": "0 auto"})
