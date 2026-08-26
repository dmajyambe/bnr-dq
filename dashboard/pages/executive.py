from __future__ import annotations
from dash import html
from dashboard.theme import MUTED, TEXT, FONT


def _executive_page(role: str, le_books: list | None = None) -> html.Div:
    return html.Div([
        html.Div("Management Dashboard", style={
            "fontSize": "22px", "fontWeight": "700", "color": TEXT,
            "marginBottom": "8px",
        }),
        html.Div("Executive overview coming soon.", style={
            "color": MUTED, "fontSize": "14px", "fontFamily": FONT,
        }),
    ], style={"padding": "40px"})
