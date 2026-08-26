from __future__ import annotations

import json

from dash import Input, Output, State, dcc, html

from dashboard.app import app
from dashboard.components import _empty_state
from dashboard.theme import (
    BG, BRAND, C_AMBER, C_GREEN, C_RED, CARD, DIVIDER, MUTED, TABLE_NAMES_PRETTY, TEXT,
)

_FONT = "'Inter','Franklin Gothic Medium',Arial,sans-serif"


def _available_run_dates(le_book: str, table: str) -> list[str]:
    from dq.profiling.storage import available_run_dates
    return available_run_dates(le_book, table)


def _load_profile(le_book: str, table: str, run_date: str) -> list[dict]:
    from dq.profiling.storage import load_profile
    return load_profile(le_book, table, run_date)


# shared UI builders 

def _pct_bar(pct: float, fill_color: str, bg_color: str = "#e9e3dd",
             height: str = "8px") -> html.Div:
    """Horizontal bar showing a percentage — same style as Informatica."""
    pct = max(0.0, min(100.0, pct or 0.0))
    return html.Div([
        html.Div(style={
            "width": f"{pct:.1f}%", "height": height,
            "background": fill_color, "borderRadius": "3px 0 0 3px",
            "minWidth": "2px" if pct > 0 else "0",
        }),
        html.Div(style={
            "flex": "1", "height": height,
            "background": bg_color, "borderRadius": "0 3px 3px 0",
        }),
    ], style={"display": "flex", "width": "100%", "borderRadius": "3px"})


def _null_color(pct: float) -> str:
    if pct >= 20:  return C_RED
    if pct >= 5:   return C_AMBER
    return C_GREEN


def _top_vals(raw: str | None) -> html.Div:
    if not raw:
        return html.Span("—", style={"color": MUTED, "fontSize": "11px"})
    try:
        pairs = json.loads(raw)   # [[value, count], ...]
    except Exception:
        return html.Span("—", style={"color": MUTED, "fontSize": "11px"})
    chips = []
    for val, cnt in pairs[:5]:
        label = str(val) if val is not None else "NULL"
        if len(label) > 16:
            label = label[:14] + "…"
        chips.append(html.Span(
            f"{label} ({cnt:,})",
            style={
                "fontSize": "10px", "background": BG,
                "border": f"1px solid {DIVIDER}",
                "borderRadius": "4px", "padding": "1px 6px",
                "marginRight": "4px", "whiteSpace": "nowrap",
                "display": "inline-block",
            },
        ))
    return html.Div(chips, style={"display": "flex", "flexWrap": "wrap", "gap": "2px"})


def _summary_cards(rows: list[dict]) -> html.Div:
    if not rows:
        return html.Div()
    row_count    = rows[0].get("row_count") or 0
    n_cols       = len(rows)
    avg_null_pct = sum(r.get("null_pct") or 0 for r in rows) / n_cols if n_cols else 0
    completeness = 100 - avg_null_pct

    def _card(value: str, label: str, color: str) -> html.Div:
        return html.Div([
            html.Div(value, style={"fontSize": "26px", "fontWeight": "900",
                                   "color": color, "lineHeight": "1"}),
            html.Div(label, style={"fontSize": "10px", "color": MUTED,
                                   "textTransform": "uppercase",
                                   "letterSpacing": "0.05em", "marginTop": "5px"}),
        ], style={
            "background": CARD, "borderRadius": "8px", "padding": "16px 20px",
            "border": f"1px solid {DIVIDER}", "borderTop": f"3px solid {color}",
            "minWidth": "140px",
        })

    clr = C_GREEN if completeness >= 95 else C_AMBER if completeness >= 80 else C_RED
    return html.Div([
        _card(f"{row_count:,}",      "Total Rows",       BRAND),
        _card(str(n_cols),           "Columns Profiled",  BRAND),
        _card(f"{completeness:.1f}%", "Avg Completeness", clr),
    ], style={"display": "flex", "gap": "14px", "flexWrap": "wrap"})


def _column_table(rows: list[dict]) -> html.Div:
    _H = {"fontSize": "10px", "fontWeight": "900", "color": MUTED,
          "textTransform": "uppercase", "letterSpacing": "0.05em",
          "padding": "7px 12px", "whiteSpace": "nowrap",
          "display": "flex", "alignItems": "center"}

    hdr = html.Div([
        html.Div("Column",     style={**_H, "flex": "1"}),
        html.Div("% Null",     style={**_H, "width": "150px"}),
        html.Div("% Distinct", style={**_H, "width": "150px"}),
        html.Div("Data Type",  style={**_H, "width": "120px"}),
    ], style={"display": "flex", "background": BG,
              "borderRadius": "8px 8px 0 0",
              "borderBottom": f"2px solid {DIVIDER}"})

    ROW_H = "52px"

    def _data_cell(style_extra: dict, children) -> html.Div:
        return html.Div(children, style={
            "display": "flex", "alignItems": "center",
            "padding": "0 12px", "height": ROW_H, "boxSizing": "border-box",
            **style_extra,
        })

    def _pct_cell(pct: float, count: int, color: str, width: str) -> html.Div:
        return html.Div([
            html.Div([
                html.Span(f"{pct:.1f}%", style={
                    "fontSize": "11px", "color": color, "fontWeight": "700",
                }),
                html.Span(f"{count:,}", style={
                    "fontSize": "10px", "color": MUTED,
                    "marginLeft": "6px",
                }),
            ], style={"marginBottom": "4px"}),
            _pct_bar(pct, color),
        ], style={
            "width": width, "padding": "0 12px", "height": ROW_H,
            "display": "flex", "flexDirection": "column", "justifyContent": "center",
            "boxSizing": "border-box",
        })

    _TYPE_COLORS = {
        "Numerical": "#3b82f6",
        "Text":      "#6b7280",
        "Date":      "#8b5cf6",
        "Boolean":   "#f59e0b",
    }

    table_rows = []
    for i, r in enumerate(rows):
        null_pct     = r.get("null_pct")     or 0.0
        distinct_pct = r.get("distinct_pct") or 0.0
        null_count   = r.get("null_count")   or 0
        dist_count   = r.get("distinct_count") or 0
        data_type    = r.get("data_type") or "—"
        n_clr        = _null_color(null_pct)
        bg           = "rgba(244,246,249,0.7)" if i % 2 == 0 else CARD
        type_color   = _TYPE_COLORS.get(data_type, MUTED)

        table_rows.append(html.Div([
            # Column name
            _data_cell({"flex": "1"}, html.Span(r["column_name"], style={
                "fontSize": "12px", "fontWeight": "600", "color": TEXT,
            })),
            # % Null — bar + count inline
            _pct_cell(null_pct, null_count, n_clr, "150px"),
            # % Distinct — bar + count inline
            _pct_cell(distinct_pct, dist_count, BRAND, "150px"),
            # Data Type
            html.Div(html.Span(data_type, style={
                "fontSize": "11px", "fontWeight": "600", "color": type_color,
                "background": f"{type_color}18",
                "border": f"1px solid {type_color}40",
                "borderRadius": "4px", "padding": "2px 8px",
            }), style={
                "width": "120px", "height": ROW_H, "padding": "0 12px",
                "display": "flex", "alignItems": "center",
                "boxSizing": "border-box",
            }),
        ], style={
            "display": "flex", "alignItems": "stretch",
            "background": bg, "borderBottom": f"1px solid {DIVIDER}",
        }))

    return html.Div(
        [hdr, *table_rows],
        style={"background": CARD, "borderRadius": "8px",
               "border": f"1px solid {DIVIDER}"},
    )


#BNR callbacks 

@app.callback(
    Output("prof-run-date-dd", "children"),
    Input("prof-inst-dd",  "value"),
    Input("prof-table-dd", "value"),
    prevent_initial_call=False,
)
def _prof_run_dates(le_book, table):
    if not le_book or not table:
        return html.Span("—", style={"fontSize": "12px", "color": MUTED,
                                      "lineHeight": "38px", "display": "block"})
    dates = _available_run_dates(le_book, table)
    if not dates:
        return html.Span("No data yet", style={"fontSize": "12px", "color": MUTED,
                                                "lineHeight": "38px", "display": "block"})
    return dcc.Dropdown(
        id="prof-run-date-select",
        options=[{"label": d, "value": d} for d in dates],
        value=dates[0],
        clearable=False,
        style={"fontSize": "13px", "minWidth": "160px"},
    )


@app.callback(
    Output("prof-summary-cards", "children"),
    Output("prof-column-table",  "children"),
    Input("prof-inst-dd",  "value"),
    Input("prof-table-dd", "value"),
    Input("prof-run-date-dd", "children"),   # re-fires when run-date dropdown rebuilt
    State("prof-run-date-select", "value"),
    prevent_initial_call=False,
    # prof-run-date-select is rendered dynamically; suppress missing-component errors
)
def _prof_content(le_book, table, _run_date_rebuilt, run_date):
    # when the run-date dropdown hasn't been rendered yet, run_date comes in as None
    if run_date is None and le_book and table:
        dates = _available_run_dates(le_book, table)
        run_date = dates[0] if dates else None
    if not le_book or not table:
        return html.Div(), _empty_state(
            "Select an institution and table",
            "Choose both filters above to view column-level profiling statistics.",
            icon="⊞",
        )
    if not run_date:
        return html.Div(), _empty_state(
            "No profile data yet",
            "Profiling statistics are generated during the monthly pipeline run.",
            icon="⊞",
        )
    rows = _load_profile(le_book, table, run_date)
    if not rows:
        tbl_label = TABLE_NAMES_PRETTY.get(table, table)
        return html.Div(), _empty_state(
            f"No profile data for {tbl_label}",
            "This table has not been profiled yet for the selected run.",
            icon="⊞",
        )
    return _summary_cards(rows), _column_table(rows)


# institution portal callbacks 

@app.callback(
    Output("inst-prof-run-date-dd", "children"),
    Input("inst-prof-table-dd", "value"),
    State("auth-store", "data"),
    prevent_initial_call=False,
)
def _inst_prof_run_dates(table, auth_data):
    le_books = list((auth_data or {}).get("le_books", []))
    le_book  = str(le_books[0]) if le_books else None
    if not le_book or not table:
        return html.Span("—", style={"fontSize": "12px", "color": MUTED,
                                      "lineHeight": "38px", "display": "block"})
    dates = _available_run_dates(le_book, table)
    if not dates:
        return html.Span("No data yet", style={"fontSize": "12px", "color": MUTED,
                                                "lineHeight": "38px", "display": "block"})
    return dcc.Dropdown(
        id="inst-prof-run-date-select",
        options=[{"label": d, "value": d} for d in dates],
        value=dates[0],
        clearable=False,
        style={"fontSize": "13px", "minWidth": "160px"},
    )


@app.callback(
    Output("inst-prof-summary-cards", "children"),
    Output("inst-prof-column-table",  "children"),
    Input("inst-prof-table-dd",    "value"),
    Input("inst-prof-run-date-dd", "children"),
    State("inst-prof-run-date-select", "value"),
    State("auth-store", "data"),
    prevent_initial_call=False,
)
def _inst_prof_content(table, _run_date_rebuilt, run_date, auth_data):
    le_books = list((auth_data or {}).get("le_books", []))
    le_book  = str(le_books[0]) if le_books else None
    if run_date is None and le_book and table:
        dates = _available_run_dates(le_book, table)
        run_date = dates[0] if dates else None
    if not le_book or not table:
        return html.Div(), _empty_state(
            "Select a table",
            "Choose a table above to view its column-level profiling statistics.",
            icon="⊞",
        )
    if not run_date:
        return html.Div(), _empty_state(
            "No profile data yet",
            "Profiling statistics are generated during the monthly pipeline run.",
            icon="⊞",
        )
    rows = _load_profile(le_book, table, run_date)
    if not rows:
        tbl_label = TABLE_NAMES_PRETTY.get(table, table)
        return html.Div(), _empty_state(
            f"No profile data for {tbl_label}",
            "This table has not been profiled yet for the selected run.",
            icon="⊞",
        )
    return _summary_cards(rows), _column_table(rows)
