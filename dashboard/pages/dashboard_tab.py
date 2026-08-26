# Main "Dashboard" tab (category/institution view)
from __future__ import annotations
from dash import dcc, html
from dashboard.components import (
    _kpi_card, _stale_banner, _table_score_heatmap,
)
from dashboard.data import (
    _DIR, _cat_scores, _filter_institutions, _inst_scores,
    _table_dim_scores, _today_entry, _trend_entries,
)
from dashboard.theme import (
    BG, BRAND, CARD, CAT_LABELS, DIMS, DIVIDER, FONT,
    MUTED, RESOLVED_GREEN, TEXT,
)

def _dashboard_content(cat: str, inst: str | None) -> html.Div:
    """Renders the dashboard for a specific category, optionally filtered to one institution."""
    today        = _today_entry()
    trend        = _trend_entries(7)
    banner       = _stale_banner()
    institutions = _filter_institutions(today, cat)
    cat_label    = CAT_LABELS.get(cat, cat)

    # Institution dropdown options — "All [Category]" first, then sorted by name
    inst_options = [{"label": f"All {cat_label}", "value": ""}]
    for code, data in sorted(institutions.items(),
                              key=lambda kv: (kv[1].get("name") or kv[0]).lower()):
        name = (data.get("name") or code).title()
        inst_options.append({"label": name, "value": code})

    if inst and inst in institutions:
        cards = []
        for dim in DIMS:
            now   = float(_inst_scores(today, inst).get(dim, 0))
            spark = [float(_inst_scores(e, inst).get(dim, 0)) for e in trend]
            cards.append(_kpi_card(dim, now, spark))
        tim_now   = float(_inst_scores(today, inst).get("timeliness", 0))
        tim_spark = [float(_inst_scores(e, inst).get("timeliness", 0)) for e in trend]
        cards.append(_kpi_card("timeliness", tim_now if tim_now > 0 else None, tim_spark))
        display_insts    = {inst: institutions[inst]}
        inst_name        = (institutions[inst].get("name") or inst).title()
        table_title      = f"ISSUE REPORT — {inst_name.upper()}"
        table_breakdown  = _table_dim_scores(inst)
    else:
        cards = []
        for dim in DIMS:
            now   = float(_cat_scores(today, cat).get(dim) or 0)
            spark = [float(_cat_scores(e, cat).get(dim) or 0) for e in trend]
            cards.append(_kpi_card(dim, now, spark))
        tim_now   = float(_cat_scores(today, cat).get("timeliness") or 0)
        tim_spark = [float(_cat_scores(e, cat).get("timeliness") or 0) for e in trend]
        cards.append(_kpi_card("timeliness", tim_now if tim_now > 0 else None, tim_spark))
        display_insts    = institutions
        n                = len(institutions)
        table_title      = f"ISSUE REPORTS — {cat_label.upper()}  ({n} institutions)"
        table_breakdown  = {}

    return html.Div([
        banner if banner else html.Div(),

        # breadcrumb row
        html.Div([
            html.Span(
                "← All Categories",
                id={"type": "nav-action", "index": "back"},
                n_clicks=0,
                style={
                    "cursor":     "pointer",
                    "color":      BRAND,
                    "fontSize":   "12px",
                    "fontWeight": "700",
                    "userSelect": "none",
                },
            ),
            html.Span(" / ", style={
                "color": MUTED, "margin": "0 8px", "fontSize": "12px",
            }),
            html.Span(cat_label, style={"fontSize": "12px", "color": MUTED}),
        ], style={"marginBottom": "16px", "display": "flex", "alignItems": "center"}),

        # category header + institution filter row
        html.Div([
            html.Div([
                html.Div(cat_label, style={
                    "fontSize":   "20px",
                    "fontWeight": "900",
                    "color":      TEXT,
                    "lineHeight": "1.15",
                }),
                html.Div(
                    f"{len(institutions)} institution" + ("s" if len(institutions) != 1 else ""),
                    style={"fontSize": "12px", "color": MUTED, "marginTop": "3px"},
                ),
            ]),
            html.Div([
                html.Span("Filter by institution:", style={
                    "fontSize":      "11px",
                    "fontWeight":    "900",
                    "color":         MUTED,
                    "textTransform": "uppercase",
                    "letterSpacing": "0.05em",
                    "marginRight":   "10px",
                    "whiteSpace":    "nowrap",
                }),
                dcc.Dropdown(
                    id={"type": "inst-dd", "index": "main"},
                    options=inst_options,
                    value=inst or "",
                    clearable=False,
                    style={
                        "fontSize":   "12px",
                        "fontFamily": FONT,
                        "minWidth":   "280px",
                    },
                ),
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={
            "display":        "flex",
            "alignItems":     "center",
            "justifyContent": "space-between",
            "flexWrap":       "wrap",
            "gap":            "12px",
            "marginBottom":   "20px",
            "background":     CARD,
            "padding":        "16px 20px",
            "borderRadius":   "8px",
            "border":         f"1px solid {DIVIDER}",
            "boxShadow":      "0 1px 4px rgba(117,57,24,0.06)",
        }),

        # KPI cards
        html.Div(
            html.Div(id="kpi-row", children=cards, style={
                "display":  "flex",
                "gap":      "12px",
                "flexWrap": "wrap",
            }),
            style={
                "background":   CARD,
                "padding":      "20px",
                "borderRadius": "8px",
                "boxShadow":    "0 2px 8px rgba(117,57,24,0.07)",
                "border":       f"1px solid {DIVIDER}",
                "marginBottom": "20px",
            },
        ),

        # Per-table score breakdown — only when a single institution is selected
        html.Div(
            _table_score_heatmap(table_breakdown),
            style={
                "background":   CARD,
                "padding":      "20px",
                "borderRadius": "8px",
                "boxShadow":    "0 1px 4px rgba(117,57,24,0.06)",
                "border":       f"1px solid {DIVIDER}",
                "marginBottom": "20px",
            },
        ) if table_breakdown else html.Div(),

        # Issue reports table
        html.Div([
            html.Div(table_title, id="table-title", style={
                "fontSize":      "12px",
                "fontWeight":    "900",
                "color":         TEXT,
                "letterSpacing": "0.03em",
                "marginBottom":  "12px",
                "lineHeight":    "1.15",
            }),
            _institution_reports_table(display_insts),
        ]),

    ])


def _institution_reports_table(display_insts: dict) -> html.Div:
    """Issue reports table — one row per institution, ZIP download button."""
    import zipfile as _zf

    # NOTE: was `Path(__file__).parent / "issue_reports"` in the original —
    # that resolved relative to dq_dashboard_dash.py's own directory (the
    # project root). Moved into dashboard/pages/, so __file__ would now
    # resolve one level too deep; using dashboard.data._DIR (still the
    # project root) instead to preserve the actual path.
    issue_dir = _DIR / "issue_reports"

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em",
         "lineHeight": "1.15", "flexShrink": "0"}

    header = html.Div([
        html.Span("Code",        style={**H, "width": "56px"}),
        html.Span("Institution", style={**H, "flex": "1"}),
        html.Span("Tables with issues", style={**H, "width": "220px", "textAlign": "left"}),
        html.Span("ZIP size",    style={**H, "width": "74px", "textAlign": "right"}),
        html.Span("Report",      style={**H, "width": "66px", "textAlign": "center"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "10px",
        "padding": "9px 14px",
        "borderBottom": "2px solid rgba(117,57,24,0.18)",
        "background": BG, "borderRadius": "8px 8px 0 0",
    })

    # Build rows — institutions with ZIPs first (sorted by size desc), then clean ones
    with_zip = []
    no_zip   = []

    for lb, data in display_insts.items():
        name = (data.get("name") or lb).title()
        zips = sorted(
            (p for p in issue_dir.glob(f"{lb}_*.zip") if not p.stem.endswith("_resolved")),
            reverse=True,
        ) if issue_dir.exists() else []
        if zips:
            zp = zips[0]
            try:
                size_kb    = zp.stat().st_size // 1024
                namelist   = _zf.ZipFile(zp).namelist()
                table_names = list(dict.fromkeys(
                    n.rsplit(".", 1)[0] for n in namelist
                    if not n.startswith("README_")
                ))
            except Exception:
                size_kb, table_names = 0, []
            with_zip.append((lb, name, size_kb, table_names, zp))
        else:
            no_zip.append((lb, name))

    with_zip.sort(key=lambda x: -x[2])

    data_rows = []
    for i, (lb, name, size_kb, table_names, zp) in enumerate(with_zip):
        bg = "#c9956c" if i % 2 == 0 else BG

        # table chips
        table_chips = html.Div(
            [html.Span(t, style={
                "fontSize": "9px", "fontWeight": "700",
                "background": "rgba(117,57,24,0.10)", "color": BRAND,
                "borderRadius": "3px", "padding": "2px 5px",
                "whiteSpace": "nowrap",
            }) for t in table_names],
            style={"display": "flex", "flexWrap": "wrap", "gap": "3px",
                   "width": "220px", "flexShrink": "0"},
        )

        size_label = f"{size_kb:,} KB" if size_kb >= 1 else "< 1 KB"

        dl_btn = html.Div("⬇ ZIP",
            id={"type": "open-issue-dl-btn", "index": lb},
            n_clicks=0,
            title=f"Download issue report for {name}",
            style={
                "cursor": "pointer", "background": BRAND, "color": CARD,
                "fontSize": "10px", "fontWeight": "700",
                "padding": "3px 10px", "borderRadius": "4px",
                "userSelect": "none", "width": "66px",
                "textAlign": "center", "flexShrink": "0",
            },
        )

        data_rows.append(html.Div([
            html.Span(lb, style={"fontWeight": "900", "fontSize": "12px",
                "color": TEXT, "width": "56px", "flexShrink": "0"}),
            html.Span(name, style={"fontSize": "12px", "color": TEXT,
                "flex": "1", "overflow": "hidden",
                "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            table_chips,
            html.Span(size_label, style={"fontSize": "11px", "color": MUTED,
                "width": "74px", "textAlign": "right", "flexShrink": "0"}),
            dl_btn,
        ], style={
            "display": "flex", "alignItems": "center", "gap": "10px",
            "padding": "8px 14px", "background": bg,
            "borderBottom": f"1px solid rgba(117,57,24,0.12)",
        }))

    # clean institutions (no ZIP) — greyed out at bottom
    for i, (lb, name) in enumerate(sorted(no_zip, key=lambda x: x[1])):
        bg = CARD if i % 2 == 0 else BG
        data_rows.append(html.Div([
            html.Span(lb, style={"fontWeight": "900", "fontSize": "12px",
                "color": MUTED, "width": "56px", "flexShrink": "0"}),
            html.Span(name, style={"fontSize": "12px", "color": MUTED,
                "flex": "1", "overflow": "hidden",
                "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
            html.Span("No issues found", style={"fontSize": "11px",
                "color": RESOLVED_GREEN, "fontWeight": "700",
                "width": "220px", "flexShrink": "0"}),
            html.Span("—", style={"fontSize": "11px", "color": MUTED,
                "width": "74px", "textAlign": "right", "flexShrink": "0"}),
            html.Span("—", style={"fontSize": "11px", "color": MUTED,
                "width": "66px", "textAlign": "center", "flexShrink": "0"}),
        ], style={
            "display": "flex", "alignItems": "center", "gap": "10px",
            "padding": "8px 14px", "background": bg,
            "borderBottom": f"1px solid rgba(117,57,24,0.12)",
        }))

    if not data_rows:
        return html.Div("No institution data for this category.",
            style={"color": MUTED, "fontSize": "12px",
                   "padding": "32px", "textAlign": "center"})

    return html.Div(
        [header] + data_rows,
        style={"border": f"1px solid {DIVIDER}", "borderRadius": "8px",
               "overflow": "hidden"},
    )
