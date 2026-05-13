from __future__ import annotations
import csv
import io
import json
import logging
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
import dash
from dash import dcc, html, Input, Output, ALL, ctx, State
import plotly.graph_objects as go
from dq_rules import (
    get_all_rules, get_user_rules, get_draft_rules,
    add_user_rule, next_user_rule_id,
    approve_draft_rule, delete_draft_rule,
)
log = logging.getLogger("dq_dashboard")

# ── per-institution report generation state (server-side) ─────────────────────
# le_book → subprocess.Popen while running, "done", or "error:<msg>"
_gen_procs: dict = {}
_gen_lock = threading.Lock()

#file paths
_DIR            = Path(__file__).parent
HISTORY_FILE    = _DIR / "dq_history.json"
CATEGORIES_FILE = _DIR / "le_book_categories.json"
PIPELINE_FILE   = _DIR / "pipeline_run.json"
REPORTS_DIR     = _DIR / "reports"

# design tokens
BNR_GOLD = "#C8A42C"
BNR_NAVY = "#1A3A6B"
BG       = "#F4F6F9"
CARD     = "#FFFFFF"
TEXT     = "#1A1A2E"
MUTED    = "#6B7280"
DIVIDER  = "#E2E8F0"
C_GREEN  = "#16A34A"
C_AMBER  = "#D97706"
C_RED    = "#DC2626"
FONT     = "'BentonSans','Franklin Gothic Medium','Arial Narrow',Arial,sans-serif"

# one color per dimension
DIM_COLORS = {
    "completeness": "#2563EB",
    "accuracy":     "#16A34A",
    "timeliness":   "#D97706",
    "validity":     "#7C3AED",
}

DIMS = ["completeness", "accuracy", "timeliness", "validity"]
DIM_LABELS = {
    "completeness": "Completeness",
    "accuracy":     "Accuracy",
    "timeliness":   "Timeliness",
    "validity":     "Validity",
}

# Internal category codes (kept for data access helpers)
CATEGORIES = ["ALL", "B", "MF", "SACCO", "OSACCO"]
CAT_LABELS = {
    "ALL":    "All Institutions",
    "B":      "Banks",
    "MF":     "Microfinance",
    "SACCO":  "SACCOs",
    "OSACCO": "OSACCOs",
}

# Landing page category definitions
# "SACCO" combines both SACCO and OSACCO institution types
LANDING_CATS = [
    {
        "code":     "B",
        "label":    "Banks",
        "subtitle": "Commercial & savings banks",
        "color":    "#2563EB",
        "types":    ["B"],
    },
    {
        "code":     "MF",
        "label":    "Microfinance",
        "subtitle": "Microfinance institutions",
        "color":    "#16A34A",
        "types":    ["MF"],
    },
    {
        "code":     "SACCO",
        "label":    "SACCO",
        "subtitle": "Savings & credit cooperatives (incl. OSACCOs)",
        "color":    "#D97706",
        "types":    ["SACCO", "OSACCO"],
    },
]


# ── data loading ───────────────────────────────────────────────────────────────

def _load_history() -> list:
    if not HISTORY_FILE.exists():
        return []
    try:
        data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as exc:
        log.warning("History load failed: %s", exc)
        return []

def _load_pipeline_run() -> dict:
    if not PIPELINE_FILE.exists():
        return {}
    try:
        return json.loads(PIPELINE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

_HISTORY  = _load_history()
_PIPELINE = _load_pipeline_run()


# ── data access helpers ────────────────────────────────────────────────────────

def _today_entry()      -> dict: return _HISTORY[-1]         if _HISTORY           else {}
def _yesterday_entry()  -> dict: return _HISTORY[-2]         if len(_HISTORY) >= 2 else {}
def _trend_entries(n=7) -> list: return _HISTORY[-n:]        if _HISTORY           else []


def _cat_scores(entry: dict, cat: str) -> dict:
    """Overall or per-category scores from one history entry.
    cat='SACCO' returns the average of SACCO+OSACCO (data stores these as OSACCO)."""
    if not entry:
        return {d: 0.0 for d in DIMS}
    if cat == "ALL":
        return entry.get("overall", {})
    if cat == "SACCO":
        by_cat = entry.get("by_category", {})
        sacco  = by_cat.get("SACCO",  {})
        osacco = by_cat.get("OSACCO", {})
        combined = {}
        for d in DIMS:
            vals = [float(src.get(d) or 0) for src in (sacco, osacco) if src]
            combined[d] = sum(vals) / len(vals) if vals else 0.0
        return combined
    return entry.get("by_category", {}).get(cat, {})


def _filter_institutions(entry: dict, cat: str) -> dict:
    """Return institutions dict filtered to the given category.
    cat='SACCO' includes both SACCO and OSACCO institution types."""
    inst = entry.get("by_institution", {}) if entry else {}
    if cat == "ALL":
        return inst
    if cat == "SACCO":
        return {lb: d for lb, d in inst.items()
                if d.get("category_type") in ("SACCO", "OSACCO")}
    return {lb: d for lb, d in inst.items() if d.get("category_type") == cat}


def _inst_scores(entry: dict, inst_code: str) -> dict:
    """Return dimension scores for a specific institution from one history entry."""
    if not entry or not inst_code:
        return {d: 0.0 for d in DIMS}
    d = entry.get("by_institution", {}).get(inst_code, {})
    return {dim: float(d.get(dim) or 0) for dim in DIMS}


def _category_counts(entry: dict) -> dict:
    counts = {c: 0 for c in CATEGORIES}
    counts["ALL"] = 0
    for data in entry.get("by_institution", {}).values():
        counts["ALL"] += 1
        ct = data.get("category_type", "")
        if ct in counts:
            counts[ct] += 1
    return counts


# ── score styling ──────────────────────────────────────────────────────────────

def _score_color(s: float) -> str:
    return C_GREEN if s >= 90 else C_AMBER if s >= 75 else C_RED

def _score_bg(s: float) -> str:
    return ("rgba(22,163,74,.10)"   if s >= 90 else
            "rgba(217,119,6,.10)"   if s >= 75 else
            "rgba(220,38,38,.10)")


# ── component builders ─────────────────────────────────────────────────────────

def _sparkline(values: list, color: str) -> dcc.Graph:
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    fig = go.Figure(go.Scatter(
        y=values or [0], mode="lines",
        line=dict(color=color, width=1.5),
        fill="tozeroy",
        fillcolor=f"rgba({r},{g},{b},0.12)",
    ))
    fig.update_layout(
        height=36,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False, range=[0, 100]),
        showlegend=False,
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False},
                     style={"height": "36px", "marginTop": "8px"})


def _kpi_card(dim: str, score: float, delta: float, spark: list) -> html.Div:
    col     = _score_color(score)
    d_col   = C_GREEN if delta > 0 else C_RED if delta < 0 else MUTED
    d_icon  = "▲" if delta > 0 else "▼" if delta < 0 else "─"
    d_label = f"{d_icon} {abs(delta):.1f}%"

    return html.Div([
        html.Div(DIM_LABELS[dim], style={
            "fontSize": "11px", "fontWeight": "900",
            "color": MUTED, "letterSpacing": "0.06em",
            "textTransform": "uppercase", "lineHeight": "1.15",
        }),
        html.Div(f"{score:.1f}%", style={
            "fontSize": "30px", "fontWeight": "700",
            "color": col, "lineHeight": "1.1", "marginTop": "6px",
            "fontVariantNumeric": "tabular-nums",
        }),
        html.Div([
            html.Span(d_label, style={
                "color": d_col, "fontWeight": "700", "fontSize": "12px",
            }),
            html.Span(" vs yesterday", style={
                "color": MUTED, "fontSize": "11px",
            }),
        ], style={"marginTop": "4px", "lineHeight": "1.15"}),
        _sparkline(spark, col),
    ], style={
        "background":   CARD,
        "borderRadius": "8px",
        "padding":      "16px",
        "flex":         "1",
        "minWidth":     "150px",
        "borderTop":    f"3px solid {col}",
        "boxShadow":    "0 1px 4px rgba(26,58,107,0.08)",
    })


def _trend_figure(trend: list, cat: str, inst_code: str | None = None) -> go.Figure:
    """Build trend chart. When inst_code is given, shows that institution's scores."""
    dates = [e.get("date", "") for e in trend]
    fig   = go.Figure()
    for dim in DIMS:
        if inst_code:
            scores = [float(_inst_scores(e, inst_code).get(dim, 0)) for e in trend]
        else:
            scores = [float(_cat_scores(e, cat).get(dim) or 0) for e in trend]
        fig.add_trace(go.Scatter(
            x=dates, y=scores,
            name=DIM_LABELS[dim],
            mode="lines+markers",
            line=dict(color=DIM_COLORS[dim], width=2),
            marker=dict(size=5, color=DIM_COLORS[dim]),
            hovertemplate=(
                f"<b>{DIM_LABELS[dim]}</b><br>"
                "%{x}<br>%{y:.1f}%<extra></extra>"
            ),
        ))
    fig.update_layout(
        height=250,
        paper_bgcolor=CARD, plot_bgcolor=CARD,
        margin=dict(l=8, r=8, t=36, b=8),
        font=dict(family=FONT, size=11, color=TEXT),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0, font=dict(size=11),
        ),
        yaxis=dict(
            range=[0, 100], gridcolor=DIVIDER,
            ticksuffix="%", tickfont=dict(size=10),
            zeroline=False,
        ),
        xaxis=dict(gridcolor=DIVIDER, tickfont=dict(size=10), showgrid=False),
        hovermode="x unified",
    )
    return fig


def _institution_table(institutions: dict, gen_status: dict | None = None) -> html.Div:
    if not institutions:
        return html.Div(
            "No institution data for this category.",
            style={"color": MUTED, "fontSize": "12px",
                   "padding": "32px", "textAlign": "center"},
        )

    rows = sorted(institutions.items(), key=lambda kv: kv[1].get("overall", 0))

    n_critical = sum(
        1 for _, d in rows
        if any(d.get(dim, 100) < 75 for dim in DIMS)
    )

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em",
         "lineHeight": "1.15", "flexShrink": "0"}
    COL_W = "74px"
    DL_W  = "52px"

    header = html.Div([
        html.Span("Institution", style={**H, "flex": "1", "flexShrink": "1"}),
        *[html.Span(DIM_LABELS[d][:5], style={**H, "width": COL_W, "textAlign": "center"})
          for d in DIMS],
        html.Span("Overall", style={**H, "width": COL_W, "textAlign": "center"}),
        html.Span("Report",  style={**H, "width": DL_W,  "textAlign": "center"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "4px",
        "padding": "9px 14px",
        "borderBottom": f"2px solid {DIVIDER}",
        "background": BG, "borderRadius": "8px 8px 0 0",
    })

    data_rows = []
    for i, (lb, d) in enumerate(rows):
        name    = (d.get("name") or lb).title()
        overall = float(d.get("overall") or 0)
        bg      = CARD if i % 2 == 0 else "#FAFBFC"

        cells = [
            html.Span(name, title=name, style={
                "flex": "1", "flexShrink": "1",
                "fontSize": "12px", "color": TEXT, "lineHeight": "1.15",
                "overflow": "hidden", "textOverflow": "ellipsis",
                "whiteSpace": "nowrap",
            }),
        ]
        for dim in DIMS:
            s = float(d.get(dim) or 0)
            cells.append(html.Span(f"{s:.0f}%", style={
                "width": COL_W, "textAlign": "center", "flexShrink": "0",
                "fontSize": "12px", "fontWeight": "700",
                "color": _score_color(s), "background": _score_bg(s),
                "borderRadius": "4px", "padding": "2px 0",
                "lineHeight": "1.15",
            }))
        cells.append(html.Span(f"{overall:.1f}%", style={
            "width": COL_W, "textAlign": "center", "flexShrink": "0",
            "fontSize": "12px", "fontWeight": "900",
            "color": _score_color(overall), "lineHeight": "1.15",
        }))

        report_exists = REPORTS_DIR.exists() and bool(list(REPORTS_DIR.glob(f"{lb}_*.xlsx")))
        gen_st        = (gen_status or {}).get(lb)

        if report_exists:
            dl_btn = html.Div(
                "⬇",
                id={"type": "inst-dl-btn", "index": lb},
                n_clicks=0,
                title=f"Download {name} issues report",
                style={
                    "width": DL_W, "textAlign": "center", "flexShrink": "0",
                    "fontSize": "15px", "lineHeight": "1.15",
                    "cursor": "pointer", "color": BNR_NAVY, "userSelect": "none",
                },
            )
        elif gen_st == "running":
            dl_btn = html.Div(
                "generating…",
                style={
                    "width": DL_W, "textAlign": "center", "flexShrink": "0",
                    "fontSize": "10px", "color": C_AMBER,
                    "fontStyle": "italic", "lineHeight": "1.15",
                },
            )
        else:
            btn_title = "Generate issues report"
            if gen_st and gen_st.startswith("error"):
                btn_title = f"Error — click to retry"
            dl_btn = html.Div(
                "Generate",
                id={"type": "gen-btn", "index": lb},
                n_clicks=0,
                title=btn_title,
                style={
                    "width": DL_W, "textAlign": "center", "flexShrink": "0",
                    "fontSize": "10px", "fontWeight": "700", "lineHeight": "1.15",
                    "cursor": "pointer",
                    "color": C_RED if (gen_st and gen_st.startswith("error")) else BNR_NAVY,
                    "background": "rgba(26,58,107,0.07)",
                    "border": f"1px solid rgba(26,58,107,0.22)",
                    "borderRadius": "4px", "padding": "3px 0",
                    "userSelect": "none",
                },
            )
        cells.append(dl_btn)

        data_rows.append(html.Div(cells, className="inst-row", style={
            "display": "flex", "alignItems": "center", "gap": "4px",
            "padding": "7px 14px", "background": bg,
            "borderBottom": f"1px solid {DIVIDER}",
        }))

    alert = None
    if n_critical:
        alert = html.Div(
            f"⚠  {n_critical} institution{'s' if n_critical > 1 else ''} "
            "with at least one dimension below 75% — requires attention",
            style={
                "background": "rgba(220,38,38,.07)",
                "border": "1px solid rgba(220,38,38,.25)",
                "borderRadius": "6px", "padding": "9px 14px",
                "fontSize": "12px", "color": C_RED,
                "marginBottom": "12px", "lineHeight": "1.15",
            },
        )

    table = html.Div(
        [header] + data_rows,
        style={"border": f"1px solid {DIVIDER}", "borderRadius": "8px",
               "overflow": "hidden"},
    )
    return html.Div([alert, table] if alert else [table])


def _stale_banner() -> html.Div | None:
    if not _HISTORY:
        return html.Div(
            "No pipeline data found. Run:  python dq_pipeline_2m.py --load",
            style={
                "background": "#FEF2F2", "border": f"1px solid {C_RED}",
                "borderRadius": "6px", "padding": "10px 16px",
                "fontSize": "12px", "color": C_RED,
                "marginBottom": "16px", "lineHeight": "1.15",
            },
        )
    last_date = _HISTORY[-1].get("date", "")
    if last_date != datetime.now().strftime("%Y-%m-%d"):
        return html.Div(
            f"⚠  Last pipeline run: {last_date} — today's run may not have completed yet.",
            style={
                "background": "#FFFBEB", "border": "1px solid #F59E0B",
                "borderRadius": "6px", "padding": "10px 16px",
                "fontSize": "12px", "color": "#92400E",
                "marginBottom": "16px", "lineHeight": "1.15",
            },
        )
    return None


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
                "boxShadow":    "0 2px 8px rgba(26,58,107,0.07)",
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


def _dashboard_content(cat: str, inst: str | None, gen_status: dict | None = None) -> html.Div:
    """Renders the dashboard for a specific category, optionally filtered to one institution."""
    today        = _today_entry()
    yesterday    = _yesterday_entry()
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

    # KPI cards, trend figure, and table depend on whether one institution is selected
    if inst and inst in institutions:
        cards = []
        for dim in DIMS:
            now   = float(_inst_scores(today,     inst).get(dim, 0))
            prev  = float(_inst_scores(yesterday, inst).get(dim, 0))
            delta = round(now - prev, 1)
            spark = [float(_inst_scores(e, inst).get(dim, 0)) for e in trend]
            cards.append(_kpi_card(dim, now, delta, spark))

        fig              = _trend_figure(trend, cat, inst_code=inst)
        display_insts    = {inst: institutions[inst]}
        inst_name        = (institutions[inst].get("name") or inst).title()
        table_title      = f"{inst_name.upper()}  —  {cat_label.upper()}"
    else:
        cards = []
        for dim in DIMS:
            now   = float(_cat_scores(today,     cat).get(dim) or 0)
            prev  = float(_cat_scores(yesterday, cat).get(dim) or 0)
            delta = round(now - prev, 1)
            spark = [float(_cat_scores(e, cat).get(dim) or 0) for e in trend]
            cards.append(_kpi_card(dim, now, delta, spark))

        fig           = _trend_figure(trend, cat)
        display_insts = institutions
        n             = len(institutions)
        table_title   = f"INSTITUTIONS — {cat_label.upper()}  ({n})"

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
                    "color":      BNR_NAVY,
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
            "boxShadow":      "0 1px 4px rgba(26,58,107,0.06)",
        }),

        # KPI cards + trend chart
        html.Div([
            html.Div(id="kpi-row", children=cards, style={
                "display":      "flex",
                "gap":          "12px",
                "flexWrap":     "wrap",
                "marginBottom": "16px",
            }),
            html.Div([
                html.Div("7-DAY QUALITY TREND", style={
                    "fontSize":      "11px",
                    "fontWeight":    "900",
                    "color":         MUTED,
                    "letterSpacing": "0.06em",
                    "textTransform": "uppercase",
                    "marginBottom":  "8px",
                    "lineHeight":    "1.15",
                }),
                dcc.Graph(id="trend-graph", figure=fig,
                          config={"displayModeBar": False}),
            ]),
        ], style={
            "background":   CARD,
            "padding":      "20px",
            "borderRadius": "8px",
            "boxShadow":    "0 2px 8px rgba(26,58,107,0.07)",
            "border":       f"1px solid {DIVIDER}",
            "marginBottom": "20px",
        }),

        # Institution table
        html.Div([
            html.Div(table_title, id="table-title", style={
                "fontSize":      "12px",
                "fontWeight":    "900",
                "color":         TEXT,
                "letterSpacing": "0.03em",
                "marginBottom":  "12px",
                "lineHeight":    "1.15",
            }),
            html.Div(id="inst-table", children=_institution_table(display_insts, gen_status)),
        ]),
    ])


# ── bootstrap values (rendered once at startup) ────────────────────────────────

_today_e   = _today_entry()
_counts    = _category_counts(_today_e)
_run_ts    = _PIPELINE.get("data_processed", "")
_run_date  = _PIPELINE.get("run_date", _today_e.get("date", "—"))
_run_label = (
    f"Last run: {_run_date}"
    + (f"  ·  {_run_ts[11:16]} UTC" if len(_run_ts) >= 16 else "")
)


# ── Validations page helpers ───────────────────────────────────────────────────

_DIM_PILL_COLOR = {
    "completeness": "#2563EB",
    "accuracy":     "#16A34A",
    "timeliness":   "#D97706",
    "validity":     "#7C3AED",
}


def _dim_pill(dim: str) -> html.Span:
    color = _DIM_PILL_COLOR.get(dim, MUTED)
    r = int(color[1:3], 16); g = int(color[3:5], 16); b = int(color[5:7], 16)
    return html.Span(dim.capitalize(), style={
        "background":    f"rgba({r},{g},{b},0.12)",
        "color":         color,
        "border":        f"1px solid rgba({r},{g},{b},0.30)",
        "borderRadius":  "4px",
        "padding":       "2px 7px",
        "fontSize":      "11px",
        "fontWeight":    "700",
        "whiteSpace":    "nowrap",
    })


_KNOWN_TABLES = [
    "accounts", "customers_expanded", "contracts_disburse",
    "contract_loans", "contract_schedules", "contracts_expanded",
    "loan_applications_2", "prev_loan_applications",
]

_CHECK_TYPES = [
    ("not_null",        "Field must not be null"),
    ("positive",        "Numeric field must be > 0"),
    ("non_negative",    "Numeric field must be ≥ 0"),
    ("date_not_future", "Date field must not be in the future"),
    ("domain",          "Field value must be in an allowed set"),
    ("range",           "Numeric field must be between min and max"),
    ("pattern",         "Field must match a regex pattern"),
]

_STATUS_STYLE = {
    "draft":   {"color": "#6B21A8", "background": "rgba(107,33,168,.10)",
                "border": "1px solid rgba(107,33,168,.30)"},
    "pending": {"color": "#92400E", "background": "rgba(245,158,11,.12)",
                "border": "1px solid rgba(245,158,11,.35)"},
    "active":  {"color": "#065F46", "background": "rgba(16,185,129,.12)",
                "border": "1px solid rgba(16,185,129,.35)"},
    "error":   {"color": "#991B1B", "background": "rgba(239,68,68,.12)",
                "border": "1px solid rgba(239,68,68,.35)"},
}


def _rules_charts(builtin_rules: list[dict], user_rules: list[dict]) -> html.Div:
    """Two-panel chart: by dimension (left) + by table (right)."""
    from collections import defaultdict

    dim_order = ["completeness", "accuracy", "timeliness", "validity"]

    pending = [r for r in user_rules if r.get("status") == "pending"]
    active  = [r for r in user_rules if r.get("status") == "active"]

    builtin_dim = {d: sum(1 for r in builtin_rules if r["dimension"] == d) for d in dim_order}
    active_dim  = {d: sum(1 for r in active        if r.get("dimension") == d) for d in dim_order}
    pending_dim = {d: sum(1 for r in pending        if r.get("dimension") == d) for d in dim_order}

    has_pending = any(pending_dim[d] > 0 for d in dim_order)

    dim_traces = [
        go.Bar(
            name="Built-in rules",
            x=[d.capitalize() for d in dim_order],
            y=[builtin_dim[d] + active_dim[d] for d in dim_order],
            marker_color=[_DIM_PILL_COLOR[d] for d in dim_order],
            text=[str(builtin_dim[d] + active_dim[d]) for d in dim_order],
            textposition="outside",
            hovertemplate="%{x}: %{y} run rules<extra></extra>",
            showlegend=False,
        ),
    ]
    if has_pending:
        dim_traces.append(go.Bar(
            name="Pending (not yet run)",
            x=[d.capitalize() for d in dim_order],
            y=[pending_dim[d] for d in dim_order],
            marker=dict(
                color="rgba(148,163,184,0.35)",
                pattern=dict(shape="/", fgcolor="rgba(100,116,139,0.6)", size=6),
                line=dict(color="rgba(100,116,139,0.5)", width=1),
            ),
            text=[str(pending_dim[d]) if pending_dim[d] else "" for d in dim_order],
            textposition="outside",
            hovertemplate="%{x}: %{y} pending rules<extra></extra>",
        ))

    fig_dim = go.Figure(dim_traces)
    fig_dim.update_layout(
        barmode="group",
        height=240,
        paper_bgcolor=CARD, plot_bgcolor=CARD,
        margin=dict(l=8, r=8, t=36, b=8),
        font=dict(family=FONT, size=11, color=TEXT),
        yaxis=dict(title=None, gridcolor=DIVIDER, zeroline=False, tickfont=dict(size=10)),
        xaxis=dict(tickfont=dict(size=11), showgrid=False),
        bargap=0.25, bargroupgap=0.08,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=10)),
        showlegend=has_pending,
    )

    table_dim: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def _add_table_rules(rule_list, dim_key="dimension"):
        for r in rule_list:
            dim        = r.get(dim_key, r.get("dimension", ""))
            tables_str = r.get("tables", "")
            if "→" in tables_str:
                # RI rule: "child_table → parent_table" — credit the child table
                child = tables_str.split("→")[0].strip()
                if child:
                    table_dim[child][dim] += 1
            else:
                for t in tables_str.split(","):
                    t = t.strip()
                    if t:
                        table_dim[t][dim] += 1

    _add_table_rules(builtin_rules)
    _add_table_rules(active)

    pending_table: dict[str, int] = defaultdict(int)
    for r in pending:
        t = (r.get("tables") or "").split(",")[0].strip()
        if t:
            pending_table[t] += 1

    all_tables = sorted(
        set(table_dim.keys()) | set(pending_table.keys()),
        key=lambda t: sum(table_dim[t].values()) + pending_table.get(t, 0),
        reverse=True,
    )

    tbl_traces = []
    for dim in dim_order:
        tbl_traces.append(go.Bar(
            name=dim.capitalize(),
            y=all_tables,
            x=[table_dim[t].get(dim, 0) for t in all_tables],
            orientation="h",
            marker_color=_DIM_PILL_COLOR[dim],
            hovertemplate=f"<b>{dim.capitalize()}</b><br>%{{y}}: %{{x}} rules<extra></extra>",
        ))
    if any(pending_table.get(t, 0) for t in all_tables):
        tbl_traces.append(go.Bar(
            name="Pending (not yet run)",
            y=all_tables,
            x=[pending_table.get(t, 0) for t in all_tables],
            orientation="h",
            marker=dict(
                color="rgba(148,163,184,0.35)",
                pattern=dict(shape="/", fgcolor="rgba(100,116,139,0.6)", size=6),
                line=dict(color="rgba(100,116,139,0.5)", width=1),
            ),
            hovertemplate="<b>Pending (not yet run)</b><br>%{y}: %{x} rules<extra></extra>",
        ))

    fig_tbl = go.Figure(tbl_traces)
    fig_tbl.update_layout(
        barmode="stack",
        height=max(240, 36 * len(all_tables) + 60),
        paper_bgcolor=CARD, plot_bgcolor=CARD,
        margin=dict(l=8, r=8, t=36, b=8),
        font=dict(family=FONT, size=11, color=TEXT),
        xaxis=dict(title=None, gridcolor=DIVIDER, zeroline=False, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=10), showgrid=False, automargin=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=10)),
        bargap=0.25,
    )

    def _chart_card(title: str, fig: go.Figure) -> html.Div:
        return html.Div([
            html.Div(title, style={
                "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                "textTransform": "uppercase", "letterSpacing": "0.06em",
                "lineHeight": "1.15", "marginBottom": "4px",
            }),
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
        ], style={
            "flex": "1", "minWidth": "0",
            "background": CARD, "borderRadius": "8px",
            "padding": "16px 16px 8px",
            "boxShadow": "0 1px 4px rgba(26,58,107,0.08)",
            "border": f"1px solid {DIVIDER}",
        })

    return html.Div([
        _chart_card("RULES BY DIMENSION", fig_dim),
        _chart_card("RULES BY TABLE",     fig_tbl),
    ], style={"display": "flex", "gap": "16px", "marginBottom": "20px"})


def _rule_form(next_id: str) -> html.Div:
    """The Add Rule form. Always rendered in the DOM; toggled via display style."""
    inp = {
        "width": "100%", "padding": "7px 10px",
        "border": f"1px solid {DIVIDER}", "borderRadius": "5px",
        "fontSize": "12px", "color": TEXT, "fontFamily": FONT,
        "boxSizing": "border-box", "outline": "none",
    }
    lbl = {
        "fontSize": "11px", "fontWeight": "900", "color": MUTED,
        "textTransform": "uppercase", "letterSpacing": "0.05em",
        "marginBottom": "4px", "display": "block",
    }

    def _field(label: str, child) -> html.Div:
        return html.Div([html.Span(label, style=lbl), child],
                        style={"display": "flex", "flexDirection": "column"})

    def _dd(id_, opts, placeholder="Select…") -> dcc.Dropdown:
        return dcc.Dropdown(
            id=id_, options=opts, placeholder=placeholder,
            clearable=False,
            style={"fontSize": "12px", "fontFamily": FONT},
        )

    dim_opts        = [{"label": d.capitalize(), "value": d}
                       for d in ["completeness", "accuracy", "timeliness", "validity"]]
    table_opts      = [{"label": t, "value": t} for t in _KNOWN_TABLES]
    check_type_opts = [{"label": label, "value": val} for val, label in _CHECK_TYPES]

    return html.Div([
        html.Div([
            _field("Rule ID",
                dcc.Input(id="new-rule-id", value=next_id, debounce=False, style=inp)),
            _field("Dimension", _dd("new-rule-dim", dim_opts)),
        ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px",
                  "marginBottom": "12px"}),

        html.Div([
            _field("Category",
                dcc.Input(id="new-rule-cat", placeholder="e.g. Format Validity",
                          debounce=False, style=inp)),
            _field("Rule Name / Description",
                dcc.Input(id="new-rule-name",
                          placeholder="e.g. Email address must be valid",
                          debounce=False, style=inp)),
        ], style={"display": "grid", "gridTemplateColumns": "1fr 2fr", "gap": "14px",
                  "marginBottom": "12px"}),

        html.Div([
            _field("Table",      _dd("new-rule-table", table_opts, "Select table…")),
            _field("Field (column)",
                dcc.Input(id="new-rule-field", placeholder="e.g. email_id",
                          debounce=False, style=inp)),
            _field("Check Type", _dd("new-rule-check-type", check_type_opts, "Select check…")),
        ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr", "gap": "14px",
                  "marginBottom": "12px"}),

        html.Div([
            _field("Allowed Values (comma-separated)",
                dcc.Input(id="new-rule-domain-vals",
                          placeholder="e.g.  M, F, C",
                          debounce=False, style=inp)),
        ], id="param-domain", style={"marginBottom": "12px", "display": "none"}),

        html.Div([
            _field("Minimum",
                dcc.Input(id="new-rule-range-min", type="number",
                          placeholder="0", debounce=False, style=inp)),
            _field("Maximum",
                dcc.Input(id="new-rule-range-max", type="number",
                          placeholder="100", debounce=False, style=inp)),
        ], id="param-range",
           style={"display": "none", "marginBottom": "12px",
                  "gridTemplateColumns": "1fr 1fr", "gap": "14px"}),

        html.Div([
            _field("Regex Pattern",
                dcc.Input(id="new-rule-pattern",
                          placeholder="e.g.  ^[A-Z]{3}$",
                          debounce=False, style=inp)),
        ], id="param-pattern", style={"marginBottom": "12px", "display": "none"}),

        html.Div([
            html.Div(
                "Submit Rule",
                id="new-rule-submit", n_clicks=0,
                style={
                    "cursor": "pointer", "background": BNR_NAVY,
                    "color": CARD, "fontSize": "12px", "fontWeight": "700",
                    "padding": "9px 22px", "borderRadius": "6px",
                    "userSelect": "none", "display": "inline-block",
                },
            ),
            html.Div(id="new-rule-feedback", style={
                "fontSize": "12px", "lineHeight": "1.4",
                "marginLeft": "14px", "flex": "1",
            }),
        ], style={"display": "flex", "alignItems": "center"}),

    ], id="rule-form-panel", style={
        "background":    BG,
        "border":        f"1px solid {DIVIDER}",
        "borderRadius":  "8px",
        "padding":       "20px",
        "marginBottom":  "20px",
        "display":       "none",
    })


def _rules_table_row(r: dict, i: int, is_user: bool = False) -> html.Div:
    bg     = CARD if i % 2 == 0 else "#FAFBFC"
    status = r.get("status") if is_user else None
    cells  = [
        html.Span(r["rule_id"], style={
            "width": "80px", "flexShrink": "0",
            "fontSize": "12px", "fontWeight": "900",
            "color": BNR_NAVY, "fontFamily": "monospace", "lineHeight": "1.4",
        }),
        html.Div(_dim_pill(r["dimension"]), style={"width": "110px", "flexShrink": "0"}),
        html.Span(r.get("category") or "—", style={
            "width": "160px", "flexShrink": "0",
            "fontSize": "11px", "color": MUTED, "lineHeight": "1.4",
        }),
        html.Span(r["rule_name"], style={
            "flex": "1", "flexShrink": "1",
            "fontSize": "12px", "color": TEXT, "lineHeight": "1.4",
        }),
        html.Span(r["tables"], style={
            "width": "200px", "flexShrink": "0",
            "fontSize": "11px", "color": MUTED,
            "overflow": "hidden", "textOverflow": "ellipsis",
            "whiteSpace": "nowrap", "lineHeight": "1.4",
        }),
        html.Span(r.get("fields") or "—", style={
            "width": "180px", "flexShrink": "0",
            "fontSize": "11px", "color": MUTED,
            "overflow": "hidden", "textOverflow": "ellipsis",
            "whiteSpace": "nowrap", "lineHeight": "1.4",
        }),
    ]
    if is_user:
        sty = _STATUS_STYLE.get(status, _STATUS_STYLE["pending"])
        label = (status or "pending").upper()
        cells.append(html.Span(label, style={
            **sty,
            "width": "72px", "flexShrink": "0",
            "fontSize": "10px", "fontWeight": "900",
            "borderRadius": "4px", "padding": "2px 6px",
            "textAlign": "center", "lineHeight": "1.5",
        }))
    else:
        cells.append(html.Span("", style={"width": "72px", "flexShrink": "0"}))

    return html.Div(cells, style={
        "display": "flex", "alignItems": "center", "gap": "12px",
        "padding": "8px 16px", "background": bg,
        "borderBottom": f"1px solid {DIVIDER}",
    })


def _draft_review_section(draft_rules: list[dict]) -> html.Div | None:
    """Pending-review panel shown only when there are draft rules awaiting approval."""
    if not draft_rules:
        return None

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em", "flexShrink": "0"}

    header = html.Div([
        html.Span("Rule ID",   style={**H, "width": "80px"}),
        html.Span("Dimension", style={**H, "width": "100px"}),
        html.Span("Rule",      style={**H, "flex": "1"}),
        html.Span("Table(s)",  style={**H, "width": "180px"}),
        html.Span("Type",      style={**H, "width": "110px"}),
        html.Span("Actions",   style={**H, "width": "170px", "textAlign": "center"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "10px",
        "padding": "9px 14px",
        "borderBottom": f"2px solid {DIVIDER}",
        "background": "rgba(107,33,168,.06)", "borderRadius": "8px 8px 0 0",
    })

    rows = []
    for i, r in enumerate(draft_rules):
        rid    = r["rule_id"]
        bg     = CARD if i % 2 == 0 else "#FAFBFC"
        rows.append(html.Div([
            html.Span(rid, style={
                "width": "80px", "flexShrink": "0", "fontSize": "12px",
                "fontWeight": "900", "color": "#6B21A8", "fontFamily": "monospace",
            }),
            html.Div(_dim_pill(r["dimension"]), style={"width": "100px", "flexShrink": "0"}),
            html.Span(r["rule_name"], style={
                "flex": "1", "fontSize": "12px", "color": TEXT,
                "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap",
            }),
            html.Span(r.get("tables", ""), style={
                "width": "180px", "flexShrink": "0", "fontSize": "11px",
                "color": MUTED, "overflow": "hidden", "textOverflow": "ellipsis",
                "whiteSpace": "nowrap",
            }),
            html.Span(r.get("check_type", ""), style={
                "width": "110px", "flexShrink": "0", "fontSize": "11px", "color": MUTED,
            }),
            html.Div([
                html.Div("Approve", id={"type": "approve-btn", "index": rid}, n_clicks=0,
                    style={
                        "cursor": "pointer", "background": C_GREEN, "color": CARD,
                        "fontSize": "11px", "fontWeight": "700", "padding": "5px 12px",
                        "borderRadius": "5px", "userSelect": "none", "marginRight": "6px",
                    }),
                html.Div("Delete", id={"type": "delete-draft-btn", "index": rid}, n_clicks=0,
                    style={
                        "cursor": "pointer", "background": C_RED, "color": CARD,
                        "fontSize": "11px", "fontWeight": "700", "padding": "5px 12px",
                        "borderRadius": "5px", "userSelect": "none",
                    }),
            ], style={"width": "170px", "flexShrink": "0", "display": "flex", "alignItems": "center"}),
        ], style={
            "display": "flex", "alignItems": "center", "gap": "10px",
            "padding": "8px 14px", "background": bg,
            "borderBottom": f"1px solid {DIVIDER}",
        }))

    return html.Div([
        html.Div([
            html.Div("PENDING ADMIN REVIEW", style={
                "fontSize": "12px", "fontWeight": "900", "color": "#6B21A8",
                "letterSpacing": "0.04em",
            }),
            html.Div(
                f"{len(draft_rules)} rule{'s' if len(draft_rules) != 1 else ''} submitted — "
                "approve to queue for next pipeline run, or delete to reject.",
                style={"fontSize": "11px", "color": MUTED, "marginTop": "3px"},
            ),
        ], style={"marginBottom": "12px"}),
        html.Div(
            [header] + rows,
            style={
                "border": "1px solid rgba(107,33,168,.30)",
                "borderRadius": "8px", "overflow": "hidden",
            },
        ),
    ], style={
        "background": "rgba(107,33,168,.04)",
        "border": "1px solid rgba(107,33,168,.20)",
        "borderRadius": "10px", "padding": "16px 16px 8px",
        "marginBottom": "20px",
    })


def _complex_rule_form(next_id: str) -> html.Div:
    """Form for rules that cannot be expressed with a simple check type."""
    inp = {
        "width": "100%", "padding": "7px 10px",
        "border": f"1px solid {DIVIDER}", "borderRadius": "5px",
        "fontSize": "12px", "color": TEXT, "fontFamily": FONT,
        "boxSizing": "border-box", "outline": "none",
    }
    ta = {**inp, "resize": "vertical", "minHeight": "72px", "fontFamily": "monospace"}
    lbl = {
        "fontSize": "11px", "fontWeight": "900", "color": MUTED,
        "textTransform": "uppercase", "letterSpacing": "0.05em",
        "marginBottom": "4px", "display": "block",
    }

    def _field(label, child):
        return html.Div([html.Span(label, style=lbl), child],
                        style={"display": "flex", "flexDirection": "column"})

    dim_opts = [{"label": d.capitalize(), "value": d}
                for d in ["completeness", "accuracy", "timeliness", "validity"]]

    return html.Div([
        html.Div(
            "Use this form for business rules that cannot be expressed with simple check "
            "types. If you provide a SQL condition it will be auto-evaluated by the "
            "pipeline (as a pandas query expression identifying failing rows). "
            "Otherwise the rule is tracked as manual.",
            style={"fontSize": "12px", "color": MUTED, "marginBottom": "16px",
                   "lineHeight": "1.5"},
        ),

        html.Div([
            _field("Rule ID",
                dcc.Input(id="cx-rule-id", value=f"CX-{next_id[4:]}",
                          debounce=False, style=inp)),
            _field("Dimension",
                dcc.Dropdown(id="cx-rule-dim", options=dim_opts, clearable=False,
                             style={"fontSize": "12px", "fontFamily": FONT})),
        ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                  "gap": "14px", "marginBottom": "12px"}),

        _field("Rule Name / Short Description",
            dcc.Input(id="cx-rule-name",
                      placeholder="e.g. Loan disbursement cannot exceed approved limit",
                      debounce=False, style={**inp, "marginBottom": "12px"})),

        html.Div([
            _field("Table(s) (comma-separated)",
                dcc.Input(id="cx-rule-tables",
                          placeholder="e.g. contracts_disburse, contract_loans",
                          debounce=False, style=inp)),
            _field("Field(s) (optional)",
                dcc.Input(id="cx-rule-fields",
                          placeholder="e.g. disbursed_amount, approved_amount",
                          debounce=False, style=inp)),
        ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                  "gap": "14px", "marginBottom": "12px"}),

        _field("Business Logic — describe what this rule checks and why",
            dcc.Textarea(id="cx-rule-logic",
                         placeholder="e.g. The disbursed amount on a contract must never "
                                     "exceed the originally approved amount. Breaches indicate "
                                     "control failures in the disbursement workflow.",
                         style={**ta, "marginBottom": "12px"})),

        html.Div([
            html.Span("SQL / Pandas Condition", style=lbl),
            html.Span(
                " (optional — a pandas df.query() expression that selects FAILING rows)",
                style={"fontSize": "10px", "color": MUTED, "marginLeft": "4px"},
            ),
        ], style={"display": "flex", "alignItems": "baseline", "marginBottom": "4px"}),
        dcc.Textarea(
            id="cx-rule-condition",
            placeholder="e.g.  disbursed_amount > approved_amount",
            style={**ta, "marginBottom": "12px"},
        ),

        html.Div([
            html.Div("Submit for Review", id="cx-rule-submit", n_clicks=0, style={
                "cursor": "pointer", "background": BNR_NAVY, "color": CARD,
                "fontSize": "12px", "fontWeight": "700", "padding": "9px 22px",
                "borderRadius": "6px", "userSelect": "none", "display": "inline-block",
            }),
            html.Div(id="cx-rule-feedback",
                     style={"fontSize": "12px", "lineHeight": "1.4",
                            "marginLeft": "14px", "flex": "1"}),
        ], style={"display": "flex", "alignItems": "center"}),

    ], id="complex-form-panel", style={
        "background": BG, "border": f"1px solid {DIVIDER}", "borderRadius": "8px",
        "padding": "20px", "marginBottom": "20px", "display": "none",
    })


def _validations_page() -> html.Div:
    builtin_rules = get_all_rules()
    user_rules    = get_user_rules()       # non-draft: pending / active / error
    draft_rules   = get_draft_rules()
    total         = len(builtin_rules) + len(user_rules)
    n_pending     = sum(1 for r in user_rules if r.get("status") == "pending")
    next_id       = next_user_rule_id()

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em",
         "lineHeight": "1.15", "flexShrink": "0"}

    header = html.Div([
        html.Span("Rule ID",   style={**H, "width": "80px"}),
        html.Span("Dimension", style={**H, "width": "110px"}),
        html.Span("Category",  style={**H, "width": "160px"}),
        html.Span("Rule",      style={**H, "flex": "1", "flexShrink": "1"}),
        html.Span("Table(s)",  style={**H, "width": "200px"}),
        html.Span("Fields",    style={**H, "width": "180px"}),
        html.Span("Status",    style={**H, "width": "72px"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "12px",
        "padding": "9px 16px",
        "borderBottom": f"2px solid {DIVIDER}",
        "background": BG, "borderRadius": "8px 8px 0 0",
    })

    data_rows = [_rules_table_row(r, i, is_user=False) for i, r in enumerate(builtin_rules)]

    if user_rules:
        data_rows.append(html.Div(
            "USER-DEFINED RULES",
            style={
                "padding": "7px 16px", "fontSize": "10px", "fontWeight": "900",
                "color": MUTED, "letterSpacing": "0.07em",
                "background": BG, "borderBottom": f"1px solid {DIVIDER}",
            },
        ))
        offset = len(builtin_rules) + 1
        data_rows += [_rules_table_row(r, offset + i, is_user=True)
                      for i, r in enumerate(user_rules)]

    table = html.Div(
        [header] + data_rows,
        style={"border": f"1px solid {DIVIDER}", "borderRadius": "8px", "overflow": "hidden"},
    )

    subtitle = f"{total} rules across 4 dimensions"
    if n_pending:
        subtitle += f"  ·  {n_pending} pending (will run on next pipeline)"
    if draft_rules:
        subtitle += f"  ·  {len(draft_rules)} awaiting review"

    draft_section = _draft_review_section(draft_rules)

    return html.Div([
        # ── pending review (admin panel) ──────────────────────────────────────
        draft_section if draft_section else html.Div(),

        # ── header row: title + action buttons ───────────────────────────────
        html.Div([
            html.Div([
                html.Div("VALIDATION RULES", style={
                    "fontSize": "13px", "fontWeight": "900",
                    "color": TEXT, "letterSpacing": "0.04em", "lineHeight": "1.15",
                }),
                html.Div(subtitle,
                         style={"fontSize": "11px", "color": MUTED, "marginTop": "3px"}),
            ]),
            html.Div([
                html.Div("+ Add Rule", id="form-toggle-btn", n_clicks=0, style={
                    "cursor": "pointer", "background": CARD, "color": BNR_NAVY,
                    "fontSize": "12px", "fontWeight": "700", "padding": "8px 16px",
                    "borderRadius": "6px", "border": f"1px solid {BNR_NAVY}",
                    "userSelect": "none", "marginRight": "8px",
                }),
                html.Div("+ Complex Rule", id="complex-form-toggle-btn", n_clicks=0, style={
                    "cursor": "pointer", "background": CARD, "color": "#6B21A8",
                    "fontSize": "12px", "fontWeight": "700", "padding": "8px 16px",
                    "borderRadius": "6px", "border": "1px solid #6B21A8",
                    "userSelect": "none", "marginRight": "8px",
                }),
                html.Div("Download CSV", id="rules-download-btn", n_clicks=0, style={
                    "cursor": "pointer", "background": BNR_NAVY, "color": CARD,
                    "fontSize": "12px", "fontWeight": "700", "padding": "8px 18px",
                    "borderRadius": "6px", "userSelect": "none",
                }),
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={
            "display": "flex", "alignItems": "center",
            "justifyContent": "space-between", "marginBottom": "16px",
        }),

        # ── standard rule form ────────────────────────────────────────────────
        _rule_form(next_id),

        # ── complex rule form ─────────────────────────────────────────────────
        _complex_rule_form(next_id),

        # ── charts + table ────────────────────────────────────────────────────
        _rules_charts(builtin_rules, user_rules),
        table,
        dcc.Download(id="rules-download"),
    ])


# ── app ────────────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    title="BNR Data Quality Monitoring",
    suppress_callback_exceptions=True,
)
server = app.server  # exposed for gunicorn: gunicorn dq_dashboard_dash:server


def _nav_tabs(active: str) -> html.Div:
    items = [("dashboard", "Dashboard"), ("validations", "Validations")]
    tabs = []
    for key, label in items:
        is_active = key == active
        tabs.append(html.Div(
            label,
            id={"type": "page-nav", "index": key},
            n_clicks=0,
            style={
                "cursor":       "pointer",
                "padding":      "11px 24px",
                "fontSize":     "13px",
                "fontWeight":   "900" if is_active else "400",
                "color":        CARD if is_active else "rgba(255,255,255,0.60)",
                "borderBottom": f"3px solid {BNR_GOLD}" if is_active
                                else "3px solid transparent",
                "whiteSpace":   "nowrap",
                "userSelect":   "none",
                "transition":   "color .15s, border-color .15s",
            },
        ))
    return html.Div(tabs, style={
        "display":    "flex",
        "background": BNR_NAVY,
        "padding":    "0 32px",
        "borderTop":  "1px solid rgba(255,255,255,0.12)",
    })


app.layout = html.Div([

    # ── header ────────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Img(
                src="/assets/bnr_img.png",
                style={"height": "50px", "marginRight": "16px", "flexShrink": "0"},
            ),
            html.Div([
                html.Div("FINANCIAL SECTOR DATA QUALITY MONITORING", style={
                    "fontSize": "14px", "fontWeight": "700",
                    "color": CARD, "letterSpacing": "0.06em",
                    "lineHeight": "1.15",
                }),
                html.Div(
                    "National Bank of Rwanda — BNR Data Quality Programme",
                    style={
                        "fontSize": "11px", "fontWeight": "400",
                        "color": "rgba(255,255,255,0.65)",
                        "lineHeight": "1.15", "marginTop": "3px",
                    },
                ),
            ]),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div(_run_label, style={
            "fontSize": "11px", "fontWeight": "400",
            "color": "rgba(255,255,255,0.55)", "lineHeight": "1.15",
        }),
    ], style={
        "background":     BNR_NAVY,
        "padding":        "14px 32px",
        "display":        "flex",
        "alignItems":     "center",
        "justifyContent": "space-between",
        "boxShadow":      "0 2px 8px rgba(0,0,0,0.18)",
    }),

    # ── page nav ──────────────────────────────────────────────────────────────
    html.Div(id="page-nav-bar"),

    # ── page content ──────────────────────────────────────────────────────────
    html.Div(id="page-content", style={
        "maxWidth":   "1440px",
        "margin":     "0 auto",
        "padding":    "24px 32px",
        "fontFamily": FONT,
    }),

    # ── stores ────────────────────────────────────────────────────────────────
    # nav-state: {"cat": None|"B"|"MF"|"SACCO", "inst": None|"<code>"}
    # cat=None means landing page; inst=None means show all in category
    dcc.Store(id="nav-state",    data={"cat": None, "inst": None}),
    dcc.Store(id="active-page",  data="dashboard"),
    dcc.Store(id="rules-version", data=0),
    dcc.Store(id="gen-status",   data={}),
    dcc.Interval(id="gen-poll",  interval=2000, n_intervals=0, disabled=True),
    dcc.Download(id="inst-download"),
    dcc.Download(id="gen-download"),

], style={"background": BG, "minHeight": "100vh", "fontFamily": FONT})


# ── callbacks ──────────────────────────────────────────────────────────────────

@app.callback(
    Output("active-page", "data"),
    Input({"type": "page-nav", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _on_page_nav(_n_clicks):
    triggered = ctx.triggered_id
    if isinstance(triggered, dict) and "index" in triggered:
        return triggered["index"]
    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("page-nav-bar", "children"),
    Output("page-content",  "children"),
    Input("active-page",    "data"),
    Input("nav-state",      "data"),
    Input("rules-version",  "data"),
    Input("gen-status",     "data"),
)
def _render_page(page: str, nav_state, _rv, gen_status):
    page = page or "dashboard"
    nav  = nav_state or {"cat": None, "inst": None}
    cat  = nav.get("cat")
    inst = nav.get("inst")

    nav_bar = _nav_tabs(page)

    if page == "validations":
        return nav_bar, _validations_page()

    # Show landing page when no category has been selected
    if not cat:
        return nav_bar, _landing_page(_counts)

    # Show category dashboard (gen_status drives Generate / ⬇ button states)
    return nav_bar, _dashboard_content(cat, inst, gen_status or {})


@app.callback(
    Output("nav-state", "data"),
    Input({"type": "cat-landing-btn", "index": ALL}, "n_clicks"),
    Input({"type": "nav-action",      "index": ALL}, "n_clicks"),
    Input({"type": "inst-dd",         "index": ALL}, "value"),
    State("nav-state", "data"),
    prevent_initial_call=True,
)
def _nav_handler(landing_clicks, nav_action_clicks, inst_values, current_nav):
    """Single callback that owns all navigation state changes."""
    nav = dict(current_nav or {"cat": None, "inst": None})
    tid = ctx.triggered_id
    triggered_val = ctx.triggered[0]["value"] if ctx.triggered else None

    if isinstance(tid, dict):
        t = tid.get("type")

        if t == "nav-action" and tid.get("index") == "back":
            if triggered_val and triggered_val > 0:
                return {"cat": None, "inst": None}
            raise dash.exceptions.PreventUpdate

        if t == "cat-landing-btn":
            if triggered_val and triggered_val > 0:
                return {"cat": tid["index"], "inst": None}
            raise dash.exceptions.PreventUpdate

        if t == "inst-dd":
            new_inst = triggered_val or None
            new_nav  = {**nav, "inst": new_inst}
            # prevent spurious re-renders when the dropdown first appears in the DOM
            if new_nav == nav:
                raise dash.exceptions.PreventUpdate
            return new_nav

    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("rule-form-panel", "style"),
    Input("form-toggle-btn",  "n_clicks"),
    State("rule-form-panel",  "style"),
    prevent_initial_call=True,
)
def _toggle_form(n_clicks, current_style):
    style = dict(current_style or {})
    style["display"] = "none" if style.get("display") != "none" else "block"
    return style


@app.callback(
    Output("param-domain",  "style"),
    Output("param-range",   "style"),
    Output("param-pattern", "style"),
    Input("new-rule-check-type", "value"),
    prevent_initial_call=True,
)
def _show_params(check_type):
    hidden  = {"display": "none"}
    visible = {"display": "block", "marginBottom": "12px"}
    grid_v  = {"display": "grid", "gridTemplateColumns": "1fr 1fr",
                "gap": "14px", "marginBottom": "12px"}
    return (
        visible  if check_type == "domain"  else hidden,
        grid_v   if check_type == "range"   else hidden,
        visible  if check_type == "pattern" else hidden,
    )


@app.callback(
    Output("new-rule-feedback", "children"),
    Output("rules-version",     "data"),
    Input("new-rule-submit",    "n_clicks"),
    State("new-rule-id",         "value"),
    State("new-rule-dim",        "value"),
    State("new-rule-cat",        "value"),
    State("new-rule-name",       "value"),
    State("new-rule-table",      "value"),
    State("new-rule-field",      "value"),
    State("new-rule-check-type", "value"),
    State("new-rule-domain-vals","value"),
    State("new-rule-range-min",  "value"),
    State("new-rule-range-max",  "value"),
    State("new-rule-pattern",    "value"),
    State("rules-version",       "data"),
    prevent_initial_call=True,
)
def _submit_rule(n_clicks, rule_id, dim, cat, name, table, field,
                 check_type, domain_vals, range_min, range_max, pattern, version):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    errors = []
    if not (rule_id or "").strip():
        errors.append("Rule ID is required.")
    if not dim:
        errors.append("Dimension is required.")
    if not (name or "").strip():
        errors.append("Rule Name is required.")
    if not table:
        errors.append("Table is required.")
    if not (field or "").strip():
        errors.append("Field is required.")
    if not check_type:
        errors.append("Check Type is required.")
    if check_type == "domain" and not (domain_vals or "").strip():
        errors.append("Allowed Values are required for a domain check.")
    if check_type == "range" and (range_min is None or range_max is None):
        errors.append("Both Min and Max are required for a range check.")
    if check_type == "pattern" and not (pattern or "").strip():
        errors.append("Regex Pattern is required for a pattern check.")

    if errors:
        return (
            html.Span("  ".join(errors), style={"color": C_RED}),
            version,
        )

    import json as _json
    check_params = None
    if check_type == "domain":
        vals = [v.strip() for v in domain_vals.split(",") if v.strip()]
        check_params = _json.dumps({"values": vals})
    elif check_type == "range":
        check_params = _json.dumps({"min": float(range_min), "max": float(range_max)})
    elif check_type == "pattern":
        check_params = _json.dumps({"pattern": pattern.strip()})

    try:
        add_user_rule({
            "rule_id":      rule_id.strip(),
            "dimension":    dim,
            "category":     (cat or "").strip() or dim.capitalize(),
            "rule_name":    name.strip(),
            "tables":       table,
            "fields":       (field or "").strip(),
            "check_type":   check_type,
            "check_params": check_params,
        })
    except Exception as exc:
        return (
            html.Span(f"Could not save: {exc}", style={"color": C_RED}),
            version,
        )

    feedback = html.Span([
        html.Span("✓ ", style={"color": C_GREEN, "fontWeight": "900"}),
        html.Span(f"{rule_id.strip()} submitted for admin review. "
                  "Once approved it will run on the next pipeline.",
                  style={"color": C_GREEN}),
    ])
    return feedback, (version or 0) + 1


@app.callback(
    Output("inst-download", "data"),
    Input({"type": "inst-dl-btn", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _on_inst_download(n_clicks):
    if not any(n for n in (n_clicks or []) if n):
        raise dash.exceptions.PreventUpdate
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict) or "index" not in triggered:
        raise dash.exceptions.PreventUpdate
    le_book = triggered["index"]
    if not REPORTS_DIR.exists():
        raise dash.exceptions.PreventUpdate
    matches = sorted(REPORTS_DIR.glob(f"{le_book}_*.xlsx"))
    if not matches:
        raise dash.exceptions.PreventUpdate
    return dcc.send_file(str(matches[0]))


@app.callback(
    Output("rules-download", "data"),
    Input("rules-download-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _on_rules_download(n_clicks):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    rules = get_all_rules()
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["rule_id", "dimension", "category", "rule_name", "tables", "fields"])
    writer.writeheader()
    writer.writerows(rules)
    return dict(content=buf.getvalue(), filename="dq_validation_rules.csv")


# ── admin review: approve / delete draft rules ────────────────────────────────

@app.callback(
    Output("rules-version", "data", allow_duplicate=True),
    Input({"type": "approve-btn", "index": ALL}, "n_clicks"),
    State("rules-version", "data"),
    prevent_initial_call=True,
)
def _approve_draft(clicks, version):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "approve-btn":
        raise dash.exceptions.PreventUpdate
    if not (ctx.triggered[0]["value"] or 0) > 0:
        raise dash.exceptions.PreventUpdate
    approve_draft_rule(tid["index"])
    return (version or 0) + 1


@app.callback(
    Output("rules-version", "data", allow_duplicate=True),
    Input({"type": "delete-draft-btn", "index": ALL}, "n_clicks"),
    State("rules-version", "data"),
    prevent_initial_call=True,
)
def _delete_draft(clicks, version):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "delete-draft-btn":
        raise dash.exceptions.PreventUpdate
    if not (ctx.triggered[0]["value"] or 0) > 0:
        raise dash.exceptions.PreventUpdate
    delete_draft_rule(tid["index"])
    return (version or 0) + 1


# ── complex rule form toggle + submit ─────────────────────────────────────────

@app.callback(
    Output("complex-form-panel", "style"),
    Input("complex-form-toggle-btn", "n_clicks"),
    State("complex-form-panel", "style"),
    prevent_initial_call=True,
)
def _toggle_complex_form(n_clicks, current_style):
    style = dict(current_style or {})
    style["display"] = "none" if style.get("display") != "none" else "block"
    return style


@app.callback(
    Output("cx-rule-feedback", "children"),
    Output("rules-version", "data", allow_duplicate=True),
    Input("cx-rule-submit",     "n_clicks"),
    State("cx-rule-id",         "value"),
    State("cx-rule-dim",        "value"),
    State("cx-rule-name",       "value"),
    State("cx-rule-tables",     "value"),
    State("cx-rule-fields",     "value"),
    State("cx-rule-logic",      "value"),
    State("cx-rule-condition",  "value"),
    State("rules-version",      "data"),
    prevent_initial_call=True,
)
def _submit_complex_rule(n_clicks, rule_id, dim, name,
                         tables, fields, logic, condition, version):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    errors = []
    if not (rule_id or "").strip():
        errors.append("Rule ID is required.")
    if not dim:
        errors.append("Dimension is required.")
    if not (name or "").strip():
        errors.append("Rule Name is required.")
    if not (tables or "").strip():
        errors.append("Table(s) is required.")
    if not (logic or "").strip():
        errors.append("Business Logic description is required.")

    if errors:
        return html.Span("  ".join(errors), style={"color": C_RED}), version

    import json as _json
    check_type   = "sql_condition" if (condition or "").strip() else "description"
    check_params = None
    if check_type == "sql_condition":
        check_params = _json.dumps({"condition": condition.strip(), "logic": (logic or "").strip()})
    else:
        check_params = _json.dumps({"logic": (logic or "").strip()})

    try:
        add_user_rule({
            "rule_id":      rule_id.strip(),
            "dimension":    dim,
            "category":     "Complex Rule",
            "rule_name":    name.strip(),
            "tables":       (tables or "").strip(),
            "fields":       (fields or "").strip(),
            "check_type":   check_type,
            "check_params": check_params,
        })
    except Exception as exc:
        return html.Span(f"Could not save: {exc}", style={"color": C_RED}), version

    return html.Span([
        html.Span("✓ ", style={"color": C_GREEN, "fontWeight": "900"}),
        html.Span(
            f"{rule_id.strip()} submitted for admin review"
            + (" (will auto-evaluate via SQL condition)."
               if check_type == "sql_condition"
               else " (manual evaluation — no SQL condition provided)."),
            style={"color": C_GREEN},
        ),
    ]), (version or 0) + 1


# ── on-demand report generation ────────────────────────────────────────────────

@app.callback(
    Output("gen-status", "data"),
    Output("gen-poll",   "disabled"),
    Input({"type": "gen-btn", "index": ALL}, "n_clicks"),
    State("gen-status", "data"),
    prevent_initial_call=True,
)
def _start_gen(clicks, current):
    """Start background report generation for the clicked institution."""
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    triggered_val = ctx.triggered[0]["value"] if ctx.triggered else 0
    if not isinstance(tid, dict) or tid.get("type") != "gen-btn":
        raise dash.exceptions.PreventUpdate
    if not triggered_val:
        raise dash.exceptions.PreventUpdate

    le_book = tid["index"]
    status  = dict(current or {})

    if status.get(le_book) == "running":
        raise dash.exceptions.PreventUpdate

    proc = subprocess.Popen(
        [sys.executable, str(_DIR / "generate_one_report.py"), "--le-book", le_book],
        cwd=str(_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    with _gen_lock:
        _gen_procs[le_book] = proc

    status[le_book] = "running"
    return status, False   # enable the polling interval


@app.callback(
    Output("gen-status",   "data",     allow_duplicate=True),
    Output("gen-poll",     "disabled", allow_duplicate=True),
    Output("gen-download", "data"),
    Input("gen-poll",      "n_intervals"),
    State("gen-status",    "data"),
    prevent_initial_call=True,
)
def _poll_gen(n, current_status):
    """Every 2 s: check whether any running jobs finished; trigger download if so."""
    if not current_status:
        raise dash.exceptions.PreventUpdate

    updated    = dict(current_status)
    newly_done = []
    changed    = False

    with _gen_lock:
        for lb, proc in _gen_procs.items():
            if updated.get(lb) != "running":
                continue
            rc = proc.poll()
            if rc is None:
                continue   # still running
            changed = True
            if rc == 0:
                updated[lb] = "done"
                newly_done.append(lb)
            else:
                stderr = proc.stderr.read().decode(errors="replace")
                updated[lb] = "error:" + (stderr[-120:] if stderr else str(rc))

    if not changed:
        raise dash.exceptions.PreventUpdate

    still_running = any(v == "running" for v in updated.values())

    dl_data = dash.no_update
    if newly_done:
        matches = sorted(REPORTS_DIR.glob(f"{newly_done[0]}_*.xlsx"))
        if matches:
            dl_data = dcc.send_file(str(matches[0]))

    return updated, not still_running, dl_data


# ── dev server ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
