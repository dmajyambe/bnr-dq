"""
dq_inst_portal.py — Institution-facing portal pages.

An institution user sees only their own le_book data:
  - Dashboard:    scores + trend for their institution
  - Issues:       open / delayed / resolved issues (filtered to their le_book)
  - Remediation:  their CRs — can Start Work and Submit only (no Create/Approve)
  - Validations:  their validation rules (read-only)
  - Notifications: bell icon + dropdown in nav
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import dash
from dash import dcc, html, Input, Output, State, ctx, ALL
import plotly.graph_objects as go

log = logging.getLogger("dq_inst_portal")

# ── design tokens (mirrored from main dashboard) ──────────────────────────────
BG      = "#f2ede9"
CARD    = "#FFFFFF"
TEXT    = "#1c1c27"
MUTED   = "#68686f"
DIVIDER = "#e7e1dc"
C_GREEN = "#16A34A"
C_RED   = "#DC2626"
C_AMBER = "#D97706"
BRAND   = "#753918"
FONT    = "'Inter','Franklin Gothic Medium',Arial,sans-serif"

_URGENCY_COLORS = {
    "new":       "#2563EB",
    "attention": "#D97706",
    "urgent":    "#EA580C",
    "critical":  "#DC2626",
}

DIM_COLORS = {
    "completeness": "#753918",
    "accuracy":     "#B8860B",
    "timeliness":   "#7C3D1E",
    "validity":     "#C9956C",
}

SCRIPT_DIR   = Path(__file__).parent
HISTORY_FILE = SCRIPT_DIR / "dq_history.json"
REPORTS_DIR  = SCRIPT_DIR / "reports"


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_history() -> list:
    try:
        return json.loads(HISTORY_FILE.read_text())
    except Exception:
        return []


def _inst_score(entry: dict, le_book: str, dim: str) -> float | None:
    return (entry.get("by_institution", {})
                 .get(le_book, {})
                 .get(dim))


def _score_color(s: float) -> str:
    if s >= 90:  return C_GREEN
    if s >= 75:  return C_AMBER
    return C_RED


def _kpi(label: str, value: float | None) -> html.Div:
    display = f"{value:.1f}%" if value is not None else "—"
    clr = _score_color(value) if value is not None else MUTED
    return html.Div([
        html.Div(display, style={
            "fontSize": "28px", "fontWeight": "900", "color": clr,
            "lineHeight": "1",
        }),
        html.Div(label, style={
            "fontSize": "11px", "color": MUTED, "marginTop": "4px",
            "textTransform": "uppercase", "letterSpacing": "0.05em",
        }),
    ], style={
        "background": CARD, "borderRadius": "10px",
        "padding": "18px 22px", "flex": "1",
        "border": f"1px solid {DIVIDER}",
        "borderTop": f"3px solid {clr if value is not None else DIVIDER}",
    })


# ── nav bar ───────────────────────────────────────────────────────────────────

def inst_nav_bar(active_page: str, user_name: str, le_book: str,
                 unread_count: int = 0) -> html.Div:
    pages = [
        ("inst_dashboard",   "Dashboard"),
        ("inst_issues",      "Issues"),
        ("inst_remediation", "Remediation"),
        ("inst_validations", "Validations"),
    ]
    tabs = []
    for page_id, label in pages:
        active = page_id == active_page
        tabs.append(html.Div(
            label,
            id={"type": "inst-nav-tab", "index": page_id},
            n_clicks=0,
            style={
                "padding":      "12px 18px",
                "cursor":       "pointer",
                "fontSize":     "13px",
                "fontWeight":   "700" if active else "500",
                "color":        CARD if active else "rgba(255,255,255,0.7)",
                "borderBottom": f"3px solid {CARD}" if active else "3px solid transparent",
                "userSelect":   "none",
                "whiteSpace":   "nowrap",
            },
        ))

    # Notification bell
    bell_badge = []
    if unread_count > 0:
        bell_badge = [html.Span(
            str(unread_count) if unread_count < 100 else "99+",
            style={
                "position": "absolute", "top": "6px", "right": "6px",
                "background": C_RED, "color": CARD,
                "borderRadius": "50%", "fontSize": "9px", "fontWeight": "900",
                "minWidth": "16px", "height": "16px", "lineHeight": "16px",
                "textAlign": "center", "padding": "0 2px",
            }
        )]

    bell = html.Div(
        ["🔔"] + bell_badge,
        id="inst-bell-btn",
        n_clicks=0,
        style={
            "position": "relative", "cursor": "pointer",
            "fontSize": "18px", "padding": "8px 12px",
            "color": "rgba(255,255,255,0.85)",
            "userSelect": "none",
        }
    )

    display = user_name.split()[0] if user_name else le_book
    return html.Div([
        # Logo + institution name
        html.Div([
            html.Div([
                html.Div("BNR DQ", style={
                    "fontSize": "13px", "fontWeight": "900",
                    "color": CARD, "letterSpacing": "0.05em",
                }),
                html.Div(f"Institution Portal — {le_book}", style={
                    "fontSize": "10px", "color": "rgba(255,255,255,0.6)",
                    "marginTop": "1px",
                }),
            ]),
        ], style={"display": "flex", "alignItems": "center", "marginRight": "24px"}),

        # Nav tabs
        html.Div(tabs, style={"display": "flex", "flex": "1", "gap": "4px"}),

        # Bell + user info + logout
        html.Div([
            bell,
            html.Span(display, style={
                "fontSize": "12px", "color": "rgba(255,255,255,0.85)",
                "marginLeft": "12px", "marginRight": "8px",
            }),
            html.Div("Logout", id="logout-btn", n_clicks=0, style={
                "fontSize": "11px", "color": "rgba(255,255,255,0.6)",
                "cursor": "pointer", "padding": "4px 8px",
                "border": "1px solid rgba(255,255,255,0.2)",
                "borderRadius": "4px", "userSelect": "none",
            }),
        ], style={"display": "flex", "alignItems": "center"}),

    ], style={
        "display":         "flex",
        "alignItems":      "center",
        "background":      BRAND,
        "padding":         "0 24px",
        "height":          "52px",
        "boxShadow":       "0 2px 8px rgba(0,0,0,0.15)",
        "position":        "sticky",
        "top":             "0",
        "zIndex":          "100",
    })


# ── notification dropdown ─────────────────────────────────────────────────────

def inst_notification_panel(user_id: str) -> html.Div:
    from dq_notifications import get_notifications, NOTIF_ICONS
    notifs = get_notifications(user_id, limit=20)

    if not notifs:
        rows = [html.Div("No notifications yet.",
                         style={"padding": "16px", "color": MUTED, "fontSize": "12px"})]
    else:
        rows = []
        for n in notifs:
            icon   = NOTIF_ICONS.get(n["type"], "•")
            unread = not n["is_read"]
            rows.append(html.Div([
                html.Div([
                    html.Span(icon, style={"marginRight": "8px", "fontSize": "14px"}),
                    html.Span(n["message"], style={
                        "fontSize": "12px", "color": TEXT if unread else MUTED,
                        "fontWeight": "600" if unread else "400",
                        "lineHeight": "1.4", "flex": "1",
                    }),
                ], style={"display": "flex", "alignItems": "flex-start"}),
                html.Div(n["created_at"][:10], style={
                    "fontSize": "10px", "color": MUTED, "marginTop": "4px",
                    "marginLeft": "22px",
                }),
            ], style={
                "padding": "10px 16px",
                "background": "rgba(117,57,24,0.05)" if unread else CARD,
                "borderBottom": f"1px solid {DIVIDER}",
                "cursor": "pointer",
            }))

    return html.Div([
        html.Div([
            html.Span("Notifications", style={
                "fontSize": "13px", "fontWeight": "900", "color": TEXT,
            }),
            html.Div("Mark all read", id="inst-mark-all-read", n_clicks=0, style={
                "fontSize": "11px", "color": BRAND, "cursor": "pointer",
            }),
        ], style={
            "display": "flex", "justifyContent": "space-between",
            "alignItems": "center", "padding": "12px 16px",
            "borderBottom": f"2px solid {DIVIDER}",
        }),
        html.Div(rows, style={"maxHeight": "400px", "overflowY": "auto"}),
    ], style={
        "position":   "absolute",
        "top":        "52px",
        "right":      "80px",
        "width":      "380px",
        "background": CARD,
        "borderRadius": "8px",
        "boxShadow":  "0 8px 24px rgba(0,0,0,0.15)",
        "border":     f"1px solid {DIVIDER}",
        "zIndex":     "200",
    })


# ── pages ─────────────────────────────────────────────────────────────────────

def inst_dashboard_page(le_books: list[str], categories: dict) -> html.Div:
    """Scores + 7-day trend table + line chart + report download for each institution."""
    history = _load_history()
    today   = history[-1] if history else {}
    trend   = history[-7:] if history else []
    dims    = ["completeness", "accuracy", "timeliness", "validity"]

    sections = []
    for lb in sorted(le_books):
        cat_info = categories.get(lb, {})
        name     = (cat_info.get("name") or lb).title()
        cat_type = cat_info.get("category_type", "")

        # ── KPI cards ─────────────────────────────────────────────────────────
        kpis = html.Div(
            [_kpi(d.title(), _inst_score(today, lb, d)) for d in dims],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "20px"},
        )

        # ── Report download button ─────────────────────────────────────────────
        rpt_files = sorted(REPORTS_DIR.glob(f"{lb}_*.xlsx"), reverse=True) if REPORTS_DIR.exists() else []
        if rpt_files:
            stem     = rpt_files[0].stem
            parts    = stem.rsplit("_", 3)
            rpt_date = parts[-1] if len(parts) >= 2 and len(parts[-1]) == 10 else "—"
            dl_btn = html.Div(
                [html.Span("⬇ ", style={"fontSize": "13px"}),
                 html.Span(f"Download Report  {rpt_date}",
                           style={"fontSize": "11px"})],
                id={"type": "inst-dl-btn", "index": lb},
                n_clicks=0,
                title=f"Download {name} report ({rpt_date})",
                style={
                    "display":        "inline-flex",
                    "alignItems":     "center",
                    "gap":            "4px",
                    "cursor":         "pointer",
                    "background":     BRAND,
                    "color":          CARD,
                    "fontSize":       "12px",
                    "fontWeight":     "700",
                    "padding":        "7px 16px",
                    "borderRadius":   "6px",
                    "userSelect":     "none",
                    "marginLeft":     "auto",
                },
            )
        else:
            dl_btn = html.Span()

        # ── 7-day trend table ──────────────────────────────────────────────────
        if len(trend) > 1:
            trend_rows = []
            for entry in reversed(trend):
                dt = entry.get("date", "")
                row_cells = [html.Td(dt, style={"padding": "5px 10px", "fontSize": "11px", "color": MUTED})]
                for d in dims:
                    val    = _inst_score(entry, lb, d)
                    scored = val is not None and float(val) > 0
                    disp   = f"{val:.1f}%" if scored else "—"
                    clr    = _score_color(val) if scored else MUTED
                    row_cells.append(html.Td(disp, style={
                        "padding":    "5px 10px",
                        "fontSize":   "12px",
                        "fontWeight": "700" if scored else "400",
                        "color":      clr,
                        "textAlign":  "right",
                    }))
                trend_rows.append(html.Tr(row_cells,
                    style={"borderBottom": f"1px solid {DIVIDER}"}))

            trend_table = html.Table([
                html.Thead(html.Tr([
                    html.Th("Date", style={"padding": "6px 10px", "fontSize": "10px",
                                           "color": MUTED, "textTransform": "uppercase",
                                           "textAlign": "left"}),
                ] + [
                    html.Th(d.title(), style={"padding": "6px 10px", "fontSize": "10px",
                                              "color": MUTED, "textTransform": "uppercase",
                                              "textAlign": "right"})
                    for d in dims
                ])),
                html.Tbody(trend_rows),
            ], style={"width": "100%", "borderCollapse": "collapse",
                      "background": CARD, "borderRadius": "8px",
                      "border": f"1px solid {DIVIDER}", "marginBottom": "20px"})

            # ── 7-day line chart ───────────────────────────────────────────────
            dates = [e.get("date", "") for e in trend]
            fig   = go.Figure()
            for d in dims:
                scores = [float(_inst_score(e, lb, d) or 0) for e in trend]
                fig.add_trace(go.Scatter(
                    x=dates, y=scores,
                    name=d.title(),
                    mode="lines+markers",
                    line=dict(color=DIM_COLORS[d], width=2),
                    marker=dict(size=5, color=DIM_COLORS[d]),
                    hovertemplate=f"<b>{d.title()}</b><br>%{{x}}<br>%{{y:.1f}}%<extra></extra>",
                ))
            fig.update_layout(
                height=260,
                paper_bgcolor=CARD, plot_bgcolor=CARD,
                margin=dict(l=8, r=8, t=36, b=8),
                font=dict(family=FONT, size=11, color=TEXT),
                legend=dict(orientation="h", yanchor="bottom", y=1.02,
                            xanchor="left", x=0, font=dict(size=11)),
                yaxis=dict(range=[0, 100], gridcolor=DIVIDER,
                           ticksuffix="%", tickfont=dict(size=10), zeroline=False),
                xaxis=dict(gridcolor=DIVIDER, tickfont=dict(size=10), showgrid=False),
                hovermode="x unified",
            )
            trend_chart = dcc.Graph(figure=fig, config={"displayModeBar": False})
        else:
            trend_table = html.Div("Not enough history for trend.",
                                   style={"color": MUTED, "fontSize": "12px",
                                          "marginBottom": "20px"})
            trend_chart = html.Div()

        sections.append(html.Div([
            # Header row: name + LE book + download button
            html.Div([
                html.Div([
                    html.H3(name, style={"fontSize": "15px", "fontWeight": "900",
                                         "color": TEXT, "margin": "0"}),
                    html.Span(f"LE Book: {lb}  ·  {cat_type}", style={
                        "fontSize": "11px", "color": MUTED, "marginLeft": "10px",
                    }),
                ], style={"display": "flex", "alignItems": "center"}),
                dl_btn,
            ], style={"display": "flex", "alignItems": "center",
                      "marginBottom": "16px"}),

            kpis,

            html.Div("7-DAY TREND", style={
                "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                "letterSpacing": "0.06em", "textTransform": "uppercase",
                "marginBottom": "10px",
            }),
            trend_table,
            trend_chart,
        ], style={
            "background": CARD, "borderRadius": "10px",
            "padding": "20px 24px", "marginBottom": "20px",
            "border": f"1px solid {DIVIDER}",
        }))

    return html.Div([
        html.H2("My Institution Dashboard", style={
            "fontSize": "18px", "fontWeight": "900", "color": TEXT,
            "marginTop": "0", "marginBottom": "20px",
        }),
        *sections,
    ], style={"padding": "28px 32px", "maxWidth": "1200px", "margin": "0 auto"})


_TABLE_PRETTY = {
    "accounts":               "Accounts",
    "contract_loans":         "Contract Loans",
    "contract_schedules":     "Contract Schedules",
    "contracts_disburse":     "Contracts Disburse",
    "contracts_expanded":     "Contracts",
    "customers_expanded":     "Customers",
    "loan_applications_2":    "Loan Applications",
    "prev_loan_applications": "Prev Loan Apps",
}


def inst_issues_page(le_books: list[str]) -> html.Div:
    """Institution-facing issues page — filterable CSV download."""
    from dq_issue_tracker import get_issues

    lb_set = set(str(lb) for lb in le_books)

    # ── urgency summary chips ─────────────────────────────────────────────────
    open_issues = [i for i in get_issues(status="open") if i["le_book"] in lb_set]
    band_counts = {"new": 0, "attention": 0, "urgent": 0, "critical": 0, "overdue": 0}
    for iss in open_issues:
        b = iss.get("urgency_band", "new")
        if b in band_counts:
            band_counts[b] += 1

    chips = []
    for band, label in [("overdue", "⚠ SLA Overdue"), ("critical", "Critical"),
                         ("urgent", "Urgent"), ("attention", "Attention"), ("new", "New")]:
        n   = band_counts.get(band, 0)
        clr = _URGENCY_COLORS.get(band, MUTED)
        chips.append(html.Div([
            html.Span(str(n), style={"fontWeight": "900", "fontSize": "22px", "color": clr}),
            html.Span(label,  style={"fontSize": "11px", "color": MUTED, "marginTop": "2px"}),
        ], style={
            "display": "flex", "flexDirection": "column", "alignItems": "center",
            "background": CARD, "borderRadius": "8px", "padding": "12px 20px",
            "border": f"2px solid {clr}", "minWidth": "100px",
        }))

    # available table options (only tables that have any issues for this institution)
    active_tables = sorted({i["table_name"] for i in open_issues})
    table_options = [
        {"label": _TABLE_PRETTY.get(t, t.replace("_", " ").title()), "value": t}
        for t in active_tables
    ]

    # ── filter bar ────────────────────────────────────────────────────────────
    filter_bar = html.Div([
        # status toggle
        html.Div([
            html.Span("Status:", style={"fontSize": "12px", "color": MUTED,
                                         "marginRight": "8px", "alignSelf": "center",
                                         "whiteSpace": "nowrap"}),
            dcc.RadioItems(
                id="inst-issue-filter",
                options=[
                    {"label": "Open",     "value": "open"},
                    {"label": "Delayed",  "value": "penalized"},
                    {"label": "Resolved", "value": "resolved"},
                ],
                value="open",
                inline=True,
                inputStyle={"marginRight": "4px"},
                labelStyle={"marginRight": "14px", "fontSize": "12px",
                            "fontWeight": "700", "cursor": "pointer"},
            ),
        ], style={"display": "flex", "alignItems": "center"}),

        html.Span(style={"width": "1px", "background": DIVIDER,
                          "alignSelf": "stretch", "margin": "0 16px"}),

        # table multi-select
        html.Div([
            html.Span("Tables:", style={"fontSize": "12px", "color": MUTED,
                                         "marginRight": "8px", "whiteSpace": "nowrap",
                                         "alignSelf": "center"}),
            dcc.Dropdown(
                id="inst-table-filter",
                options=table_options,
                value=None,
                multi=True,
                placeholder="All tables…",
                clearable=True,
                style={"fontSize": "12px", "fontFamily": FONT, "minWidth": "280px"},
            ),
        ], style={"display": "flex", "alignItems": "center", "flex": "1"}),

        html.Span(style={"width": "1px", "background": DIVIDER,
                          "alignSelf": "stretch", "margin": "0 16px"}),

        # CSV download button
        html.Div([
            html.Span("⬇ ", style={"fontSize": "13px"}),
            html.Span("Download CSV", style={"fontSize": "12px"}),
        ], id="inst-csv-dl-btn", n_clicks=0,
           title="Download filtered issues as CSV",
           style={
               "display": "inline-flex", "alignItems": "center", "gap": "4px",
               "cursor": "pointer", "background": BRAND, "color": CARD,
               "fontSize": "12px", "fontWeight": "700",
               "padding": "7px 16px", "borderRadius": "6px",
               "userSelect": "none", "whiteSpace": "nowrap",
           }),

        dcc.Download(id="inst-csv-download"),
    ], style={
        "display": "flex", "alignItems": "center", "flexWrap": "wrap",
        "gap": "6px", "background": CARD, "borderRadius": "8px",
        "padding": "10px 14px", "border": f"1px solid {DIVIDER}",
        "marginBottom": "16px",
    })

    return html.Div([
        html.Div([
            html.H2("My Issues", style={"fontSize": "18px", "fontWeight": "900",
                                         "color": TEXT, "margin": "0 0 4px"}),
            html.P(
                "Filter by table, then download as CSV. "
                "Each row in the table below is a data category with open issues.",
                style={"fontSize": "12px", "color": MUTED, "margin": "0"},
            ),
        ], style={"marginBottom": "20px"}),

        html.Div(chips, style={"display": "flex", "gap": "10px",
                                "flexWrap": "wrap", "marginBottom": "20px"}),
        filter_bar,
        html.Div(id="inst-issue-list"),
    ], style={"padding": "28px 32px", "maxWidth": "1000px", "margin": "0 auto"})


def inst_remediation_page(le_books: list[str], role: str = "inst_user") -> html.Div:
    """CRs for this user's institutions. Can Start Work / Submit only."""
    import dq_change_request as cr_mod
    from dq_auth import is_admin

    lb_set = set(le_books)
    all_crs = cr_mod.get_crs()
    my_crs  = [c for c in all_crs if c["le_book"] in lb_set]

    stats = {}
    for cr in my_crs:
        s = cr["status"]
        stats[s] = stats.get(s, 0) + 1

    chips = []
    for key, lbl in cr_mod.STATUS_LABELS.items():
        n   = stats.get(key, 0)
        clr = cr_mod.STATUS_COLORS[key]
        chips.append(html.Div([
            html.Span(str(n),  style={"fontSize": "22px", "fontWeight": "900", "color": clr}),
            html.Span(lbl,     style={"fontSize": "10px", "color": MUTED, "marginTop": "3px"}),
        ], style={
            "display": "flex", "flexDirection": "column", "alignItems": "center",
            "background": CARD, "borderRadius": "8px", "padding": "12px 18px",
            "border": f"1px solid {clr}", "minWidth": "90px",
        }))

    from dq_dashboard_dash import _build_cr_list, BG as _BG, DIVIDER as _DIV

    filter_opts = [{"label": f"{lbl} ({stats.get(k, 0)})", "value": k}
                   for k, lbl in cr_mod.STATUS_LABELS.items()] + [{"label": "All", "value": "all"}]

    return html.Div([
        html.H2("My Change Requests", style={
            "fontSize": "18px", "fontWeight": "900", "color": TEXT,
            "marginTop": "0", "marginBottom": "6px",
        }),
        html.P(
            "Change Requests are created by BNR and assigned to your institution. "
            "Start work, make the corrections in your source system, then Submit for review.",
            style={"fontSize": "12px", "color": MUTED, "marginBottom": "20px"},
        ),
        html.Div(chips, style={
            "display": "flex", "gap": "10px", "flexWrap": "wrap", "marginBottom": "20px",
        }),
        html.Div([
            html.Span("Show: ", style={"fontSize": "12px", "color": MUTED,
                                       "alignSelf": "center", "marginRight": "8px"}),
            dcc.Dropdown(
                id="inst-cr-status-filter",
                options=filter_opts,
                value="open",
                clearable=False,
                style={"width": "180px", "fontSize": "12px"},
            ),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "16px"}),
        html.Div(id="inst-cr-list"),
        html.Div(id="inst-cr-feedback", style={"marginTop": "10px", "fontSize": "12px"}),
    ], style={"padding": "28px 32px", "maxWidth": "1400px", "margin": "0 auto"})


def inst_validations_page(le_books: list[str]) -> html.Div:
    """Read-only view of validation rules in scope."""
    from dq_rules import (COMP_RULE_META, ACC_RULE_META,
                          TIM_RULE_META, VAL_RULE_META, REL_RULE_META)

    all_rules = {
        **COMP_RULE_META, **ACC_RULE_META,
        **TIM_RULE_META,  **VAL_RULE_META, **REL_RULE_META,
    }

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "padding": "7px 10px"}

    rows = []
    for rid, meta in sorted(all_rules.items()):
        rows.append(html.Div([
            html.Span(rid,                           style={"width": "90px",  "fontSize": "11px", "fontWeight": "700", "color": BRAND, "padding": "7px 10px"}),
            html.Span(meta.get("category", ""),      style={"width": "120px", "fontSize": "11px", "color": MUTED, "padding": "7px 10px"}),
            html.Span(meta.get("name", ""),          style={"flex": "1",      "fontSize": "12px", "color": TEXT, "padding": "7px 10px"}),
            html.Span(meta.get("child_table", meta.get("table", "")), style={"width": "180px", "fontSize": "11px", "color": MUTED, "padding": "7px 10px"}),
        ], style={
            "display": "flex", "alignItems": "center",
            "borderBottom": f"1px solid {DIVIDER}",
        }))

    hdr = html.Div([
        html.Span("Rule ID",   style={**H, "width": "90px"}),
        html.Span("Category",  style={**H, "width": "120px"}),
        html.Span("Rule Name", style={**H, "flex": "1"}),
        html.Span("Table",     style={**H, "width": "180px"}),
    ], style={"display": "flex", "background": BG,
              "borderRadius": "8px 8px 0 0", "borderBottom": f"2px solid {DIVIDER}"})

    return html.Div([
        html.H2("Validation Rules", style={
            "fontSize": "18px", "fontWeight": "900", "color": TEXT,
            "marginTop": "0", "marginBottom": "6px",
        }),
        html.P(
            "These are the DQ rules applied to your institution's data on each pipeline run.",
            style={"fontSize": "12px", "color": MUTED, "marginBottom": "20px"},
        ),
        html.Div([hdr, *rows], style={
            "background": CARD, "borderRadius": "8px",
            "border": f"1px solid {DIVIDER}",
        }),
    ], style={"padding": "28px 32px", "maxWidth": "1200px", "margin": "0 auto"})
