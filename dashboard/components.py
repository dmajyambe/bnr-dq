# Shared, mostly-stateless UI builders used by multiple pages
from __future__ import annotations
import json
from dash import dcc, html
import plotly.graph_objects as go
from dashboard.data import (
    REPORTS_DIR, WATERMARK_FILE,
    _cat_scores, _inst_scores,
)
from dashboard.theme import (
    BG, BNR_GOLD, BRAND, CARD, C_AMBER, C_GREEN, C_RED, DIM_COLORS, DIM_LABELS,
    DIMS, DIVIDER, FONT, MUTED, TABLE_NAMES_PRETTY, TEXT,
    _DIM_PILL_COLOR, _URGENCY_COLORS, _URGENCY_LABELS,
)


#small UI helpers
def _fmt_int(n, fallback: str = "—") -> str:
    """Thousands-separated integer for display; falls back to a dash for non-numbers."""
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return fallback


def _empty_state(title: str, subtitle: str = "", icon: str = "✓") -> html.Div:
    """Friendly empty-state block, shown when a list/table has nothing to display."""
    kids = [
        html.Div(icon,  style={"fontSize": "32px", "marginBottom": "8px", "opacity": "0.85"}),
        html.Div(title, style={"fontSize": "14px", "fontWeight": "900", "color": TEXT}),
    ]
    if subtitle:
        kids.append(html.Div(subtitle, style={
            "fontSize": "12px", "color": MUTED, "marginTop": "4px",
            "maxWidth": "380px", "textAlign": "center"}))
    return html.Div(kids, style={
        "display": "flex", "flexDirection": "column", "alignItems": "center",
        "justifyContent": "center", "padding": "44px 24px", "textAlign": "center",
        "background": CARD, "border": f"1px dashed {DIVIDER}", "borderRadius": "10px",
    })


# ── score styling ──────────────────────────────────────────────────────────────

def _score_color(s: float) -> str:
    return C_GREEN if s >= 90 else C_AMBER if s >= 75 else C_RED

def _score_bg(s: float) -> str:
    return ("rgba(184,134,11,.10)"  if s >= 90 else
            "rgba(160,120,74,.10)"  if s >= 75 else
            "rgba(124,61,30,.10)")


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


def _kpi_card(dim: str, score: float | None, spark: list) -> html.Div:
    # score=None means the dimension was not evaluated (e.g. timeliness before
    # source columns exist) — render "—" instead of a misleading red 0.0%.
    col = _score_color(score) if score is not None else MUTED

    return html.Div([
        html.Div(DIM_LABELS[dim], style={
            "fontSize": "11px", "fontWeight": "900",
            "color": MUTED, "letterSpacing": "0.06em",
            "textTransform": "uppercase", "lineHeight": "1.15",
        }),
        html.Div(f"{score:.1f}%" if score is not None else "—", style={
            "fontSize": "30px", "fontWeight": "700",
            "color": col, "lineHeight": "1.1", "marginTop": "6px",
            "fontVariantNumeric": "tabular-nums",
        }),
        _sparkline(spark, col),
    ], style={
        "background":   CARD,
        "borderRadius": "8px",
        "padding":      "16px",
        "flex":         "1",
        "minWidth":     "150px",
        "borderTop":    f"3px solid {col}",
        "boxShadow":    "0 1px 4px rgba(117,57,24,0.08)",
    })


def _count_sparkline(values: list, color: str) -> dcc.Graph:
    """Sparkline for raw-count metrics (y-axis scales to data, not 0-100)."""
    r, g, b  = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    max_val  = max(values) if values and max(values) > 0 else 1
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
        yaxis=dict(visible=False, range=[0, max_val * 1.25]),
        showlegend=False,
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False},
                     style={"height": "36px", "marginTop": "8px"})


def _dup_card(count: int, delta: int, spark: list) -> html.Div:
    col    = C_GREEN if count == 0 else C_AMBER if count <= 10 else C_RED
    d_col  = C_RED if delta > 0 else C_GREEN if delta < 0 else MUTED
    d_icon = "▲" if delta > 0 else "▼" if delta < 0 else "─"
    return html.Div([
        html.Div("CUSTOMER DUPLICATES", style={
            "fontSize": "11px", "fontWeight": "900",
            "color": MUTED, "letterSpacing": "0.06em",
            "textTransform": "uppercase", "lineHeight": "1.15",
        }),
        html.Div(f"{count:,}", style={
            "fontSize": "30px", "fontWeight": "700",
            "color": col, "lineHeight": "1.1", "marginTop": "6px",
            "fontVariantNumeric": "tabular-nums",
        }),
        html.Div([
            html.Span(f"{d_icon} {abs(delta)}", style={
                "color": d_col, "fontWeight": "700", "fontSize": "12px",
            }),
            html.Span(" vs yesterday", style={
                "color": MUTED, "fontSize": "11px",
            }),
        ], style={"marginTop": "4px", "lineHeight": "1.15"}),
        _count_sparkline(spark, col),
    ], style={
        "background":   CARD,
        "borderRadius": "8px",
        "padding":      "16px",
        "flex":         "1",
        "minWidth":     "150px",
        "borderTop":    f"3px solid {col}",
        "boxShadow":    "0 1px 4px rgba(117,57,24,0.08)",
    })


def _table_card(table_name: str, fail_count: int, delta: int,
                spark: list, null_cols: dict) -> html.Div:
    """Per-table failing-rules card with collapsible mandatory-column null detail."""
    col    = C_GREEN if fail_count == 0 else C_AMBER if fail_count <= 3 else C_RED
    d_col  = C_RED   if delta > 0        else C_GREEN  if delta < 0        else MUTED
    d_icon = "▲"     if delta > 0        else "▼"      if delta < 0        else "─"
    pretty = TABLE_NAMES_PRETTY.get(table_name,
                                    table_name.replace("_", " ").title())

    null_section: list = []
    if null_cols:
        null_section = [
            html.Div([
                html.Span(
                    f"Mandatory columns with nulls: {len(null_cols)}",
                    style={"fontSize": "10px", "color": C_RED, "fontWeight": "700"},
                ),
                html.Span(
                    " ▶ show",
                    id={"type": "null-col-btn", "index": table_name},
                    n_clicks=0,
                    style={
                        "fontSize": "10px", "color": BRAND,
                        "cursor": "pointer", "marginLeft": "6px", "fontWeight": "700",
                    },
                ),
            ], style={"marginTop": "6px"}),
            html.Div(
                [html.Div(
                    f"└ {col}: {cnt:,} nulls",
                    style={"fontSize": "10px", "color": MUTED, "paddingLeft": "4px",
                           "lineHeight": "1.6"},
                ) for col, cnt in sorted(null_cols.items(), key=lambda x: -x[1])],
                id={"type": "null-col-body", "index": table_name},
                style={"display": "none", "marginTop": "4px"},
            ),
        ]

    other_count = max(0, fail_count - (1 if null_cols else 0))
    other_line  = html.Div(
        f"Other failing rules: {other_count}" if other_count > 0 else
        ("All rules passing" if fail_count == 0 else ""),
        style={
            "fontSize":   "10px",
            "color":      C_RED if other_count > 0 else C_GREEN if fail_count == 0 else MUTED,
            "marginTop":  "4px",
            "fontWeight": "700" if other_count > 0 else "400",
        },
    )

    return html.Div([
        html.Div(pretty.upper(), style={
            "fontSize": "10px", "fontWeight": "900", "color": MUTED,
            "letterSpacing": "0.06em", "textTransform": "uppercase",
        }),
        html.Div(str(fail_count), style={
            "fontSize": "28px", "fontWeight": "700", "color": col,
            "lineHeight": "1.1", "marginTop": "4px",
            "fontVariantNumeric": "tabular-nums",
        }),
        html.Div("failing rules", style={"fontSize": "10px", "color": MUTED}),
        html.Div([
            html.Span(f"{d_icon} {abs(delta)}", style={
                "color": d_col, "fontWeight": "700", "fontSize": "11px",
            }),
            html.Span(" vs yesterday", style={"color": MUTED, "fontSize": "10px"}),
        ], style={"marginTop": "3px"}),
        *null_section,
        other_line,
        _count_sparkline(spark, col),
    ], style={
        "background":   CARD,
        "borderRadius": "8px",
        "padding":      "14px 16px",
        "flex":         "1",
        "minWidth":     "140px",
        "borderTop":    f"3px solid {col}",
        "boxShadow":    "0 1px 4px rgba(117,57,24,0.08)",
    })



def _issue_counts_trend(n_days: int = 30,
                        le_books: set | None = None) -> list[dict]:
    """
    For each of the last n_days, compute:
      open     — issues open (detected but not yet resolved) on that date
      overdue  — open issues past their sla_deadline on that date
      resolved — issues resolved on that specific date

    Reconstructed from dq_open_issues dates — no separate history table needed.
    le_books: if given, restrict to those institution codes.
    """
    from datetime import date as _date, timedelta
    try:
        from issues.repositories import get_issues
        all_issues = get_issues()          # all statuses
    except Exception:
        return []

    if le_books is not None:
        all_issues = [i for i in all_issues if str(i["le_book"]) in le_books]

    today = _date.today()
    rows  = []
    for offset in range(n_days - 1, -1, -1):
        day = today - timedelta(days=offset)
        ds  = day.isoformat()

        open_n = overdue_n = resolved_n = 0
        for iss in all_issues:
            det = iss.get("detected_at") or ""
            res = iss.get("resolved_at") or ""
            sla = iss.get("sla_deadline") or ""
            if not det or det > ds:
                continue
            if res and res <= ds:
                if res == ds:
                    resolved_n += 1
            else:
                open_n += 1
                if sla and sla < ds:
                    overdue_n += 1

        rows.append({"date": ds, "open": open_n,
                     "overdue": overdue_n, "resolved": resolved_n})
    return rows


def _issue_trend_figure(data: list) -> go.Figure:
    """30-day issue count trend: open, overdue, resolved per day."""
    dates    = [r["date"]    for r in data]
    open_v   = [r["open"]    for r in data]
    over_v   = [r["overdue"] for r in data]
    res_v    = [r["resolved"] for r in data]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=open_v, name="Open",
        mode="lines+markers",
        line=dict(color=C_AMBER, width=2),
        marker=dict(size=4),
        hovertemplate="<b>Open</b>: %{y}<br>%{x}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=over_v, name="Overdue",
        mode="lines+markers",
        line=dict(color=_URGENCY_COLORS["overdue"], width=2, dash="dot"),
        marker=dict(size=4),
        hovertemplate="<b>Overdue</b>: %{y}<br>%{x}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=res_v, name="Resolved (daily)",
        mode="lines+markers",
        line=dict(color=C_GREEN, width=2),
        marker=dict(size=4),
        hovertemplate="<b>Resolved</b>: %{y}<br>%{x}<extra></extra>",
    ))
    fig.update_layout(
        height=250,
        paper_bgcolor=CARD, plot_bgcolor=CARD,
        margin=dict(l=8, r=8, t=36, b=8),
        font=dict(family=FONT, size=11, color=TEXT),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=11)),
        yaxis=dict(gridcolor=DIVIDER, tickfont=dict(size=10),
                   zeroline=False, rangemode="tozero"),
        xaxis=dict(gridcolor=DIVIDER, tickfont=dict(size=10), showgrid=False),
        hovermode="x unified",
    )
    return fig


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


def _institution_table(institutions: dict, issue_summary: dict | None = None) -> html.Div:
    if not institutions:
        return html.Div(
            "No institution data for this category.",
            style={"color": MUTED, "fontSize": "12px",
                   "padding": "32px", "textAlign": "center"},
        )

    rows = sorted(institutions.items(), key=lambda kv: kv[1].get("overall", 0))

    _isu  = issue_summary or {}
    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em",
         "lineHeight": "1.15", "flexShrink": "0"}
    COL_W  = "74px"
    DL_W   = "52px"
    NTFY_W = "36px"

    header = html.Div([
        html.Span("Institution", style={**H, "flex": "1", "flexShrink": "1"}),
        *[html.Span(DIM_LABELS[d][:5], style={**H, "width": COL_W, "textAlign": "center"})
          for d in DIMS],
        html.Span("Overall", style={**H, "width": COL_W, "textAlign": "center"}),
        html.Span("Report",  style={**H, "width": DL_W,  "textAlign": "center"}),
        html.Span("Alert",   style={**H, "width": NTFY_W, "textAlign": "center"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "4px",
        "padding": "9px 14px",
        "borderBottom": "2px solid rgba(117,57,24,0.18)",
        "background": BG, "borderRadius": "8px 8px 0 0",
    })

    data_rows = []
    for i, (lb, d) in enumerate(rows):
        name    = (d.get("name") or lb).title()
        overall = float(d.get("overall") or 0)
        bg      = "#c9956c" if i % 2 == 0 else BG

        # urgency left-border based on tracked issues
        isu_info = _isu.get(lb, {})
        worst    = isu_info.get("worst_urgency")
        left_clr = _URGENCY_COLORS.get(worst, "transparent") if worst else "transparent"
        n_issues = isu_info.get("total", 0)

        # institution name cell with urgency dot
        name_children = []
        if worst:
            tip = f"{n_issues} open issue(s) — {_URGENCY_LABELS.get(worst, worst)}"
            name_children.append(html.Span("●", title=tip, style={
                "color": left_clr, "fontSize": "9px",
                "marginRight": "5px", "flexShrink": "0",
            }))
        name_children.append(html.Span(name, title=name, style={
            "fontSize": "12px", "color": TEXT, "lineHeight": "1.15",
            "overflow": "hidden", "textOverflow": "ellipsis",
            "whiteSpace": "nowrap",
        }))

        cells = [
            html.Div(name_children, style={
                "flex": "1", "flexShrink": "1", "display": "flex",
                "alignItems": "center", "minWidth": "0",
                "borderLeft": f"3px solid {left_clr}",
                "paddingLeft": "6px", "marginLeft": "-6px",
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

        # prefer CSV over XLSX for the date label
        csv_files = sorted(REPORTS_DIR.glob(f"*_{lb}_*.csv"), reverse=True) if REPORTS_DIR.exists() else []
        rpt_files = sorted(REPORTS_DIR.glob(f"{lb}_*.xlsx"),  reverse=True) if REPORTS_DIR.exists() else []
        has_report = bool(csv_files or rpt_files)

        if has_report:
            if csv_files:
                # extract month: {table}_{lb}_{YYYY-MM}.csv
                parts    = csv_files[0].stem.rsplit("_", 2)
                rpt_date = parts[2] if len(parts) == 3 and len(parts[2]) == 7 else "—"
                fmt_label = "CSV"
            else:
                parts    = rpt_files[0].stem.rsplit("_", 3)
                rpt_date = parts[-1] if len(parts) >= 2 and len(parts[-1]) == 10 else "—"
                fmt_label = "XLSX"
            dl_btn = html.Div(
                [html.Span("⬇ ", style={"fontSize": "13px"}),
                 html.Span(rpt_date, style={"fontSize": "9px", "opacity": "0.75"})],
                id={"type": "inst-dl-btn", "index": lb},
                n_clicks=0,
                title=f"Download {name} report ({fmt_label}, {rpt_date})",
                style={
                    "width": DL_W, "textAlign": "center", "flexShrink": "0",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                    "cursor": "pointer", "color": BRAND, "userSelect": "none",
                    "lineHeight": "1.15",
                },
            )
        else:
            dl_btn = html.Span("—", style={
                "width": DL_W, "textAlign": "center", "flexShrink": "0",
                "fontSize": "11px", "color": MUTED, "lineHeight": "1.15",
            })
        cells.append(dl_btn)

        # Notify button — shown when institution has open issues
        if n_issues > 0:
            notify_btn = html.Div(
                "🔔",
                id={"type": "notify-btn", "index": lb},
                n_clicks=0,
                title=f"Send reminder to {name} ({n_issues} open issue(s))",
                style={
                    "width": NTFY_W, "textAlign": "center", "flexShrink": "0",
                    "fontSize": "13px", "lineHeight": "1.15",
                    "cursor": "pointer", "userSelect": "none",
                    "color": left_clr,
                },
            )
        else:
            notify_btn = html.Span("", style={"width": NTFY_W, "flexShrink": "0"})
        cells.append(notify_btn)

        data_rows.append(html.Div(cells, className="inst-row", style={
            "display": "flex", "alignItems": "center", "gap": "4px",
            "padding": "7px 14px", "background": bg,
            "borderBottom": "1px solid rgba(117,57,24,0.12)",
        }))

    table = html.Div(
        [header] + data_rows,
        style={"border": f"1px solid {DIVIDER}", "borderRadius": "8px",
               "overflow": "hidden"},
    )
    return html.Div([table])


def _watermark_date() -> str:
    """Return the earliest watermark date (the 'data as of' cut-off)."""
    try:
        wm = json.loads(WATERMARK_FILE.read_text(encoding="utf-8"))
        dates = [str(v)[:10] for v in wm.values() if v]
        return min(dates) if dates else "—"
    except Exception:
        return "—"


def _unscored_section(cat: str, all_categories: dict, scored_lbs: set,
                      activity: dict) -> html.Div:
    """Count card + collapsible table for institutions absent from today's run."""
    if cat == "SACCO":
        types = {"SACCO", "OSACCO"}
    else:
        types = {cat}

    unscored = {
        lb: info for lb, info in all_categories.items()
        if info.get("category_type") in types and lb not in scored_lbs
    }
    n = len(unscored)
    if n == 0:
        return html.Div()

    wm_date = _watermark_date()

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em",
         "lineHeight": "1.15", "flexShrink": "0"}

    # ── count card ─────────────────────────────────────────────────────────────
    count_card = html.Div([
        html.Div("NOT SCORED THIS RUN", style={
            "fontSize": "11px", "fontWeight": "900",
            "color": MUTED, "letterSpacing": "0.06em",
            "textTransform": "uppercase", "lineHeight": "1.15",
        }),
        html.Div(str(n), style={
            "fontSize": "30px", "fontWeight": "700",
            "color": BRAND, "lineHeight": "1.1", "marginTop": "6px",
            "fontVariantNumeric": "tabular-nums",
        }),
        html.Div(
            f"institution{'s' if n != 1 else ''}",
            style={"fontSize": "12px", "color": MUTED, "lineHeight": "1.15"},
        ),
        html.Div([
            html.Span("No data changes since ", style={"color": MUTED}),
            html.Span(wm_date, style={"color": BRAND, "fontWeight": "700"}),
        ], style={"fontSize": "11px", "marginTop": "8px", "lineHeight": "1.4"}),
    ], style={
        "background":   CARD,
        "borderRadius": "8px",
        "padding":      "16px",
        "borderTop":    f"3px solid {MUTED}",
        "boxShadow":    "0 1px 4px rgba(117,57,24,0.08)",
        "display":      "inline-block",
        "minWidth":     "200px",
    })

    # ── table ──────────────────────────────────────────────────────────────────
    header = html.Div([
        html.Span("LE Book",       style={**H, "width": "76px"}),
        html.Span("Institution",   style={**H, "flex": "1"}),
        html.Span("Last Modified", style={**H, "width": "124px", "textAlign": "center"}),
        html.Span("Last Created",  style={**H, "width": "124px", "textAlign": "center"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "8px",
        "padding": "9px 14px",
        "borderBottom": f"2px solid rgba(117,57,24,0.18)",
        "background": BG, "borderRadius": "8px 8px 0 0",
    })

    data_rows = []
    for i, (lb, info) in enumerate(
        sorted(unscored.items(), key=lambda kv: (kv[1].get("name") or kv[0]).lower())
    ):
        name     = (info.get("name") or lb).title()
        act      = activity.get(lb, {})
        last_mod = act.get("last_modified") or "—"
        last_cre = act.get("last_created")  or "—"
        bg       = "#c9956c" if i % 2 == 0 else BG

        data_rows.append(html.Div([
            html.Span(lb, style={
                "width": "76px", "flexShrink": "0",
                "fontSize": "11px", "fontWeight": "700",
                "color": BRAND, "fontFamily": "monospace",
            }),
            html.Span(name, style={
                "flex": "1", "fontSize": "12px", "color": TEXT,
                "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap",
            }),
            html.Span(last_mod, style={
                "width": "124px", "flexShrink": "0",
                "fontSize": "11px", "color": MUTED, "textAlign": "center",
            }),
            html.Span(last_cre, style={
                "width": "124px", "flexShrink": "0",
                "fontSize": "11px", "color": MUTED, "textAlign": "center",
            }),
        ], style={
            "display": "flex", "alignItems": "center", "gap": "8px",
            "padding": "7px 14px", "background": bg,
            "borderBottom": f"1px solid rgba(117,57,24,0.10)",
        }))

    table = html.Div([header] + data_rows, style={
        "border": f"1px solid {DIVIDER}", "borderRadius": "8px",
        "overflow": "hidden", "marginTop": "10px",
    })

    # ── toggle button ──────────────────────────────────────────────────────────
    toggle_btn = html.Div(
        id="unscored-toggle",
        n_clicks=0,
        children=f"▶  Show {n} unscored institution{'s' if n != 1 else ''}",
        style={
            "cursor": "pointer", "color": BRAND,
            "fontSize": "12px", "fontWeight": "700",
            "userSelect": "none", "marginTop": "14px",
            "display": "inline-block",
        },
    )

    return html.Div([
        # divider
        html.Div(style={
            "borderTop": f"1px solid {DIVIDER}", "margin": "24px 0 16px",
        }),
        html.Div([count_card], style={"marginBottom": "4px"}),
        toggle_btn,
        html.Div(
            id="unscored-body",
            children=table,
            style={"display": "none"},
        ),
    ])


def _stale_banner() -> html.Div | None:
    from dashboard.data import _fresh_history
    history = _fresh_history()
    if not history:
        return html.Div(
            "No detection data found. Run:  python -m jobs.monthly_detection",
            style={
                "background": "#FEF2F2", "border": f"1px solid {C_RED}",
                "borderRadius": "6px", "padding": "10px 16px",
                "fontSize": "12px", "color": C_RED,
                "marginBottom": "16px", "lineHeight": "1.15",
            },
        )
    from datetime import date as _date
    last_date = history[-1].get("date", "")
    try:
        days_since = (_date.today() - _date.fromisoformat(last_date)).days
    except Exception:
        days_since = 999
    if days_since > 35:
        return html.Div(
            f"⚠  Last monthly detection: {last_date} — {days_since} days ago. "
            "Run jobs/monthly_detection.py to refresh.",
            style={
                "background": "#FFFBEB", "border": "1px solid #F59E0B",
                "borderRadius": "6px", "padding": "10px 16px",
                "fontSize": "12px", "color": "#92400E",
                "marginBottom": "16px", "lineHeight": "1.15",
            },
        )
    return None


def _table_score_heatmap(table_scores: dict, show_dims: list | None = None) -> html.Div:
    """Compact quality-by-table heatmap.

    table_scores: {table: {dim: score}}
    show_dims: ordered dim list (defaults to COMP/ACC/VAL/UNI, skips TIM when empty).
    Rows are sorted worst-average-first so the most actionable table is at the top.
    """
    if not table_scores:
        return html.Div()

    all_dims = show_dims or ["completeness", "accuracy", "validity", "uniqueness", "timeliness"]
    # Only show a dim column when at least one table has a real score for it
    active_dims = [d for d in all_dims
                   if any(d in scores for scores in table_scores.values())]
    if not active_dims:
        return html.Div()

    _SHORT = {
        "completeness": "COMP", "accuracy": "ACC",
        "validity": "VAL", "uniqueness": "UNI", "timeliness": "TIM",
    }
    COL_W = "68px"

    def _avg(scores: dict) -> float:
        vals = [v for v in scores.values() if v is not None]
        return sum(vals) / len(vals) if vals else 100.0

    def _score_cell(score: float | None) -> html.Div:
        if score is None:
            return html.Div("—", style={
                "width": COL_W, "flexShrink": "0", "textAlign": "center",
                "fontSize": "11px", "color": MUTED,
                "fontVariantNumeric": "tabular-nums",
            })
        col = _score_color(score)
        bg  = _score_bg(score)
        return html.Div(f"{score:.1f}%", style={
            "width": COL_W, "flexShrink": "0", "textAlign": "center",
            "fontSize": "11px", "fontWeight": "700", "color": col,
            "background": bg, "borderRadius": "4px", "padding": "3px 0",
            "fontVariantNumeric": "tabular-nums",
        })

    sorted_tables = sorted(table_scores.items(), key=lambda kv: _avg(kv[1]))

    H = {"fontSize": "10px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.06em",
         "width": COL_W, "flexShrink": "0", "textAlign": "center"}

    header = html.Div([
        html.Span("TABLE", style={**H, "width": "auto", "flex": "1", "textAlign": "left"}),
        *[html.Span(_SHORT.get(d, d[:4].upper()), style=H) for d in active_dims],
    ], style={
        "display": "flex", "alignItems": "center", "gap": "6px",
        "padding": "7px 14px",
        "borderBottom": "2px solid rgba(117,57,24,0.18)",
        "background": BG, "borderRadius": "8px 8px 0 0",
    })

    data_rows = []
    for i, (table, scores) in enumerate(sorted_tables):
        pretty = TABLE_NAMES_PRETTY.get(table, table.replace("_", " ").title())
        avg    = _avg(scores)
        is_worst = i == 0 and avg < 90
        bg = "#c9956c" if i % 2 == 0 else BG

        data_rows.append(html.Div([
            html.Span(pretty, title=table, style={
                "fontSize": "12px", "color": TEXT, "flex": "1",
                "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap",
                "fontWeight": "700" if is_worst else "400",
            }),
            *[_score_cell(scores.get(d)) for d in active_dims],
        ], style={
            "display": "flex", "alignItems": "center", "gap": "6px",
            "padding": "7px 14px", "background": bg,
            "borderBottom": "1px solid rgba(117,57,24,0.10)",
            "borderLeft": f"3px solid {_score_color(avg)}" if is_worst else "3px solid transparent",
        }))

    return html.Div([
        html.Div("QUALITY BY TABLE", style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "letterSpacing": "0.06em", "textTransform": "uppercase",
            "marginBottom": "10px",
        }),
        html.Div([header] + data_rows, style={
            "border": f"1px solid {DIVIDER}",
            "borderRadius": "8px", "overflow": "hidden",
        }),
    ])


def _dim_pill(dim: str) -> html.Span:
    color = _DIM_PILL_COLOR.get(dim, MUTED)
    return html.Span(dim.capitalize(), style={
        "background":   CARD,
        "color":        color,
        "border":       f"1px solid {color}",
        "borderRadius": "4px",
        "padding":      "2px 7px",
        "fontSize":     "11px",
        "fontWeight":   "700",
        "whiteSpace":   "nowrap",
    })


def _nav_tabs(active: str) -> html.Div:
    items = [("profiling", "Data Profiling"), ("dashboard", "Dashboard"), ("remediation", "Request Data Correction"), ("alerts", "Check Resolved Issues"), ("validations", "Documentation")]
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
        "background": "#753918",
        "padding":    "0 32px",
        "borderTop":  "1px solid rgba(255,255,255,0.12)",
    })


def _executive_nav(name: str = "") -> html.Div:
    """Minimal nav bar for the executive (Management) view — single section."""
    return html.Div([
        html.Div("MANAGEMENT OVERVIEW", style={
            "padding": "11px 24px", "fontSize": "13px", "fontWeight": "900",
            "color": CARD, "borderBottom": f"3px solid {BNR_GOLD}",
            "whiteSpace": "nowrap", "userSelect": "none",
        }),
    ], style={
        "display": "flex", "background": "#753918",
        "padding": "0 32px", "borderTop": "1px solid rgba(255,255,255,0.12)",
    })
