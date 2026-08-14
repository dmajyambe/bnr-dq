# Executive / Management overview page — moved from dq_dashboard_dash.py.
from __future__ import annotations

from dash import dcc, html
import plotly.graph_objects as go

from dashboard.components import _fmt_int, _score_color
from dashboard.data import _inst_scores, _today_entry, _yesterday_entry
from dashboard.theme import (
    BRAND, CARD, DIM_COLORS, DIM_LABELS, DIMS, DIVIDER, FONT, LANDING_CATS,
    MUTED, TEXT, C_GREEN, C_RED,
)


def _exec_overall_avg(entry: dict, le_books: list[str] | None) -> float | None:
    """Mean of by_institution[*]['overall'] — the per-institution score already
    excludes dims that didn't run (e.g. timeliness), unlike the raw entry['overall']
    dict which phantom-zeros unscored dims. le_books=None -> every institution in
    the entry; otherwise restrict to just those le_books (institution-scoped view)."""
    insts = entry.get("by_institution", {}) if entry else {}
    if le_books is None:
        vals = [d.get("overall", 0.0) for d in insts.values()]
    else:
        vals = [insts[lb]["overall"] for lb in le_books if lb in insts]
    return round(sum(vals) / len(vals), 2) if vals else None


def _exec_delta_sub(delta: float | None, invert: bool = False,
                    no_data_label: str = "No prior data") -> html.Div:
    """Small delta line for an executive KPI card ('▲ 1.2pp vs last month')."""
    if delta is None:
        return html.Div(no_data_label, style={"color": MUTED, "fontSize": "11px"})
    good   = (delta < 0) if invert else (delta > 0)
    bad    = (delta > 0) if invert else (delta < 0)
    d_col  = C_GREEN if good else C_RED if bad else MUTED
    d_icon = "▲" if delta > 0 else "▼" if delta < 0 else "─"
    return html.Div([
        html.Span(f"{d_icon} {abs(delta):.1f}", style={
            "color": d_col, "fontWeight": "700", "fontSize": "12px"}),
        html.Span(" vs last month", style={"color": MUTED, "fontSize": "11px"}),
    ], style={"marginTop": "4px", "lineHeight": "1.15"})


def _exec_kpi_card(label: str, value_str: str, sub=None,
                   color: str = TEXT, accent: str = BRAND) -> html.Div:
    kids = [
        html.Div(label, style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "letterSpacing": "0.06em", "textTransform": "uppercase", "lineHeight": "1.15",
        }),
        html.Div(value_str, style={
            "fontSize": "30px", "fontWeight": "700", "color": color,
            "lineHeight": "1.1", "marginTop": "6px", "fontVariantNumeric": "tabular-nums",
        }),
    ]
    if sub is not None:
        kids.append(sub)
    return html.Div(kids, style={
        "background":   CARD,
        "borderRadius": "8px",
        "padding":      "16px",
        "flex":         "1",
        "minWidth":     "180px",
        "borderTop":    f"3px solid {accent}",
        "boxShadow":    "0 1px 4px rgba(117,57,24,0.08)",
    })


def _exec_hero_kpis(role: str, le_books: list[str]) -> html.Div:
    """Hero KPI row: overall DQ score (+Δ), open-issue coverage, overdue count,
    on-time remediation rate. Scoped to all institutions for bnr_executive, or to
    the executive's own institution(s) for inst_executive."""
    from issues.repositories import get_issues

    is_bnr   = (role == "bnr_executive")
    today    = _today_entry()
    prev     = _yesterday_entry()
    scope_lb = None if is_bnr else [str(lb) for lb in le_books]

    overall_now  = _exec_overall_avg(today, scope_lb)
    overall_prev = _exec_overall_avg(prev,  scope_lb)
    score_delta  = (round(overall_now - overall_prev, 1)
                    if overall_now is not None and overall_prev is not None else None)

    all_open = get_issues(status="open")
    if not is_bnr:
        lb_set   = set(scope_lb or [])
        all_open = [i for i in all_open if str(i.get("le_book")) in lb_set]

    if is_bnr:
        kpi2_label = "INSTITUTIONS WITH OPEN ISSUES"
        kpi2_value = len({i["le_book"] for i in all_open})
    else:
        kpi2_label = "TABLES WITH OPEN ISSUES"
        kpi2_value = len({i["table_name"] for i in all_open})

    from datetime import datetime as _datetime
    from dashboard.data import latest_run_month
    this_month  = latest_run_month()
    month_label = _datetime.strptime(this_month, "%Y-%m").strftime("%B %Y")
    new_this_month = sum(1 for i in all_open
                         if (i.get("detected_at") or "").startswith(this_month))

    all_resolved = get_issues(status="resolved")
    if not is_bnr:
        all_resolved = [i for i in all_resolved if str(i.get("le_book")) in (scope_lb or [])]
    on_time = sum(1 for i in all_resolved
                  if i.get("sla_deadline") and i.get("resolved_at")
                  and i["resolved_at"] <= i["sla_deadline"])
    remediation_rate = round(on_time / len(all_resolved) * 100, 1) if all_resolved else None

    score_color = _score_color(overall_now) if overall_now is not None else MUTED
    rate_color  = _score_color(remediation_rate) if remediation_rate is not None else MUTED

    cards = [
        _exec_kpi_card(
            "OVERALL DQ SCORE",
            f"{overall_now:.1f}%" if overall_now is not None else "—",
            sub=_exec_delta_sub(score_delta),
            color=score_color, accent=score_color if overall_now is not None else DIVIDER,
        ),
        _exec_kpi_card(
            kpi2_label, _fmt_int(kpi2_value),
            color=C_RED if kpi2_value else TEXT,
            accent=C_RED if kpi2_value else C_GREEN,
        ),
        _exec_kpi_card(
            f"NEW ISSUES — {month_label}", _fmt_int(new_this_month),
            color=C_RED if new_this_month else TEXT,
            accent=C_RED if new_this_month else C_GREEN,
        ),
        _exec_kpi_card(
            "REMEDIATION RATE",
            f"{remediation_rate:.1f}%" if remediation_rate is not None else "—",
            sub=None if remediation_rate is not None else html.Div(
                "No issues resolved yet", style={"fontSize": "11px", "color": MUTED}),
            color=rate_color, accent=rate_color if remediation_rate is not None else DIVIDER,
        ),
    ]
    return html.Div(cards, style={
        "display": "flex", "gap": "16px", "marginBottom": "20px", "flexWrap": "wrap",
    })


def _exec_sector_block(role: str, le_books: list[str]) -> html.Div:
    """bnr_executive: B / MF / SACCO sector comparison bars (blended overall score).
    inst_executive: 4-dimension breakdown bars for their own institution(s) instead,
    since there's nothing to compare against."""
    is_bnr = (role == "bnr_executive")
    entry  = _today_entry()

    if is_bnr:
        insts   = entry.get("by_institution", {}) if entry else {}
        buckets: dict[str, list[float]] = {}
        for lb, d in insts.items():
            ct = d.get("category_type")
            if ct == "OSACCO":
                ct = "SACCO"
            if ct in {c["code"] for c in LANDING_CATS}:
                buckets.setdefault(ct, []).append(float(d.get("overall") or 0.0))

        title   = "SECTOR COMPARISON"
        x_vals  = [c["label"] for c in LANDING_CATS]
        colors  = [c["color"] for c in LANDING_CATS]
        values  = [round(sum(buckets[c["code"]]) / len(buckets[c["code"]]), 1)
                   if buckets.get(c["code"]) else None for c in LANDING_CATS]
        counts  = [len(buckets.get(c["code"], [])) for c in LANDING_CATS]
    else:
        lb_set   = [str(lb) for lb in le_books]
        dim_vals = {d: [_inst_scores(entry, lb)[d] for lb in lb_set if lb in
                        (entry.get("by_institution", {}) if entry else {})] for d in DIMS}

        title  = "DIMENSION BREAKDOWN"
        x_vals = [DIM_LABELS[d] for d in DIMS]
        colors = [DIM_COLORS[d] for d in DIMS]
        values = [round(sum(dim_vals[d]) / len(dim_vals[d]), 1) if dim_vals[d] else None
                  for d in DIMS]
        counts = [len(dim_vals[d]) for d in DIMS]

    bar_y = [v if v is not None else 0 for v in values]
    text  = [f"{v:.1f}%" if v is not None else "No data" for v in values]

    fig = go.Figure(go.Bar(
        x=x_vals, y=bar_y,
        marker=dict(color=colors),
        text=text, textposition="outside",
        customdata=counts,
        hovertemplate="%{x}: %{y:.1f}%<br>%{customdata} institution(s)<extra></extra>",
    ))
    fig.update_layout(
        height=260,
        paper_bgcolor=CARD, plot_bgcolor=CARD,
        margin=dict(l=8, r=8, t=24, b=8),
        font=dict(family=FONT, size=11, color=TEXT),
        yaxis=dict(title="Score (%)", range=[0, 108], gridcolor=DIVIDER, zeroline=False),
        xaxis=dict(tickfont=dict(size=11)),
        showlegend=False,
    )

    return html.Div([
        html.Div(title, style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginBottom": "6px",
        }),
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ], style={
        "background": CARD, "borderRadius": "8px",
        "padding": "16px 16px 8px",
        "boxShadow": "0 1px 4px rgba(117,57,24,0.08)",
        "border": f"1px solid {DIVIDER}",
    })


def _executive_page(role: str, le_books: list | None = None) -> html.Div:
    """Executive / Management high-level view: hero KPIs + sector/dimension
    comparison. Scope: bnr_executive → all institutions; inst_executive → own
    institution(s)."""
    le_books = le_books or []

    return html.Div([
        _exec_hero_kpis(role, le_books),
        _exec_sector_block(role, le_books),
    ], style={"padding": "32px"})
