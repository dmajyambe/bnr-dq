# Validations tab (read-only list of active rules) — moved from dq_dashboard_dash.py.
from __future__ import annotations

from dash import dcc, html
import plotly.graph_objects as go

from dashboard.components import _dim_pill
from dashboard.theme import BG, BRAND, CARD, DIVIDER, FONT, MUTED, TEXT
from dq.rules.registry import get_all_rules


def _rules_charts(builtin_rules: list[dict], user_rules: list[dict]) -> html.Div:
    """Single horizontal bar chart: total rules per table (no dimension breakdown)."""
    from collections import defaultdict

    pending = [r for r in user_rules if r.get("status") == "pending"]
    active  = [r for r in user_rules if r.get("status") == "active"]

    _TABLE_LABELS = {
        "accounts":              "Accounts",
        "contract_loans":        "Contract Loans",
        "contract_schedules":    "Contract Schedules",
        "contracts_disburse":    "Contracts Disburse",
        "contracts_expanded":    "Contracts",
        "customers_expanded":    "Customers",
        "loan_applications_2":   "Loan Applications",
        "prev_loan_applications": "Prev Loan Apps",
    }

    def _count_per_table(rule_list: list[dict]) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for r in rule_list:
            tables_str = r.get("tables", "")
            if "→" in tables_str:
                child = tables_str.split("→")[0].strip()
                if child:
                    counts[child] += 1
            else:
                for t in tables_str.split(","):
                    t = t.strip()
                    if t:
                        counts[t] += 1
        return counts

    run_counts     = _count_per_table(builtin_rules + active)
    pending_counts = _count_per_table(pending)

    all_tables = sorted(
        set(run_counts.keys()) | set(pending_counts.keys()),
        key=lambda t: run_counts.get(t, 0) + pending_counts.get(t, 0),
    )

    y_labels  = [_TABLE_LABELS.get(t, t.replace("_", " ").title()) for t in all_tables]
    x_run     = [run_counts.get(t, 0) for t in all_tables]
    x_pending = [pending_counts.get(t, 0) for t in all_tables]
    has_pending = any(x_pending)

    traces = [go.Bar(
        name="Built-in + active rules",
        y=y_labels,
        x=x_run,
        orientation="h",
        marker_color=BRAND,
        text=[str(v) for v in x_run],
        textposition="outside",
        hovertemplate="%{y}: %{x} rules<extra></extra>",
        showlegend=False,
    )]
    if has_pending:
        traces.append(go.Bar(
            name="Pending (not yet run)",
            y=y_labels,
            x=x_pending,
            orientation="h",
            marker=dict(
                color="rgba(148,163,184,0.35)",
                pattern=dict(shape="/", fgcolor="rgba(100,116,139,0.6)", size=6),
                line=dict(color="rgba(100,116,139,0.5)", width=1),
            ),
            text=[str(v) if v else "" for v in x_pending],
            textposition="outside",
            hovertemplate="Pending — %{y}: %{x} rules<extra></extra>",
        ))

    fig = go.Figure(traces)
    fig.update_layout(
        barmode="stack",
        height=max(260, 40 * len(all_tables) + 70),
        paper_bgcolor=CARD, plot_bgcolor=CARD,
        margin=dict(l=8, r=40, t=20, b=8),
        font=dict(family=FONT, size=11, color=TEXT),
        xaxis=dict(title="Number of rules", gridcolor=DIVIDER, zeroline=False,
                   tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=11), showgrid=False, automargin=True),
        bargap=0.3,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=10)),
        showlegend=has_pending,
    )

    return html.Div([
        html.Div("RULES BY TABLE", style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "textTransform": "uppercase", "letterSpacing": "0.06em",
            "marginBottom": "6px",
        }),
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ], style={
        "background": CARD, "borderRadius": "8px",
        "padding": "16px 16px 8px",
        "boxShadow": "0 1px 4px rgba(117,57,24,0.08)",
        "border": f"1px solid {DIVIDER}",
        "marginBottom": "20px",
    })


def _rules_table(rules: list[dict]) -> html.Div:
    """Full, scannable list of every rule currently being checked, grouped by
    dimension. Sourced from the live registry, so it always matches what the
    pipeline actually runs."""
    from collections import defaultdict
    by_dim: dict[str, list] = defaultdict(list)
    for r in rules:
        by_dim[r.get("dimension", "")].append(r)
    dim_order = ["completeness", "accuracy", "validity",
                 "uniqueness", "timeliness", "relationship"]

    H = {"fontSize": "10px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em", "padding": "8px 10px"}
    hdr = html.Div([
        html.Span("Rule ID",   style={**H, "width": "92px"}),
        html.Span("Dimension", style={**H, "width": "120px"}),
        html.Span("Rule",      style={**H, "flex": "3"}),
        html.Span("Tables",    style={**H, "flex": "2"}),
        html.Span("Fields",    style={**H, "flex": "2"}),
    ], style={"display": "flex", "alignItems": "center", "background": BG,
              "borderBottom": f"2px solid {DIVIDER}", "borderRadius": "8px 8px 0 0"})

    rows = [hdr]
    for dim in dim_order:
        drules = sorted(by_dim.get(dim, []), key=lambda r: r.get("rule_id", ""))
        for r in drules:
            mut = {"padding": "9px 10px", "fontSize": "11px", "color": MUTED}
            rows.append(html.Div([
                html.Span(r.get("rule_id", ""),
                          style={"width": "92px", "padding": "9px 10px",
                                 "fontSize": "12px", "fontWeight": "800",
                                 "fontFamily": "monospace", "color": TEXT}),
                html.Div(_dim_pill(dim), style={"width": "120px", "padding": "9px 10px"}),
                html.Div([
                    html.Div(r.get("rule_name", ""),
                             style={"fontSize": "12px", "color": TEXT}),
                    html.Div(r.get("category", ""),
                             style={"fontSize": "10px", "color": MUTED, "marginTop": "2px"}),
                ], style={"flex": "3", "padding": "9px 10px"}),
                html.Span(r.get("tables", ""), style={**mut, "flex": "2"}),
                html.Span(r.get("fields", ""), style={**mut, "flex": "2"}),
            ], style={"display": "flex", "alignItems": "center",
                      "borderBottom": f"1px solid {DIVIDER}"}))

    return html.Div([
        html.Div("ALL RULES CURRENTLY CHECKED", style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginBottom": "8px"}),
        html.Div(rows, style={
            "background": CARD, "borderRadius": "8px",
            "border": f"1px solid {DIVIDER}", "overflow": "hidden"}),
    ], style={"marginBottom": "20px"})


def _validations_page() -> html.Div:
    builtin_rules = get_all_rules()
    n_tables  = len({t.strip() for r in builtin_rules
                     for t in (r.get("tables") or "").replace("→", ",").split(",")
                     if t.strip()})
    subtitle  = f"{len(builtin_rules)} rules across {n_tables} tables"

    return html.Div([
        # ── header: title + download ──────────────────────────────────────────
        html.Div([
            html.Div([
                html.Div("VALIDATION RULES", style={
                    "fontSize": "13px", "fontWeight": "900",
                    "color": TEXT, "letterSpacing": "0.04em", "lineHeight": "1.15",
                }),
                html.Div(subtitle,
                         style={"fontSize": "11px", "color": MUTED, "marginTop": "3px"}),
            ]),
            html.Div("Download CSV", id="rules-download-btn", n_clicks=0, style={
                "cursor": "pointer", "background": BRAND, "color": CARD,
                "fontSize": "12px", "fontWeight": "700", "padding": "8px 18px",
                "borderRadius": "6px", "userSelect": "none",
            }),
        ], style={
            "display": "flex", "alignItems": "center",
            "justifyContent": "space-between", "marginBottom": "16px",
        }),

        # ── chart + full rule list + download ─────────────────────────────────
        _rules_charts(builtin_rules, []),
        _rules_table(builtin_rules),
        dcc.Download(id="rules-download"),
    ])
