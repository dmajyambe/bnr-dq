# Validations tab (read-only list of active rules) — moved from dq_dashboard_dash.py.
from __future__ import annotations
from dash import dcc, html
import plotly.graph_objects as go
from dashboard.components import _dim_pill
from dashboard.theme import BG, BRAND, CARD, DIVIDER, FONT, MUTED, TEXT

def _get_rules_from_db() -> list[dict]:
    try:
        from storage.postgres.app_db import get_connection
        con = get_connection()
        rows = con.execute(
            "SELECT rule_id, dimension, category, rule_name, tables, fields "
            "FROM dq_rules ORDER BY dimension, rule_id"
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception:
        from dq.rules.registry import get_all_rules
        return get_all_rules()


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
        html.Div("", style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "textTransform": "uppercase", "letterSpacing": "0.06em", "marginBottom": "8px"}),
        html.Div(rows, style={
            "background": CARD, "borderRadius": "8px",
            "border": f"1px solid {DIVIDER}", "overflow": "hidden"}),
    ], style={"marginBottom": "20px"})


def _doc_accordion(title: str, children: list, open_: bool = False) -> html.Details:
    """A styled native-HTML accordion section — no callback needed."""
    S = lambda extra={}: {
        "fontFamily": "inherit",
        **extra,
    }
    summary_style = {
        "cursor":        "pointer",
        "listStyle":     "none",
        "display":       "flex",
        "alignItems":    "center",
        "justifyContent":"space-between",
        "padding":       "13px 18px",
        "fontSize":      "12px",
        "fontWeight":    "900",
        "color":         TEXT,
        "letterSpacing": "0.03em",
        "userSelect":    "none",
    }
    return html.Details(
        [
            html.Summary(
                [html.Span(title), html.Span("▸", style={"fontSize": "11px", "color": MUTED})],
                style=summary_style,
            ),
            html.Div(children, style={"padding": "4px 20px 18px"}),
        ],
        open=open_,
        style={
            "background":    CARD,
            "border":        f"1px solid {DIVIDER}",
            "borderRadius":  "8px",
            "marginBottom":  "8px",
        },
    )


def _doc_p(text: str) -> html.P:
    return html.P(text, style={"fontSize": "13px", "color": TEXT,
                                "lineHeight": "1.7", "margin": "8px 0"})


def _doc_h(text: str) -> html.Div:
    return html.Div(text, style={"fontSize": "12px", "fontWeight": "900",
                                  "color": BRAND, "margin": "14px 0 4px",
                                  "textTransform": "uppercase", "letterSpacing": "0.04em"})


def _doc_kv_table(rows: list[tuple[str, str]]) -> html.Table:
    def _td(val, bold=False):
        return html.Td(val, style={
            "padding": "7px 12px", "fontSize": "12px",
            "color": TEXT, "borderBottom": f"1px solid {DIVIDER}",
            "fontWeight": "700" if bold else "400",
            "verticalAlign": "top",
        })
    return html.Table(
        [html.Tr([_td(k, bold=True), _td(v)]) for k, v in rows],
        style={"width": "100%", "borderCollapse": "collapse",
               "background": BG, "borderRadius": "6px",
               "border": f"1px solid {DIVIDER}", "margin": "8px 0"},
    )


def _doc_status_table(rows: list[tuple[str, str]]) -> html.Table:
    hdr_style = {"padding": "7px 12px", "fontSize": "10px", "fontWeight": "900",
                 "color": MUTED, "textTransform": "uppercase",
                 "letterSpacing": "0.05em", "background": BG,
                 "borderBottom": f"2px solid {DIVIDER}"}

    def _td(val):
        return html.Td(val, style={"padding": "7px 12px", "fontSize": "12px",
                                    "color": TEXT, "borderBottom": f"1px solid {DIVIDER}",
                                    "verticalAlign": "top"})

    return html.Table(
        [html.Thead(html.Tr([
            html.Th("Status",  style=hdr_style),
            html.Th("Meaning", style=hdr_style),
        ]))] +
        [html.Tr([_td(s), _td(m)]) for s, m in rows],
        style={"width": "100%", "borderCollapse": "collapse",
               "background": CARD, "borderRadius": "6px",
               "border": f"1px solid {DIVIDER}", "margin": "8px 0"},
    )


def _documentation_section() -> html.Div:
    """Official BNR DQ programme documentation"""

    lifecycle_text = (
        "Detection Run (monthly)  →  Failing rows found for a rule at Institution X  →  "
        "Issue Created (Status: New, SLA: 30 days)  →  Urgency escalates: "
        "NEW (0–7 d) → ATTENTION (8–14 d) → URGENT (15–21 d) → CRITICAL (22–29 d) → OVERDUE (30+ d)  →  "
        "Inspector creates a Data Correction Request and assigns it to the institution  →  "
        "Institution corrects data and resubmits  →  "
        "Next run finds zero failing rows  →  PENDING RESOLUTION  →  "
        "Resolution scan re-checks  →  RESOLVED or stays OPEN"
    )

    return html.Div([
        html.Div(" ", style={
            "fontSize": "13px", "fontWeight": "900", "color": TEXT,
            "letterSpacing": "0.04em", "marginBottom": "14px",
        }),

        # 1. What is this system?
        _doc_accordion("1.  What Is This System?", [
            _doc_p(
                "The BNR Data Quality (DQ) Dashboard is a data quality tool used by the National Bank of Rwanda (BNR) "
                "and stakeholders to ensure the quality of data is maintained. Every month, data submitted to the EDWH "
                "(Electronic Data Warehouse) is automatically checked against stakeholder-verified data quality rules. "
                "Institutions that fail a rule are assigned an issue. The system tracks those issues, assigns "
                "remediation/data correction tasks, and confirms when the institution has corrected the data."
            ),
        ], open_=True),

        # 2. Key Terms
        _doc_accordion("2.  Key Terms and Definitions", [
            _doc_kv_table([
                ("Institution",       "A financial entity (bank, MFI, or SACCO) that submits data to the EDWH."),
                ("Reporting Period",  "The month for which an institution submitted its data (e.g., July 2026)."),
                ("Data Table",        "One of the datasets in the submission (e.g., Customers, Accounts, Contracts)."),
                ("Rule",              "A specific data quality check applied to one or more fields in a data table. Each rule has a unique ID (e.g., COMP-001, ACC-016)."),
                ("Dimension",         "A category that groups related rules by the type of quality problem they detect. There are five dimensions: Completeness, Accuracy, Validity, Uniqueness, and Timeliness."),
                ("Score",             "A percentage (0–100%) representing how well an institution's data passed the rules within a dimension. 100% means all records passed every rule."),
                ("Failing Row",       "A single record in a data table that violated a rule. For example, one customer record with a missing mandatory field."),
                ("Issue",             "A logged problem created when one or more failing rows are found for a specific rule at a specific institution. Issues have a 30-day SLA."),
                ("SLA",               "Service Level Agreement — the deadline for resolving an issue. All issues must be corrected within 30 days of detection (subject to change upon agreement with stakeholders)."),
                ("Urgency",           "How close an issue is to its SLA deadline. Escalates automatically: New → Attention → Urgent → Critical → Overdue."),
            ]),
        ]),

        # 3. Five dimensions
        _doc_accordion("3.  The Five Data Quality Dimensions", [
            _doc_h("Completeness"),
            _doc_p("All mandatory fields in a submitted record must contain a value. A field is 'incomplete' if it is blank or null when data is required."),
            _doc_h("Accuracy"),
            _doc_p(
                "Data values must be correct, consistent, and internally coherent. This also includes referential integrity "
                "rules — checks between more than one table. For example: every account in the Accounts table must have a "
                "corresponding customer in the Customers table. Referential integrity scores are averaged with the other "
                "accuracy scores to produce a single Accuracy dimension score."
            ),
            _doc_h("Validity"),
            _doc_p("Data values must be in the correct format and within acceptable ranges, and must make logical sense when considered against other fields or business rules."),
            _doc_h("Uniqueness"),
            _doc_p(
                "Records that should be unique must not appear more than once. This covers both duplicate rows within a "
                "single submission and records that are repeated unchanged across two consecutive reporting periods when "
                "changes would be expected."
            ),
            _doc_h("Timeliness"),
            _doc_p(
                "Data must reflect up-to-date account activity. An account marked as active must have had at least one "
                "transaction within the past 180 days. Accounts with no transaction activity for 180 days or more are flagged."
            ),
        ]),

        # 4. Scores
        _doc_accordion("4.  Quality Scores Explained", [
            _doc_p("Each institution receives a score from 0% to 100% for each of the five dimensions, calculated every month when the detection pipeline runs."),
            _doc_h("How Scores Are Calculated"),
            _doc_kv_table([
                ("Per-rule score",    "(Passing rows ÷ total rows) × 100 — calculated for every rule run against a table."),
                ("Per-dimension score","Scores for all rules within a dimension are averaged across all relevant tables for that institution."),
                ("Accuracy score",    "Referential integrity rules (REL-001 to REL-008) are averaged together with other Accuracy rule scores to produce one Accuracy dimension score."),
                ("Green (≥ 95%)",     "Acceptable"),
                ("Amber (80–94%)",    "Needs attention"),
                ("Red (< 80%)",       "Critical issue"),
            ]),
        ]),

        # 5. Issue lifecycle
        _doc_accordion("5.  The Issue Lifecycle", [
            _doc_p(lifecycle_text),
            html.Div([
                html.Span("Two-pass resolution:", style={"fontWeight": "700", "color": TEXT}),
                html.Span(
                    " An issue is only closed after two independent clean checks to prevent false closures "
                    "caused by mid-upload data snapshots.",
                    style={"color": TEXT},
                ),
            ], style={"fontSize": "13px", "lineHeight": "1.7", "margin": "10px 0",
                      "background": BG, "padding": "10px 14px", "borderRadius": "6px",
                      "border": f"1px solid {DIVIDER}"}),
            _doc_p(
                "Recurrence: If the same issue reappears within 30 days of being resolved, it is reopened with a "
                "recurrence counter (shown as ↺ 2, ↺ 3, etc.), signalling a pattern of persistent non-compliance."
            ),
        ]),

        # 6. Data profiling
        _doc_accordion("6.  Data Profiling", [
            _doc_p(
                "The Data Profiling tab provides column-level statistics for every institution and table "
                "in the EDWH. Statistics are computed once per monthly pipeline run and the last three runs are retained, "
                "allowing month-over-month comparison."
            ),
            _doc_h("Statistics Shown Per Column"),
            _doc_kv_table([
                ("% Null / # Null",     "Percentage and count of records where this field is empty (NULL). Red ≥ 20%, amber ≥ 5%, green < 5%."),
                ("% Distinct / # Distinct", "Percentage and count of unique values in this column — a proxy for cardinality."),
                ("Min / Max",           "Lowest and highest values present in the column for the selected run date."),
                ("Top Values",          "Up to five most frequent values with their record counts. Suppressed for high-cardinality columns (> 1,000 distinct values)."),
            ]),
            _doc_h("Selecting a Run Date"),
            _doc_p(
                "Use the Run Date dropdown to compare profiling results across the last three monthly runs. "
                "If no run date is available for a table, profiling has not yet been executed — run the profiling "
                "pipeline after the monthly detection completes."
            ),
            _doc_h("BNR Inspector View"),
            _doc_p("Inspectors can select any institution and any table from the filter bar to see that institution's column-level statistics."),
            _doc_h("Institution Portal View"),
            _doc_p("Institution users see the same statistics scoped to their own institution only. The institution filter is not shown — it is applied automatically from their login credentials."),
        ]),

        # 7. Dashboard guide
        _doc_accordion("7.  Dashboard User Guide — BNR Inspector", [
            _doc_h("Data Profiling Tab"),
            _doc_p("The first tab. Select an institution and a table to view column-level completeness, cardinality, and value distribution statistics. Use the Run Date dropdown to compare across the last three pipeline runs. See Section 6 for full details."),
            _doc_h("Home — Category View"),
            _doc_p("The home page shows all institutions grouped by category: B (Commercial Banks), MF (Microfinance Institutions), OSACCO / SACCO (Savings and Credit Cooperatives). Click a category card to see its institutions and scores."),
            _doc_h("Category / Institution View"),
            _doc_p("After selecting a category or institution: five KPI tiles show the current score per dimension. The Institution Reports Table lists available issue evidence packages — each row shows the institution name, tables with issues, and a Download ZIP button. The ZIP contains Excel files of the actual failing rows, broken down by table and rule."),
            _doc_h("Data Correction Tab"),
            _doc_p("Used to create and manage Data Correction Requests. See Section 9 for the full workflow."),
            _doc_h("Issues Tab"),
            _doc_p("Contains resolved issues and is the primary reference before approving a data correction submission by institutions."),
            _doc_h("Documentation Tab"),
            _doc_p("This tab — visible to all logged-in users — contains the full list of rules being checked and this programme documentation."),
        ]),

        # 8. Portal guide
        _doc_accordion("8.  Portal User Guide — Institution Staff", [
            _doc_h("Data Profile Tab"),
            _doc_p("The first tab. Shows column-level statistics for the institution's own data — null rates, distinct value counts, min/max, and top values per column and table. Use the Run Date dropdown to review and compare the last three monthly snapshots. See Section 6 for full details."),
            _doc_h("Dashboard Page"),
            _doc_p("Shows the five dimension scores scoped to the institution and the last seven (7) runs per dimension."),
            _doc_h("Issues Page (My Issues)"),
            _doc_p(
                "Lists all open issues for the institution. For each issue: which table and rule triggered it, "
                "how many records are failing, and the SLA deadline and urgency level. Each issue row has a button "
                "to download the CSV of failing records for that rule, so the institution's data team knows exactly "
                "which records need to be corrected. The page also shows resolved issues after data correction "
                "has been confirmed."
            ),
            _doc_h("Data Correction Page"),
            _doc_p(
                "Shows Data Correction Requests assigned to the institution. Institution users can: Start Work on a "
                "request (moves it from Open to In Progress), and Submit a corrected data request for review once "
                "corrections have been made. Institution users cannot create or approve Data Correction Requests — "
                "those actions belong to the BNR Inspector."
            ),
        ]),

        # 9. Remediation workflow
        _doc_accordion("9.  The Data Correction Workflow", [
            _doc_h("Step 1 — Inspector Creates a Data Correction Request"),
            _doc_p(
                "An inspector goes to the Data Correction tab, selects the institution, chooses the relevant issues "
                "from the issue list, and fills in: Title, Description, Assigned Officer, and Target Date. "
                "The request is saved and the institution officer is notified."
            ),
            _doc_h("Step 2 — Institution Starts Work"),
            _doc_p("The institution's user finds the Data Correction Request in their portal, clicks Start Work (status → In Progress), downloads the failing-row reports, corrects the data in their source systems, and prepares a resubmission."),
            _doc_h("Step 3 — Institution Submits for Review"),
            _doc_p("Once corrections are made and data is resubmitted, the institution clicks Submit. Status changes to Submitted and BNR is notified."),
            _doc_h("Step 4 — Inspector Reviews and Approves"),
            _doc_p("An inspector reviews the resubmission in the Data Correction tab. The inspector can Approve individual tables (if some are corrected and others are not) or Reject the entire request with written notes. When all tables are approved the request moves to Approved then Closed."),
            _doc_h("Status Reference"),
            _doc_status_table([
                ("Open",        "Request created by BNR; institution has not started work yet."),
                ("In Progress", "Institution has accepted the request and is correcting data."),
                ("Submitted",   "Institution has resubmitted corrected data and is awaiting BNR review."),
                ("Approved",    "BNR Inspector has confirmed the corrections are satisfactory."),
                ("Rejected",    "BNR Inspector has reviewed and found issues still outstanding; institution must re-correct."),
                ("Closed",      "All tables approved; data correction complete."),
            ]),
        ]),

        # 10. FAQ
        _doc_accordion("10.  Frequently Asked Questions", [
            _doc_h("How often does the system check data quality?"),
            _doc_p("The full detection pipeline runs monthly (subject to change depending on specific data needs). Score histories are updated to reflect any data corrections made between monthly runs."),
            _doc_h("What is the difference between a failing row and an issue?"),
            _doc_p("A failing row is a single data record that violated a rule. An issue is the aggregated problem logged for a specific rule at a specific institution — it groups all failing rows for that rule into one trackable item with an SLA and urgency level. One issue can represent hundreds or thousands of failing rows."),
            _doc_h("What does 'Pending Resolution' mean?"),
            _doc_p("After a clean detection run (no failing rows found), the issue is set to Pending Resolution rather than closed immediately. The system runs a second independent check to confirm the data is genuinely clean before fully closing the issue. This prevents false closures caused by partial or mid-upload data snapshots."),
            _doc_h("Can an institution user see other institutions' data?"),
            _doc_p("No. The institution portal enforces strict data isolation — each institution user can only see data for their own institution. BNR Inspectors in the main dashboard can see all institutions."),
        ]),

    ], style={"marginBottom": "32px"})


def _validations_page() -> html.Div:
    builtin_rules = _get_rules_from_db()
    n_tables  = len({t.strip() for r in builtin_rules
                     for t in (r.get("tables") or "").replace("→", ",").split(",")
                     if t.strip()})
    subtitle  = f"{len(builtin_rules)} rules across {n_tables} tables"

    _tab_style = {
        "padding":       "9px 28px",
        "fontSize":      "12px",
        "fontWeight":    "700",
        "color":         MUTED,
        "background":    CARD,
        "border":        f"1px solid {DIVIDER}",
        "borderRadius":  "6px",
        "cursor":        "pointer",
        "letterSpacing": "0.04em",
        "marginRight":   "8px",
        "userSelect":    "none",
    }
    _tab_selected = {
        **_tab_style,
        "color":         CARD,
        "background":    BRAND,
        "border":        f"1px solid {BRAND}",
        "borderBottom":  f"1px solid {BRAND}",
    }

    rules_content = html.Div([
        html.Div([
            html.Div([
                html.Div("", style={
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
        _rules_table(builtin_rules),
    ])

    return html.Div([
        dcc.Tabs(
            value="docs",
            children=[
                dcc.Tab(
                    label="Documentation",
                    value="docs",
                    children=html.Div(_documentation_section(),
                                      style={"paddingTop": "24px"}),
                    style=_tab_style,
                    selected_style=_tab_selected,
                ),
                dcc.Tab(
                    label="Rule Reference",
                    value="rules",
                    children=html.Div(rules_content,
                                      style={"paddingTop": "24px"}),
                    style=_tab_style,
                    selected_style=_tab_selected,
                ),
            ],
            colors={"border": "transparent", "primary": BRAND, "background": "transparent"},
            style={"borderBottom": "none"},
            content_style={"borderTop": f"1px solid {DIVIDER}", "paddingTop": "0"},
        ),
        dcc.Download(id="rules-download"),
    ])
