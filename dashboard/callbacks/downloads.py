# Report/CR/issue download callbacks
from __future__ import annotations

import csv
import io
import json

import dash
from dash import Input, Output, State, ctx, dcc, html

from dashboard.app import app
from dashboard.data import CATEGORIES_FILE, REPORTS_DIR, _DIR
from dashboard.theme import BRAND, DIVIDER, MUTED, TEXT
from dq.rules.registry import get_all_rules


@app.callback(
    Output("dl-preview-lb",    "data"),
    Output("dl-preview-modal", "style"),
    Input({"type": "inst-dl-btn", "index": dash.ALL}, "n_clicks"),
    Input("dl-modal-cancel", "n_clicks"),
    prevent_initial_call=True,
)
def _on_inst_dl_btn(n_clicks, _cancel):
    tid = ctx.triggered_id
    # close modal
    if tid == "dl-modal-cancel":
        return None, {"display": "none"}
    # open modal
    if not isinstance(tid, dict) or "index" not in tid:
        raise dash.exceptions.PreventUpdate
    if not any(n for n in (n_clicks or []) if n):
        raise dash.exceptions.PreventUpdate
    return tid["index"], {"display": "block"}


@app.callback(
    Output("dl-modal-title",    "children"),
    Output("dl-modal-subtitle", "children"),
    Output("dl-modal-table",    "children"),
    Input("dl-preview-lb", "data"),
    prevent_initial_call=True,
)
def _populate_dl_modal(le_book):
    if not le_book:
        raise dash.exceptions.PreventUpdate

    from datetime import datetime as _datetime
    from dashboard.data import latest_run_month
    from issues.repositories import get_issues

    this_month  = latest_run_month()
    month_label = _datetime.strptime(this_month, "%Y-%m").strftime("%B %Y")

    all_issues = get_issues(le_book=le_book)
    issues     = [i for i in all_issues
                  if (i.get("detected_at") or "").startswith(this_month)]

    # institution name from categories JSON
    try:
        cats = json.loads(CATEGORIES_FILE.read_text())
        info = cats.get(str(le_book), cats.get(le_book, {}))
        inst_name = (info.get("name") or str(le_book)).title()
    except Exception:
        inst_name = str(le_book)

    title    = f"{inst_name}  ({le_book})"
    n_issues = len(issues)
    tables   = len({i.get("table_name", "") for i in issues})
    subtitle = (
        f"{n_issues} new issue{'s' if n_issues != 1 else ''} across "
        f"{tables} table{'s' if tables != 1 else ''} detected in {month_label}"
        if issues else f"No new issues detected in {month_label} — report will contain schema data only."
    )

    # ── table header ─────────────────────────────────────────────────────────
    _TH = {
        "padding": "8px 14px", "fontSize": "11px", "fontWeight": "700",
        "color": "#FFFFFF", "textTransform": "uppercase",
        "letterSpacing": "0.05em", "whiteSpace": "nowrap",
        "background": BRAND,
    }
    header = html.Div([
        html.Span("Table",        style={**_TH, "flex": "1.4"}),
        html.Span("Rule",         style={**_TH, "flex": "0.9"}),
        html.Span("Issue",        style={**_TH, "flex": "3"}),
        html.Span("Dimension",    style={**_TH, "flex": "1"}),
        html.Span("Failing Rows", style={**_TH, "flex": "0.8", "textAlign": "right"}),
        html.Span("Status",       style={**_TH, "flex": "0.7", "textAlign": "center"}),
    ], style={"display": "flex", "position": "sticky", "top": "0", "zIndex": "1"})

    # ── urgency colour map ────────────────────────────────────────────────────
    _URGENCY_CLR = {
        "overdue":  "#7C3D1E", "critical": "#A0784A",
        "urgent":   "#B8860B", "attention": "#68686f",
        "new":      "#68686f",
    }
    _STATUS_BG = {
        "open": "rgba(117,57,24,.10)", "penalized": "rgba(124,61,30,.15)",
        "resolved": "rgba(40,160,80,.10)",
    }

    rows = []
    for i, iss in enumerate(issues):
        bg  = "rgba(244,246,249,0.7)" if i % 2 == 0 else "#FFFFFF"
        st  = (iss.get("status") or "open").lower()
        urg = (iss.get("urgency_band") or "new").lower()
        _TD = {
            "padding": "8px 14px", "fontSize": "12px",
            "color": TEXT, "display": "flex", "alignItems": "center",
        }
        rows.append(html.Div([
            html.Span(iss.get("table_name", "—"),
                      style={**_TD, "flex": "1.4", "fontFamily": "monospace",
                             "fontSize": "11px"}),
            html.Span(iss.get("rule_id", "—"),
                      style={**_TD, "flex": "0.9", "fontFamily": "monospace",
                             "fontSize": "11px", "color": BRAND,
                             "fontWeight": "700"}),
            html.Span(iss.get("rule_name") or iss.get("dimension", "—"),
                      style={**_TD, "flex": "3", "color": MUTED}),
            html.Span((iss.get("dimension") or "").title(),
                      style={**_TD, "flex": "1", "fontSize": "11px"}),
            html.Span(f"{iss.get('failing_rows', '—'):,}" if isinstance(
                          iss.get('failing_rows'), int) else "—",
                      style={**_TD, "flex": "0.8", "textAlign": "right",
                             "justifyContent": "flex-end", "fontWeight": "700",
                             "color": _URGENCY_CLR.get(urg, TEXT)}),
            html.Div(st.title(), style={
                **_TD, "flex": "0.7", "justifyContent": "center",
                "fontSize": "10px", "fontWeight": "700",
                "borderRadius": "4px",
                "background": _STATUS_BG.get(st, "transparent"),
                "color": _URGENCY_CLR.get(urg, MUTED),
            }),
        ], style={
            "display": "flex", "background": bg,
            "borderBottom": f"1px solid {DIVIDER}",
        }))

    if not rows:
        rows = [html.Div("No issues on record for this institution.",
                         style={"padding": "18px 14px", "fontSize": "13px",
                                "color": MUTED, "textAlign": "center"})]

    table = html.Div([header] + rows)
    return title, subtitle, table


@app.callback(
    Output("inst-download", "data"),
    Output("dl-nav", "href", allow_duplicate=True),
    Input("dl-modal-confirm", "n_clicks"),
    State("dl-preview-lb", "data"),
    prevent_initial_call=True,
)
def _on_inst_download_confirm(n_clicks, le_book):
    """
    Download the latest issue report for this institution.
    Preference order:
      1. issue_reports/{le_book}_{YYYY-MM}.zip  (monthly detection ZIP, via /download route)
      2. reports/ CSV bundle (legacy pipeline CSVs)
      3. reports/ XLSX fallback
    """
    if not n_clicks or not le_book:
        raise dash.exceptions.PreventUpdate

    # ── 1. monthly detection issue ZIP — streamed via Flask route (any size) ──
    issue_reports_dir = _DIR / "issue_reports"
    if issue_reports_dir.exists() and sorted(issue_reports_dir.glob(f"{le_book}_*.zip")):
        return dash.no_update, f"/download/issue-report/{le_book}?t={n_clicks}"

    # ── 2. legacy pipeline CSVs ───────────────────────────────────────────────
    if REPORTS_DIR.exists():
        import zipfile, io as _io
        csv_files = sorted(REPORTS_DIR.glob(f"*_{le_book}_*.csv"), reverse=True)
        if csv_files:
            latest_month = None
            for f in csv_files:
                parts = f.stem.rsplit("_", 2)
                if len(parts) == 3 and len(parts[2]) == 7:
                    m = parts[2]
                    if latest_month is None or m > latest_month:
                        latest_month = m
            month_files = [f for f in csv_files
                           if f.stem.endswith(f"_{latest_month}")]
            if len(month_files) == 1:
                return dcc.send_file(str(month_files[0])), dash.no_update
            if len(month_files) > 1:
                buf = _io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    for f in month_files:
                        zf.write(f, arcname=f.name)
                buf.seek(0)
                return (dcc.send_bytes(buf.read(),
                                       filename=f"dq_report_{le_book}_{latest_month}.zip"),
                        dash.no_update)

        # ── 3. XLSX fallback ──────────────────────────────────────────────────
        matches = sorted(REPORTS_DIR.glob(f"{le_book}_*.xlsx"), reverse=True)
        if matches:
            return dcc.send_file(str(matches[0])), dash.no_update

    raise dash.exceptions.PreventUpdate


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



@app.callback(
    Output("dl-nav",            "href",     allow_duplicate=True),
    Output("dl-no-report-toast", "children"),
    Output("dl-no-report-toast", "style"),
    Input("resolved-dl-lb", "data"),
    prevent_initial_call=True,
)
def _resolved_dl_generate(le_book):
    if not le_book:
        raise dash.exceptions.PreventUpdate
    issue_reports_dir = _DIR / "issue_reports"
    prebuilt = sorted(issue_reports_dir.glob(f"{le_book}_*_resolved.zip"),
                      reverse=True) if issue_reports_dir.exists() else []
    if prebuilt:
        return f"/download/resolved/{le_book}", [], {"display": "none"}
    # No pre-built file — pipeline hasn't run yet for this institution
    from dashboard.theme import CARD, DIVIDER, FONT, MUTED, TEXT
    toast_children = [
        html.Div("No resolved report yet", style={
            "fontWeight": "700", "fontSize": "13px", "color": TEXT,
            "marginBottom": "4px",
        }),
        html.Div("The resolved-issues file is built automatically after the next "
                 "resolution scan. Check back after the pipeline runs.",
                 style={"fontSize": "12px", "color": MUTED, "lineHeight": "1.5"}),
    ]
    toast_style = {
        "position": "fixed", "bottom": "24px", "right": "24px",
        "zIndex": "999", "background": CARD,
        "border": f"1px solid {DIVIDER}", "borderRadius": "8px",
        "padding": "14px 18px", "boxShadow": "0 4px 20px rgba(28,28,39,0.18)",
        "fontFamily": FONT, "maxWidth": "320px",
        "display": "block",
    }
    return dash.no_update, toast_children, toast_style


@app.callback(
    Output("dl-nav", "href", allow_duplicate=True),
    Input({"type": "inst-tbl-resolved-dl-btn", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _inst_resolved_table_dl(clicks):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "inst-tbl-resolved-dl-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate
    raw = tid["index"]
    parts = raw.split("|")
    if len(parts) < 2:
        raise dash.exceptions.PreventUpdate
    le_book, table_name = parts[0], parts[1]
    month = parts[2] if len(parts) > 2 else ""
    issue_reports_dir = _DIR / "issue_reports"
    has_resolved = any(True for _ in issue_reports_dir.glob(f"{le_book}_*_resolved.zip")) \
        if issue_reports_dir.exists() else False
    if not has_resolved:
        raise dash.exceptions.PreventUpdate
    month_param = f"&month={month}" if month else ""
    return f"/download/resolved/{le_book}/{table_name}?t={ctx.triggered[0]['value']}{month_param}"



@app.callback(
    Output("dl-nav", "href", allow_duplicate=True),
    Input({"type": "open-issue-dl-btn", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _on_open_issue_dl(clicks):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "open-issue-dl-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    lb = tid["index"]
    issue_reports_dir = _DIR / "issue_reports"
    if issue_reports_dir.exists() and sorted(issue_reports_dir.glob(f"{lb}_*.zip")):
        # stream via the Flask route (handles 100 MB+ reports; no base64 inlining).
        # ?t= makes the href change each click so dcc.Location re-navigates.
        return f"/download/issue-report/{lb}?t={ctx.triggered[0]['value']}"

    raise dash.exceptions.PreventUpdate
