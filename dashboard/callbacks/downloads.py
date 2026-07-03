# Report/CR/issue download callbacks — moved from dq_dashboard_dash.py.
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

    from issues.repositories import get_issues

    issues = get_issues(le_book=le_book)

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
        f"{n_issues} issue{'s' if n_issues != 1 else ''} across "
        f"{tables} table{'s' if tables != 1 else ''} will be included in the report"
        if issues else "No open issues recorded — report will contain schema data only."
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


def _build_resolved_zip(le_books):
    """Build a ZIP of the EXACT resolved-issue rows (pulled from the report xlsx
    sheets) for the given institution(s). Returns zip bytes, or None if nothing
    matches. Shared by the BNR and institution resolved-download flows."""
    import zipfile, io as _io
    import pandas as pd
    from issues.repositories import get_issues
    from dq.rules.completeness import COMP_RULE_META

    lb_set = {str(x) for x in (le_books or [])}
    if not lb_set:
        return None
    resolved = [i for i in get_issues(status="resolved") if str(i["le_book"]) in lb_set]
    if not resolved:
        return None

    issue_reports_dir = _DIR / "issue_reports"
    comp_rule_ids     = set(COMP_RULE_META.keys())
    out_buf           = _io.BytesIO()
    found_any         = False

    # group by (le_book, detected month, table)
    groups: dict[tuple, list] = {}
    for iss in resolved:
        month = (iss.get("detected_at") or "")[:7]
        if month:
            groups.setdefault((str(iss["le_book"]), month, iss["table_name"]), []).append(iss)

    with zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as out_zf:
        for (le_book, month, table), issues in sorted(groups.items()):
            zip_path = issue_reports_dir / f"{le_book}_{month}.zip"
            if not zip_path.exists():
                continue
            try:
                with zipfile.ZipFile(zip_path) as src_zf:
                    if f"{table}.xlsx" not in src_zf.namelist():
                        continue
                    xls = pd.ExcelFile(_io.BytesIO(src_zf.read(f"{table}.xlsx")))
            except Exception:
                continue

            # one Excel sheet per issue: completeness → "Missing *" sheets;
            # dimension issues → sheet whose name starts with the rule_id.
            comp_present = any(i["rule_id"] in comp_rule_ids for i in issues)
            dim_issues   = [i for i in issues if i["rule_id"] not in comp_rule_ids]
            sheet_issue: dict[str, dict] = {}
            for sheet in xls.sheet_names:
                name = str(sheet)
                if comp_present and name.startswith("Missing "):
                    sheet_issue[sheet] = next(
                        (i for i in issues if i["rule_id"] in comp_rule_ids), {})
                else:
                    iss = next((i for i in dim_issues if name.startswith(i["rule_id"])), None)
                    if iss is not None:
                        sheet_issue[sheet] = iss
            if not sheet_issue:
                continue

            frames = []
            for sheet, iss in sheet_issue.items():
                try:
                    sdf = xls.parse(sheet)
                except Exception:
                    continue
                if sdf.empty:
                    continue
                sdf["issue_type"]   = sheet
                sdf["resolved_at"]  = iss.get("resolved_at", "")
                sdf["sla_deadline"] = iss.get("sla_deadline", "")
                frames.append(sdf)
            if not frames:
                continue

            filtered = pd.concat(frames, ignore_index=True)
            filtered["on_time"] = filtered.apply(
                lambda r: ("On Time" if r["resolved_at"] and r["sla_deadline"]
                           and r["resolved_at"] <= r["sla_deadline"]
                           else "Late" if r["resolved_at"] and r["sla_deadline"] else ""),
                axis=1)
            out_zf.writestr(f"{table}_{le_book}_{month}_resolved.csv",
                            filtered.to_csv(index=False, encoding="utf-8-sig"))
            found_any = True

    return out_buf.getvalue() if found_any else None


def _stage_resolved_download(zip_bytes: bytes, lb_label: str) -> str:
    """Write resolved-zip bytes to a temp file and return the streaming route URL."""
    import re as _re, uuid as _uuid, time as _time
    tmp_dir = _DIR / "issue_reports" / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for _f in tmp_dir.glob("*.zip"):          # sweep orphaned temp downloads (>10 min)
        try:
            if _time.time() - _f.stat().st_mtime > 600:
                _f.unlink()
        except OSError:
            pass
    if not _re.fullmatch(r"[A-Za-z0-9_-]{1,20}", str(lb_label or "")):
        lb_label = "issues"
    token = _uuid.uuid4().hex
    (tmp_dir / f"{token}.zip").write_bytes(zip_bytes)
    return f"/download/resolved/{lb_label}/{token}"


@app.callback(
    Output("dl-nav", "href", allow_duplicate=True),
    Input("resolved-dl-lb", "data"),
    prevent_initial_call=True,
)
def _resolved_dl_generate(le_book):
    if not le_book:
        raise dash.exceptions.PreventUpdate
    zb = _build_resolved_zip([le_book])
    if not zb:
        raise dash.exceptions.PreventUpdate
    return _stage_resolved_download(zb, str(le_book))


@app.callback(
    Output("dl-nav", "href", allow_duplicate=True),
    Input({"type": "cr-tbl-dl-btn", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _cr_table_dl(clicks):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "cr-tbl-dl-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    raw = tid["index"]
    if "|" not in raw:
        raise dash.exceptions.PreventUpdate
    le_book, table_name = raw.split("|", 1)

    issue_reports_dir = _DIR / "issue_reports"
    zips = (sorted([z for z in issue_reports_dir.glob(f"{le_book}_*.zip")
                    if not z.name.endswith("_resolved.zip")], reverse=True)
            if issue_reports_dir.exists() else [])
    if not zips:
        raise dash.exceptions.PreventUpdate

    # stream the single {table}.xlsx via the Flask route (no base64 inlining)
    return f"/download/issue-report/{le_book}/{table_name}?t={ctx.triggered[0]['value']}"


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
