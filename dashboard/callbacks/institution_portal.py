# Institution-portal callbacks (BNR-file-owned, render inst pages' interactive
# bits) — moved from dq_dashboard_dash.py.
from __future__ import annotations

import dash
from dash import ALL, Input, Output, State, ctx, html

from dashboard.app import app
from dashboard.components import _empty_state
from dashboard.data import REPORTS_DIR, _DIR
from dashboard.pages.remediation import _build_cr_list
from dashboard.theme import (
    BG, BRAND, CARD, DIVIDER, MUTED, RESOLVED_GREEN_BG,
    TABLE_NAMES_PRETTY, TEXT, C_GREEN, C_RED, _URGENCY_COLORS,
)


@app.callback(
    Output("active-page", "data", allow_duplicate=True),
    Input({"type": "inst-nav-tab", "index": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def _inst_nav_click(clicks):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if isinstance(tid, dict):
        return tid["index"]
    raise dash.exceptions.PreventUpdate


@app.callback(
    Output("inst-issue-list", "children"),
    Input("inst-issue-filter",  "value"),
    Input("inst-table-filter",  "value"),
    Input("notif-poll",         "n_intervals"),
    State("auth-store", "data"),
    prevent_initial_call=False,
)
def _inst_issue_list(status, table_filter, _poll, auth_data):
    from issues.repositories import get_issues
    from issues.queries import get_issues_by_table

    le_books = set(str(lb) for lb in (auth_data or {}).get("le_books", []))
    le_book  = next(iter(le_books), None)
    status   = status or "open"

    if status == "resolved":
        from collections import defaultdict, Counter
        from datetime import datetime as _datetime
        issues = [i for i in get_issues(status="resolved") if i["le_book"] in le_books]
        if not issues:
            return _empty_state(
                "No resolved issues",
                "Resolved issues will appear here once fixes are confirmed.",
                icon="✓")

        # group by table
        by_table: dict[str, list] = defaultdict(list)
        for iss in issues:
            by_table[iss["table_name"]].append(iss)

        # scan resolved ZIPs to find which tables have a downloadable XLSX
        import zipfile as _zf
        _issue_dir = _DIR / "issue_reports"
        resolved_tables: set[str] = set()
        for zp in (_issue_dir.glob(f"{le_book}_*_resolved.zip") if _issue_dir.exists() else []):
            try:
                with _zf.ZipFile(zp) as _z:
                    for m in _z.namelist():
                        if m.endswith(".xlsx"):
                            resolved_tables.add(m[:-5])
            except Exception:
                pass

        _H = {"fontSize": "10px", "fontWeight": "900", "color": MUTED,
              "textTransform": "uppercase", "letterSpacing": "0.05em",
              "padding": "7px 14px", "whiteSpace": "nowrap"}
        hdr = html.Div([
            html.Span("Table",        style={**_H, "flex": "1"}),
            html.Span("Resolved",     style={**_H, "width": "80px",  "textAlign": "center"}),
            html.Span("Rows Fixed",   style={**_H, "width": "110px", "textAlign": "right"}),
            html.Span("Report Month", style={**_H, "width": "120px"}),
            html.Span("Resolved On",  style={**_H, "width": "100px"}),
            html.Span("Download",     style={**_H, "width": "90px",  "textAlign": "center"}),
        ], style={"display": "flex", "background": BG,
                  "borderRadius": "8px 8px 0 0",
                  "borderBottom": f"2px solid {DIVIDER}"})

        rows = [hdr]
        for i, (table, t_issues) in enumerate(
            sorted(by_table.items(),
                   key=lambda kv: max((x.get("resolved_at") or "") for x in kv[1]),
                   reverse=True)
        ):
            # rows fixed: sum last_failing_rows (pre-resolution count), fall back to failing_rows
            rows_fixed = sum(
                (iss.get("last_failing_rows") or iss.get("failing_rows") or 0)
                for iss in t_issues
            )

            # report month: most common detection month across this table's issues
            top_month  = ""
            det_months = [(iss.get("detected_at") or "")[:7]
                          for iss in t_issues if (iss.get("detected_at") or "")[:7]]
            if det_months:
                top_month = Counter(det_months).most_common(1)[0][0]
                try:
                    report_month = _datetime.strptime(top_month, "%Y-%m").strftime("%B %Y")
                except Exception:
                    report_month = top_month
            else:
                report_month = "—"

            resolved_on = max((x.get("resolved_at") or "") for x in t_issues)
            tbl_label   = TABLE_NAMES_PRETTY.get(table, table.replace("_", " ").title())
            bg          = "rgba(22,163,74,0.15)" if i % 2 == 0 else RESOLVED_GREEN_BG

            if le_book and table in resolved_tables:
                dl_btn = html.Div(
                    "⬇ xlsx",
                    id={"type": "inst-tbl-resolved-dl-btn",
                        "index": f"{le_book}|{table}|{top_month}"},
                    n_clicks=0,
                    title=f"Download resolved report for {tbl_label} ({report_month})",
                    style={
                        "cursor": "pointer", "background": BRAND, "color": CARD,
                        "fontSize": "10px", "fontWeight": "700",
                        "padding": "4px 10px", "borderRadius": "4px",
                        "userSelect": "none", "whiteSpace": "nowrap",
                    },
                )
            else:
                dl_btn = html.Span("—", style={"color": MUTED, "fontSize": "12px",
                                               "textAlign": "center", "display": "block"},
                                   title="Report not yet available for this table")

            rows.append(html.Div([
                html.Div([
                    html.Span("✓ ", style={"color": C_GREEN, "fontSize": "11px",
                                           "marginRight": "6px"}),
                    html.Span(tbl_label, style={"fontSize": "13px", "fontWeight": "700",
                                                "color": TEXT}),
                ], style={"flex": "1", "display": "flex", "alignItems": "center",
                          "padding": "10px 14px",
                          "borderLeft": f"3px solid {C_GREEN}"}),
                html.Span(str(len(t_issues)),
                          style={"width": "80px", "textAlign": "center",
                                 "fontSize": "13px", "fontWeight": "700",
                                 "color": C_GREEN, "padding": "10px 0"}),
                html.Span(f"{rows_fixed:,}" if rows_fixed else "—",
                          style={"width": "110px", "textAlign": "right",
                                 "fontSize": "13px", "fontWeight": "700",
                                 "color": C_GREEN, "padding": "10px 14px"}),
                html.Span(report_month,
                          style={"width": "120px", "fontSize": "12px", "fontWeight": "700",
                                 "color": TEXT, "padding": "10px 14px"}),
                html.Span(resolved_on,
                          style={"width": "100px", "fontSize": "11px",
                                 "color": MUTED, "padding": "10px 14px"}),
                html.Div(dl_btn,
                         style={"width": "90px", "display": "flex",
                                "justifyContent": "center", "alignItems": "center",
                                "padding": "10px 0"}),
            ], style={
                "display": "flex", "alignItems": "center",
                "background": bg, "borderBottom": f"1px solid {DIVIDER}",
            }))

        return html.Div(
            rows,
            style={"background": CARD, "borderRadius": "8px",
                   "border": f"1px solid {DIVIDER}"},
        )

    # ── open: one row per affected table ──────────────────────────────────────
    from datetime import datetime as _datetime
    from dashboard.data import latest_run_month
    this_month  = latest_run_month()
    month_label = _datetime.strptime(this_month, "%Y-%m").strftime("%B %Y")

    all_by_table = get_issues_by_table(status="open")

    # apply table filter
    selected_tables = set(table_filter) if table_filter else set()

    # aggregate per table for this institution — current month issues only
    table_summary: list[dict] = []
    for table, rules in all_by_table.items():
        if selected_tables and table not in selected_tables:
            continue
        total_rows      = 0
        last_total_rows = 0
        n_rules         = 0
        earliest_dl     = None
        for rule in rules:
            my_insts = [i for i in rule["institutions"]
                        if i["le_book"] in le_books
                        and (i.get("detected_at") or "").startswith(this_month)]
            if not my_insts:
                continue
            total_rows      += sum(i["failing_rows"] for i in my_insts)
            last_total_rows += sum(i.get("last_failing_rows") or i["failing_rows"]
                                   for i in my_insts)
            n_rules += 1
            for inst in my_insts:
                dl = inst.get("sla_deadline")
                if dl and (earliest_dl is None or dl < earliest_dl):
                    earliest_dl = dl
        if n_rules:
            table_summary.append({
                "table":           table,
                "label":           TABLE_NAMES_PRETTY.get(table, table.replace("_", " ").title()),
                "n_rules":         n_rules,
                "total_rows":      total_rows,
                "last_total_rows": last_total_rows,
                "deadline":        earliest_dl or "—",
            })

    if not table_summary:
        return _empty_state(
            f"No new issues in {month_label}",
            "No new data quality issues were detected this month for your institution.",
            icon="✓")

    # sort by most failing rows
    table_summary.sort(key=lambda r: -r["total_rows"])

    # ── find the latest institution report file (mirrors _on_inst_download_confirm order) ──
    rpt_file  = None
    rpt_date  = "—"
    if le_book:
        _issue_dir = _DIR / "issue_reports"
        if _issue_dir.exists():
            _zips = sorted([z for z in _issue_dir.glob(f"{le_book}_*.zip")
                            if not z.name.endswith("_resolved.zip")], reverse=True)
            if _zips:
                rpt_file = _zips[0]
                rpt_date = rpt_file.stem.split("_", 1)[-1]  # YYYY-MM
        if rpt_file is None and REPORTS_DIR.exists():
            _csvs = sorted(REPORTS_DIR.glob(f"*_{le_book}_*.csv"), reverse=True)
            if _csvs:
                rpt_file = _csvs[0]
                parts    = rpt_file.stem.rsplit("_", 2)
                rpt_date = parts[2] if len(parts) == 3 and len(parts[2]) == 7 else "—"
            if rpt_file is None:
                _xlsxs = sorted(REPORTS_DIR.glob(f"{le_book}_*.xlsx"), reverse=True)
                if _xlsxs:
                    rpt_file = _xlsxs[0]
                    stem     = rpt_file.stem.rsplit("_", 1)
                    rpt_date = stem[-1] if len(stem) == 2 and len(stem[-1]) == 10 else "—"

    def _failing_rows_cell(current: int, previous: int) -> html.Div:
        _base = {"width": "110px", "textAlign": "right", "padding": "10px 14px",
                 "display": "flex", "alignItems": "center", "justifyContent": "flex-end"}
        if previous and previous != current:
            return html.Div([
                html.Span(f"{previous:,}", style={"textDecoration": "line-through",
                                                   "color": MUTED, "fontSize": "10px",
                                                   "marginRight": "4px"}),
                html.Span(f"{current:,}", style={"fontWeight": "700", "color": C_RED,
                                                  "fontSize": "13px"}),
            ], style=_base)
        return html.Div(
            html.Span(f"{current:,}", style={"fontWeight": "700", "color": C_RED,
                                              "fontSize": "13px"}),
            style=_base,
        )

    # ── table header ──────────────────────────────────────────────────────────
    _H = {"fontSize": "10px", "fontWeight": "900", "color": MUTED,
          "textTransform": "uppercase", "letterSpacing": "0.05em",
          "padding": "7px 14px", "whiteSpace": "nowrap"}
    hdr = html.Div([
        html.Span("Table",        style={**_H, "flex": "1"}),
        html.Span("Issues",       style={**_H, "width": "68px", "textAlign": "center"}),
        html.Span("Failing Rows", style={**_H, "width": "110px", "textAlign": "right"}),
        html.Span("Detected",     style={**_H, "width": "90px", "textAlign": "center"}),
        html.Span("Deadline", style={**_H, "width": "100px"}),
        html.Span("Report",       style={**_H, "width": "80px", "textAlign": "center"}),
    ], style={"display": "flex", "background": BG,
              "borderRadius": "8px 8px 0 0",
              "borderBottom": f"2px solid {DIVIDER}"})

    _new_col = _URGENCY_COLORS.get("new", MUTED)
    rows = []
    for i, row in enumerate(table_summary):
        bg = "rgba(244,246,249,0.7)" if i % 2 == 0 else CARD

        # month chip
        urg_chip = html.Span(
            month_label,
            style={"fontSize": "10px", "fontWeight": "700", "color": _new_col,
                   "background": f"rgba({','.join(str(int(_new_col.lstrip('#')[j:j+2],16)) for j in (0,2,4))},.10)",
                   "border": f"1px solid {_new_col}",
                   "borderRadius": "4px", "padding": "2px 7px",
                   "whiteSpace": "nowrap"},
        )

        # download icon — table-specific xlsx pulled from the institution's report zip
        if rpt_file:
            dl_icon = html.Div(
                "⬇",
                id={"type": "inst-tbl-dl-btn", "index": f"{le_book}|{row['table']}"},
                n_clicks=0,
                title=f"Download {row['label']} report ({rpt_date})",
                style={"fontSize": "16px", "color": BRAND,
                       "cursor": "pointer", "textAlign": "center",
                       "userSelect": "none"},
            )
        else:
            dl_icon = html.Span("—", style={"color": MUTED, "fontSize": "12px",
                                             "textAlign": "center", "display": "block"})

        rows.append(html.Div([
            html.Div([
                html.Span("●", style={"color": _new_col, "fontSize": "9px",
                                       "marginRight": "8px"}),
                html.Span(row["label"], style={"fontSize": "13px", "fontWeight": "700",
                                                "color": TEXT}),
            ], style={"flex": "1", "display": "flex", "alignItems": "center",
                      "padding": "10px 14px",
                      "borderLeft": f"3px solid {_new_col}"}),
            html.Span(
                str(row["n_rules"]),
                style={"width": "68px", "textAlign": "center",
                       "fontSize": "13px", "color": MUTED, "padding": "10px 0"},
            ),
            _failing_rows_cell(row["total_rows"], row["last_total_rows"]),
            html.Div(urg_chip,
                     style={"width": "90px", "padding": "10px 6px",
                            "display": "flex", "justifyContent": "center"}),
            html.Span(
                row["deadline"],
                style={"width": "100px", "fontSize": "11px",
                       "color": MUTED, "padding": "10px 14px"},
            ),
            html.Div(dl_icon,
                     style={"width": "80px", "display": "flex",
                            "justifyContent": "center", "alignItems": "center"}),
        ], style={
            "display": "flex", "alignItems": "center",
            "background": bg,
            "borderBottom": f"1px solid {DIVIDER}",
        }))

    return html.Div(
        [hdr, *rows],
        style={"background": CARD, "borderRadius": "8px",
               "border": f"1px solid {DIVIDER}"},
    )


@app.callback(
    Output("dl-nav", "href", allow_duplicate=True),
    Input({"type": "inst-tbl-dl-btn", "index": ALL}, "n_clicks"),
    State("auth-store", "data"),
    prevent_initial_call=True,
)
def _inst_table_dl(clicks, auth_data):
    """My Issues page: per-row download — just that table's xlsx, not the full zip."""
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "inst-tbl-dl-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    raw = tid["index"]
    if "|" not in raw:
        raise dash.exceptions.PreventUpdate
    le_book, table_name = raw.split("|", 1)

    le_books = set(str(lb) for lb in (auth_data or {}).get("le_books", []))
    if le_book not in le_books:
        raise dash.exceptions.PreventUpdate

    return f"/download/issue-report/{le_book}/{table_name}?t={ctx.triggered[0]['value']}"


@app.callback(
    Output("inst-cr-list", "children"),
    Input("inst-cr-status-filter", "value"),
    Input("notif-poll",            "n_intervals"),
    State("auth-store", "data"),
    prevent_initial_call=False,
)
def _inst_cr_list(status_filter, _poll, auth_data):
    import remediation.change_requests as cr_mod
    le_books = set((auth_data or {}).get("le_books", []))
    role     = (auth_data or {}).get("role", "inst_user")
    all_crs  = cr_mod.get_crs(status=status_filter if status_filter != "all" else None)
    my_crs   = [c for c in all_crs if c["le_book"] in le_books]
    return _build_cr_list(my_crs, role=role)


@app.callback(
    Output("inst-cr-list",     "children", allow_duplicate=True),
    Output("inst-cr-feedback", "children"),
    Input({"type": "cr-action-btn", "index": ALL}, "n_clicks"),
    State("inst-cr-status-filter", "value"),
    State("auth-store", "data"),
    State("cr-version", "data"),
    prevent_initial_call=True,
)
def _inst_cr_action(clicks, status_filter, auth_data, version):
    if not any(c for c in (clicks or []) if c):
        raise dash.exceptions.PreventUpdate
    tid = ctx.triggered_id
    if not isinstance(tid, dict) or tid.get("type") != "cr-action-btn":
        raise dash.exceptions.PreventUpdate
    if not ctx.triggered[0]["value"]:
        raise dash.exceptions.PreventUpdate

    import remediation.change_requests as cr_mod
    raw     = tid["index"]
    cr_id, new_status = raw.split("|", 1)
    actor    = (auth_data or {}).get("email", "inst_user")
    le_books = set((auth_data or {}).get("le_books", []))
    role     = (auth_data or {}).get("role", "inst_user")

    # Verify the CR belongs to this institution before mutating
    owned_cr = cr_mod.get_cr(cr_id)
    if not owned_cr or owned_cr.get("le_book") not in le_books:
        return _build_cr_list([], role=role), html.Span(
            "Unauthorized: this CR does not belong to your institution.",
            style={"color": C_RED},
        )

    ok, msg = cr_mod.update_status(cr_id, new_status, actor=actor)
    all_crs  = cr_mod.get_crs(status=status_filter if status_filter != "all" else None)
    my_crs   = [c for c in all_crs if c["le_book"] in le_books]
    cr_list  = _build_cr_list(my_crs, role=role)

    if ok:
        label    = cr_mod.STATUS_LABELS.get(new_status, new_status)
        feedback = html.Span([
            html.Span("✓ ", style={"color": C_GREEN, "fontWeight": "900"}),
            html.Span(f"{cr_id} moved to \"{label}\".", style={"color": C_GREEN}),
        ])
    else:
        feedback = html.Span(msg, style={"color": C_RED})

    return cr_list, feedback
