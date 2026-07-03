# Pipeline-run status banner callback — moved from dq_dashboard_dash.py.
from __future__ import annotations

from dash import Input, Output, html

from dashboard.app import app
from dashboard.data import _DIR, _fresh_pipeline, _load_pipeline_status


@app.callback(
    Output("pipeline-status-banner", "children"),
    Output("status-poll", "interval"),
    Input("status-poll", "n_intervals"),
)
def _update_pipeline_banner(_):
    """
    Refresh pipeline status. Poll every 5 s while running, 30 s otherwise.
    When running, shows live stage detection + log tail instead of a single line.
    """
    from datetime import datetime as _dt
    import re as _re

    run    = _fresh_pipeline()
    status = _load_pipeline_status()
    data_date = run.get("run_date", "—")
    s = status.get("status", "")

    # ── dynamic poll interval ──────────────────────────────────────────────────
    poll_ms = 5_000 if s == "running" else 30_000

    # ── not running: compact banner line ──────────────────────────────────────
    if s != "running":
        if s == "success":
            color, label = "#4ADE80", "Success"
            ts_raw = status.get("finished_at", "")
        elif s == "failed":
            color, label = "#F87171", "Failed"
            ts_raw = status.get("finished_at", "")
        else:
            color = label = ts_raw = ""

        ts_lbl = f"{ts_raw[11:16]}" if len(ts_raw or "") >= 16 else ""
        children = [
            html.Span(f"Data as of: {data_date}",
                      style={"color": "rgba(255,255,255,0.55)"}),
        ]
        if label:
            children += [
                html.Span("  ·  ", style={"color": "rgba(255,255,255,0.30)"}),
                html.Span(f"● {label}", style={"color": color, "fontWeight": "700"}),
                html.Span(f"  ({ts_lbl})" if ts_lbl else "",
                          style={"color": "rgba(255,255,255,0.40)"}),
            ]
        return html.Span(children, style={"fontSize": "11px"}), poll_ms

    # ── running: rich live panel ───────────────────────────────────────────────
    started_raw = status.get("started_at", "")
    try:
        started  = _dt.strptime(started_raw, "%Y-%m-%d %H:%M:%S")
        elapsed  = _dt.now() - started
        mins, sec = divmod(int(elapsed.total_seconds()), 60)
        elapsed_str = f"{mins}m {sec:02d}s"
    except Exception:
        elapsed_str = "—"

    # Read today's log file for stage + table detection
    from datetime import date as _date
    log_path = _DIR / "logs" / f"pipeline_{_date.today().isoformat()}.log"
    log_lines: list[str] = []
    stage_label = "Initialising…"
    current_table = ""
    try:
        with open(log_path, "r", errors="replace") as f:
            all_lines = f.readlines()
        log_lines = [l.rstrip() for l in all_lines if l.strip()][-6:]

        # Detect current stage from log content
        full_text = "".join(all_lines)
        if "Stage 3" in full_text or "Resolution scan" in full_text:
            stage_label = "Stage 3 — Resolution scanner"
        elif "Stage 2" in full_text or "RI checks" in full_text or "--reports" in full_text:
            stage_label = "Stage 2 — RI checks + XLSX reports"
        elif "Stage 1" in full_text or "--load" in full_text:
            stage_label = "Stage 1 — SQL engines + scoring"

        # Detect current table from most recent ━━ separator
        tables_seen = _re.findall(r"━━\s+([\w]+)", full_text)
        if tables_seen:
            current_table = tables_seen[-1].replace("_", " ")
    except Exception:
        pass

    # Compact log tail rows
    _LOG_STYLE = {
        "fontFamily": "monospace", "fontSize": "9px",
        "color": "rgba(255,255,255,0.55)", "lineHeight": "1.4",
        "display": "block", "whiteSpace": "nowrap",
        "overflow": "hidden", "textOverflow": "ellipsis",
        "maxWidth": "480px",
    }
    log_tail = [html.Span(l, style=_LOG_STYLE) for l in log_lines[-4:]]

    panel = html.Div([
        # Top row: animated dot + stage + elapsed
        html.Div([
            html.Span("⬤ ", style={
                "color": "#FCD34D", "fontSize": "10px",
                "animation": "pulse 1.2s ease-in-out infinite",
            }),
            html.Span(stage_label, style={
                "color": "#FCD34D", "fontWeight": "700", "fontSize": "11px",
            }),
            html.Span(f"  ·  {elapsed_str}", style={
                "color": "rgba(255,255,255,0.50)", "fontSize": "10px",
            }),
            *([ html.Span(f"  ·  {current_table}", style={
                    "color": "rgba(255,255,255,0.40)", "fontSize": "10px",
                    "fontStyle": "italic",
                })] if current_table else []),
        ], style={"marginBottom": "3px"}),
        # Log tail
        html.Div(log_tail),
    ], style={"textAlign": "right"})

    return panel, poll_ms
