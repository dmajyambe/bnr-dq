# Remediation tab (Data Correction Request workflow) — moved from dq_dashboard_dash.py.
from __future__ import annotations

import json

from dash import dcc, html

from dashboard.components import _empty_state, _fmt_int
from dashboard.theme import BG, BRAND, CARD, DIVIDER, FONT, MUTED, TABLE_NAMES_PRETTY, TEXT, C_GREEN, C_RED


def _build_cr_list(crs: list[dict], role: str = "bnr_admin") -> html.Div:
    """Render the change-request table.  Called at page load and after each CR action."""
    import remediation.change_requests as cr_mod
    from auth import users as _auth

    is_bnr_admin = _auth.is_admin(role)
    is_inst      = role == "inst_user"

    if not crs:
        return _empty_state(
            "No Data Correction Requests",
            "Nothing matches this filter. Data Correction Requests assigned to you will appear here.",
            icon="🗂")

    H = {"fontSize": "11px", "fontWeight": "900", "color": MUTED,
         "textTransform": "uppercase", "letterSpacing": "0.05em",
         "padding": "8px 10px", "flexShrink": "0"}

    hdr = html.Div([
        html.Span("CR ID",        style={**H, "width": "148px"}),
        html.Span("Institution",  style={**H, "flex": "1"}),
        html.Span("Title",        style={**H, "flex": "2"}),
        html.Span("Status",       style={**H, "width": "108px"}),
        html.Span("Issues",       style={**H, "width": "56px",  "textAlign": "center"}),
        html.Span("Failing",      style={**H, "width": "74px",  "textAlign": "right"}),
        html.Span("Target",       style={**H, "width": "86px"}),
        html.Span("Assigned To",  style={**H, "width": "154px"}),
        html.Span("Actions",      style={**H, "width": "228px"}),
    ], style={
        "display": "flex", "alignItems": "center",
        "background": BG, "borderRadius": "8px 8px 0 0",
        "borderBottom": f"2px solid {DIVIDER}",
    })

    def _action_btn(label: str, cr_id: str, action: str, bg_color: str) -> html.Div:
        return html.Div(
            label,
            id={"type": "cr-action-btn", "index": f"{cr_id}|{action}"},
            n_clicks=0,
            title=f"{label} — {cr_id}",
            style={
                "display":      "inline-block",
                "background":   bg_color,
                "color":        CARD,
                "padding":      "4px 9px",
                "borderRadius": "4px",
                "fontSize":     "11px",
                "fontWeight":   "700",
                "cursor":       "pointer",
                "userSelect":   "none",
                "marginRight":  "4px",
                "marginBottom": "2px",
                "whiteSpace":   "nowrap",
            },
        )

    rows = []
    for i, cr in enumerate(crs):
        bg     = "#C9956C" if i % 2 == 0 else BG
        status = cr["status"]
        clr    = cr_mod.STATUS_COLORS.get(status, MUTED)
        label  = cr_mod.STATUS_LABELS.get(status, status.title())
        r, g, b_val = int(clr[1:3], 16), int(clr[3:5], 16), int(clr[5:7], 16)

        try:
            n_issues = len(json.loads(cr.get("issue_ids") or "[]"))
        except Exception:
            n_issues = 0

        status_chip = html.Span(label, style={
            "background":   f"rgba({r},{g},{b_val},0.12)",
            "color":        clr,
            "border":       f"1px solid {clr}",
            "borderRadius": "4px",
            "padding":      "2px 7px",
            "fontSize":     "10px",
            "fontWeight":   "700",
            "whiteSpace":   "nowrap",
        })

        # Contextual action buttons — gated by role
        # Flow: BNR creates → institution starts + submits → BNR approves/rejects (table by table)
        try:
            cr_tables = json.loads(cr.get("tables") or "[]")
        except Exception:
            cr_tables = []
        try:
            table_approvals = json.loads(cr.get("table_approvals") or "{}")
        except Exception:
            table_approvals = {}

        action_btns: list = []
        if status == "open":
            if is_inst:
                action_btns.append(_action_btn("Start Work", cr["cr_id"], "in_progress", "#D97706"))
            if is_bnr_admin:
                action_btns.append(_action_btn("Cancel", cr["cr_id"], "closed", "#6B7280"))
        elif status == "in_progress":
            if is_inst:
                action_btns.append(_action_btn("Submit for Review", cr["cr_id"], "submitted", "#7C3D1E"))
            if is_bnr_admin:
                action_btns.append(_action_btn("Cancel", cr["cr_id"], "closed", "#6B7280"))
        elif status == "submitted":
            if is_bnr_admin:
                action_btns.append(_action_btn("Reject", cr["cr_id"], "rejected", "#DC2626"))
                if cr_tables:
                    from issues.repositories import get_issues as _gi
                    try:
                        lb_open: dict[str, int] = {}
                        for _i in _gi(status="open"):
                            if _i["le_book"] == cr["le_book"]:
                                lb_open[_i["table_name"]] = lb_open.get(_i["table_name"], 0) + 1
                    except Exception:
                        lb_open = {}
                    tbl_rows = []
                    for tbl in cr_tables:
                        tbl_label  = TABLE_NAMES_PRETTY.get(tbl, tbl.replace("_", " ").title())
                        tbl_status = table_approvals.get(tbl, {}).get("status", "pending")
                        n_open     = lb_open.get(tbl, 0)
                        chip_color = "#16A34A" if tbl_status == "approved" else "#D97706"
                        chip_label = "Approved" if tbl_status == "approved" else "Pending"
                        approve_btn = html.Span() if tbl_status == "approved" else html.Div(
                            "✓ Approve Table",
                            id={"type": "cr-tbl-approve-btn", "index": f"{cr['cr_id']}|{tbl}"},
                            n_clicks=0,
                            style={"display": "inline-block", "background": "#16A34A",
                                   "color": CARD, "fontSize": "10px", "fontWeight": "700",
                                   "padding": "3px 10px", "borderRadius": "4px",
                                   "cursor": "pointer", "userSelect": "none", "marginLeft": "6px"},
                        )
                        tbl_rows.append(html.Div([
                            html.Span(tbl_label, style={"fontSize": "11px", "color": TEXT,
                                                        "fontWeight": "700", "width": "130px",
                                                        "flexShrink": "0"}),
                            html.Span(f"{n_open} open", style={"fontSize": "10px",
                                                               "color": C_RED if n_open else C_GREEN,
                                                               "width": "60px", "flexShrink": "0"}),
                            html.Span(chip_label, style={"fontSize": "10px", "fontWeight": "700",
                                                          "color": chip_color,
                                                          "border": f"1px solid {chip_color}",
                                                          "borderRadius": "4px", "padding": "1px 6px"}),
                            approve_btn,
                        ], style={"display": "flex", "alignItems": "center",
                                  "gap": "6px", "marginBottom": "4px"}))
                    action_btns.append(html.Div(tbl_rows, style={
                        "background": BG, "border": f"1px solid {DIVIDER}",
                        "borderRadius": "6px", "padding": "8px 12px",
                        "marginTop": "6px", "width": "100%",
                    }))
                else:
                    action_btns.append(_action_btn("Approve", cr["cr_id"], "approved", "#16A34A"))
        elif status == "rejected":
            if is_inst:
                action_btns.append(_action_btn("Reopen", cr["cr_id"], "in_progress", "#D97706"))
            if is_bnr_admin:
                action_btns.append(_action_btn("Close", cr["cr_id"], "closed", "#6B7280"))
        elif status == "approved":
            if is_bnr_admin:
                action_btns = [_action_btn("Close", cr["cr_id"], "closed", "#6B7280")]

        # Reviewer note shown under approved/rejected
        reviewer_note = html.Span()
        if status in ("approved", "rejected") and cr.get("reviewed_by"):
            reviewer_note = html.Div(
                f"by {cr['reviewed_by']}" +
                (f" — \"{cr['review_notes']}\"" if cr.get("review_notes") else ""),
                style={"fontSize": "10px", "color": MUTED, "marginTop": "2px",
                       "wordBreak": "break-word"},
            )

        _C = {"paddingTop": "10px", "paddingBottom": "10px", "flexShrink": "0"}
        rows.append(html.Div([
            html.Span(
                cr["cr_id"],
                style={**_C, "width": "148px", "fontSize": "11px", "fontWeight": "700",
                       "color": BRAND, "paddingLeft": "10px", "paddingRight": "10px"},
            ),
            html.Span(
                (cr.get("institution_name") or cr["le_book"]).title(),
                style={**_C, "flex": "1", "fontSize": "12px", "color": TEXT,
                       "paddingLeft": "10px", "paddingRight": "10px",
                       "overflow": "hidden", "textOverflow": "ellipsis",
                       "whiteSpace": "nowrap", "flexShrink": "1", "minWidth": "0"},
            ),
            html.Div([
                html.Span(cr["title"],
                          style={"fontSize": "12px", "color": TEXT,
                                 "display": "block", "lineHeight": "1.3"}),
                html.Span(cr.get("description") or "",
                          style={"fontSize": "10px", "color": MUTED,
                                 "display": "block", "lineHeight": "1.3",
                                 "overflow": "hidden", "textOverflow": "ellipsis",
                                 "whiteSpace": "nowrap", "maxWidth": "260px"})
                if cr.get("description") else html.Span(),
            ], style={**_C, "flex": "2", "paddingLeft": "10px", "paddingRight": "10px",
                      "overflow": "hidden", "minWidth": "0", "flexShrink": "1"}),
            html.Div(status_chip,
                     style={**_C, "width": "108px",
                            "paddingLeft": "10px", "paddingRight": "10px"}),
            html.Span(
                str(n_issues),
                style={**_C, "width": "56px", "textAlign": "center", "fontSize": "12px",
                       "color": TEXT, "paddingLeft": "10px", "paddingRight": "10px"},
            ),
            html.Span(
                f"{cr.get('failing_rows', 0):,}",
                style={**_C, "width": "74px", "textAlign": "right", "fontSize": "12px",
                       "fontWeight": "700", "color": TEXT,
                       "paddingLeft": "10px", "paddingRight": "10px"},
            ),
            html.Span(
                cr.get("target_date") or "—",
                style={**_C, "width": "86px", "fontSize": "11px", "color": MUTED,
                       "paddingLeft": "10px", "paddingRight": "10px"},
            ),
            html.Span(
                cr.get("assigned_to") or "—",
                style={**_C, "width": "154px", "fontSize": "11px", "color": MUTED,
                       "paddingLeft": "10px", "paddingRight": "10px",
                       "overflow": "hidden", "textOverflow": "ellipsis",
                       "whiteSpace": "nowrap"},
            ),
            html.Div(
                action_btns + ([reviewer_note] if reviewer_note.children else []),  # type: ignore[attr-defined]
                style={**_C, "width": "228px", "paddingLeft": "10px", "paddingRight": "10px",
                       "display": "flex", "alignItems": "flex-start", "flexWrap": "wrap"},
            ),
        ], style={
            "display":      "flex",
            "alignItems":   "flex-start",
            "background":   bg,
            "borderBottom": f"1px solid {DIVIDER}",
        }))

    return html.Div(
        html.Div(
            [hdr] + rows,
            style={"minWidth": "1060px"},
        ),
        style={"border": f"1px solid {DIVIDER}", "borderRadius": "8px",
               "overflow": "hidden", "overflowX": "auto"},
    )


def _remediation_page(role: str = "bnr_admin", cat: str = "") -> html.Div:
    """
    Full Data Quality Remediation page.

    Implements the SAP MDG DQR workflow:
      1. Specialist selects open issues (filtered by institution / urgency).
      2. Creates a Data Correction Request (CR) linking the selected issues.
      3. Assigned data officer marks CR In Progress then Submitted.
      4. BNR specialist Approves or Rejects with review notes.
      5. Approved CRs are Closed once the next pipeline run confirms resolution.
    """
    import json as _json
    import remediation.change_requests as cr_mod
    from auth.users import is_admin
    from issues.repositories import ensure_tables as _ensure_issues, get_open_issues
    from dashboard.data import CATEGORIES_FILE
    from dashboard.theme import CAT_LABELS

    _ensure_issues()
    cr_mod.ensure_table()

    # Build allowed le_book set for the selected category
    try:
        _cats = _json.loads(CATEGORIES_FILE.read_text())
    except Exception:
        _cats = {}

    _sacco = {"SACCO", "OSACCO"}
    if cat:
        allowed_lebooks = {
            str(lb) for lb, info in _cats.items()
            if (info.get("category_type") or "").upper() in (
                _sacco if cat == "SACCO" else {cat.upper()}
            )
        }
    else:
        allowed_lebooks = None  # no filter — show all

    # Institution options for the create-CR form (scoped to selected category)
    open_issues = get_open_issues()
    inst_seen: dict[str, str] = {}
    for iss in open_issues:
        lb = iss["le_book"]
        if allowed_lebooks is not None and lb not in allowed_lebooks:
            continue
        if lb not in inst_seen:
            inst_seen[lb] = (iss.get("institution_name") or lb).title()

    inst_options = [{"label": "Select institution…", "value": ""}] + [
        {"label": name, "value": lb}
        for lb, name in sorted(inst_seen.items(), key=lambda kv: kv[1])
    ]

    # Summary banner
    stats  = cr_mod.get_stats()
    chips  = []
    for key in ("open", "in_progress", "submitted"):   # minimalist: active pipeline only
        lbl = cr_mod.STATUS_LABELS.get(key, key.title())
        n   = stats.get(key, 0)
        clr = cr_mod.STATUS_COLORS[key]
        chips.append(html.Div([
            html.Span(_fmt_int(n, str(n)), style={
                "fontSize": "24px", "fontWeight": "900",
                "color": clr, "lineHeight": "1",
            }),
            html.Span(lbl, style={
                "fontSize": "10px", "color": MUTED,
                "marginTop": "3px", "lineHeight": "1.2",
                "textAlign": "center",
            }),
        ], style={
            "display":        "flex",
            "flexDirection":  "column",
            "alignItems":     "center",
            "background":     CARD,
            "borderRadius":   "8px",
            "padding":        "12px 18px",
            "border":         f"2px solid {clr}",
            "minWidth":       "90px",
        }))

    summary_bar = html.Div(chips, id="cr-summary-bar", style={
        "display": "flex", "gap": "10px", "flexWrap": "wrap",
        "marginBottom": "24px",
    })

    INP = {
        "width": "100%", "padding": "8px 10px",
        "border": f"1px solid {DIVIDER}", "borderRadius": "6px",
        "fontSize": "12px", "fontFamily": FONT,
        "background": BG, "color": TEXT,
        "boxSizing": "border-box", "outline": "none",
    }

    # ── Create-CR form (hidden by default, toggled) ────────────────────────────
    form_panel = html.Div([
        html.Div("NEW DATA CORRECTION REQUEST", style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "letterSpacing": "0.06em", "textTransform": "uppercase",
            "marginBottom": "16px",
        }),

        # Row 1: institution selector
        html.Div([
            html.Label("Institution *", style={
                "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                "display": "block", "marginBottom": "5px",
            }),
            dcc.Dropdown(
                id="cr-inst-filter",
                options=inst_options,
                value="",
                clearable=False,
                placeholder="Select institution…",
                style={"fontSize": "12px", "fontFamily": FONT},
            ),
            html.Div(
                "Only institutions with open issues appear here.",
                style={"fontSize": "10px", "color": MUTED, "marginTop": "4px"},
            ),
        ], style={"marginBottom": "16px"}),

        # Row 2: issue checklist (options filled by callback)
        html.Div([
            html.Div([
                html.Label("Select Issues to Address *", style={
                    "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                }),
                html.Div([
                    html.Span("Select all", id="cr-select-all", n_clicks=0, style={
                        "cursor": "pointer", "fontSize": "10px", "fontWeight": "700",
                        "color": BRAND, "userSelect": "none",
                    }),
                    html.Span("·", style={"color": MUTED, "fontSize": "10px"}),
                    html.Span("Clear", id="cr-clear-all", n_clicks=0, style={
                        "cursor": "pointer", "fontSize": "10px", "fontWeight": "700",
                        "color": MUTED, "userSelect": "none",
                    }),
                ], style={"display": "flex", "gap": "8px", "alignItems": "center"}),
            ], style={
                "display": "flex", "justifyContent": "space-between",
                "alignItems": "center", "marginBottom": "6px",
            }),
            html.Div(
                dcc.Checklist(
                    id="cr-issue-checklist",
                    options=[],
                    value=[],
                    inputStyle={"marginRight": "6px"},
                    labelStyle={
                        "display": "block",
                        "fontSize": "12px",
                        "lineHeight": "1.8",
                        "color": TEXT,
                        "cursor": "pointer",
                    },
                ),
                id="cr-issue-list",
                style={
                    "background":   BG,
                    "padding":      "10px 14px",
                    "borderRadius": "6px",
                    "border":       f"1px solid {DIVIDER}",
                    "maxHeight":    "220px",
                    "overflowY":    "auto",
                    "minHeight":    "44px",
                },
            ),
            html.Div(
                "Select an institution above to see its tables with open issues.",
                id="cr-issue-hint",
                style={"fontSize": "10px", "color": MUTED, "marginTop": "4px"},
            ),
        ], style={"marginBottom": "16px"}),

        # Row 3: title
        html.Div([
            html.Label("Data Correction Request Title *", style={
                "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                "display": "block", "marginBottom": "5px",
            }),
            dcc.Input(
                id="cr-title",
                type="text",
                placeholder="Brief description of the correction required…",
                debounce=False,
                style={**INP},
            ),
        ], style={"marginBottom": "14px"}),

        # Row 4: description
        html.Div([
            html.Label("Correction Plan / Description", style={
                "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                "display": "block", "marginBottom": "5px",
            }),
            dcc.Textarea(
                id="cr-description",
                placeholder=(
                    "Describe what data needs correcting, which records are affected, "
                    "and the steps the institution must follow to resolve the issue…"
                ),
                style={**INP, "height": "80px", "resize": "vertical"},
            ),
        ], style={"marginBottom": "14px"}),

        # Row 5: assigned to + target date (side by side)
        html.Div([
            html.Div([
                html.Label("Assigned To", style={
                    "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                    "display": "block", "marginBottom": "5px",
                }),
                dcc.Dropdown(
                    id="cr-assigned-to",
                    options=[],
                    value=None,
                    placeholder="Select an institution first…",
                    clearable=True,
                    style={"fontSize": "12px", "fontFamily": FONT},
                ),
            ], style={"flex": "1"}),
            html.Div([
                html.Label("Target Resolution Date", style={
                    "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                    "display": "block", "marginBottom": "5px",
                }),
                dcc.Input(
                    id="cr-target-date",
                    type="date",
                    debounce=False,
                    style={**INP},
                ),
            ], style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "18px"}),

        # Submit row
        html.Div([
            html.Div("Create Data Correction Request", id="cr-create-btn", n_clicks=0,
                     style={
                         "display":      "inline-block",
                         "background":   BRAND,
                         "color":        CARD,
                         "padding":      "9px 20px",
                         "borderRadius": "6px",
                         "fontSize":     "12px",
                         "fontWeight":   "900",
                         "cursor":       "pointer",
                         "userSelect":   "none",
                         "letterSpacing": "0.03em",
                     }),
            html.Div(id="cr-feedback",
                     style={"marginTop": "8px", "fontSize": "12px",
                            "lineHeight": "1.4"}),
        ]),
    ], id="cr-form-panel", style={
        "background":   CARD,
        "border":       f"1px solid {DIVIDER}",
        "borderRadius": "8px",
        "padding":      "20px 24px",
        "marginBottom": "20px",
        "display":      "none",
    })

    # ── Review-notes box (shared; reviewer fills in before clicking Approve/Reject)
    review_box = html.Div([
        html.Div("REVIEWER NOTES", style={
            "fontSize": "11px", "fontWeight": "900", "color": MUTED,
            "letterSpacing": "0.06em", "textTransform": "uppercase",
            "marginBottom": "6px",
        }),
        html.P(
            "Fill in your notes here before clicking Approve or Reject on a submitted CR below.",
            style={"fontSize": "11px", "color": MUTED, "margin": "0 0 8px"},
        ),
        dcc.Textarea(
            id="cr-review-notes",
            placeholder="e.g. Verified in the source system — 47 customer records corrected.",
            style={
                **INP,
                "height":  "56px",
                "resize":  "vertical",
            },
        ),
        html.Div(id="cr-action-feedback",
                 style={"marginTop": "6px", "fontSize": "12px", "lineHeight": "1.4"}),
    ], style={
        "background":   CARD,
        "border":       f"1px solid {DIVIDER}",
        "borderRadius": "8px",
        "padding":      "16px 20px",
        "marginBottom": "20px",
    })

    # ── CR list section ────────────────────────────────────────────────────────
    status_options = [{"label": "All Statuses", "value": "all"}] + [
        {"label": lbl, "value": key}
        for key, lbl in cr_mod.STATUS_LABELS.items()
    ]

    cr_list_section = html.Div([
        html.Div([
            html.Div("DATA CORRECTION REQUESTS", style={
                "fontSize": "11px", "fontWeight": "900", "color": MUTED,
                "letterSpacing": "0.06em", "textTransform": "uppercase",
            }),
            dcc.Dropdown(
                id="cr-status-filter",
                options=status_options,
                value="all",
                clearable=False,
                style={"fontSize": "12px", "fontFamily": FONT, "minWidth": "170px"},
            ),
        ], style={
            "display": "flex", "alignItems": "center",
            "justifyContent": "space-between",
            "marginBottom": "14px",
        }),
        html.Div(id="cr-list-container"),
    ], style={
        "background":   CARD,
        "border":       f"1px solid {DIVIDER}",
        "borderRadius": "8px",
        "padding":      "20px",
    })

    cat_label = CAT_LABELS.get(cat, "") if cat else ""

    return html.Div([

        # Hidden store — passes selected category to CR-list/stats callbacks
        dcc.Store(id="cr-cat-filter", data=cat or ""),

        # Page header
        html.Div([
            html.Div([
                html.H2("Data Quality Remediation", style={
                    "fontSize": "18px", "fontWeight": "900", "color": TEXT,
                    "margin": "0", "lineHeight": "1.2",
                }),
                *([] if not cat_label else [html.Span(
                    f"· {cat_label}",
                    style={"fontSize": "13px", "fontWeight": "700",
                           "color": BRAND, "marginLeft": "10px"},
                )]),
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={"marginBottom": "24px"}),

        # Status summary bar
        summary_bar,

        # + New Data Correction Request toggle — BNR admin only
        *([] if not is_admin(role) else [html.Div(
            html.Div("+ New Data Correction Request", id="cr-form-toggle-btn", n_clicks=0,
                     style={
                         "display":      "inline-block",
                         "background":   BRAND,
                         "color":        CARD,
                         "padding":      "9px 18px",
                         "borderRadius": "6px",
                         "fontSize":     "12px",
                         "fontWeight":   "900",
                         "cursor":       "pointer",
                         "userSelect":   "none",
                     }),
            style={"marginBottom": "16px"},
        )]),

        form_panel,
        review_box,
        cr_list_section,

    ], style={"padding": "28px 32px", "maxWidth": "1400px", "margin": "0 auto"})
