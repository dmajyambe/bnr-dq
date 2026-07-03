# Style constants and label/category dicts shared across pages and callbacks
# — moved from dq_dashboard_dash.py.
#
# NOTE: dashboard/portal/pages.py has its OWN, independently-defined copy of
# this theme ("mirrored from main dashboard" per that file's comment) — and
# the two have drifted: this dashboard's C_GREEN/C_AMBER/C_RED are
# "#B8860B"/"#A0784A"/"#7C3D1E" (warm browns), while the portal's are
# "#16A34A"/"#D97706"/"#DC2626" (standard green/amber/red). Not unified here
# since that's a visual-design decision, not a mechanical move — flagging it.
from __future__ import annotations

BNR_GOLD = "#f7c35f"   # --thm-primary  (website gold accent)
BG       = "#f2ede9"   # --thm-gray     (website light warm background)
CARD     = "#FFFFFF"
TEXT     = "#1c1c27"   # --thm-black    (website near-black)
MUTED    = "#68686f"   # --thm-color    (website body text / secondary)
DIVIDER  = "#e7e1dc"   # --thm-border-color
C_GREEN  = "#B8860B"   # dark goldenrod  — good score
C_AMBER  = "#A0784A"   # mid warm tan    — medium score
C_RED    = "#7C3D1E"   # burnt rust      — poor score
BRAND    = "#753918"   # --thm-base     (website primary brand brown)
FONT     = "'Inter','Franklin Gothic Medium',Arial,sans-serif"

# one color per dimension
DIM_COLORS = {
    "completeness": "#753918",   # brand primary
    "accuracy":     "#B8860B",   # dark goldenrod
    "timeliness":   "#7C3D1E",   # burnt rust
    "validity":     "#C9956C",   # peach terra cotta
}

DIMS = ["completeness", "accuracy", "timeliness", "validity"]
DIM_LABELS = {
    "completeness": "Completeness",
    "accuracy":     "Accuracy",
    "timeliness":   "Timeliness",
    "validity":     "Validity",
}

# Internal category codes (kept for data access helpers)
CATEGORIES = ["ALL", "B", "MF", "SACCO", "OSACCO"]
CAT_LABELS = {
    "ALL":    "All Institutions",
    "B":      "Banks",
    "MF":     "Microfinance",
    "SACCO":  "SACCOs",
    "OSACCO": "OSACCOs",
}

# Landing page category definitions
# "SACCO" combines both SACCO and OSACCO institution types
LANDING_CATS = [
    {
        "code":     "B",
        "label":    "Banks",
        "subtitle": "Commercial & savings banks",
        "color":    "#753918",
        "types":    ["B"],
    },
    {
        "code":     "MF",
        "label":    "Microfinance",
        "subtitle": "Microfinance institutions",
        "color":    "#B8860B",
        "types":    ["MF"],
    },
    {
        "code":     "SACCO",
        "label":    "SACCO",
        "subtitle": "Savings & credit cooperatives (incl. OSACCOs)",
        "color":    "#A0784A",
        "types":    ["SACCO", "OSACCO"],
    },
]

_URGENCY_COLORS = {
    "new":       "#A0784A",   # Mid Warm Tan — calm, not alarming
    "attention": "#B8860B",   # Dark Goldenrod — caution
    "urgent":    "#7C3D1E",   # Burnt Rust — stronger warning
    "critical":  "#DC2626",   # Red — universal critical
    "overdue":   "#991B1B",   # Deep Red — SLA breached
}
_URGENCY_LABELS = {
    "new":       "New (1–3d)",
    "attention": "Needs Attention (4–15d)",
    "urgent":    "Urgent (16–20d)",
    "critical":  "About to Breach (21–30d)",
    "overdue":   "⚠ Overdue — SLA Breached",
}

_DIM_PILL_COLOR = {
    "completeness": "#753918",
    "accuracy":     "#B8860B",
    "timeliness":   "#7C3D1E",
    "validity":     "#C9956C",
    "uniqueness":   "#A0784A",
    "relationship": "#92400E",
}

TABLE_NAMES_PRETTY: dict[str, str] = {
    "accounts":               "Accounts",
    "contract_loans":         "Contract Loans",
    "contract_schedules":     "Contract Schedules",
    "contracts_disburse":     "Contracts Disburse",
    "contracts_expanded":     "Contracts",
    "customers_expanded":     "Customers",
    "loan_applications_2":    "Loan Applications",
    "prev_loan_applications": "Prev Loan Apps",
}

ALL_TABLES = list(TABLE_NAMES_PRETTY.keys())

# Dead (orphaned by removing the custom-rule-submission feature earlier):
# _KNOWN_TABLES and _CHECK_TYPES were only used by the deleted _rule_form;
# _STATUS_STYLE was only used by the deleted _rules_table_row. Zero callers
# now (confirmed via grep) — not carried into this module.
