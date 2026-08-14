#issue status life cycle and urgency bands
from __future__ import annotations
import hashlib
from datetime import date

SLA_DAYS = 30   # days before SLA warning escalates to overdue

# Urgency bands
_URGENCY_STEPS = [(3, "new"), (15, "attention"), (20, "urgent"), (30, "critical")]
URGENCY_COLORS = {
    "new":       "#2563EB",
    "attention": "#D97706",
    "urgent":    "#EA580C",
    "critical":  "#DC2626",
    "overdue":   "#7C3D1E",
}

# Notification cadence per urgency band (minimum days between notifications
# for the same issue_id)
NOTIFY_INTERVAL = {"new": None, "attention": 7, "urgent": 3, "critical": 1, "overdue": 1}

# Statuses that count as an unresolved ("open") issue in every tally and UI view.
OPEN_STATUSES = ("open", "pending_resolution")
OPEN_SQL      = "('" + "','".join(OPEN_STATUSES) + "')"


def urgency_band(detected_at: str, sla_deadline: str | None = None) -> str:
    try:
        days = (date.today() - date.fromisoformat(detected_at)).days
    except Exception:
        return "critical"
    # Past SLA → overdue warning (no penalization, just escalated label)
    if sla_deadline:
        try:
            if date.today() > date.fromisoformat(sla_deadline):
                return "overdue"
        except Exception:
            pass
    for max_days, band in _URGENCY_STEPS:
        if days <= max_days:
            return band
    return "critical"


def issue_id(le_book: str, table: str, rule_id: str) -> str:
    raw = f"{le_book}|{table}|{rule_id}"
    return hashlib.sha1(raw.encode()).hexdigest()
