#data loading
from __future__ import annotations

import json
import logging
from pathlib import Path

from dashboard.theme import CATEGORIES, DIMS

log = logging.getLogger("dashboard.data")

# file paths
_DIR            = Path(__file__).resolve().parent.parent
HISTORY_FILE    = _DIR / "dq_history.json"
CATEGORIES_FILE = _DIR / "le_book_categories.json"
ACTIVITY_FILE   = _DIR / "institution_activity.json"
PIPELINE_FILE        = _DIR / "pipeline_run.json"
PIPELINE_STATUS_FILE = _DIR / "pipeline_status.json"
WATERMARK_FILE       = _DIR / "watermark.json"
REPORTS_DIR     = _DIR / "reports"
REPORT_FILE     = _DIR / "dq_report.json"


def latest_run_month() -> str:
    """Return the YYYY-MM of the last completed pipeline run.

    Falls back to the current calendar month if the file is missing, so callers
    always get a valid prefix string to filter detected_at against.
    """
    from datetime import date
    try:
        info = json.loads(PIPELINE_FILE.read_text())
        run_date = info.get("run_date") or ""
        if len(run_date) >= 7:
            return run_date[:7]
    except Exception:
        pass
    return date.today().strftime("%Y-%m")


# ── issue tracker (loaded fresh each render — SQLite is fast) ─────────────────
def _issue_summary() -> dict:
    try:
        from issues.queries import get_institution_issue_summary
        return get_institution_issue_summary()
    except Exception:
        return {}

def _institution_issues(le_book: str) -> list:
    try:
        from issues.repositories import get_open_issues
        return get_open_issues(le_book)
    except Exception:
        return []


# ── data loading ───────────────────────────────────────────────────────────────

def _load_history() -> list:
    if not HISTORY_FILE.exists():
        return []
    try:
        data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as exc:
        log.warning("History load failed: %s", exc)
        return []

def _load_pipeline_run() -> dict:
    if not PIPELINE_FILE.exists():
        return {}
    try:
        return json.loads(PIPELINE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_pipeline_status() -> dict:
    if not PIPELINE_STATUS_FILE.exists():
        return {}
    try:
        return json.loads(PIPELINE_STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _load_activity() -> dict:
    if not ACTIVITY_FILE.exists():
        return {}
    try:
        return json.loads(ACTIVITY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _pipeline_le_books() -> set:
    """Return the set of le_books included in the current pipeline report."""
    try:
        data = json.loads(REPORT_FILE.read_text(encoding="utf-8"))
        return {str(lb) for lb in (data.get("le_books") or [])}
    except Exception:
        return set()


def _load_all_categories() -> dict:
    if not CATEGORIES_FILE.exists():
        return {}
    try:
        return json.loads(CATEGORIES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

_HISTORY       = _load_history()
_PIPELINE      = _load_pipeline_run()
_history_mtime = HISTORY_FILE.stat().st_mtime  if HISTORY_FILE.exists()       else 0.0
_pipeline_mtime= PIPELINE_FILE.stat().st_mtime if PIPELINE_FILE.exists()      else 0.0


def _fresh_history() -> list:
    """Return history list, re-reading from disk only when the file has changed."""
    global _HISTORY, _history_mtime
    try:
        mtime = HISTORY_FILE.stat().st_mtime
        if mtime != _history_mtime:
            _HISTORY       = _load_history()
            _history_mtime = mtime
            log.info("History reloaded from disk (pipeline ran)")
    except Exception:
        pass
    return _HISTORY


def _fresh_pipeline() -> dict:
    """Return pipeline run dict, re-reading from disk only when the file has changed."""
    global _PIPELINE, _pipeline_mtime
    try:
        mtime = PIPELINE_FILE.stat().st_mtime
        if mtime != _pipeline_mtime:
            _PIPELINE       = _load_pipeline_run()
            _pipeline_mtime = mtime
    except Exception:
        pass
    return _PIPELINE


# ── data access helpers ────────────────────────────────────────────────────────

def _today_entry()      -> dict:
    # Most recent run that actually scored institutions — skip empty/no-data runs
    # (e.g. a month with no rows in scope) so the overview never renders blank.
    h = _fresh_history()
    for e in reversed(h):
        if e.get("by_institution"):
            return e
    return h[-1] if h else {}
def _yesterday_entry()  -> dict:
    h = [e for e in _fresh_history() if e.get("by_institution")]
    return h[-2] if len(h) >= 2 else {}
def _trend_entries(n=7) -> list: h = _fresh_history(); return h[-n:]        if h           else []


def _cat_scores(entry: dict, cat: str) -> dict:
    """Overall or per-category scores from one history entry.
    cat='SACCO' returns the average of SACCO+OSACCO (data stores these as OSACCO)."""
    if not entry:
        return {d: 0.0 for d in DIMS}
    if cat == "ALL":
        return entry.get("overall", {})
    if cat == "SACCO":
        by_cat = entry.get("by_category", {})
        sacco  = by_cat.get("SACCO",  {})
        osacco = by_cat.get("OSACCO", {})
        combined = {}
        all_dims = list(DIMS) + (["timeliness"] if "timeliness" not in DIMS else [])
        for d in all_dims:
            vals = [float(src.get(d) or 0) for src in (sacco, osacco) if src]
            combined[d] = sum(vals) / len(vals) if vals else 0.0
        return combined
    return entry.get("by_category", {}).get(cat, {})


def _filter_institutions(entry: dict, cat: str) -> dict:
    """Return institutions dict filtered to the given category.
    cat='SACCO' includes both SACCO and OSACCO institution types."""
    inst = entry.get("by_institution", {}) if entry else {}
    if cat == "ALL":
        return inst
    if cat == "SACCO":
        return {lb: d for lb, d in inst.items()
                if d.get("category_type") in ("SACCO", "OSACCO")}
    return {lb: d for lb, d in inst.items() if d.get("category_type") == cat}


def _inst_scores(entry: dict, inst_code: str) -> dict:
    """Return dimension scores for a specific institution from one history entry."""
    if not entry or not inst_code:
        return {d: 0.0 for d in DIMS}
    d = entry.get("by_institution", {}).get(inst_code, {})
    scores = {dim: float(d.get(dim) or 0) for dim in DIMS}
    if "timeliness" not in DIMS and "timeliness" in d:
        scores["timeliness"] = float(d["timeliness"] or 0)
    return scores


def _category_counts(entry: dict) -> dict:
    counts = {c: 0 for c in CATEGORIES}
    counts["ALL"] = 0
    for data in entry.get("by_institution", {}).values():
        counts["ALL"] += 1
        ct = data.get("category_type", "")
        if ct in counts:
            counts[ct] += 1
    return counts


def _table_fails(entry: dict) -> dict[str, int]:
    """Return {table: failing_rule_count} from one history entry."""
    if not entry:
        return {}
    return entry.get("table_fail_counts", {})


def _table_nulls(entry: dict) -> dict[str, dict[str, int]]:
    """Return {table: {col: null_count}} for tables with null columns."""
    if not entry:
        return {}
    return entry.get("table_null_cols", {})


# ── bootstrap values (computed once at process startup) ────────────────────────
# _counts feeds the landing page's per-category institution counts. Frozen at
# import time rather than recomputed per-request — acceptable because
# run_monthly.sh restarts gunicorn right after each detection run, so this
# naturally refreshes monthly along with everything else.
#
# Dead (computed, never read anywhere): _run_ts/_run_date/_run_label used to
# exist alongside _counts here but have zero consumers — confirmed via grep.
# _update_pipeline_banner (dashboard/callbacks/pipeline_status.py) computes its
# own run_date fresh from _fresh_pipeline() on every poll instead of using these.
_today_e = _today_entry()
_counts  = _category_counts(_today_e)
